"""Isolated Flask wiring for authenticated channel events and their receipts."""
import base64
import threading

from .. import chatwoot, inbound_media, media, zernio
from .contracts import DecisionUnavailable
from .delivery import deliver_confirmation, deliver_replies, recover_confirmations
from .entrypoint import Entrypoint
from .store import Scope


def media_input(attachments):
    files = [item for item in attachments if item.get('type') != 'sticker']
    if not files:
        return ('sticker' if attachments else 'text'), None
    if any(item.get('type') != 'image' for item in files):
        return 'file', None
    return 'image', [item['payload']['url'] for item in files]


class ImageObserver:
    def __init__(self, fetch, match):
        self.fetch = fetch
        self.match = match

    def __call__(self, scope, kind, references, products):
        if kind != 'image':
            return dict(status='unsupported')
        if not references or len(references) > 4:
            return dict(status='ambiguous')
        base = chatwoot.configuration()['chatwoot_api_url'] if scope.channel == 'chatwoot' else ''
        matches = set()
        for url in references:
            if not inbound_media.trusted_source(url, base):
                return dict(status='unknown')
            attachment = self.fetch(url, 'image', base)
            if not attachment or attachment[1] != 'image' or len(attachment[0]) > inbound_media.MAX_BYTES:
                return dict(status='unknown')
            data, _, _, mime = attachment
            if media.known_sticker(data):
                if len(references) == 1:
                    return dict(status='sticker')
                return dict(status='ambiguous')
            encoded = base64.b64encode(data).decode('ascii')
            result = self.match(f'data:{mime};base64,{encoded}', products)
            if not isinstance(result, dict) or result.get('service_error') or result.get('pending'):
                raise DecisionUnavailable('vision_service_unavailable')
            if not result.get('product_found') or not result.get('product_id'):
                return dict(status='unknown')
            matches.add(result['product_id'])
        return dict(status='matched' if len(matches) == 1 else 'ambiguous', product_ids=sorted(matches))


class ChannelPreview:
    def __init__(self, entry, send, enabled):
        self.entry = entry
        self.send = send
        self.enabled = enabled
        # The isolated instance has one worker and serializes state and delivery.
        self.lease = threading.RLock()

    def receive(self, channel, store, account, sender, message_id, text, attachments, ad):
        scope = Scope(store, channel, account, sender)
        kind, reference = media_input(attachments)
        with self.lease:
            entry = self.entry()
            result = entry.receive(store_id=store, channel=channel, account_id=account,
                                   sender_id=sender, message_id=message_id, text=text,
                                   ad=ad, media_kind=kind, media_ref=reference,
                                   ai_enabled=self.enabled(scope))
            action = result['action']
            deliveries = []
            if action in {'continue', 'booked', 'cancelled', 'media_unresolved'}:
                deliveries = self.deliver(entry, scope)
            # Return only an acknowledgement to the webhook sender.
            return dict(ok=True, action=action, deliveries=deliveries)

    def deliver(self, entry, scope):
        def send(payload, key):
            return self.send(scope, dict(type='text', **payload) if 'type' not in payload else payload, key)

        task = entry.store.db.execute('''SELECT id FROM sales_v2_order_tasks
            WHERE scope=? AND kind='confirmation' AND status NOT IN ('accepted','superseded')
            ORDER BY id LIMIT 1''', (scope.key,)).fetchone()
        if task:
            result = deliver_confirmation(entry.store, scope, task[0], send, self.enabled)
            if result['status'] != 'accepted':
                return [result['status']]
            return ['accepted', *deliver_replies(entry, scope, send, self.enabled)]
        return deliver_replies(entry, scope, send, self.enabled)

    def staff_control(self, scope, action_id, mode):
        """Used by the trusted test operator, never by webhook content."""
        if mode not in {'active', 'human'}:
            raise ValueError('invalid_control_mode')
        with self.lease:
            store = self.entry().store
            event = store.append(scope, f'control:{action_id}', kind='message', actor='staff', payload={'mode': mode})
            latest = store.db.execute("SELECT MAX(id) FROM sales_v2_events WHERE scope=? AND kind<>'receipt'",
                                      (scope.key,)).fetchone()[0]
            if latest != event['id']:
                return dict(action='superseded')
            return store.set_control(scope, f'control:{event["id"]}', store.snapshot(scope)['version'],
                                     event['id'], mode)['result']


def install(module, *, products, delivery_fee, decide, writer, fetch_image, match_image, post, enabled):
    """Explicitly opt a test application into the preview using an injected transport."""
    if not module.app.testing:
        raise ValueError('sales_preview_requires_testing_app')
    if 'sales_runtime_preview' in module.app.extensions:
        raise ValueError('sales_preview_already_installed')
    if not all(callable(callback) for callback in (fetch_image, match_image, post, enabled)):
        raise ValueError('sales_preview_requires_explicit_adapters')
    observe = ImageObserver(fetch_image, match_image)

    def entry():
        return Entrypoint(module.get_db(), products, delivery_fee, decide, media=observe, writer=writer)

    def send(scope, part, key):
        if scope.channel == 'chatwoot':
            result = chatwoot.send(module, scope.sender, [part], post=post)
        else:
            result = zernio.send(module, scope.sender, [part], post=post, operation_key=key)
        if result['ok'] and result.get('provider_id'):
            return dict(accepted=True, provider_id=result['provider_id'])
        # Rejections with transport uncertainty are left for reconciliation.
        if result['status'] == 'rejected' and result['status_code'] in {400, 401, 403, 404, 422}:
            return dict(accepted=False)
        return {}

    preview = ChannelPreview(entry, send, enabled)
    with module.app.app_context():
        initial = entry()
        initial.migrate()
        initial.store.recover()
        recover_confirmations(initial.store)
    module.app.extensions['sales_runtime_preview'] = preview
    return preview
