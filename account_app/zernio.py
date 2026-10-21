"""Zernio transport for the explicitly migrated al-fatena store only."""
import hashlib
import hmac
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

import requests
from flask import jsonify, request

STORE_ID = 'al-fatena'
API_URL = 'https://zernio.com/api/v1'


def account_bindings():
    try:
        entries = json.loads(os.environ.get('ZERNIO_ACCOUNTS', '{}'))
        if not isinstance(entries, dict):
            return {}
        return {key: value for key, value in entries.items()
                if re.fullmatch(r'[a-fA-F0-9]{24}', key)
                and isinstance(value, dict) and value.get('store_id') == STORE_ID
                and value.get('platform') in ('facebook', 'instagram')}
    except (ValueError, TypeError):
        return {}


def enabled_for_store(store):
    # A broken mapping must fail closed, never silently return a migrated store to Chatwoot.
    return store == STORE_ID and os.environ.get('ZERNIO_AL_FATENA_ENABLED', '0') == '1'


def sender_store(sender_id):
    return str(sender_id or '').partition('::')[0] if '::' in str(sender_id or '') else ''


def configuration():
    config = {
        'enabled': enabled_for_store(STORE_ID), 'store_id': STORE_ID,
        'key_present': bool(os.environ.get('ZERNIO_API_KEY', '').strip()),
        'webhook_secret_present': bool(os.environ.get('ZERNIO_WEBHOOK_SECRET', '').strip()),
        'accounts': account_bindings(),
        'webhook_url': os.environ.get('PUBLIC_URL', '').rstrip('/') + '/zernio/webhook',
    }
    config['ready'] = bool(config['enabled'] and config['key_present']
                           and config['webhook_secret_present'] and config['accounts'])
    return config


def init_db(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS zernio_conversations (
            sender_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL, participant_id TEXT NOT NULL,
            platform TEXT NOT NULL, store_id TEXT NOT NULL,
            UNIQUE(account_id, participant_id));
        CREATE TABLE IF NOT EXISTS zernio_ad_events (
            event_id TEXT PRIMARY KEY, sender_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL, data TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS zernio_ads_sender ON zernio_ad_events(sender_id, occurred_at);
    ''')


def event_time(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        raise ValueError('Invalid event timestamp')


def _binding(module, data):
    account, conversation = data.get('account'), data.get('conversation')
    if not isinstance(account, dict) or not isinstance(conversation, dict):
        raise ValueError('Missing account or conversation')
    account_id = str(account.get('accountId') or account.get('id') or '')
    if account.get('id') and account.get('accountId') and account['id'] != account['accountId']:
        raise ValueError('Conflicting account identifiers')
    config = account_bindings().get(account_id)
    if not config or not enabled_for_store(config['store_id']):
        raise ValueError('Unmapped Zernio account')
    if account.get('platform') != config['platform']:
        raise ValueError('Unexpected account platform')
    conv_id = str(conversation.get('id') or '').strip()
    message_conversation = (data.get('message') or {}).get('conversationId')
    if message_conversation and message_conversation != conv_id:
        raise ValueError('Conflicting conversation identifiers')
    sender = (data.get('message') or {}).get('sender') or data.get('sender') or {}
    if not isinstance(sender, dict):
        raise ValueError('Invalid sender')
    participant = str(conversation.get('participantId') or sender.get('id') or '').strip()
    if sender.get('id') and conversation.get('participantId') and str(sender['id']) != participant:
        raise ValueError('Conflicting participant identifiers')
    if not conv_id or not participant or len(conv_id) > 500 or len(participant) > 250:
        raise ValueError('Missing conversation or participant identity')
    # Account and platform-scoped participant IDs, never names, identify a customer.
    identity = hashlib.sha256(participant.encode()).hexdigest()[:32]
    sender_id = f'{STORE_ID}::zn-{account_id}-{identity}'
    db = module.get_db()
    if not db.execute('SELECT 1 FROM stores WHERE store_id=?', (STORE_ID,)).fetchone():
        raise ValueError('Store is not configured')
    old = db.execute('SELECT * FROM zernio_conversations WHERE sender_id=?', (sender_id,)).fetchone()
    if old and (old['account_id'] != account_id or old['participant_id'] != participant):
        raise ValueError('Conversation identity conflict')
    db.execute('''INSERT INTO zernio_conversations VALUES(?,?,?,?,?,?)
        ON CONFLICT(sender_id) DO UPDATE SET conversation_id=excluded.conversation_id''',
        (sender_id, account_id, conv_id, participant, config['platform'], STORE_ID))
    module._current_store_id.set(STORE_ID)
    module.get_or_create_customer(db, sender_id, 'zernio-' + account_id, config['platform'])
    name = sender.get('name') or conversation.get('participantName')
    if isinstance(name, str) and name.strip():
        db.execute('UPDATE customers SET name=? WHERE sender_id=?', (name.strip()[:250], sender_id))
    db.commit()
    return sender_id, account_id, config['platform']


def _ad_context(module, data, occurred_at):
    context = module.ad_attribution.extract(data)
    if not context:
        return {}
    try:
        context['captured_at'] = event_time(context.get('captured_at'))
    except ValueError:
        context['captured_at'] = occurred_at
    return context


def _save_ad(module, sender_id, data, occurred_at):
    context = _ad_context(module, data, occurred_at)
    if not context:
        return
    db = module.get_db()
    # Referral-only events must not create an incoming message or trigger AI.
    db.execute('INSERT OR IGNORE INTO zernio_ad_events VALUES(?,?,?,?)',
               ('zernio:' + str(data['account'].get('accountId') or data['account']['id']) + ':' + data['id'],
                sender_id, occurred_at, json.dumps(context, ensure_ascii=False)))
    latest = db.execute('SELECT MAX(id) FROM messages').fetchone()[0] or 0
    module.ad_attribution.save(db, sender_id, latest, {'_ad_context': context})
    db.commit()


def product_event(module, db, event, products):
    """Use saved attribution once it is newer than the customer's product decisions."""
    sender_id = event.get('sender_id') or ''
    if not enabled_for_store(sender_store(sender_id)):
        return event
    # Historical Chatwoot customers and other stores must never inherit this context.
    if not db.execute('SELECT 1 FROM zernio_conversations WHERE sender_id=? AND store_id=?',
                      (sender_id, STORE_ID)).fetchone():
        return event
    context = module.ad_attribution.load(db, sender_id)
    if not context or not (context.get('ad_id') or context.get('ref')):
        return event
    result = dict(event, ad_id=None, ref=None)
    source = {key: context[key] for key in ('ad_id', 'ref', 'headline', 'post_id', 'captured_at') if context.get(key)}
    source['resolution'] = 'historical'
    result['_ad_source'] = source
    try:
        captured = datetime.fromisoformat(event_time(context.get('captured_at')))
    except ValueError:
        return result
    decisions = db.execute('''SELECT last_seen_at, rejected_at FROM customer_product_interests
        WHERE sender_id=? AND COALESCE(source,'')<>'auto_default_product'
        AND COALESCE(match_method,'') NOT IN ('auto_default_product','catalog_search','reply_product_context')''',
        (sender_id,)).fetchall()
    for decision in decisions:
        for value in decision:
            if not value:
                continue
            try:
                chosen = datetime.fromisoformat(value)
                if chosen.tzinfo is None:
                    chosen = chosen.replace(tzinfo=module.BAGHDAD_TZ)
                # Product decisions are stored to seconds; favour the customer's
                # choice when provider milliseconds fall in that same second.
                if chosen >= captured.replace(microsecond=0):
                    return result
            except (ValueError, TypeError):
                continue
    text = event.get('text') or ''
    named = module.first_message_named_products(text, products)
    explicit = module.customer_product_target(text, products)
    if (event.get('image_url') or event.get('attachments') or event.get('_selection_resolved')
            or module.is_product_objection(text) or module.sales_strategy.signals(text) == ['refusal']
            or explicit or len(named) == 1
            or module.sales_context.browse_request(text)
            or module.pending_product_choice(db, sender_id)):
        source['resolution'] = 'customer_choice'
        return result
    candidates = [product for product in products
                  if product.get('store_id', STORE_ID) == STORE_ID and any(
                      context.get(key) and str(product.get(key) or '').strip() == str(context[key]).strip()
                      for key in ('ad_id', 'ref'))]
    if len(candidates) != 1:
        source['resolution'] = 'ambiguous' if candidates else 'unmapped'
        result['_ad_product_unresolved'] = True
        return result
    if module.select_customer_context_product(text, candidates) is None:
        source['resolution'] = 'customer_choice'
        return result
    source.update(resolution='matched', product_id=candidates[0]['product_id'])
    result.update(ad_id=context.get('ad_id'), ref=context.get('ref'))
    return result


def send(module, sender_id, messages):
    result = {'ok': False, 'status_code': 0, 'status': 'configuration_error', 'message': '', 'response': None}
    if not enabled_for_store(sender_store(sender_id)):
        return dict(result, status='wrong_store', message='Zernio is enabled for al-fatena only')
    token = os.environ.get('ZERNIO_API_KEY', '').strip()
    if not token:
        return dict(result, status='missing_key', message='Configure ZERNIO_API_KEY')
    row = module.get_db().execute('SELECT * FROM zernio_conversations WHERE sender_id=?', (sender_id,)).fetchone()
    if not row:
        return dict(result, status='unmapped_conversation',
                    message='تحتاج هذه المحادثة رسالة واردة عبر Zernio قبل الإرسال؛ المحادثة القديمة محفوظة.')
    binding = account_bindings().get(row['account_id'])
    if not binding or binding['platform'] != row['platform'] or row['store_id'] != STORE_ID:
        return dict(result, status='wrong_account', message='Zernio account is not mapped to this store')
    if not messages:
        return dict(result, status='empty_messages')
    # Per-job retries share keys; deliberate manual sends without a job remain distinct.
    batch = module.ai_jobs.active_job.get() or uuid.uuid4().hex
    for index, message in enumerate(messages):
        if not isinstance(message, dict):
            return dict(result, status='unsupported_message')
        payload = {'accountId': row['account_id']}
        if message.get('type') == 'text' and str(message.get('text') or '').strip():
            payload['message'] = message['text']
        elif message.get('type') in {'image', 'audio'}:
            try:
                audio = message.get('type') == 'audio'
                reference = (message.get('url') or '') if audio else module.build_public_image_url(message.get('url') or '')
                parsed = urlparse(reference)
                public = urlparse(module.PUBLIC_URL or '')
                if (parsed.scheme != 'https' or not parsed.netloc or parsed.netloc != public.netloc
                        or parsed.username or parsed.password):
                    return dict(result, status='invalid_audio' if audio else 'invalid_image', message='Attachment must use this public HTTPS host')
                if audio:
                    path = Path(message.get('local_path') or '').resolve()
                    roots = [Path(module.OUTGOING_AUDIO_DIR).resolve()]
                    if not re.fullmatch(r'/outgoing_audio/[a-f0-9]{64}\.(?:webm|ogg|m4a|mp3)', parsed.path):
                        return dict(result, status='invalid_audio')
                else:
                    path = Path(module._local_image_path_from_reference(reference)).resolve()
                    roots = [Path(module.PRODUCT_IMAGE_DIR).resolve(), Path(module.UPLOADS_DIR).resolve()]
                    if not parsed.path.startswith('/product_image/'):
                        return dict(result, status='invalid_image')
                if not any(path.is_relative_to(root) for root in roots) or not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
                    return dict(result, status='invalid_audio' if audio else 'invalid_image', message='Attachment missing or exceeds 8 MB')
            except (ValueError, TypeError, OSError):
                return dict(result, status='invalid_audio' if message.get('type') == 'audio' else 'invalid_image', message='Invalid attachment path')
            payload.update(attachmentUrl=reference, attachmentType='audio' if audio else 'image')
            if message.get('caption'):
                payload['message'] = message['caption']
        else:
            return dict(result, status='unsupported_message')
        key = hashlib.sha256(json.dumps([batch, sender_id, index, payload], sort_keys=True).encode()).hexdigest()
        try:
            response = requests.post(API_URL + '/inbox/conversations/' + quote(row['conversation_id'], safe='') + '/messages',
                json=payload, headers={'Authorization': 'Bearer ' + token, 'Idempotency-Key': key},
                timeout=30, allow_redirects=False)
            result['status_code'] = response.status_code
            if not 200 <= response.status_code < 300:
                return dict(result, status='rejected', message=f'Zernio HTTP {response.status_code}')
            body = response.json()
            data = (body.get('data') or {}) if isinstance(body, dict) else {}
            if (not isinstance(body, dict) or body.get('success') is not True or not isinstance(data, dict)
                    or not (data.get('messageId') or data.get('messageIds')) or data.get('partialFailure')):
                return dict(result, status='unconfirmed', message='Zernio did not confirm the complete message')
        except (requests.RequestException, ValueError, OSError):
            return dict(result, status='transport_error', message='Zernio delivery could not be confirmed')
    return dict(result, ok=True, status='success')


def install(module):
    @module.app.post('/zernio/webhook')
    @module.app.post('/zernio/webhook/<store_key>')
    def zernio_webhook(store_key=''):
        if not enabled_for_store(STORE_ID):
            return jsonify(error='Zernio migration is disabled'), 409
        if store_key and store_key != STORE_ID:
            return jsonify(error='Zernio is enabled for al-fatena only'), 403
        secret = os.environ.get('ZERNIO_WEBHOOK_SECRET', '').strip()
        if not secret:
            return jsonify(error='Configure ZERNIO_WEBHOOK_SECRET'), 503
        expected = hmac.new(secret.encode(), request.get_data(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, request.headers.get('X-Zernio-Signature', '')):
            return jsonify(error='Invalid webhook signature'), 401
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify(error='Expected a JSON object'), 400
        if data.get('event') == 'webhook.test' or data.get('test') is True:
            return jsonify(ok=True, test=True)
        if data.get('event') not in ('message.received', 'referral.received'):
            return jsonify(ok=True, ignored=True)
        try:
            if not isinstance(data.get('id'), str) or not data['id'].strip() or len(data['id']) > 250:
                raise ValueError('Missing event identifier')
            occurred_at = event_time(data.get('timestamp'))
            message = data.get('message', {})
            if not isinstance(message, dict):
                raise ValueError('Invalid message')
            metadata = data.get('metadata') or {}
            if not isinstance(metadata, dict):
                raise ValueError('Invalid metadata')
            if data['event'] == 'message.received':
                if message.get('direction') != 'incoming':
                    return jsonify(ok=True, ignored=True)
                if message.get('platform') not in ('facebook', 'instagram'):
                    raise ValueError('Unsupported message platform')
                if message.get('platform') != (data.get('account') or {}).get('platform'):
                    raise ValueError('Message platform does not match account')
                if not (message.get('platformMessageId') or message.get('id')):
                    raise ValueError('Missing message identifier')
                if not isinstance(message.get('text') or '', str) or not isinstance(message.get('attachments', []), list):
                    raise ValueError('Invalid message content')
                event_time(message.get('sentAt') or data['timestamp'])
            sender_id, account_id, platform = _binding(module, data)
        except (ValueError, TypeError, KeyError, AttributeError):
            return jsonify(error='Invalid or unmapped Zernio event'), 422
        _save_ad(module, sender_id, data, occurred_at)
        if data['event'] == 'referral.received':
            return jsonify(ok=True, referral=True)
        # Preserve referrals while another app owns a thread; never auto-take control.
        if metadata.get('standby') is True:
            return jsonify(ok=True, standby=True)
        text = message.get('text') or ''
        attachments = []
        for item in message.get('attachments', []):
            if isinstance(item, dict) and module.ad_attribution.safe_url(item.get('url')):
                kind = item.get('type')
                # Posts/reels may contain image URLs: the existing media resolver classifies files.
                kind = kind if kind in ('image', 'video', 'audio', 'file') else 'file'
                attachments.append({'type': kind, 'payload': {'url': item['url']}})
        if not text.strip() and not attachments:
            return jsonify(ok=True, ignored=True)
        context = _ad_context(module, data, occurred_at)
        native_id = str(message.get('platformMessageId') or message['id'])
        body = {'_ad_context': context, 'object': 'page' if platform == 'facebook' else platform,
                'entry': [{'id': 'zernio-' + account_id, '_store_id': STORE_ID, 'messaging': [{
                    'sender': {'id': sender_id}, 'recipient': {'id': 'zernio-' + account_id},
                    'timestamp': int(datetime.fromisoformat(event_time(message.get('sentAt') or data['timestamp'])).timestamp() * 1000),
                    'message': {'mid': 'zernio-' + account_id + '-' + native_id, 'text': text, 'attachments': attachments},
                    'referral': {'ad_id': context.get('ad_id'), 'ref': context.get('ref')} if context else {},
                }]}]}
        module._process_manychat_webhook_async(body, sender_id, platform, sender_id, background_after_intake=True)
        return jsonify(ok=True)

    @module.app.get('/api/zernio/diag')
    @module._dash_require
    def zernio_diag():
        return jsonify(configuration())
