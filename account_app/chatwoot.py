"""Chatwoot transport. Credentials are loaded only from server environment."""
import hashlib
import hmac
import json
import mimetypes
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from flask import jsonify, request


def enabled():
    return os.environ.get('MESSAGING_PROVIDER', 'chatwoot').lower() == 'chatwoot'


def configuration():
    return {
        'provider': 'chatwoot' if enabled() else 'manychat',
        'chatwoot_api_url': os.environ.get('CHATWOOT_BASE_URL', '').rstrip('/'),
        'chatwoot_key_present': bool(os.environ.get('CHATWOOT_API_TOKEN', '').strip()),
        'chatwoot_webhook_secret_present': bool(os.environ.get('CHATWOOT_WEBHOOK_SECRET', '').strip()),
        'chatwoot_webhook_url': os.environ.get('PUBLIC_URL', '').rstrip('/') + '/chatwoot/webhook',
        'chatwoot_account_id': os.environ.get('CHATWOOT_ACCOUNT_ID', '').strip(),
    }


def send(module, sender_id, messages):
    result = {'ok': False, 'status_code': 0, 'status': 'configuration_error', 'message': '', 'response': None}
    config = configuration()
    base = config['chatwoot_api_url']
    token = os.environ.get('CHATWOOT_API_TOKEN', '').strip()
    if not token or urlparse(base).scheme != 'https' or not urlparse(base).netloc:
        return dict(result, message='Configure CHATWOOT_BASE_URL (HTTPS) and CHATWOOT_API_TOKEN')
    match = re.fullmatch(r'cw-(\d+)-(\d+)', str(sender_id).split('::')[-1])
    if not match:
        return dict(result, status='unmapped_conversation', message='This customer needs an incoming Chatwoot conversation before sending')
    account, conversation = match.groups()
    if config['chatwoot_account_id'] and account != config['chatwoot_account_id']:
        return dict(result, status='wrong_account', message='Conversation belongs to a different Chatwoot account')
    url = f'{base}/api/v1/accounts/{account}/conversations/{conversation}/messages'
    if not messages:
        return dict(result, status='empty_messages')
    for message in messages:
        payload = {'content': message.get('text', ''), 'message_type': 'outgoing', 'private': False}
        try:
            kwargs = {'headers': {'api_access_token': token}, 'timeout': 30, 'allow_redirects': False}
            if message.get('type') == 'text':
                response = requests.post(url, json=payload, **kwargs)
            elif message.get('type') == 'image':
                reference = message.get('url', '')
                # Upload catalog files directly; do not fetch arbitrary customer URLs.
                if '/product_image/' not in urlparse(reference).path:
                    return dict(result, status='unsupported_image', message='Image must be in the product catalog')
                path = Path(module._local_image_path_from_reference(reference)).resolve()
                roots = [Path(module.PRODUCT_IMAGE_DIR).resolve(), Path(module.UPLOADS_DIR).resolve()]
                if not any(path.is_relative_to(root) for root in roots):
                    return dict(result, status='invalid_image_path')
                if not path.is_file() or path.stat().st_size > 20 * 1024 * 1024:
                    return dict(result, status='invalid_image', message='Image missing or exceeds 20 MB')
                with path.open('rb') as attachment:
                    response = requests.post(url, data={'message_type': 'outgoing', 'private': 'false'},
                        files={'attachments[]': (path.name, attachment, mimetypes.guess_type(path.name)[0] or 'application/octet-stream')}, **kwargs)
            else:
                return dict(result, status='unsupported_message')
            result['status_code'] = response.status_code
            if not 200 <= response.status_code < 300:
                return dict(result, status='rejected', message=f'Chatwoot HTTP {response.status_code}')
            body = response.json()
            if not isinstance(body, dict) or not body.get('id') or body.get('status') == 'failed':
                return dict(result, status='unconfirmed', message='Chatwoot did not confirm message creation')
        except (requests.RequestException, ValueError, OSError):
            return dict(result, status='transport_error', message='Chatwoot request or image upload failed')
    return dict(result, ok=True, status='success')


def install(module):
    @module.app.route('/chatwoot/webhook', methods=['POST'])
    @module.app.route('/chatwoot/webhook/<store_key>', methods=['POST'])
    def chatwoot_webhook(store_key=''):
        if not enabled():
            return jsonify(error='Chatwoot is disabled'), 409
        secret = os.environ.get('CHATWOOT_WEBHOOK_SECRET', '').strip()
        if not secret:
            return jsonify(error='CHATWOOT_WEBHOOK_SECRET is not configured'), 503
        timestamp = request.headers.get('X-Chatwoot-Timestamp', '')
        signature = request.headers.get('X-Chatwoot-Signature', '')
        try:
            if abs(time.time() - int(timestamp)) > 300:
                raise ValueError()
        except ValueError:
            return jsonify(error='Invalid webhook timestamp'), 401
        expected = 'sha256=' + hmac.new(secret.encode(), timestamp.encode() + b'.' + request.get_data(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            return jsonify(error='Invalid webhook signature'), 401
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify(error='Expected a JSON object'), 400
        if data.get('event') != 'message_created' or data.get('message_type') not in ('incoming', 0) or data.get('private'):
            return jsonify(ok=True, ignored=True)
        try:
            account = int(data['account']['id'])
            conversation = int(data['conversation']['id'])
            inbox = int(data['inbox']['id'])
            message_id = int(data['id'])
            if min(account, conversation, inbox, message_id) <= 0:
                raise ValueError()
            configured_account = configuration()['chatwoot_account_id']
            if configured_account and str(account) != configured_account:
                return jsonify(error='Unexpected Chatwoot account'), 403
            mapping = json.loads(os.environ.get('CHATWOOT_INBOX_STORES', '{}'))
            mapped_store = mapping.get(f'{account}:{inbox}', '')
            if store_key and mapped_store and store_key != mapped_store:
                raise ValueError()
            # Never silently route an unknown inbox into another store.
            route = store_key or mapped_store
            if not route:
                return jsonify(error='Configure CHATWOOT_INBOX_STORES for this account:inbox'), 422
            db = module.get_db()
            store = module.resolve_webhook_store(db, {}, route)
        except (KeyError, TypeError, ValueError, AttributeError):
            return jsonify(error='Invalid account, conversation, inbox or store'), 400
        module._current_store_id.set(store)
        sender_id = f'{store}::cw-{account}-{conversation}'
        platform = {'Channel::Instagram': 'instagram', 'Channel::Whatsapp': 'whatsapp'}.get(data['conversation'].get('channel'), 'facebook')
        attachments = []
        for attachment in data.get('attachments') or []:
            if isinstance(attachment, dict) and attachment.get('data_url'):
                kind = attachment.get('file_type', 'file')
                attachments.append({'type': kind, 'payload': {'url': attachment['data_url']}})
        ad_context = module.ad_attribution.extract(data)
        body = {'_ad_context': ad_context, 'object': platform if platform != 'facebook' else 'page', 'entry': [{
            'id': f'chatwoot-{account}-{inbox}', '_store_id': store,
            'messaging': [{'sender': {'id': sender_id}, 'recipient': {'id': f'chatwoot-{account}-{inbox}'},
                'timestamp': int(time.time() * 1000), 'message': {
                    'mid': f'chatwoot-{account}-{message_id}', 'text': str(data.get('content') or ''), 'attachments': attachments}}]}]}
        if ad_context:
            body['entry'][0]['messaging'][0]['referral'] = {
                'ad_id': ad_context.get('ad_id'), 'ref': ad_context.get('ref'),
                'source': ad_context.get('source'), 'type': ad_context.get('source_type')}
        module.get_or_create_customer(db, sender_id, f'chatwoot-{account}-{inbox}', platform)
        sender = data.get('sender') or {}
        name = sender.get('name', '') if isinstance(sender, dict) else ''
        if name:
            db.execute('UPDATE customers SET name=? WHERE sender_id=?', (str(name), sender_id))
            db.commit()
        # Use the existing per-sender queue, debounce, order logic and delivery review.
        module._process_manychat_webhook_async(body, sender_id, platform, sender_id, background_after_intake=True)
        return jsonify(ok=True)

    @module.app.route('/api/chatwoot/diag')
    @module._dash_require
    def chatwoot_diag():
        return jsonify(configuration())

    @module.app.route('/api/chatwoot/test')
    @module._dash_require
    def chatwoot_test():
        config = configuration()
        if not config['chatwoot_key_present'] or not config['chatwoot_api_url'].startswith('https://'):
            return jsonify(ok=False, error='Configure CHATWOOT_BASE_URL and CHATWOOT_API_TOKEN'), 503
        try:
            response = requests.get(config['chatwoot_api_url'] + '/api/v1/profile',
                headers={'api_access_token': os.environ['CHATWOOT_API_TOKEN']}, timeout=15, allow_redirects=False)
            return jsonify(ok=response.status_code == 200, status_code=response.status_code)
        except requests.RequestException:
            return jsonify(ok=False, error='Chatwoot connection failed'), 502
