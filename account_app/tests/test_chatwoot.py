import hashlib
import hmac
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from account_app import chatwoot
import account_app.app as m


class ChatwootTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old_db = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'chatwoot.db')
        m.init_db()
        self.env = patch.dict(os.environ, {
            'MESSAGING_PROVIDER': 'chatwoot', 'CHATWOOT_BASE_URL': 'https://chat.example',
            'CHATWOOT_API_TOKEN': 'test-api-token', 'CHATWOOT_WEBHOOK_SECRET': 'test-signature',
            'CHATWOOT_ACCOUNT_ID': '1',
            'CHATWOOT_INBOX_STORES': json.dumps({'1:2': m.DEFAULT_STORE_ID}),
        })
        self.env.start()
        self.client = m.app.test_client()
        self.payload = {'event': 'message_created', 'id': 7, 'account': {'id': 1},
            'inbox': {'id': 2}, 'conversation': {'id': 3, 'channel': 'Channel::Instagram'},
            'sender': {'name': 'Test Customer'}, 'message_type': 'incoming', 'content': 'hello', 'private': False}

    def tearDown(self):
        self.env.stop()
        m.DB_PATH = self.old_db
        self.temp.cleanup()

    def post(self, payload=None, timestamp=None, secret='test-signature'):
        raw = json.dumps(self.payload if payload is None else payload).encode()
        timestamp = str(int(time.time()) if timestamp is None else timestamp)
        signature = 'sha256=' + hmac.new(secret.encode(), timestamp.encode() + b'.' + raw, hashlib.sha256).hexdigest()
        return self.client.post('/chatwoot/webhook', data=raw, content_type='application/json',
            headers={'X-Chatwoot-Timestamp': timestamp, 'X-Chatwoot-Signature': signature})

    def test_signed_intake_persists_before_ack_and_deduplicates(self):
        self.payload['attachments'] = [{'file_type': 'audio', 'data_url': 'https://cdn.example/voice.ogg'}]
        with patch.object(m.threading, 'Thread') as thread:
            self.assertEqual(self.post().status_code, 200)
            self.assertEqual(self.post().status_code, 200)
            self.assertEqual(thread.call_count, 1)
        with m.app.app_context():
            db = m.get_db()
            rows = db.execute("SELECT * FROM messages WHERE direction='incoming'").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['sender_id'], f'{m.DEFAULT_STORE_ID}::cw-1-3')
            self.assertEqual(rows[0]['message_type'], 'audio')
            self.assertEqual(db.execute('SELECT name FROM customers').fetchone()[0], 'Test Customer')

    def test_invalid_signature_stale_missing_secret(self):
        self.assertEqual(self.post(secret='wrong').status_code, 401)
        self.assertEqual(self.post(timestamp=1).status_code, 401)
        with patch.dict(os.environ, CHATWOOT_WEBHOOK_SECRET=''):
            self.assertEqual(self.post().status_code, 503)

    def test_ad_metadata_survives_webhook_and_messages_endpoint(self):
        self.payload['conversation']['additional_attributes'] = {'referral': {
            'ad_id': '555', 'ads_context_data': {'ad_title': 'Example ad', 'photo_url': 'https://cdn.example/ad.jpg'}}}
        with patch.object(m.threading, 'Thread'):
            self.assertEqual(self.post().status_code, 200)
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        data = self.client.get(f'/api/conversations/{m.DEFAULT_STORE_ID}::cw-1-3/messages').get_json()
        self.assertEqual(data['ad_context']['ad_id'], '555')
        self.assertEqual(data['ad_context']['headline'], 'Example ad')
        self.assertEqual(data['messages'][0]['ad_id'], '555')
        self.assertEqual(data['messages'][0]['media'], [])

    def test_outgoing_private_other_events_ignored(self):
        with patch.object(m, '_process_manychat_webhook_async') as process:
            for change in ({'message_type': 'outgoing'}, {'private': True}, {'event': 'conversation_updated'}):
                self.assertTrue(self.post(dict(self.payload, **change)).get_json()['ignored'])
            process.assert_not_called()

    def test_unknown_inbox_and_malformed_payload_rejected(self):
        self.assertEqual(self.post(dict(self.payload, inbox={'id': 99})).status_code, 422)
        self.assertEqual(self.post(dict(self.payload, conversation=None)).status_code, 400)
        self.assertEqual(self.post([]).status_code, 400)

    def test_legacy_endpoint_disabled(self):
        self.assertEqual(self.client.post('/manychat/webhook', json={}).status_code, 410)

    def test_text_transport_uses_chatwoot_only(self):
        response = Mock(status_code=200)
        response.json.return_value = {'id': 22}
        with patch.object(chatwoot.requests, 'post', return_value=response) as post:
            result = m._post_manychat_send(f'{m.DEFAULT_STORE_ID}::cw-1-3', [{'type': 'text', 'text': 'reply'}])
            self.assertTrue(result['ok'])
            self.assertEqual(post.call_args.args[0], 'https://chat.example/api/v1/accounts/1/conversations/3/messages')
            self.assertEqual(post.call_args.kwargs['headers'], {'api_access_token': 'test-api-token'})
            self.assertFalse(post.call_args.kwargs['json']['private'])
            self.assertFalse(post.call_args.kwargs['allow_redirects'])

    def test_old_customer_and_missing_config_fail_without_network(self):
        with patch.object(chatwoot.requests, 'post') as post:
            self.assertEqual(chatwoot.send(m, 'old-manychat-id', [{'type': 'text'}])['status'], 'unmapped_conversation')
            with patch.dict(os.environ, CHATWOOT_BASE_URL=''):
                self.assertFalse(chatwoot.send(m, 'cw-1-3', [{'type': 'text'}])['ok'])
            post.assert_not_called()

    def test_other_account_is_rejected_in_both_directions(self):
        self.assertEqual(self.post(dict(self.payload, account={'id': 2})).status_code, 403)
        with patch.object(chatwoot.requests, 'post') as post:
            self.assertEqual(chatwoot.send(m, 'cw-2-3', [{'type': 'text', 'text': 'hello'}])['status'], 'wrong_account')
            post.assert_not_called()

    def test_image_uploaded_as_attachment(self):
        folder = Path(self.temp.name) / 'product_image'
        folder.mkdir()
        (folder / 'test.png').write_bytes(b'image-fixture')
        response = Mock(status_code=200)
        response.json.return_value = {'id': 44}
        with patch.object(m, 'PRODUCT_IMAGE_DIR', str(folder)), patch.object(chatwoot.requests, 'post', return_value=response) as post:
            result = chatwoot.send(m, 'cw-1-3', [{'type': 'image', 'url': 'https://store.example/product_image/test.png'}])
            self.assertTrue(result['ok'])
            self.assertIn('attachments[]', post.call_args.kwargs['files'])
            self.assertEqual(post.call_args.kwargs['data']['private'], 'false')

    def test_transport_failure_not_reported_as_success(self):
        for status in (401, 403, 500):
            with patch.object(chatwoot.requests, 'post', return_value=Mock(status_code=status)):
                self.assertFalse(chatwoot.send(m, 'cw-1-3', [{'type': 'text', 'text': 'test'}])['ok'])

    def test_diag_does_not_expose_secrets(self):
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        response = self.client.get('/api/chatwoot/diag')
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('test-api-token', response.text)
        self.assertNotIn('test-signature', response.text)

    def test_eight_inboxes_four_stores_keep_incoming_ads_and_replies_isolated(self):
        stores = {'default': [136899, 136895], 'khuyoot': [136900, 136896],
                  'golden-threads': [136901, 136898], 'al-fatena': [135226, 135225]}
        mapping = {f'1:{inbox}': store for store, inboxes in stores.items() for inbox in inboxes}
        with m.app.app_context():
            for store in stores:
                m.ensure_store(m.get_db(), store, store)
        expected = []
        with patch.dict(os.environ, CHATWOOT_INBOX_STORES=json.dumps(mapping)), patch.object(m.threading, 'Thread') as threads:
            for number, (key, store) in enumerate(mapping.items(), 100):
                inbox = int(key.split(':')[1])
                channel = 'Channel::Instagram' if number % 2 == 0 else 'Channel::FacebookPage'
                payload = dict(self.payload, id=number, inbox={'id': inbox}, sender={'id': 777, 'name': 'Same contact'},
                    conversation={'id': number, 'channel': channel, 'additional_attributes': {'referral': {'ad_id': f'ad-{number}'}}})
                self.assertEqual(self.post(payload).status_code, 200)
                expected.append((store, number, inbox))
            self.assertEqual(threads.call_count, 8)
            workers = [(call.kwargs['target'], call.kwargs['args']) for call in threads.call_args_list]
        with m.app.app_context():
            db = m.get_db()
            self.assertEqual(db.execute('SELECT count(*) FROM customers').fetchone()[0], 8)
            for store, number, inbox in expected:
                sender = f'{store}::cw-1-{number}'
                customer = db.execute('SELECT store_id,page_id FROM customers WHERE sender_id=?', (sender,)).fetchone()
                self.assertEqual(customer['store_id'], store)
                self.assertEqual(customer['page_id'], f'chatwoot-1-{inbox}')
                self.assertEqual(m.ad_attribution.load(db, sender)['ad_id'], f'ad-{number}')
                self.assertEqual(db.execute('SELECT store_id FROM messages WHERE sender_id=?', (sender,)).fetchone()[0], store)
        processed = []
        def simulated_ai(db, body, **kwargs):
            event = m.extract_facebook_event(body)
            self.assertEqual(m.current_store_id(), event['store_id'])
            processed.append(event['sender_id'])
            return {'reply': 'Reply for ' + event['sender_id']}
        response = Mock(status_code=200)
        response.json.return_value = {'id': 222}
        with patch.object(m, 'DEBOUNCE_DELAY', 0), patch.object(m, 'process_webhook', side_effect=simulated_ai), patch.object(chatwoot.requests, 'post', return_value=response) as post:
            # Finish in reverse order to catch accidental reuse of the last inbox/store.
            for target, args in reversed(workers):
                target(*args)
            self.assertEqual(len(processed), 8)
            self.assertEqual(post.call_count, 8)
            for call, (store, number, inbox) in zip(post.call_args_list, reversed(expected)):
                self.assertEqual(call.args[0], f'https://chat.example/api/v1/accounts/1/conversations/{number}/messages')
                self.assertIn(f'{store}::cw-1-{number}', call.kwargs['json']['content'])
            post.reset_mock()
            for store, number, inbox in expected:
                self.assertTrue(m.send_reply_via_manychat(f'{store}::cw-1-{number}', 'Manual reply'))
            self.assertEqual([call.args[0] for call in post.call_args_list],
                [f'https://chat.example/api/v1/accounts/1/conversations/{number}/messages' for store, number, inbox in expected])
