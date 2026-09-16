"""Provider failures and staff drafts, with no real model or customer requests."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import requests
import account_app.app as m


class AIConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'test.db')
        m.init_db()
        self.ctx = m.app.app_context(); self.ctx.push()
        self.token = m._current_store_id.set('khuyoot')
        self.db = m.get_db()
        self.sender = 'khuyoot::configuration-test'
        self.customer = m.get_or_create_customer(self.db, self.sender, 'page', 'facebook')
        self.product = dict(product_id='P003', product_name='تنورة', store_id='khuyoot', status='active', price='12000', sizes='38 إلى 52')
        self.ev = dict(sender_id=self.sender, text='شنو قياسات التنورة المتوفرة؟\nشكد سعر التنورة', attachments=[])
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True

    def tearDown(self):
        m._current_store_id.reset(self.token)
        self.ctx.pop(); m.DB_PATH = self.old; self.temp.cleanup()

    def test_legacy_display_name_recovers_to_global_model(self):
        m.set_store_setting(self.db, 'ai_main_model', 'Gemini 3.7 Flash', 'khuyoot')
        m.set_store_setting(self.db, 'ai_main_model', 'google/gemini-3.1-pro-preview', 'default')
        response = requests.Response(); response.status_code = 200
        response._content = json.dumps({'choices': [{'message': {'content': json.dumps({'reply': 'قياسات التنورة من 38 إلى 52، وسعرها 12 ألف.', 'create_order': False})}}]}).encode()
        with patch.object(m.requests, 'post', return_value=response) as post:
            result = m.call_main_ai(self.ev, 'text', self.customer, [], [self.product], self.product, None, '', [])
        self.assertIn('12', result['reply'])
        self.assertEqual(post.call_count, 1)
        self.assertEqual(post.call_args.kwargs['json']['model'], 'google/gemini-3.1-pro-preview')
        self.assertEqual(m.get_app_setting('ai_main_model', db=self.db), 'Gemini 3.7 Flash')

    def test_reply_payload_keeps_context_once_without_image_or_product_duplicates(self):
        history = [
            {'id': 1, 'direction': 'incoming', 'text': 'احب اللون الزيتي', 'created_at': ''},
            {'id': 2, 'direction': 'outgoing', 'text': 'اي قياس تحتاجين؟', 'created_at': ''},
            {'id': 3, 'direction': 'incoming', 'text': 'قياس 44', 'created_at': ''},
        ]
        ev = dict(self.ev, text='قياس 44', image_url='https://images.test/customer.jpg')
        response = requests.Response(); response.status_code = 200
        response._content = json.dumps({'choices': [{'message': {'content': json.dumps({'reply': 'متوفر عيني', 'create_order': False})}}]}).encode()
        with patch.object(m.requests, 'post', return_value=response) as post:
            result = m._call_main_ai_once(ev, 'image', self.customer, history,
                [self.product], self.product, {'product_found': True}, '', [],
                customer_products=[self.product],
                conversation_history=[{'role': 'user', 'content': 'احب اللون الزيتي'}])
        self.assertFalse(result.get('failed'))
        messages = post.call_args.kwargs['json']['messages']
        self.assertEqual(len(messages), 2)
        self.assertTrue(all(isinstance(item['content'], str) for item in messages))
        text = messages[-1]['content']
        self.assertEqual(text.count('احب اللون الزيتي'), 1)
        self.assertEqual(text.count('قياس 44'), 1)
        self.assertEqual(text.count('"product_id":"P003"'), 1)
        self.assertIn('[غير مجابة]', text)
        self.assertEqual(self.db.execute('SELECT count(*) FROM ai_usage_events').fetchone()[0], 1)

    def test_price_and_delivery_use_store_fees_without_guessing_province(self):
        with patch.object(m, 'get_delivery_settings', return_value={'baghdad_fee': 4000, 'other_fee': 6000}):
            result = m.known_product_questions_reply('السعر\nتوصيل اشكد', self.product)
        self.assertIn('12,000', result['reply'])
        self.assertIn('4,000', result['reply'])
        self.assertIn('6,000', result['reply'])
        self.assertFalse(result['create_order'])

    def test_staff_rewrite_is_one_request_preserving_draft_without_image_pipeline(self):
        m.ai_reply_draft_table(self.db)
        self.db.execute('INSERT OR REPLACE INTO ai_reply_drafts VALUES(?,?,?,?,?)',
                        (self.sender, 'old', '{}', 0, m.now_baghdad_iso()))
        self.db.commit()
        response = requests.Response(); response.status_code = 200
        response._content = json.dumps({'choices': [{'message': {'content': 'من عيوني، القماش قطن.'}, 'finish_reason': 'stop'}]}).encode()
        with patch.object(m, 'OPENROUTER_KEY', 'test'), patch.object(m.requests, 'post', return_value=response) as provider, \
             patch.object(m, 'collect_unanswered_event') as collect, patch.object(m, 'call_main_ai') as main, \
             patch.object(m, 'match_product') as vision:
            result = self.client.post(f'/api/conversations/{self.sender}/ask_ai', json={
                'text': 'القماش قطن', 'mode': 'rewrite', 'allow_empty': True})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json['reply'], 'من عيوني، القماش قطن.')
        provider.assert_called_once(); collect.assert_not_called(); main.assert_not_called(); vision.assert_not_called()
        request = provider.call_args.kwargs['json']
        self.assertEqual(json.loads(request['messages'][-1]['content'])['draft'], 'القماش قطن')
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='outgoing'", (self.sender,)).fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM ai_reply_drafts WHERE sender_id=?', (self.sender,)).fetchone()[0], 1)

    def test_staff_rewrite_timeout_retries_only_once(self):
        with patch.object(m, 'OPENROUTER_KEY', 'test'), patch.object(m.requests, 'post', side_effect=requests.ReadTimeout) as provider:
            response = self.client.post(f'/api/conversations/{self.sender}/ask_ai', json={
                'text': 'القماش قطن', 'mode': 'rewrite', 'allow_empty': True})
        self.assertEqual(response.status_code, 502)
        self.assertEqual(provider.call_count, 2)
        self.assertEqual(response.json['reply'], '')

    def test_staff_rewrite_rejects_truncated_suggestion(self):
        response = requests.Response(); response.status_code = 200
        response._content = json.dumps({'choices': [{'message': {'content': 'نص مبتور'}, 'finish_reason': 'length'}]}).encode()
        with patch.object(m, 'OPENROUTER_KEY', 'test'), patch.object(m.requests, 'post', return_value=response):
            result = self.client.post(f'/api/conversations/{self.sender}/ask_ai', json={
                'text': 'المسودة', 'mode': 'rewrite'})
        self.assertEqual(result.status_code, 502)
        self.assertEqual(result.json['reply'], '')

    def test_invalid_save_is_atomic(self):
        m.set_store_setting(self.db, 'ai_enabled', '1', 'khuyoot')
        for name in ['Gemini 3.7 Flash', '', 'google/model with spaces']:
            response = self.client.post('/api/settings/ai?store_id=khuyoot', json={'enabled': False, 'main_model': name})
            self.assertEqual(response.status_code, 400)
            self.assertEqual(m.get_app_setting('ai_enabled', db=self.db, store_id='khuyoot'), '1')

    def test_http_errors_keep_status_and_retry_only_transient_statuses(self):
        for status in [400, 401, 402, 404, 429, 503]:
            response = requests.Response(); response.status_code = status
            response._content = b'{"error":{"message":"private provider details"}}'
            with patch.object(m.requests, 'post', return_value=response) as post:
                result = m.call_main_ai(self.ev, 'text', self.customer, [], [self.product], self.product, None, '', [])
            self.assertEqual(result['failure_reason'], f'provider_http_{status}')
            self.assertEqual(post.call_count, 2 if status in (429, 503) else 1)
            self.assertNotIn('private', str(result))

    def test_staff_can_draft_while_auto_paused_and_keep_selected_product(self):
        m.set_store_setting(self.db, 'ai_enabled', '0', 'khuyoot')
        m.set_customer_ai_enabled(self.db, self.sender, False)
        m.save_message(self.db, self.sender, 'incoming', 'image', '', 'https://images.test/customer.jpg', None, None, {})
        with patch.object(m, 'load_active_products', return_value=[self.product]), patch.object(m, 'find_product_by_id', return_value=self.product), patch.object(m, 'match_product') as match, patch.object(m, 'call_main_ai', return_value={'reply': 'قياس 44 متوفر'}) as main:
            response = self.client.post(f'/api/conversations/{self.sender}/ask_ai', json={'allow_empty': True, 'product_id': 'P003'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['reply'], 'قياس 44 متوفر')
        self.assertEqual(main.call_args.args[5]['product_id'], 'P003')
        match.assert_not_called()
        self.assertFalse(m.is_customer_ai_enabled(self.db, self.sender))
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 0)

    def test_staff_sees_actionable_service_error(self):
        with patch.object(m, 'call_main_ai', return_value={'failed': True, 'failure_reason': 'provider_http_402'}):
            response = self.client.post(f'/api/conversations/{self.sender}/ask_ai', json={'text': 'صياغة جواب'})
        self.assertEqual(response.status_code, 502)
        self.assertIn('رصيد', response.get_json()['error'])
        self.assertEqual(response.get_json()['reply'], '')
