"""Batch delivery and workspace regressions. Run with an isolated database."""
import unittest
from unittest.mock import patch

from account_app import app as m
from account_app.tests import test_chatwoot as fixtures


class MobileWorkspaceTests(unittest.TestCase):
    setUp = fixtures.ChatwootTests.setUp
    tearDown = fixtures.ChatwootTests.tearDown

    def authenticate(self):
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
            session['csrf_token'] = 'mobile-test'
        with m.app.app_context():
            m.get_or_create_customer(m.get_db(), 'default::cw-1-3', 'page', 'facebook')
        return {'X-CSRF-Token': 'mobile-test'}

    def test_batch_images_are_ordered_deduplicated_and_persisted(self):
        headers = self.authenticate()
        urls = ['/product_image/a.jpg', '/product_image/b.jpg', '/product_image/a.jpg']
        with patch.object(m, 'send_image_via_manychat_detailed', return_value={'ok': True, 'status_code': 201}) as send, patch.object(m, 'apply_dashboard_ai_checkout') as checkout:
            response = self.client.post('/api/conversations/default::cw-1-3/send', json={'image_urls': urls}, headers=headers)
            self.assertTrue(response.json['ok'], response.json)
            self.assertEqual(response.json['sent_image_urls'], urls[:2])
            self.assertEqual(send.call_count, 2)
            self.assertTrue(send.call_args_list[0].args[1].endswith('a.jpg'))
            self.assertTrue(send.call_args_list[1].args[1].endswith('b.jpg'))
            checkout.assert_not_called()
        with m.app.app_context():
            rows = m.get_db().execute("SELECT message_type FROM messages WHERE direction='outgoing'").fetchall()
            self.assertEqual([row[0] for row in rows], ['image', 'image'])

    def test_partial_failure_retry_does_not_repeat_sent_images_after_incoming(self):
        headers = self.authenticate()
        body = {'image_urls': ['/product_image/a.jpg', '/product_image/b.jpg', '/product_image/c.jpg'], 'client_send_id': 'same-batch'}
        results = [{'ok': True, 'status_code': 201}, {'ok': False, 'status_code': 429, 'status': 'rate_limited'}, {'ok': True, 'status_code': 201}, {'ok': True, 'status_code': 201}]
        with patch.object(m, 'send_image_via_manychat_detailed', side_effect=results) as send:
            first = self.client.post('/api/conversations/default::cw-1-3/send', json=body, headers=headers)
            self.assertFalse(first.json['ok'])
            self.assertEqual(first.json['sent_image_urls'], body['image_urls'][:1])
            with m.app.app_context():
                m.save_message(m.get_db(), 'default::cw-1-3', 'incoming', 'text', 'رسالة جديدة', None, None, None, {})
            retried = self.client.post('/api/conversations/default::cw-1-3/send', json=body, headers=headers)
            self.assertTrue(retried.json['ok'], retried.json)
            self.assertEqual(send.call_count, 4)
            self.assertEqual([call.args[1].rsplit('/', 1)[-1] for call in send.call_args_list], ['a.jpg', 'b.jpg', 'b.jpg', 'c.jpg'])
        with m.app.app_context():
            self.assertEqual(m.get_db().execute("SELECT COUNT(*) FROM messages WHERE direction='outgoing'").fetchone()[0], 3)

    def test_batch_validation_happens_before_delivery(self):
        headers = self.authenticate()
        invalid = [{'image_urls': 'photo.jpg'}, {'image_urls': [None]}, {'image_urls': ['']}, {'image_urls': [str(n) for n in range(11)]}, {'text': 42}, {'image_url': []}, {'image_urls': ['a'], 'client_send_id': []}]
        with patch.object(m, 'send_image_via_manychat_detailed') as send:
            for payload in invalid:
                self.assertEqual(self.client.post('/api/conversations/default::cw-1-3/send', json=payload, headers=headers).status_code, 400, payload)
            send.assert_not_called()

    def test_workspace_pages_render_with_navigation_and_image_picker(self):
        self.authenticate()
        for path in ['/dashboard', '/orders', '/products', '/settings', '/advisor', '/rewards']:
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200, path)
            self.assertIn('mobile-shell.css', response.text)
            self.assertIn('soof-bottom-nav', response.text)
        page = self.client.get('/dashboard').text
        self.assertIn('multiple hidden onchange="handleImageUpload', page)
        self.assertIn('human-intervention-card', page)
        self.assertNotIn('اقتراح صوف', page)

    def test_waiting_count_is_scoped_to_selected_store(self):
        self.authenticate()
        with m.app.app_context():
            m.save_message(m.get_db(), 'default::cw-1-3', 'incoming', 'text', 'مرحبا', None, None, None, {})
        self.assertEqual(self.client.get('/api/dashboard_stats?store_id=default').json['unanswered_conversations'], 1)
        self.assertEqual(self.client.get('/api/dashboard_stats?store_id=khuyoot').json['unanswered_conversations'], 0)

    def test_product_list_exposes_exact_ad_mappings_as_strings(self):
        self.authenticate()
        product = {'product_id': 'p1', 'store_id': 'default', 'product_name': 'قطعة',
                   'ad_id': '120212345678901234', 'ad_ids': ['44', '45'], 'ref': 'owner-ref'}
        with patch.object(m, 'load_active_products', return_value=[product]):
            result = self.client.get('/api/products?store_id=default').json['products'][0]
        self.assertEqual(result['store_id'], 'default')
        self.assertEqual(result['ad_ids'], ['120212345678901234', '44', '45'])
        self.assertEqual(result['ref'], 'owner-ref')
