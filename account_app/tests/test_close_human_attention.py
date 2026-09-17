import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from account_app import app as m
from account_app.staff_access import required_permission


class CloseHumanAttentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'test.db')
        m.init_db()
        self.context = m.app.app_context(); self.context.push()
        self.db = m.get_db()
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        for sender, store in [('old', 'default'), ('other', 'al-fatena')]:
            m.get_or_create_customer(self.db, sender, 'page', 'facebook')
            self.db.execute('UPDATE customers SET store_id=? WHERE sender_id=?', (store, sender))
            self.db.execute("INSERT INTO human_reviews(sender_id,status,reason) VALUES(?,'pending','صورة')", (sender,))
            self.db.execute("INSERT INTO problem_reports(sender_id,status,reason) VALUES(?,'needs_attention','قديم')", (sender,))
        self.db.execute("INSERT INTO problem_reports(sender_id,status,reason) VALUES('old',NULL,'حالة قديمة')")
        self.db.commit()
        m.set_customer_ai_enabled(self.db, 'old', False)

    def tearDown(self):
        self.context.pop(); m.DB_PATH = self.old; self.temp.cleanup()

    def test_single_close_removes_all_sources_for_only_that_customer(self):
        response = self.client.post('/api/conversations/old/mark_reviewed', json={})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['closed_problems'], 2)
        rows = self.client.get('/api/conversations?status=problems').json['conversations']
        self.assertEqual([r['sender_id'] for r in rows], ['other'])
        self.assertTrue(m.is_customer_ai_enabled(self.db, 'old'))

    def test_bulk_zeroes_filter_without_deleting_records_and_new_issues_reappear(self):
        before = {table: self.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                  for table in ['customers', 'orders', 'messages', 'human_reviews', 'problem_reports']}
        response = self.client.post('/api/maintenance/close_human_reviews')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['closed_conversations'], 2)
        self.assertEqual(response.json['closed_reviews'], 2)
        self.assertEqual(response.json['closed_problems'], 3)
        self.assertEqual(self.client.get('/api/conversations?status=problems').json['conversations'], [])
        for table, count in before.items():
            self.assertEqual(self.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0], count)
        self.assertFalse(m.is_customer_ai_enabled(self.db, 'old'))
        self.assertEqual(self.client.post('/api/maintenance/close_human_reviews').json['closed_conversations'], 0)
        m.create_human_review(self.db, {'sender_id': 'old'}, 'طلب جديد', notify_telegram=False)
        self.assertEqual(len(self.client.get('/api/conversations?status=problems').json['conversations']), 1)

    def test_bulk_requires_owner_and_authentication(self):
        self.assertEqual(required_permission('/api/maintenance/close_human_reviews', 'POST'), 'owner')
        client = m.app.test_client()
        self.assertIn(client.post('/api/maintenance/close_human_reviews').status_code, [302, 401, 403])

    def test_silent_link_resumes_immediately_without_sending(self):
        product = {'product_id': 'P1', 'product_name': 'فستان'}
        with patch.object(m, 'find_product_by_id', return_value=product), patch.object(m, 'auto_reply_after_product_link') as reply:
            response = self.client.post('/api/conversations/old/link_product', json={'product_id': 'P1', 'silent': True})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json['ai_resumed'])
        self.assertTrue(m.is_customer_ai_enabled(self.db, 'old'))
        self.assertEqual(self.db.execute("SELECT status FROM human_reviews WHERE sender_id='old'").fetchone()[0], 'linked')
        reply.assert_not_called()

    def test_human_product_selection_resumes_conversation(self):
        review_id = self.db.execute("SELECT id FROM human_reviews WHERE sender_id='old'").fetchone()[0]
        with patch.object(m, 'find_product_by_id', return_value={'product_id': 'P1', 'product_name': 'فستان'}):
            result = m.handle_human_product_selection(self.db, review_id, 'P1')
        self.assertTrue(result['ok'])
        self.assertTrue(m.is_customer_ai_enabled(self.db, 'old'))

    def test_link_resumes_even_if_first_reply_cannot_be_sent(self):
        product = {'product_id': 'P1', 'product_name': 'فستان'}
        def reply(*args, **kwargs):
            self.assertTrue(m.is_customer_ai_enabled(self.db, 'old'))
            return {'sent': False, 'reason': 'send_failed'}
        with patch.object(m, 'find_product_by_id', return_value=product), patch.object(m, 'is_ai_enabled', return_value=True), patch.object(m, 'auto_reply_after_product_link', side_effect=reply):
            response = self.client.post('/api/conversations/old/link_product', json={'product_id': 'P1'})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json['ai_resumed'])
        self.assertFalse(response.json['auto_reply']['sent'])
