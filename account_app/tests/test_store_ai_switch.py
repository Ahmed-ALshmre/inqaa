"""A store pause is isolated and blocks automatic delivery, including old work."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from account_app import app as m


class StoreAISwitchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old_db = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'store-ai.db')
        m.init_db()
        self.ctx = m.app.app_context()
        self.ctx.push()
        self.db = m.get_db()
        m.ensure_store(self.db, 'khuyoot', 'خيوط')
        self.default_sender = 'default::store-ai-test'
        self.other_sender = 'khuyoot::store-ai-test'
        default_token = m._current_store_id.set('default')
        m.get_or_create_customer(self.db, self.default_sender, 'page')
        m._current_store_id.reset(default_token)
        other_token = m._current_store_id.set('khuyoot')
        m.get_or_create_customer(self.db, self.other_sender, 'page')
        m._current_store_id.reset(other_token)
        for sender in (self.default_sender, self.other_sender):
            self.db.execute("INSERT INTO followups(sender_id,stage,status,scheduled_at) VALUES(?,'conversation','pending',?)",
                            (sender, m.now_baghdad_iso()))
        self.db.commit()
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True

    def tearDown(self):
        self.ctx.pop()
        m.DB_PATH = self.old_db
        self.temp.cleanup()

    def test_one_store_pauses_automatic_messages_and_its_followups_only(self):
        stopped = self.client.post('/api/stores/khuyoot/ai', json={'enabled': False})
        self.assertEqual(stopped.status_code, 200)
        self.assertEqual(stopped.json['cancelled_followups'], 1)
        stores = {store['store_id']: store for store in self.client.get('/api/stores').json['stores']}
        self.assertFalse(stores['khuyoot']['ai_enabled'])
        self.assertTrue(stores['default']['ai_enabled'])
        statuses = dict(self.db.execute('SELECT sender_id,status FROM followups'))
        self.assertEqual(statuses[self.other_sender], 'cancelled')
        self.assertEqual(statuses[self.default_sender], 'pending')

        token = m._current_store_id.set('khuyoot')
        try:
            with patch.object(m, 'call_main_ai') as model, patch.object(m, 'send_text_to_facebook') as send:
                result = m.auto_reply_after_product_link(self.db, self.other_sender,
                    {'product_id': 'P1', 'product_name': 'قطعة'}, staff_action=True)
                self.assertFalse(result['sent'])
                self.assertFalse(m.send_webhook_result_to_facebook(
                    {'sender_id': self.other_sender, 'reply': 'رد متأخر'}))
                model.assert_not_called()
                send.assert_not_called()
        finally:
            m._current_store_id.reset(token)

        m.set_customer_ai_enabled(self.db, self.other_sender, False, reason='manual')
        resumed = self.client.post('/api/stores/khuyoot/ai', json={'enabled': True})
        self.assertTrue(resumed.json['ai_enabled'])
        self.assertFalse(m.is_customer_ai_enabled(self.db, self.other_sender))
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE sender_id=?',
                                         (self.other_sender,)).fetchone()['status'], 'cancelled')

    def test_invalid_store_and_payload_do_not_change_state(self):
        self.assertEqual(self.client.post('/api/stores/unknown/ai', json={'enabled': False}).status_code, 404)
        self.assertEqual(self.client.post('/api/stores/khuyoot/ai', json={'enabled': 'false'}).status_code, 400)
        self.assertEqual(m.get_app_setting('ai_enabled', '1', self.db, 'khuyoot'), '1')

    def test_ai_settings_page_uses_same_store_pause(self):
        response = self.client.post('/api/settings/ai?store_id=khuyoot', json={'enabled': False})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE sender_id=?',
                                         (self.other_sender,)).fetchone()['status'], 'cancelled')
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE sender_id=?',
                                         (self.default_sender,)).fetchone()['status'], 'pending')
