import os
import tempfile
import unittest
from pathlib import Path

os.environ['ENABLE_BACKGROUND_JOBS'] = '0'
os.environ['DISABLE_CLIP'] = '1'


class InboxFilterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import account_app.app as module
        cls.module = module
        cls.temp = tempfile.TemporaryDirectory()
        cls.old_path = module.DB_PATH
        module.DB_PATH = str(Path(cls.temp.name) / 'inbox.db')
        module.init_db()
        cls.client = module.app.test_client()
        with cls.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        with module.app.app_context():
            db = module.get_db()
            for i in range(25):
                db.execute("INSERT INTO customers(sender_id,name,store_id,lead_stage,platform,province,last_seen_at,lead_score) VALUES(?,?,?,?,?,?,?,?)",
                           (f'inbox-{i}', f'Customer {i}', 'al-fatena' if i == 0 else 'default', 'hot' if i == 0 else 'new', 'instagram' if i == 0 else 'facebook', 'بغداد', f'2026-09-{i+1:02d}T10:00:00', i))
            db.execute("INSERT INTO messages(sender_id,direction,text,created_at) VALUES(?,?,?,?)", ('inbox-0','incoming','عباية خاصة','2026-09-01T10:00:00'))
            db.execute("INSERT INTO customer_ai_settings(sender_id,enabled) VALUES(?,?)", ('inbox-0',0))
            db.commit()

    @classmethod
    def tearDownClass(cls):
        cls.module.DB_PATH = cls.old_path
        cls.temp.cleanup()

    def get(self, query=''):
        res = self.client.get('/api/conversations?' + query)
        self.assertEqual(res.status_code,200)
        return res.get_json()

    def test_filters_include_records_beyond_first_page(self):
        first = self.get()
        self.assertTrue(first['has_more'])
        self.assertNotIn('inbox-0',[c['sender_id'] for c in first['conversations']])
        data = self.get('store_id=al-fatena&stage=hot&platform=instagram&ai=0&status=unanswered&date_from=2026-09-01&date_to=2026-09-01')
        self.assertEqual([c['sender_id'] for c in data['conversations']], ['inbox-0'])
        self.assertFalse(data['has_more'])

    def test_search_and_pagination(self):
        self.assertEqual(len(self.get('q=عباية')['conversations']),1)
        self.assertEqual(len(self.get('offset=20')['conversations']),5)
        self.assertEqual(self.get('sort=oldest')['conversations'][0]['sender_id'],'inbox-0')
        self.assertEqual(self.get('sort=score')['conversations'][0]['lead_score'],24)
        self.assertEqual(self.get('q=%27%20OR%201%3D1')['conversations'],[])

    def test_validation_and_store_presence(self):
        self.assertEqual(self.client.get('/api/conversations?limit=abc').status_code,400)
        self.assertEqual(self.client.get('/api/conversations?date_from=bad').status_code,400)
        stores=self.client.get('/api/stores').get_json()['stores']
        self.assertIn('al-fatena',[s['store_id'] for s in stores])
        for route in ('/dashboard','/products','/settings/store','/settings/ai','/settings/stores'):
            self.assertEqual(self.client.get(route+'?store_id=al-fatena').status_code,200)

if __name__ == '__main__':
    unittest.main()
