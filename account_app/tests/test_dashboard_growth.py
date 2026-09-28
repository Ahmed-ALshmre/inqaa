import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

os.environ['ENABLE_BACKGROUND_JOBS'] = '0'
os.environ['DISABLE_CLIP'] = '1'


class DashboardGrowthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import account_app.app as module
        cls.module = module
        cls.temp = tempfile.TemporaryDirectory()
        cls.old_path = module.DB_PATH
        module.DB_PATH = str(Path(cls.temp.name) / 'growth.db')
        module.init_db()
        cls.client = module.app.test_client()
        with cls.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        now_time = datetime.now(module.BAGHDAD_TZ).replace(microsecond=0)
        now = (now_time - timedelta(minutes=1)).isoformat()
        older = (now_time - timedelta(hours=25)).isoformat()
        rolling = (now_time - timedelta(hours=23)).isoformat()
        with module.app.app_context():
            db = module.get_db()
            for sender in ('buyer', 'lead-1', 'lead-2', 'lead-3', 'old-buyer', 'rolling-buyer'):
                db.execute('INSERT INTO customers(sender_id,name,store_id) VALUES(?,?,?)', (sender, sender, 'default'))
            db.execute('INSERT INTO customers(sender_id,name,store_id) VALUES(?,?,?)', ('other-buyer', 'other', 'al-fatena'))
            for sender in ('buyer', 'lead-1', 'lead-2', 'lead-3'):
                db.execute('INSERT INTO messages(sender_id,direction,text,created_at,store_id) VALUES(?,?,?,?,?)',
                           (sender, 'incoming', 'استفسار', now, 'default'))
            db.execute('INSERT INTO messages(sender_id,direction,text,created_at,store_id) VALUES(?,?,?,?,?)',
                       ('old-buyer', 'incoming', 'استفسار سابق', older, 'default'))
            db.execute('INSERT INTO messages(sender_id,direction,text,created_at,store_id) VALUES(?,?,?,?,?)',
                       ('old-buyer', 'incoming', 'رجع يسأل', now, 'default'))
            db.execute('INSERT INTO messages(sender_id,direction,text,created_at,store_id) VALUES(?,?,?,?,?)',
                       ('buyer', 'incoming', 'رسالة ثانية', now, 'default'))
            db.execute('INSERT INTO messages(sender_id,direction,text,created_at,store_id) VALUES(?,?,?,?,?)',
                       ('rolling-buyer', 'incoming', 'بدأ قبل 23 ساعة', rolling, 'default'))
            db.execute('INSERT INTO messages(sender_id,direction,text,created_at,store_id) VALUES(?,?,?,?,?)',
                       ('other-buyer', 'incoming', 'استفسار', now, 'al-fatena'))
            for sender, store in (('buyer', 'default'), ('buyer', 'default'),
                                  ('old-buyer', 'default'), ('rolling-buyer', 'default'),
                                  ('other-buyer', 'al-fatena')):
                db.execute('INSERT INTO orders(sender_id,product_id,product_name,created_at,store_id) VALUES(?,?,?,?,?)',
                           (sender, 'P1', 'منتج تجريبي', now, store))
            db.execute('INSERT INTO orders(sender_id,product_id,product_name,status,created_at,store_id) VALUES(?,?,?,?,?,?)',
                       ('lead-1', 'P1', 'منتج ملغى', 'cancelled', now, 'default'))
            db.commit()

    @classmethod
    def tearDownClass(cls):
        cls.module.DB_PATH = cls.old_path
        cls.temp.cleanup()

    def test_rolling_conversion_counts_first_contacts_not_returning_people_or_messages(self):
        data = self.client.get('/api/dashboard_stats?period=24h&store_id=default').get_json()
        self.assertEqual(data['people_total'], 5)
        self.assertEqual(data['booked_people'], 2)
        self.assertEqual(data['orders_total'], 4)
        self.assertEqual(data['orders_booked_people'], 3)
        self.assertEqual(data['incoming_messages'], 7)
        self.assertEqual(data['conversation_to_order_conversion'], 40)
        self.assertEqual(data['goal_people'], 2)
        self.assertEqual(data['goal_gap'], 0)
        self.assertEqual(data['top_products'][0]['orders'], 4)

    def test_store_scope_excludes_other_stores(self):
        data = self.client.get('/api/dashboard_stats?period=24h&store_id=al-fatena').get_json()
        self.assertEqual(data['people_total'], 1)
        self.assertEqual(data['booked_people'], 1)
        self.assertEqual(data['orders_total'], 1)
        self.assertEqual(data['conversation_to_order_conversion'], 100)
        self.assertEqual(data['goal_gap'], 0)

    def test_all_stores_counts_conversations_per_store(self):
        data = self.client.get('/api/dashboard_stats?period=24h').get_json()
        self.assertEqual(data['people_total'], 6)
        self.assertEqual(data['booked_people'], 3)
        self.assertEqual(data['orders_total'], 5)
        self.assertEqual(data['conversation_to_order_conversion'], 50)


if __name__ == '__main__':
    unittest.main()
