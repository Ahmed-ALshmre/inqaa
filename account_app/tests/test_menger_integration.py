import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import account_app.app as m
from account_app import menger


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old_path = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'test.db')
        m.init_db()
        self.ctx = m.app.app_context()
        self.ctx.push()
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
            session['csrf_token'] = 'test'
        self.headers = {'X-CSRF-Token': 'test'}

    def tearDown(self):
        self.ctx.pop()
        m.DB_PATH = self.old_path
        self.temp.cleanup()

    def test_store_settings_roundtrip_and_backup_without_key(self):
        config = dict(api_url='https://menger.example/api/v1/orders', api_key='private-test-key',
                      store_id='real-id', source='lamsa-project', telegram_chat_id='-10001')
        response = self.client.put('/api/settings/menger', json=config, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('private-test-key', response.get_data(as_text=True))
        response = self.client.put('/api/stores/default', json={'menger':dict(store_id='real-id',telegram_chat_id='-10001')}, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        public = self.client.get('/api/stores').get_json()
        self.assertNotIn('private-test-key', json.dumps(public))
        stores = {s['store_id']:s for s in public['stores']}
        self.assertEqual(stores['default']['menger']['store_id'], 'real-id')
        self.assertTrue(all('api_url' not in s['menger'] for s in stores.values()))
        self.client.put('/api/settings/menger', json={'api_key':''}, headers=self.headers)
        self.assertEqual(menger.connection(m.get_db())['api_key'], 'private-test-key')
        backup = Path(self.temp.name) / 'backup.db'
        backup.write_bytes(m._backup_sqlite_file_to_bytes(m.DB_PATH).getvalue())
        self.assertNotIn(b'private-test-key', backup.read_bytes())
        self.assertEqual(menger.connection(m.get_db())['api_key'], 'private-test-key')
        bad = self.client.put('/api/settings/menger', json={'api_url':'http://bad.example'}, headers=self.headers)
        self.assertEqual(bad.status_code, 400)
        with m.app.test_request_context('/api/stores/default', method='PUT', json={'menger': config}):
            self.assertNotIn('private-test-key', json.dumps(m._safe_body_for_log()))

    def test_manual_and_ai_orders_send_complete_order_to_both(self):
        catalog = [dict(product_id='P1', product_name='فستان', price='18000', colors='', sizes='', status='active', order_name='موديل منجر 101', send_to='api'),
                   dict(product_id='P2', product_name='سوت', price='20000', colors='', sizes='', status='active', send_to='telegram')]
        items = [dict(product_id=p['product_id'], product_name=p['product_name'], quantity=1, color='أسود', size='44') for p in catalog]
        with patch.object(m, 'load_products_from_file', return_value=catalog), patch.object(m, 'send_telegram_message', return_value=True) as telegram, patch.object(m, 'save_booking_to_file'), patch.object(m, 'send_text_to_facebook', return_value=True):
            for mode in ('manual','ai'):
                sender = 'routing-' + mode
                m.get_or_create_customer(m.get_db(), sender, '', 'facebook')
                data = dict(phone='07701234567', province='بغداد', address='المنصور', items=items)
                if mode == 'manual':
                    response = self.client.post(f'/api/conversations/{sender}/create_order', json=data, headers=self.headers)
                    self.assertEqual(response.status_code, 200, response.get_json())
                else:
                    created, _ = m.create_order_if_valid(m.get_db(), sender, {'order':data}, None, cart_confirmed=True)
                    self.assertTrue(created)
                order = m.get_db().execute('SELECT * FROM orders WHERE sender_id=?', (sender,)).fetchone()
                saved = json.loads(order['order_items'])
                self.assertTrue(all('send_to' not in i for i in saved))
                payload = json.loads(m.get_db().execute('SELECT payload FROM menger_deliveries WHERE order_id=?',(order['id'],)).fetchone()[0])
                self.assertEqual([i['product_name'] for i in payload['items']], ['موديل منجر 101','سوت'])
                self.assertEqual(payload['total_price'], 43000)
                self.assertEqual(payload['employee_name'], 'صوف')
                self.assertNotIn('اسم الموظف:', payload['notes'])
                text = telegram.call_args.args[0]
                self.assertIn('سوت', text)
                self.assertIn('موديل منجر 101', text)
                self.assertNotIn('فستان', text)
                self.assertEqual(saved[0]['product_name'], 'فستان')
                self.assertEqual(saved[0]['order_name'], 'موديل منجر 101')
                self.assertIn('43000', text)

    def test_product_order_name_roundtrip(self):
        catalog_path = str(Path(self.temp.name) / 'products.json')
        with patch.object(m, 'PRODUCTS_FILE', catalog_path):
            response = self.client.post('/api/products/manage', json=dict(product_id='alias-test',product_name='اسم الزبون',order_name='اسم Menger'), headers=self.headers)
            self.assertEqual(response.status_code, 201)
            payload = self.client.get('/api/products/manage').get_json()['products']
            product = next(p for p in payload if p['product_id']=='alias-test')
            self.assertEqual(product['order_name'], 'اسم Menger')
            self.assertEqual(product['product_name'], 'اسم الزبون')
            response = self.client.put('/api/products/manage/alias-test', json={'order_name':''}, headers=self.headers)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()['product']['order_name'], '')

    def test_conversation_store_cannot_be_overridden_when_booking(self):
        m.get_or_create_customer(m.get_db(), 'own-store', '', 'facebook')
        response = self.client.post('/api/conversations/own-store/create_order?store_id=al-fatena',
                                    json={}, headers=self.headers)
        self.assertEqual(response.status_code, 409)
        token = m._current_store_id.set('al-fatena')
        try:
            created, error = m.create_order_if_valid(m.get_db(), 'own-store', {'order': {}}, None)
            self.assertFalse(created)
            self.assertIn('متجراً آخر', error)
        finally:
            m._current_store_id.reset(token)
        self.assertEqual(m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_each_store_books_into_its_own_destination(self):
        with patch.object(m, 'send_telegram_message', return_value=True), patch.object(m, 'save_booking_to_file'), patch.object(m, 'send_text_to_facebook', return_value=True):
            for store_id, remote_name in menger.REMOTE_STORE_NAMES.items():
                catalog = [dict(product_id='same-id', product_name='اسم محلي', order_name='اسم ' + store_id,
                                store_id=store_id, price='18000', status='active')]
                token = m._current_store_id.set(store_id)
                sender = 'routing-store-' + store_id
                try:
                    m.get_or_create_customer(m.get_db(), sender, '', 'facebook')
                finally:
                    m._current_store_id.reset(token)
                with patch.object(m, 'load_products_from_file', return_value=catalog):
                    response = self.client.post(f'/api/conversations/{sender}/create_order',
                        json=dict(phone='07701234567', province='بغداد', address='المنصور', notes='اتصل أولاً',
                                  items=[dict(product_id='same-id', quantity=2, color='أسود', size='44')]), headers=self.headers)
                self.assertEqual(response.status_code, 200, response.get_json())
                row = m.get_db().execute('SELECT * FROM menger_deliveries WHERE local_store=?', (store_id,)).fetchone()
                payload = json.loads(row['payload'])
                self.assertEqual(payload['store_name'], remote_name)
                self.assertEqual(payload['items'][0]['product_name'], 'اسم ' + store_id)
                self.assertEqual(payload['total_price'], 41000)
                self.assertEqual(payload['items'][0]['quantity'], 2)

    def test_remote_store_list_requires_dashboard_login(self):
        with patch.object(menger, 'remote_stores', return_value=[dict(store_id='fatena', name='ملابس الفاتنة')]) as remote:
            response = self.client.get('/api/settings/menger/stores')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()['stores'][0]['store_id'], 'fatena')
            with self.client.session_transaction() as session:
                session.clear()
            response = self.client.get('/api/settings/menger/stores')
            self.assertIn(response.status_code, (302, 401, 403))
            self.assertEqual(remote.call_count, 1)

    def test_manual_multiple_variants_paid_and_unpaid(self):
        catalog = [dict(product_id='P1', product_name='فستان', price='18000', status='active', order_name='اسم الإرسال')]
        items = [dict(product_id='P1', quantity=2, color='أسود',size='44'), dict(product_id='P1',quantity=1,color='أزرق',size='46')]
        with patch.object(m, 'load_products_from_file', return_value=catalog), patch.object(m, 'send_telegram_message', return_value=True) as telegram, patch.object(m, 'save_booking_to_file'), patch.object(m, 'send_text_to_facebook', return_value=True):
            for paid in (True, False):
                sender = 'paid-' + str(paid)
                m.get_or_create_customer(m.get_db(), sender, '', 'facebook')
                data = dict(phone='07701234567',province='بغداد',address='المنصور',items=items,is_paid=paid,notes='اتصل قبل الوصول')
                response = self.client.post(f'/api/conversations/{sender}/create_order',json=data,headers=self.headers)
                self.assertEqual(response.status_code,200,response.get_json())
                order = m.get_db().execute('SELECT * FROM orders WHERE sender_id=?',(sender,)).fetchone()
                self.assertEqual(order['is_paid'],int(paid))
                self.assertEqual(order['total_amount'],59000)
                self.assertEqual([i['quantity'] for i in json.loads(order['order_items'])],[2,1])
                payload = json.loads(m.get_db().execute('SELECT payload FROM menger_deliveries WHERE order_id=?',(order['id'],)).fetchone()[0])
                self.assertEqual(len(payload['items']),2)
                self.assertEqual(payload['total_price'],0 if paid else 59000)
                self.assertEqual(payload['total_amount'],0 if paid else 59000)
                self.assertEqual(payload['items'][0]['unit_price'],18000)
                self.assertEqual('مدفوع بالكامل' in order['notes'],paid)
                self.assertIn('اتصل قبل الوصول', payload['notes'])
                expected_amount = '0 مع التوصيل' if paid else '59000 مع التوصيل'
                self.assertIn(expected_amount, telegram.call_args.args[0].splitlines())
                self.assertIn(expected_amount, m.format_order_for_telegram(dict(order)).splitlines())
                self.assertEqual('مدفوع بالكامل' in payload['notes'],paid)
                self.assertEqual('لا يُحصّل' in telegram.call_args.args[0],paid)
                self.assertEqual('لا يُحصّل' in m.format_order_for_telegram(dict(order)),paid)
                invalid = self.client.post(f'/api/conversations/{sender}/create_order',json=dict(data,is_paid='false'),headers=self.headers)
                self.assertEqual(invalid.status_code,400)
