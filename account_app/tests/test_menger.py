import json
import sqlite3
import unittest
from unittest.mock import Mock, patch

import requests
from account_app import menger


class MengerTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        menger.init_db(self.db)
        self.env = patch.dict('os.environ', {
            'MENGER_ORDERS_API_URL': 'https://menger.example/api/v1/orders',
            'MENGER_ORDERS_API_KEY': 'test-secret', 'MENGER_STORE_ID': 'store-1',
            'MENGER_STORE_MAP': '{}'})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.addCleanup(self.db.close)
        self.order = dict(created_at='2026-09-11T18:30:00+03:00', phone='07800000000',
                          province='بغداد', address='المنصور', product_total=36000,
                          items=[dict(product_id='p1', product_name='فستان', quantity=1, send_to='api',
                                      color=color, size=size) for color, size in [('أسود','44'), ('جوزي','46')]])
        menger.enqueue(self.db, 12, self.order, [dict(product_id='p1', price=18000)])
        self.db.commit()

    def row(self):
        return self.db.execute('SELECT * FROM menger_deliveries').fetchone()

    def response(self, code=201, duplicate=False):
        return Mock(status_code=code, json=Mock(return_value=dict(ok=True, order_id='remote-12', duplicate=duplicate)))

    def test_payload_and_duplicate_success(self):
        post = Mock(return_value=self.response(200, True))
        menger.deliver_due(self.db, post, 100)
        payload = post.call_args.kwargs['json']
        self.assertEqual(payload['external_order_id'], '12')
        self.assertEqual(payload['store_id'], 'store-1')
        self.assertEqual(len(payload['items']), 2)
        self.assertEqual(sum(i['quantity'] * i['unit_price'] for i in payload['items']), 36000)
        self.assertEqual(self.row()['remote_order_id'], 'remote-12')
        menger.deliver_due(self.db, post, 1000)
        self.assertEqual(post.call_count, 1)

    def test_timeout_retries_same_frozen_payload_and_destination(self):
        post = Mock(side_effect=[requests.Timeout(), self.response()])
        menger.deliver_due(self.db, post, 100)
        self.assertEqual(self.row()['status'], 'retry')
        menger.deliver_due(self.db, post, 120)
        self.assertEqual(post.call_count, 1)
        with patch.dict('os.environ', {'MENGER_STORE_ID': 'different-store'}):
            menger.deliver_due(self.db, post, 161)
        self.assertEqual(post.call_args_list[0].kwargs['json'], post.call_args_list[1].kwargs['json'])
        self.assertEqual(self.row()['status'], 'sent')

    def test_http_errors(self):
        for code in (401, 422, 500, 501, 502, 503, 504, 507):
            with self.subTest(code=code):
                self.db.execute("UPDATE menger_deliveries SET status='pending',next_attempt=0")
                self.db.commit()
                post = Mock(return_value=self.response(code))
                menger.deliver_due(self.db, post, 100)
                self.assertEqual(self.row()['status'], 'blocked' if code in (401,422) else 'retry')

    def test_other_store_is_not_sent_to_lamsa(self):
        self.db.execute('DELETE FROM menger_deliveries')
        menger.enqueue(self.db, 12, dict(self.order, store_id='other'), [])
        self.db.commit()
        post = Mock()
        menger.deliver_due(self.db, post, 100)
        post.assert_not_called()

    def test_queue_rolls_back_with_order_transaction(self):
        menger.enqueue(self.db, 13, self.order, [])
        self.db.rollback()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM menger_deliveries').fetchone()[0], 1)

    def test_missing_config_keeps_pending(self):
        with patch.dict('os.environ', {'MENGER_ORDERS_API_URL': ''}):
            post = Mock()
            menger.deliver_due(self.db, post, 100)
        post.assert_not_called()
        self.assertEqual(self.row()['status'], 'pending')

    def test_shared_connection_with_different_store_ids(self):
        menger.save_connection(self.db, dict(api_url='https://own.example/api/v1/orders', api_key='own-secret', source='shared-source'))
        menger.save_connection(self.db, dict(api_key=''))
        menger.save_settings(self.db, 'default', dict(store_id='remote-one'))
        menger.save_settings(self.db, 'second', dict(store_id='remote-two'))
        menger.enqueue(self.db, 13, dict(self.order, store_id='second'), [])
        self.db.commit()
        self.assertNotIn('api_key', menger.connection(self.db, public=True))
        post = Mock(return_value=self.response())
        menger.deliver_due(self.db, post, 100)
        self.assertEqual(post.call_count, 2)
        calls = post.call_args_list
        self.assertEqual([c.kwargs['json']['store_id'] for c in calls], ['remote-one','remote-two'])
        for call in calls:
            self.assertEqual(call.args[0], 'https://own.example/api/v1/orders')
            self.assertEqual(call.kwargs['headers']['Authorization'], 'Bearer own-secret')
            self.assertEqual(len(call.kwargs['json']['items']), 2)

    def test_old_product_routing_is_ignored(self):
        self.db.execute('DELETE FROM menger_deliveries')
        self.order['items'][1]['product_id'] = 'p2'
        catalog = [dict(product_id='p1', price=18000, send_to='api'), dict(product_id='p2', price=18000, send_to='telegram')]
        menger.snapshot_prices(self.order, catalog)
        menger.enqueue(self.db, 12, self.order, catalog)
        payload = json.loads(self.row()['payload'])
        self.assertEqual(len(payload['items']), 2)
        self.assertEqual(payload['total_price'], 36000)
        self.assertTrue(all('send_to' not in i for i in self.order['items']))

    def test_cleared_global_key_does_not_fall_back_to_environment(self):
        menger.save_connection(self.db, dict(api_url='https://own.example/api/v1/orders', clear_api_key=True))
        self.db.commit()
        post = Mock()
        menger.deliver_due(self.db, post, 100)
        post.assert_not_called()

    def test_migration_keeps_unique_old_connection(self):
        self.db.execute("INSERT INTO menger_store_settings(local_store,api_url,api_key,store_id) VALUES('legacy','https://old.example/api/v1/orders','legacy-key','legacy-store')")
        menger.init_db(self.db)
        self.assertEqual(menger.connection(self.db)['api_key'], 'legacy-key')
        self.assertEqual(menger.settings(self.db, 'legacy')['store_id'], 'legacy-store')

    def test_sync_stores_matches_each_store_by_name(self):
        self.db.execute('CREATE TABLE stores(store_id TEXT PRIMARY KEY,name TEXT,active INTEGER)')
        self.db.executemany('INSERT INTO stores VALUES(?,?,1)', [
            ('lamsa', 'لمسة ستور'), ('kids', 'عالم البراءة لملابس الأطفال'),
            ('missing', 'متجر غير موجود'),
        ])
        response = Mock(status_code=200, json=Mock(return_value={
            'ok': True,
            'stores': [
                {'store_id': 'remote-lamsa', 'name': 'لمسة ستور'},
                {'store_id': 'remote-kids', 'name': 'عالم البراءة لملابس الأطفال'},
            ],
        }))
        get = Mock(return_value=response)

        result = menger.sync_stores(self.db, get=get)

        self.assertEqual(len(result['matched']), 2)
        self.assertEqual(result['unmatched'], ['متجر غير موجود'])
        self.assertEqual(menger.settings(self.db, 'lamsa')['store_id'], 'remote-lamsa')
        self.assertEqual(menger.settings(self.db, 'kids')['store_id'], 'remote-kids')
        self.assertEqual(menger.settings(self.db, 'missing')['store_id'], '')
        self.assertEqual(get.call_args.args[0], 'https://menger.example/api/v1/stores')
        self.assertNotIn('test-secret', str(result))

    def test_order_name_frozen_for_retry_and_customer_name_preserved(self):
        self.db.execute('DELETE FROM menger_deliveries')
        catalog = [dict(product_id='p1', price=18000, order_name='اسم منجر')]
        self.order['items'][0]['order_name'] = 'اسم غير موثوق من الطلب'
        menger.snapshot_prices(self.order, catalog)
        menger.enqueue(self.db, 12, self.order, catalog)
        self.db.commit()
        catalog[0]['order_name'] = 'اسم مختلف لاحقاً'
        post = Mock(side_effect=[requests.Timeout(), self.response()])
        menger.deliver_due(self.db, post, 100)
        menger.deliver_due(self.db, post, 161)
        for call in post.call_args_list:
            self.assertEqual(call.kwargs['json']['items'][0]['product_name'], 'اسم منجر')
        self.assertEqual(self.order['items'][0]['product_name'], 'فستان')

    def test_empty_order_name_falls_back_to_current_name(self):
        for alias in ('', '   ', None):
            menger.snapshot_prices(self.order, [dict(product_id='p1', order_name=alias)])
            self.assertEqual(self.order['items'][0]['order_name'], 'فستان')

    def test_final_price_includes_shipping_once_and_preserves_notes(self):
        self.db.execute('DELETE FROM menger_deliveries')
        order = dict(self.order, delivery_fee=5000, total_amount=41000, notes='اتصل قبل الوصول')
        order['items'][0]['notes'] = 'تغليف منفصل'
        menger.enqueue(self.db, 12, order, [])
        payload = json.loads(self.row()['payload'])
        self.assertEqual(payload['total_price'], 41000)
        self.assertEqual(payload['total_amount'], 41000)
        self.assertNotIn('delivery_fee', payload)
        self.assertEqual(payload['employee_name'], 'صوف')
        self.assertNotIn('اسم الموظف:', payload['notes'])
        self.assertIn('اتصل قبل الوصول', payload['notes'])
        self.assertIn('تغليف منفصل', payload['notes'])
        self.assertEqual([(i['color'], i['size'], i['quantity']) for i in payload['items']],
                         [('أسود', '44', 1), ('جوزي', '46', 1)])

    def test_free_shipping_discount_and_paid_totals(self):
        for paid, total in [(False, 36000), (False, 33000), (True, 41000)]:
            with self.subTest(paid=paid, total=total):
                self.db.execute('DELETE FROM menger_deliveries')
                menger.enqueue(self.db, 12, dict(self.order, is_paid=paid, total_amount=total), [])
                payload = json.loads(self.row()['payload'])
                expected = 0 if paid else total
                self.assertEqual(payload['total_price'], expected)
                # Actual receiver expression: an explicit paid zero must survive.
                self.assertEqual(payload.get('total_price') or payload.get('total_amount'), expected)
                self.assertEqual(payload['items'][0]['unit_price'], 18000)

    def test_shipping_fallback_for_older_orders(self):
        self.db.execute('DELETE FROM menger_deliveries')
        menger.enqueue(self.db, 12, dict(self.order, delivery_fee=5000), [])
        self.assertEqual(json.loads(self.row()['payload'])['total_price'], 41000)

    def test_all_known_stores_have_separate_receiver_names(self):
        self.db.execute('DELETE FROM menger_deliveries')
        for index, store in enumerate(menger.REMOTE_STORE_NAMES, 1):
            menger.enqueue(self.db, index, dict(self.order, store_id=store), [])
        self.db.commit()
        post = Mock(return_value=self.response())
        with patch.dict('os.environ', {'MENGER_STORE_ID': ''}):
            menger.deliver_due(self.db, post, 100)
        self.assertEqual([c.kwargs['json']['store_name'] for c in post.call_args_list],
                         list(menger.REMOTE_STORE_NAMES.values()))

    def test_retry_keeps_name_destination_after_mapping_changes(self):
        self.db.execute('DELETE FROM menger_deliveries')
        menger.enqueue(self.db, 12, dict(self.order, store_id='al-fatena'), [])
        self.db.commit()
        post = Mock(side_effect=[requests.Timeout(), self.response()])
        menger.deliver_due(self.db, post, 100)
        menger.save_settings(self.db, 'al-fatena', dict(store_id='changed', store_name='متجر مختلف'))
        self.db.commit()
        menger.deliver_due(self.db, post, 161)
        self.assertEqual(post.call_args_list[0].kwargs['json'], post.call_args_list[1].kwargs['json'])
        self.assertEqual(post.call_args.kwargs['json']['store_name'], 'ملابس الفاتنة')

    def test_duplicate_store_destination_is_rejected(self):
        menger.save_settings(self.db, 'default', dict(store_id='one'))
        with self.assertRaises(ValueError):
            menger.save_settings(self.db, 'al-fatena', dict(store_id='one'))

    def test_cross_store_product_is_rejected(self):
        with self.assertRaises(ValueError):
            menger.enqueue(self.db, 13, self.order, [dict(product_id='p1', store_id='al-fatena')])

    def test_remote_stores_is_read_only_and_does_not_expose_key(self):
        response = Mock(status_code=200, json=Mock(return_value=dict(ok=True, stores=[
            dict(store_id='fatena', name='ملابس الفاتنة', private='ignored')])))
        with patch.object(menger.requests, 'get', return_value=response) as get:
            result = menger.remote_stores(self.db)
        self.assertEqual(result, [dict(store_id='fatena', name='ملابس الفاتنة')])
        self.assertEqual(get.call_args.args[0], 'https://menger.example/api/v1/stores')
        self.assertFalse(get.call_args.kwargs['allow_redirects'])

    def test_connection_accepts_site_root_and_rejects_dashboard(self):
        menger.save_connection(self.db, dict(api_url='https://menger.example/'))
        self.assertEqual(menger.connection(self.db)['api_url'], 'https://menger.example/api/v1/orders')
        with self.assertRaises(ValueError):
            menger.save_connection(self.db, dict(api_url='https://menger.example/dashboard'))

    def test_product_rejection_records_explanation_without_rerouting(self):
        response = Mock(status_code=422, json=Mock(return_value=dict(
            error='لم يتم حفظ الطلب', details=['المنتج غير موجود داخل هذا المتجر'])))
        post = Mock(return_value=response)
        menger.deliver_due(self.db, post, 100)
        self.assertEqual(self.row()['status'], 'blocked')
        self.assertIn('المنتج غير موجود', self.row()['last_error'])
        menger.deliver_due(self.db, post, 1000)
        self.assertEqual(post.call_count, 1)

    def test_documented_base_url_is_supported(self):
        with patch.dict('os.environ', {'MENGER_ORDERS_API_URL': '', 'MENGER_BASE_URL': 'https://receiver.test/'}):
            self.assertEqual(menger.connection(self.db)['api_url'], 'https://receiver.test/api/v1/orders')

    def test_employee_is_separate_from_empty_customer_notes(self):
        payload = json.loads(self.row()['payload'])
        self.assertEqual(payload['employee_name'], 'صوف')
        self.assertEqual(payload['notes'], '')
        self.db.commit()
        post = Mock(return_value=self.response())
        menger.deliver_due(self.db, post, 100)
        self.assertEqual(post.call_args.kwargs['headers']['Accept'], 'application/json')
