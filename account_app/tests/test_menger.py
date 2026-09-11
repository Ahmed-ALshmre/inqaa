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
        for code in (401, 422, 500, 502, 503, 504):
            with self.subTest(code=code):
                self.db.execute("UPDATE menger_deliveries SET status='pending',next_attempt=0")
                self.db.commit()
                post = Mock(return_value=self.response(code))
                menger.deliver_due(self.db, post, 100)
                self.assertEqual(self.row()['status'], 'blocked' if code in (401,422) else 'retry')

    def test_other_store_is_not_sent_to_lamsa(self):
        self.db.execute("UPDATE menger_deliveries SET local_store='other'")
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
