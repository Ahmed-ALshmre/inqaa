"""Source identity, confirmed conversion and receiver status regressions."""
import json
import unittest
from unittest.mock import Mock, patch

import requests

from account_app import menger, conversion_growth
from account_app.tests import test_menger as delivery_fixtures, test_audit_orders as booking_fixtures


class SourceLinkTests(unittest.TestCase):
    setUp = delivery_fixtures.MengerTests.setUp
    tearDown = lambda self: None
    row = delivery_fixtures.MengerTests.row
    response = delivery_fixtures.MengerTests.response

    def test_golden_threads_is_telegram_only(self):
        self.db.execute('DELETE FROM menger_deliveries')
        menger.enqueue(self.db, 12, dict(self.order, store_id='golden-threads'), [])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM menger_deliveries').fetchone()[0], 0)
        self.db.execute("INSERT INTO menger_deliveries(order_id,local_store,payload,status) VALUES(99,'golden-threads','{}','review_required')")
        menger.init_db(self.db)
        self.assertEqual(self.db.execute('SELECT status FROM menger_deliveries WHERE order_id=99').fetchone()[0], 'telegram_only')

    def test_frozen_attribution_uses_real_first_contact_and_origin(self):
        self.db.executescript('''CREATE TABLE customers(sender_id TEXT,platform TEXT);
            CREATE TABLE messages(sender_id TEXT,store_id TEXT,direction TEXT,created_at TEXT);''')
        self.db.execute("INSERT INTO customers VALUES('person','instagram')")
        self.db.executemany('INSERT INTO messages VALUES(?,?,?,?)', [
            ('person','default','incoming','2026-09-11T12:00:00+03:00'),
            ('person','default','incoming','2026-09-11T08:00:00Z'),
            ('person','other','incoming','2026-01-01T00:00:00Z')])
        self.db.execute('DELETE FROM menger_deliveries')
        menger.enqueue(self.db, 12, dict(self.order, sender_id='person', _booking_actor='ai'), [])
        payload = json.loads(self.row()['payload'])
        self.assertEqual(payload['attribution']['first_contact_at'], '2026-09-11T08:00:00+00:00')
        self.assertEqual(payload['attribution']['conversation_id'], 'person')
        self.assertEqual(payload['attribution']['platform'], 'instagram')
        self.assertEqual(self.row()['booking_actor'], 'ai')
        self.assertEqual(payload['telegram_notification_owner'], 'source')
        self.db.execute("UPDATE customers SET platform='facebook'")
        menger.enqueue(self.db, 12, dict(self.order, sender_id='other', _booking_actor='staff'), [])
        self.assertEqual(json.loads(self.row()['payload'])['attribution'], payload['attribution'])

    def test_employee_name_does_not_infer_ai_for_unknown_historical_order(self):
        self.assertEqual(self.row()['booking_actor'], 'unknown')
        self.assertEqual(json.loads(self.row()['payload'])['employee_name'], 'صوف')

    def test_receiver_status_is_verified_and_throttled(self):
        menger.deliver_due(self.db, Mock(return_value=self.response()), 100)
        get = Mock(return_value=Mock(status_code=200, json=Mock(return_value=dict(
            ok=True, source='lamsa-project', external_order_id='12', order_id='remote-12',
            status='cancelled', updated_at='2026-10-20T08:00:00Z'))))
        self.assertEqual(menger.sync_statuses(self.db, get, 200), 1)
        self.assertEqual(self.row()['remote_status'], 'cancelled')
        self.assertEqual(self.row()['status'], 'sent')
        self.assertEqual(menger.sync_statuses(self.db, get, 201), 0)
        get.assert_called_once()

    def test_mismatched_source_or_order_cannot_replace_receiver_state(self):
        menger.deliver_due(self.db, Mock(return_value=self.response()), 100)
        get = Mock(return_value=Mock(status_code=200, json=Mock(return_value=dict(
            ok=True, source='other-project', external_order_id='12', order_id='remote-12', status='new'))))
        self.assertEqual(menger.sync_statuses(self.db, get, 200), 0)
        self.assertIsNone(self.row()['remote_status'])
        self.assertIn('invalid_status_response', self.row()['status_error'])

    def test_timeout_preserves_last_known_status_and_api_acknowledgement(self):
        menger.deliver_due(self.db, Mock(return_value=self.response()), 100)
        self.db.execute("UPDATE menger_deliveries SET remote_status='confirmed'")
        self.assertEqual(menger.sync_statuses(self.db, Mock(side_effect=requests.Timeout()), 200), 0)
        self.assertEqual(self.row()['remote_status'], 'confirmed')
        self.assertEqual(self.row()['status'], 'sent')

    def test_first_activation_holds_only_unlinked_legacy_backlog(self):
        self.db.execute("INSERT INTO menger_connection VALUES(1,'https://menger.example/api/v1/orders','','lamsa-project')")
        self.db.execute("INSERT INTO menger_deliveries(order_id,local_store,payload) VALUES(13,'default',?)",
            (json.dumps(dict(source='lamsa-project', external_order_id='13')),))
        self.db.execute('DELETE FROM menger_deliveries WHERE order_id=12')
        menger.enqueue(self.db, 12, dict(self.order, sender_id='person', _booking_actor='ai'), [])
        menger.save_connection(self.db, {'api_key': 'new-test-key'})
        self.assertEqual(self.db.execute('SELECT status FROM menger_deliveries WHERE order_id=13').fetchone()[0], 'review_required')
        self.assertEqual(self.db.execute('SELECT status FROM menger_deliveries WHERE order_id=12').fetchone()[0], 'pending')


class ConfirmedBookingCohortTests(unittest.TestCase):
    setUp = booking_fixtures.AuditFixTests.setUp
    tearDown = booking_fixtures.AuditFixTests.tearDown
    customer = booking_fixtures.AuditFixTests.customer

    def seed(self, sender, actor='ai', delivery='sent', remote='confirmed', store='default'):
        self.customer(sender, store)
        self.db.execute("INSERT INTO messages(sender_id,store_id,direction,message_type,text,created_at) VALUES(?,?,'incoming','text','استفسار','2026-10-19T09:00:00+03:00')", (sender,store))
        row = self.db.execute("INSERT INTO orders(sender_id,store_id,status,created_at) VALUES(?,?,'new','2026-10-19T10:00:00+03:00')", (sender,store))
        self.db.execute('''INSERT INTO menger_deliveries(order_id,local_store,payload,status,remote_order_id,remote_status,booking_actor)
            VALUES(?,?,?,?,'receiver-id',?,?)''', (row.lastrowid, store, '{}', delivery, remote, actor))
        self.db.commit()

    def report(self, **kwargs):
        return conversion_growth.cohort_metrics(self.db, '2026-10-19', '2026-10-20', booking_source='menger', **kwargs)

    def test_only_recorded_receiver_bookings_count_and_origins_stay_separate(self):
        self.seed('automated')
        self.seed('staff', actor='staff')
        self.seed('queued', delivery='pending', remote=None)
        self.seed('incomplete', remote='incomplete_data')
        self.seed('cancelled', remote='cancelled')
        self.seed('unknown-origin', actor='unknown')
        result = self.report()
        self.assertEqual((result['people'], result['buyers'], result['orders']), (6,3,3))
        self.assertEqual((result['ai_booked_people'], result['staff_booked_people']), (1,1))
        self.assertEqual(result['conversion'], 50)
        local = conversion_growth.cohort_metrics(self.db, '2026-10-19', '2026-10-20')
        self.assertEqual(local['buyers'], 6)

    def test_scope_and_remote_cancellation_do_not_rewrite_local_order(self):
        self.seed('one')
        self.seed('other', store='other')
        self.assertEqual(self.report(store_id='default')['buyers'], 1)
        self.db.execute("UPDATE menger_deliveries SET remote_status='cancelled' WHERE local_store='default'")
        self.assertEqual(self.report(store_id='default')['buyers'], 0)
        self.assertEqual(self.db.execute("SELECT status FROM orders WHERE sender_id='one'").fetchone()[0], 'new')
