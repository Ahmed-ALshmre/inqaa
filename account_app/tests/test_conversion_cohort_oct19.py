"""Booking conversion must never improve merely by changing its denominator."""
import socket
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from account_app import conversion_growth as growth


class ConversionCohortTests(unittest.TestCase):
    START = '2026-10-13T00:00:00+03:00'
    END = '2026-10-14T00:00:00+03:00'

    def setUp(self):
        # Only synthetic fixtures are used; any accidental network call fails.
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden in cohort tests'))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.db = sqlite3.connect(Path(self.folder.name) / 'cohort.sqlite3')
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.db.executescript('''
            CREATE TABLE messages (sender_id TEXT, store_id TEXT, direction TEXT, created_at TEXT);
            CREATE TABLE orders (sender_id TEXT, store_id TEXT, status TEXT, created_at TEXT);
            CREATE TABLE customers (sender_id TEXT,store_id TEXT,name TEXT,lead_score INTEGER,phone TEXT,address TEXT);
            CREATE TABLE followups (sender_id TEXT,status TEXT,created_at TEXT,sent_at TEXT);
            CREATE TABLE human_reviews (sender_id TEXT,status TEXT,created_at TEXT);
        ''')

    def contact(self, sender, at='2026-10-13T10:00:00+03:00', store='default', direction='incoming'):
        self.db.execute('INSERT INTO messages VALUES(?,?,?,?)', (sender, store, direction, at))

    def order(self, sender, at='2026-10-13T11:00:00+03:00', store='default', status='new'):
        self.db.execute('INSERT INTO orders VALUES(?,?,?,?)', (sender, store, status, at))

    def report(self, **kwargs):
        return growth.cohort_metrics(self.db, self.START, self.END, **kwargs)

    def test_new_contacts_repeat_orders_returning_and_manual_buyers_stay_distinct(self):
        self.contact('new-buyer')
        self.contact('new-buyer', at='2026-10-13T10:01:00+03:00')
        self.contact('new-browse')
        self.contact('returning', at='2026-10-12T12:00:00+03:00')
        self.contact('returning')
        self.order('new-buyer')
        self.order('new-buyer', at='2026-10-13T12:00:00+03:00')
        self.order('returning')
        self.order('manual-without-incoming')
        result = self.report()
        self.assertEqual((result['people'], result['buyers'], result['conversion']), (2, 1, 50))
        self.assertEqual((result['active_contacts'], result['active_buyers']), (3, 2))
        self.assertEqual((result['orders'], result['orders_booked_people']), (4, 3))

    def test_cancelled_orders_do_not_count_but_a_later_valid_booking_does(self):
        for sender, status in [('one', 'cancelled'), ('two', ' CANCELED '), ('three', None)]:
            self.contact(sender)
            self.order(sender, status=status)
        self.order('one', status='new')
        result = self.report()
        self.assertEqual((result['people'], result['buyers'], result['orders']), (3, 2, 2))
        self.assertEqual(result['conversion'], 66.67)

    def test_person_identity_includes_store_and_legacy_default_store_aliases(self):
        self.contact('same', store=None)
        self.contact('same', store='')
        self.contact('same', store='default')
        self.contact('same', store='khuyoot')
        self.order('same', store=None)
        self.order('same', store='khuyoot')
        self.assertEqual((self.report()['people'], self.report()['buyers']), (2, 2))
        default = self.report(store_id='default')
        self.assertEqual((default['people'], default['buyers'], default['orders']), (1, 1, 1))
        self.assertEqual(self.report(store_id='khuyoot')['people'], 1)
        self.assertEqual(self.report(store_id='missing')['people'], 0)

    def test_order_must_follow_contact_in_same_store_and_period(self):
        self.contact('order-before-contact')
        self.order('order-before-contact', at='2026-10-13T09:59:00+03:00')
        self.contact('later-order')
        self.order('later-order', at='2026-10-14T09:00:00+03:00')
        self.contact('wrong-store')
        self.order('wrong-store', store='khuyoot')
        result = self.report(store_id='default')
        self.assertEqual((result['people'], result['buyers']), (3, 0))
        self.assertEqual(result['orders'], 1)

    def test_period_is_half_open_and_contact_at_start_is_included(self):
        self.contact('at-start', at=self.START)
        self.order('at-start', at=self.START)
        self.contact('at-end', at=self.END)
        self.order('at-end', at=self.END)
        self.contact('booking-at-end')
        self.order('booking-at-end', at=self.END)
        result = self.report()
        self.assertEqual((result['people'], result['buyers'], result['orders']), (2, 1, 1))
        self.assertTrue(result['end_exclusive'])

    def test_offsets_and_historical_naive_timestamps_refer_to_real_instants(self):
        self.contact('utc-start', at='2026-10-12T21:00:00Z')
        self.order('utc-start', at='2026-10-13T00:00:00')  # Baghdad legacy timestamp.
        self.contact('utc-end', at='2026-10-13T21:00:00Z')
        self.order('utc-end', at=self.END)
        self.contact('before-period', at='2026-10-12T23:59:00')
        self.contact('before-period', at='2026-10-13T05:00:00+03:00')
        self.order('before-period')
        result = self.report()
        self.assertEqual((result['people'], result['buyers']), (1, 1))
        self.assertEqual(result['active_contacts'], 2)

    def test_earliest_contact_is_chronological_instead_of_lexicographic(self):
        self.contact('returning', at='2026-10-13T00:30:00+03:00')
        self.contact('returning', at='2026-10-12T23:30:00Z')
        self.order('returning')
        result = growth.cohort_metrics(self.db, '2026-10-13T01:00:00+03:00', self.END)
        self.assertEqual((result['people'], result['buyers']), (0, 0))
        self.assertEqual((result['active_contacts'], result['active_buyers']), (1, 1))

    def test_no_bounds_and_invalid_or_empty_records_do_not_invent_contacts(self):
        self.contact('real', at='2026-10-01T12:00:00')
        self.order('real')
        self.contact('', at=self.START)
        self.contact('   ', at=self.START)
        self.contact('broken', at='invalid')
        self.contact('outgoing-only', direction='outgoing')
        self.order('manual-only')
        self.order('', at='invalid')
        result = growth.cohort_metrics(self.db)
        self.assertEqual((result['people'], result['buyers'], result['orders']), (1, 1, 2))
        self.assertEqual(result['store_id'], 'all')

    def test_target_changes_goal_only_and_reporting_does_not_modify_data(self):
        for number in range(7):
            self.contact(f'person-{number}')
        self.order('person-0')
        before = self.db.total_changes
        low, high = self.report(target=25), self.report(target=30)
        self.assertEqual(self.db.total_changes, before)
        self.assertEqual((low['people'], low['buyers'], low['conversion']),
                         (high['people'], high['buyers'], high['conversion']))
        self.assertEqual((low['target_buyers'], high['target_buyers']), (2, 3))
        self.assertEqual((low['additional_buyers'], high['additional_buyers']), (1, 2))

    def test_invalid_period_and_target_fail_explicitly(self):
        for start, end in [('bad', self.END), (self.START, 'bad'), (self.END, self.START), (self.START, self.START)]:
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                growth.cohort_metrics(self.db, start, end)
        for target in [-1, 101, float('nan'), float('inf'), 'bad']:
            with self.subTest(target=target), self.assertRaises(ValueError):
                self.report(target=target)

    def test_growth_view_uses_identical_cohort_and_retains_operational_keys(self):
        self.contact('returning', at='2026-10-12T12:00:00+03:00')
        self.contact('returning')
        self.order('returning')
        self.contact('new')
        self.order('new')
        self.db.execute("INSERT INTO customers VALUES('new','default','Synthetic',80,'','')")
        self.db.execute("INSERT INTO followups VALUES('new','sent',?,?)", (self.START, self.START))
        result = growth.metrics(self.db, 'default', self.END)
        self.assertEqual((result['people'], result['buyers']), (1, 1))
        self.assertEqual((result['active_contacts'], result['orders']), (2, 2))
        self.assertEqual(result['followups'], {'sent': 1})
        self.assertEqual(result['urgent'], [])
        self.assertEqual(result['buyers_after_followup'], 1)
