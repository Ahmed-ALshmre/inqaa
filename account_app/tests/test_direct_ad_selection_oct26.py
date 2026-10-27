"""Advertised identity is selected directly; a new photo can replace it."""
import json
import unittest
from unittest.mock import patch

from account_app import app as m
from account_app.tests import test_zernio as fixtures
from account_app.tests.run_isolated import fixture_image


class DirectAdSelectionTests(unittest.TestCase):
    setUp = fixtures.ZernioTests.setUp
    tearDown = fixtures.ZernioTests.tearDown
    post = fixtures.ZernioTests.post
    sender = fixtures.ZernioTests.sender
    catalog = fixtures.ZernioTests.catalog
    incoming = fixtures.ZernioTests.incoming
    referral = fixtures.ZernioTests.referral
    process_latest = fixtures.ZernioTests.process_latest

    def historical_evidence(self, count=6, other=0):
        self.payload['metadata']['referral']['ad_id'] = 'unmapped-dress-ad'
        self.referral()
        self.incoming()
        with m.app.app_context():
            db = m.get_db()
            for index in range(count + other):
                sender = f'al-fatena::history-{index}'
                db.execute('INSERT INTO conversation_ad_context(sender_id,message_id,data) VALUES(?,?,?)',
                           (sender, 1, json.dumps({'ad_id': 'unmapped-dress-ad',
                                                  'captured_at': '2026-10-16T14:00:00+00:00'})))
                db.execute('''INSERT INTO customer_product_interests
                    (sender_id,product_id,product_name,match_method,confidence,last_seen_at,source,status)
                    VALUES(?,?,?,?,?,?,?,?)''',
                    (sender, 'F2' if index < count else 'F1', 'catalog identity',
                     'image_recognition', 100, '2026-10-16T14:05:00+00:00', 'image_recognition', 'active'))
            db.commit()

    def assert_direct(self, result):
        self.assertRegex(result['reply'], r'16 ألف|16,000')
        self.assertNotRegex(result['reply'], r'غالباً|إذا هو نفسه|هو اللي عجبج|نفس الموديل اللي تقصدينه')
        with m.app.app_context():
            db = m.get_db()
            row = db.execute("SELECT product_id,source FROM customer_product_interests WHERE sender_id=? AND status='active'",
                             (self.sender(),)).fetchone()
            self.assertEqual(tuple(row), ('F2', 'ad_ref'))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_mapped_ad_directly_answers_and_keeps_normal_followup(self):
        self.referral()
        self.incoming()
        with patch.object(m, 'saved_ad_candidate_reply') as suggest:
            self.assert_direct(self.process_latest())
        suggest.assert_not_called()
        self.incoming('شنو نوع قماشه؟')
        result = self.process_latest()
        self.assertIn('لينن', result['reply'])
        self.assertNotRegex(result['reply'], r'غالباً|نفس الموديل')

    def test_consistent_ad_history_selects_directly_without_a_photo_or_confirmation(self):
        self.historical_evidence()
        with patch.object(m, 'saved_ad_candidate_reply') as suggest, \
             patch.object(m, 'ad_creative_product') as creative:
            self.assert_direct(self.process_latest())
        suggest.assert_not_called()
        creative.assert_not_called()
        self.incoming('شكد سعره هسه؟')
        self.assert_direct(self.process_latest())

    def test_previous_confirmation_question_does_not_block_direct_selection(self):
        self.historical_evidence()
        with m.app.app_context():
            m.save_message(m.get_db(), self.sender(), 'outgoing', 'text', 'هو اللي عجبج؟', None, None, None,
                           {'ad_candidate_product_id': 'F2', 'ad_candidate_unconfirmed': True,
                            '_delivery_status': 'accepted'})
        self.incoming('شكد سعره هسه؟')
        self.assert_direct(self.process_latest())

    def test_delivery_opening_still_selects_known_ad_identity(self):
        self.historical_evidence()
        self.incoming('شلون الدفع والتوصيل؟')
        result = self.process_latest()
        self.assertIn('عند الاستلام', result['reply'])
        with m.app.app_context():
            row = m.get_db().execute("SELECT product_id FROM customer_product_interests WHERE sender_id=? AND status='active'",
                                     (self.sender(),)).fetchone()
            self.assertEqual(row[0], 'F2')

    def test_weak_or_conflicting_history_does_not_invent_an_ad_link(self):
        self.historical_evidence(count=4, other=4)
        with patch.object(m, 'ad_creative_product', return_value=(None, {}, '')):
            result = self.process_latest()
        self.assertTrue(result['meta']['ad_product_unresolved'])
        with m.app.app_context():
            self.assertFalse(m.get_db().execute('SELECT 1 FROM customer_product_interests WHERE sender_id=?',
                                               (self.sender(),)).fetchone())

    def change_by_photo(self, caption=''):
        self.referral()
        self.incoming()
        self.assert_direct(self.process_latest())
        catalog = self.catalog()
        with m.app.app_context(), patch.object(m, 'load_products_from_file', return_value=catalog), \
             patch.object(m, '_download_image_to_data_url', side_effect=fixture_image), \
             patch.object(m, 'match_customer_image_with_catalog', return_value={'product_found': True, 'product_id': 'F1'}):
            token = m._current_store_id.set('al-fatena')
            try:
                db = m.get_db()
                selected, method, result = m.match_product(db, {'sender_id': self.sender(), 'text': caption,
                    'image_url': 'https://images.test/new-model.jpg', 'ad_id': '123'}, catalog)
                self.assertEqual(selected['product_id'], 'F1')
                self.assertEqual(method, 'image_recognition')
                self.assertFalse(result.get('product_choice_pending'))
                self.assertIsNone(m.pending_product_choice(db, self.sender()))
                return {row[0] for row in db.execute("SELECT product_id FROM customer_product_interests WHERE sender_id=? AND status='active'",
                                                    (self.sender(),))}
            finally:
                m._current_store_id.reset(token)

    def test_new_photo_replaces_ad_and_followup_stays_with_new_model(self):
        self.assertEqual(self.change_by_photo(), {'F1'})
        self.incoming('شكد سعره هسه؟', referral=True)
        result = self.process_latest()
        self.assertRegex(result['reply'], r'12 ألف|12,000')
        self.assertNotRegex(result['reply'], r'16 ألف|16,000')

    def test_explicit_addition_preserves_advertised_model(self):
        self.assertEqual(self.change_by_photo('اريد هذا هم ويا الأول'), {'F1', 'F2'})

    def test_another_photo_in_ad_conversation_also_replaces_without_choice_loop(self):
        self.assertEqual(self.change_by_photo(), {'F1'})
        catalog = self.catalog()
        with m.app.app_context(), patch.object(m, 'load_products_from_file', return_value=catalog), \
             patch.object(m, '_download_image_to_data_url', side_effect=fixture_image), \
             patch.object(m, 'match_customer_image_with_catalog', return_value={'product_found': True, 'product_id': 'F2'}):
            token = m._current_store_id.set('al-fatena')
            try:
                db = m.get_db()
                selected, _, result = m.match_product(db, {'sender_id': self.sender(), 'text': '',
                    'image_url': 'https://images.test/another-model.jpg',
                    '_ad_source': {'ad_id': '123', 'resolution': 'historical'}}, catalog)
                self.assertEqual(selected['product_id'], 'F2')
                self.assertFalse(result.get('product_choice_pending'))
                self.assertIsNone(m.pending_product_choice(db, self.sender()))
                rows = db.execute("SELECT product_id FROM customer_product_interests WHERE sender_id=? AND status='active'",
                                  (self.sender(),)).fetchall()
                self.assertEqual([row[0] for row in rows], ['F2'])
            finally:
                m._current_store_id.reset(token)

    def test_sold_out_identity_is_still_resolved_without_switching_products(self):
        self.historical_evidence()
        catalog = self.catalog()
        catalog[1]['stock'] = 'نفذ'
        result = self.process_latest(catalog)
        self.assertNotIn('12 ألف', result['reply'])
        with m.app.app_context():
            row = m.get_db().execute("SELECT product_id FROM customer_product_interests WHERE sender_id=? AND status='active'",
                                     (self.sender(),)).fetchone()
            self.assertEqual(row[0], 'F2')

