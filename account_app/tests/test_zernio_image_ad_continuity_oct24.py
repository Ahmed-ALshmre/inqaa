"""A delayed, unmapped referral must not erase a verified customer photo turn."""
import unittest
from unittest.mock import patch

from account_app import app as m, zernio
from account_app.tests import test_zernio as fixtures


class ZernioImageAdContinuityOct24Tests(unittest.TestCase):
    def setUp(self):
        fixtures.ZernioTests.setUp(self)
        self.post = lambda *args, **kwargs: fixtures.ZernioTests.post(self, *args, **kwargs)
        self.assertEqual(self.post().status_code, 200)
        self.sender_id = fixtures.ZernioTests.sender(self)
        self.context = m.app.app_context()
        self.context.push()
        self.token = m._current_store_id.set('al-fatena')
        self.db = m.get_db()
        self.products = fixtures.ZernioTests.catalog(self)
        self.catalog = patch.object(m, 'load_products_from_file', return_value=self.products)
        self.catalog.start()
        self.source = 'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=synthetic-known-photo'
        self.photo_id = m.save_message(self.db, self.sender_id, 'incoming', 'image', '', self.source, None, None, {})
        self.db.execute('UPDATE messages SET created_at=? WHERE id=?', ('2026-10-24T11:22:07+03:00', self.photo_id))
        m.complete_customer_product_link(self.db, self.sender_id, self.products[0], 'image_recognition', source='image_recognition')
        self.db.execute('UPDATE customer_product_interests SET last_seen_at=? WHERE sender_id=?',
                        ('2026-10-24T11:22:51+03:00', self.sender_id))
        m.customer_image_positions(self.db, self.sender_id, [{'image_index': 1, 'image_url': self.source,
                                   'product_found': True, 'product_id': self.products[0]['product_id']}])
        self.answer_id = m.save_message(self.db, self.sender_id, 'outgoing', 'text',
                                       'هذه التنورة قماشها لينن وسعرها 12000.', None, None, None,
                                       {'_delivery_status': 'accepted'})
        self.db.execute('UPDATE messages SET created_at=? WHERE id=?', ('2026-10-24T11:22:59+03:00', self.answer_id))
        self.db.commit()

    def tearDown(self):
        self.catalog.stop()
        m._current_store_id.reset(self.token)
        self.context.pop()
        fixtures.ZernioTests.tearDown(self)

    def ad(self, identifier='synthetic-unmapped', at='2026-10-24T11:22:55+03:00'):
        m.ad_attribution.save(self.db, self.sender_id, self.answer_id,
                              {'_ad_context': {'ad_id': identifier, 'captured_at': at}})
        self.db.commit()

    def event(self, text='شتائي؟'):
        return zernio.product_event(m, self.db, dict(sender_id=self.sender_id, text=text), self.products)

    def test_unmapped_ad_captured_after_binding_but_before_photo_answer_does_not_reask_for_photo(self):
        self.ad()
        result = self.event()
        self.assertFalse(result.get('_ad_product_unresolved'))
        self.assertIsNone(result.get('ad_id'))
        self.assertEqual(m.get_active_product_binding(self.db, self.sender_id)['product_id'], self.products[0]['product_id'])

    def test_color_followup_keeps_the_photo_identity_despite_delayed_referral(self):
        self.ad()
        self.assertFalse(self.event('الوان بس اسود؟').get('_ad_product_unresolved'))

    def test_unmapped_ad_during_still_unanswered_image_turn_keeps_stronger_photo_evidence(self):
        self.db.execute('DELETE FROM messages WHERE id=?', (self.answer_id,))
        self.db.commit()
        self.ad()
        self.assertFalse(self.event('شكد سعره؟').get('_ad_product_unresolved'))

    def test_new_mapped_ad_still_selects_its_different_product(self):
        self.ad('123')
        result = self.event('شكد سعره؟')
        self.assertEqual(result.get('ad_id'), '123')
        self.assertEqual(result['_ad_source']['product_id'], self.products[1]['product_id'])

    def test_unmapped_ad_captured_after_completed_photo_turn_is_a_new_unresolved_reference(self):
        self.ad(at='2026-10-24T11:23:04+03:00')
        self.assertTrue(self.event('شكد سعره؟').get('_ad_product_unresolved'))

    def test_explicit_new_ad_request_is_not_swallowed_by_the_previous_photo_turn(self):
        self.ad()
        self.assertTrue(self.event('اقصد الاعلان الجديد شكد سعره؟').get('_ad_product_unresolved'))

    def test_explicit_named_model_correction_is_preserved(self):
        self.ad()
        result = self.event('اريد فستان دانتيل')
        self.assertFalse(result.get('_ad_product_unresolved'))
        self.assertEqual(result['_ad_source']['resolution'], 'customer_choice')
