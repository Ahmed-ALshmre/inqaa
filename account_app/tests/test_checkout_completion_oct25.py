"""Contact completion must save once and deliver one whole confirmation."""
import json
import unittest
from unittest.mock import patch

from account_app.checkout import contact_fields
from account_app.tests import test_checkout_regressions as fixtures

CATALOG = fixtures.CATALOG


class CheckoutCompletionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures.CheckoutRegressionTests.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        fixtures.CheckoutRegressionTests.tearDownClass.__func__(cls)

    setUp = fixtures.CheckoutRegressionTests.setUp
    tearDown = fixtures.CheckoutRegressionTests.tearDown
    data = fixtures.CheckoutRegressionTests.data
    incoming = fixtures.CheckoutRegressionTests.incoming

    def link_cart(self, text='دزيلي رقم الموبايل والعنوان', draft=True):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source='image_recognition')
        payload = {'checkout_draft': {'items': self.data()['items']}} if draft else {}
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', text, None, None, None, payload)

    def process(self, text):
        self.ev['text'] = text
        generated = {'reply': 'دزيلي رقم الموبايل والعنوان', 'create_order': False, 'order': {}}
        with patch.object(self.m, 'extract_facebook_event', return_value=self.ev), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_store_feature_enabled', return_value=False), \
             patch.object(self.m, '_call_main_ai_once', return_value=generated) as model:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        return result, model

    def assert_confirmation(self, result):
        self.assertEqual(result['reply'], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(result['reply_parts'], [self.m.DEFAULT_ORDER_CONFIRMATION_TEXT])
        self.assertTrue(result['_single_message'])
        self.assertFalse(result.get('send_image'))
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?',
                                        (self.sender,)).fetchone()[0], 1)

    def test_split_contact_collects_only_missing_then_books_without_model(self):
        self.link_cart()
        first, model = self.process('07701234567')
        model.assert_not_called()
        self.assertNotIn('رقم الموبايل', first['reply'])
        self.assertIn('المحافظة', first['reply'])
        self.assertIn('العنوان', first['reply'])
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)
        second, model = self.process('البصرة الجزائر قرب المدرسة')
        model.assert_not_called()
        self.assert_confirmation(second)
        order = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual(order['phone'], '07701234567')
        self.assertEqual(order['province'], 'البصرة')
        self.assertIn('الجزائر', order['address'])

    def test_full_details_book_selected_size_without_old_model_cart(self):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source='image_recognition')
        self.incoming('قياسي 44')
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'قياس 44 متوفر بالأسود. أثبتلج حجز؟', None, None, None, {})
        result, model = self.process('07701234567 بغداد حي اور قرب المدرسة')
        model.assert_not_called()
        self.assert_confirmation(result)
        saved = json.loads(self.db.execute('SELECT order_items FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0])
        self.assertEqual(saved[0]['size'], '44')

    def test_address_without_repeating_province_completes_saved_draft(self):
        self.link_cart()
        first, model = self.process('07701234567 البصرة')
        model.assert_not_called()
        self.assertIn('العنوان', first['reply'])
        self.assertNotIn('المحافظة', first['reply'])
        self.assertNotIn('رقم الموبايل', first['reply'])
        second, model = self.process('الجزائر قرب المدرسة')
        model.assert_not_called()
        self.assert_confirmation(second)

    def test_no_saved_cart_never_guesses_quantity_from_one_linked_product(self):
        self.link_cart(draft=False)
        candidate = self.m.resume_known_checkout(self.db,
            dict(self.ev, text='07701234567 بغداد حي اور'), CATALOG[0], {}, [CATALOG[0]],
            {'evidence': [{'text': 'قياسي 44'}, {'text': 'اريد قطعتين'}]})
        self.assertIsNone(candidate)
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)

    def test_old_size_from_another_product_does_not_create_a_new_cart(self):
        self.link_cart(draft=False)
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[1], source='image_recognition')
        candidate = self.m.resume_known_checkout(self.db,
            dict(self.ev, text='07701234567 بغداد حي اور'), CATALOG[1], {}, [CATALOG[1]],
            {'evidence': [{'text': 'قياسي 44'}]})
        self.assertIsNone(candidate)

    def test_newly_named_product_with_contact_does_not_resume_old_cart(self):
        self.link_cart()
        candidate = self.m.resume_known_checkout(self.db,
            dict(self.ev, text='احجزي سوت ملكي قياس 44 اسود 07701234567 بغداد حي اور'),
            CATALOG[0], {}, [CATALOG[0]], {'evidence': [{'text': 'قياسي 44'}]})
        self.assertIsNone(candidate)

    def test_contact_after_address_only_request_is_checkout_and_one_delivery(self):
        self.link_cart('باقي العنوان الكامل')
        self.m.capture_customer_contact(self.db, self.sender, '07701234567')
        result, model = self.process('بغداد حي اور قرب المدرسة')
        model.assert_not_called()
        self.assert_confirmation(result)
        with patch.object(self.m, 'send_text_to_facebook', return_value=True) as text_send, \
             patch.object(self.m, 'send_image_to_facebook', return_value=True) as image_send:
            self.m.send_webhook_result_to_facebook(result)
        text_send.assert_called_once()
        self.assertIn(self.m.DEFAULT_ORDER_CONFIRMATION_TEXT, text_send.call_args.args)
        image_send.assert_not_called()

    def test_existing_incoming_details_are_recovered_without_reasking(self):
        self.link_cart()
        self.incoming('07701234567')
        self.incoming('البصرة الجزائر قرب المدرسة')
        candidate = {'order': {'items': self.data()['items']}}
        created, reply = self.m.create_order_if_valid(self.db, self.sender, candidate, CATALOG[0])
        self.assertTrue(created)
        self.assertEqual(reply, self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.m.approved_reply_parts(candidate, reply), [reply])
        duplicate, reply = self.m.create_order_if_valid(self.db, self.sender, candidate, CATALOG[0])
        self.assertTrue(duplicate)
        self.assertIsNone(reply)
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 1)

    def test_complete_contact_does_not_bypass_refusal_or_conditions(self):
        self.link_cart()
        for text in ['لا تثبتي الحجز 07701234567 بغداد حي اور',
                     'اذا التوصيل مجاني احجزي 07701234567 بغداد حي اور',
                     '07701234567 بغداد حي اور شكد المجموع مع التوصيل؟']:
            with self.subTest(text=text):
                self.assertFalse(self.m.checkout_requested(self.db, dict(self.ev, text=text), {'order': self.data()}))
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)

    def test_address_survives_question_after_contact_details(self):
        fields = contact_fields('07701234567 بغداد حي اور وشوكت يوصل؟')
        self.assertEqual(fields['phone'], '07701234567')
        self.assertEqual(fields['province'], 'بغداد')
        self.assertIn('حي اور', fields['address'])


if __name__ == '__main__':
    unittest.main()
