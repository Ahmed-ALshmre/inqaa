"""Anonymized legacy-path regression: contact follows an intervening ETA answer."""
import json
import unittest
from unittest.mock import patch

from account_app.tests import test_checkout_completion_oct25 as fixture
from account_app import checkout, sales_playbook


class ChoiceConsentTests(unittest.TestCase):
    def test_variant_purchase_survives_a_delivery_question_and_staff_answer(self):
        context = dict(evidence=[dict(text='اريد لون اسود قياس ٤٦'), dict(text='تمام')])
        self.assertTrue(sales_playbook.checkout_consent(context))

    def test_stock_price_photos_conditions_and_hesitation_are_not_purchase_consent(self):
        for text in ('اريد اعرف قياس ٤٦ موجود؟', 'اريد صورة الاسود قياس 46',
                     'اريد لون اسود لو ازرق', 'اريد قياس 46 بس خلي افكر',
                     'اريد لون اسود اذا التوصيل مجاني', 'اريد اعرف سعر الاسود'):
            with self.subTest(text=text):
                self.assertFalse(sales_playbook.checkout_consent(dict(evidence=[dict(text=text)])))
        self.assertFalse(sales_playbook.checkout_consent(dict(evidence=[
            dict(text='اريد اسود قياس 46'), dict(text='لا ماريد')])))

    def test_dora_agricultural_address_is_not_a_generic_training_course(self):
        self.assertEqual('بغداد', checkout.contact_fields('العنوان دورة زراعي قرب مدرسة الاختبار')['province'])
        self.assertNotIn('province', checkout.contact_fields('عندي دورة تدريبية'))


class LegacyContactCheckoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.CheckoutCompletionTests.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        fixture.CheckoutCompletionTests.tearDownClass.__func__(cls)

    setUp = fixture.CheckoutCompletionTests.setUp
    tearDown = fixture.CheckoutCompletionTests.tearDown
    incoming = fixture.CheckoutCompletionTests.incoming
    assert_confirmation = fixture.CheckoutCompletionTests.assert_confirmation

    def seed_selection(self):
        self.product = fixture.CATALOG[0]
        self.m.bind_customer_to_product(self.db, self.sender, self.product, source='image_recognition')
        self.incoming('السلام عليكم موجود قياس ٤٦ لون اسود')
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'قياسج موجود عيوني.')
        self.incoming('اريد لون اسود قياس ٤٦')
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'متوفر عيوني والج حق الفحص.')
        self.incoming('تمام')
        self.incoming('شوكت توصل')
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'باجر', raw_payload={'sender_type':'staff'})
        self.incoming('تمام')

    def process(self, text):
        self.ev['text'] = text
        stale = dict(reply='التوصيل خلال 24 ساعة إلى يومين. والج حق الفحص.', create_order=False, order={})
        with patch.object(self.m, 'extract_facebook_event', return_value=self.ev), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_store_feature_enabled', return_value=False), \
             patch.object(self.m, '_call_main_ai_once', return_value=stale) as model:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        return result, model

    def test_dora_without_article_and_arabic_phone_complete_selected_checkout(self):
        self.seed_selection()
        result, model = self.process('العنوان دورة زراعي قرب مدرسة الاختبار الهاتف٠٧٧٠١٢٣٤٥٦٧')
        model.assert_not_called()
        self.assert_confirmation(result)
        row = self.db.execute('SELECT province,phone,order_items FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual(('بغداد','07701234567'), tuple(row)[:2])
        self.assertEqual('46', json.loads(row['order_items'])[0]['size'])

    def test_unknown_province_asks_only_province_instead_of_repeating_eta(self):
        self.seed_selection()
        result, model = self.process('العنوان حي الاختبار قرب المدرسة الهاتف٠٧٧٠١٢٣٤٥٦٧')
        model.assert_not_called()
        self.assertIn('المحافظة', result['reply'])
        self.assertNotIn('24', result['reply'])
        self.assertNotIn('رقم', result['reply'])
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0])
        result, model = self.process('بغداد')
        model.assert_not_called()
        self.assert_confirmation(result)
