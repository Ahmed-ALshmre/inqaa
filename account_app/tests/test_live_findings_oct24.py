"""Regressions extracted from actual provider transcripts, with safe fixtures."""
import unittest
from unittest.mock import patch
from account_app import checkout, conversation_quality, sales_strategy, sales_recovery
from account_app.tests import test_audit_orders as fixtures


class LiveFindingsPureTests(unittest.TestCase):
    def test_arrival_condition_survives_contact_and_model_assurance(self):
        history = [dict(direction='incoming', text='ثبتيه إذا يوصل باجر فقط'),
                   dict(direction='outgoing', text='أكيد يوصل باجر نثبته؟'),
                   dict(direction='incoming', text='هذا رقمي 07700000000')]
        self.assertEqual(checkout.unresolved_condition(history, 'بغداد')['kind'], 'arrival')
        self.assertIsNotNone(checkout.unresolved_condition(history, 'اي'))
        self.assertIsNone(checkout.unresolved_condition(history, 'عادي يوصل بعد يومين، ثبتي بدون شرط'))

    def test_price_question_does_not_lock_later_purchase(self):
        self.assertIsNone(checkout.unresolved_condition([dict(direction='incoming', text='شكد السعر؟')], 'اريده'))

    def test_decline_is_not_a_cancellation_or_model_correction(self):
        self.assertEqual(sales_strategy.stop_kind('تمام شكرا بس ما اريد احجز'), 'declined')
        self.assertNotEqual(sales_strategy.stop_kind('ما اريد الأسود، خلي الكريمي'), 'declined')

    def test_no_booking_suffix_does_not_block_the_photo_request(self):
        self.assertTrue(sales_recovery.simple_photo_request('ممكن أشوف صوره بدون حجز؟'))
        self.assertEqual(sales_strategy.decision_error(dict(reply='هاي الصور، ما ثبتنا طلب.', create_order=False), 'بدون حجز'), '')
        self.assertTrue(sales_strategy.decision_error(dict(reply='تم تثبيت الطلب', create_order=False), 'بدون حجز'))

    def test_redundant_booking_question_removed_without_removing_facts(self):
        reply = 'يوصل عادة خلال يومين. تحبين نثبتلج الطلب؟ شنو القياس المطلوب؟'
        cleaned = conversation_quality.one_next_question(reply)
        self.assertIn('خلال يومين', cleaned)
        self.assertIn('شنو القياس', cleaned)
        self.assertEqual(cleaned.count('؟'), 1)

    def test_booking_offer_waits_before_contact_collection(self):
        reply = 'المجموع 22 ألف. تحبين نثبته الج؟ دزيلي رقم الموبايل والمحافظة والعنوان.'
        cleaned = conversation_quality.defer_contact_until_consent(reply)
        self.assertIn('المجموع 22 ألف', cleaned)
        self.assertIn('تحبين نثبته', cleaned)
        self.assertNotIn('دزيلي', cleaned)


class LiveFindingsPipelineTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.sender = 'default::synthetic-live-findings'
        self.m._current_store_id.set('default')
        self.m.get_or_create_customer(self.db, self.sender, 'page', 'facebook')
        self.product = dict(product_id='D1', product_name='فستان الاختبار', category='فساتين',
                            price='16000', stock='متوفر', status='active', colors='اسود', sizes='38 إلى 52')
        self.cheap = dict(self.product, product_id='D2', product_name='فستان الرباط', price='15000')

    def tearDown(self):
        fixtures.AuditFixTests.tearDown(self)

    def call(self, text, history=None):
        return self.m.call_main_ai(dict(sender_id=self.sender, text=text), 'text', {}, history or [],
                                   [self.product, self.cheap], self.product, None, '', [], customer_products=[self.product])

    def test_verified_alternative_survives_original_model_price_guard(self):
        with patch.object(self.m, '_call_main_ai_once') as provider:
            result = self.call('ميزانيتي 20000 ويا التوصيل، اكو ارخص؟')
        provider.assert_not_called()
        self.assertIn('15,000', result['reply'])
        self.assertIn('20,000', result['reply'])
        self.assertIn('فستان الرباط', result['reply'])
        self.assertFalse(result['create_order'])

    def test_price_objection_answers_fabric_then_offers_real_cheaper_model(self):
        self.product['fabric'] = 'قطن'
        with patch.object(self.m, '_call_main_ai_once') as provider:
            result = self.call('غالي بس شنو قماشه؟')
        provider.assert_not_called()
        self.assertIn('قطن', result['reply'])
        self.assertIn('فستان الرباط', result['reply'])
        self.assertIn('15,000', result['reply'])
        self.assertNotIn('وزن', result['reply'])

    def test_condition_phone_does_not_advance_to_address_or_checkout(self):
        history = [dict(direction='incoming', text='ثبتيه إذا يوصل باجر فقط')]
        with patch.object(self.m, '_call_main_ai_once') as provider:
            result = self.call('هذا رقمي 07700000000', history)
        provider.assert_not_called()
        self.assertIn('رقمك وصل', result['reply'])
        self.assertIn('الشرط', result['reply'])
        self.assertNotIn('العنوان', result['reply'])
        self.assertFalse(result['create_order'])

    def test_duplicate_confirmation_after_booking_uses_saved_status(self):
        order = dict(id=1, status='new')
        ev = dict(sender_id=self.sender, text='تمام ثبتي', page_id='page', platform='facebook')
        history = [dict(direction='incoming', text='تمام ثبتي')]
        with patch.object(self.m, 'call_main_ai') as provider:
            result = self.m.handle_post_order_message(self.db, ev, {}, history, [self.product], [self.product], order)
        provider.assert_not_called()
        self.assertIn('مسجل', result['reply'])
        self.assertNotIn('جاهز', result['reply'])

    def test_child_age_correction_uses_range_without_guaranteeing_fit(self):
        product = dict(self.product, sizes='2 إلى 6 سنوات')
        token = self.m._current_store_id.set('baraah-kids')
        try:
            with patch.object(self.m, '_call_main_ai_once') as provider:
                outside = self.m.call_main_ai(dict(sender_id=self.sender,text='يناسب عمر سنة؟'), 'text', {}, [], [product], product, None, '', [])
                corrected = self.m.call_main_ai(dict(sender_id=self.sender,text='اقصد عمر سنتين'), 'text', {}, [], [product], product, None, '', [])
            provider.assert_not_called()
            self.assertIn('يبدي من عمر 2 سنوات', outside['reply'])
            self.assertNotIn('أرسلج موديلات', outside['reply'])
            self.assertIn('هالعمر موجود ضمن قياسات', corrected['reply'])
            self.assertNotIn('شنو العمر', corrected['reply'])
            self.assertNotIn('يناسبها تمام', corrected['reply'])
        finally:
            self.m._current_store_id.reset(token)


if __name__ == '__main__':
    unittest.main()
