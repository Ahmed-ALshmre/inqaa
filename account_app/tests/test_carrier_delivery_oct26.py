"""Jumara's shipping inquiry must not become a permanent purchase condition."""
import json
import unittest
from unittest.mock import patch

from account_app import checkout, sales_recovery, baraah, child_availability, color_choices, sales_context
from account_app.tests import test_audit_orders as fixtures

INQUIRY = 'حتى اذا قطعه هم مجاني التوصيل'
PRODUCT = dict(product_id='P015', product_name='فستان الاميرات', category='فستان',
               price='13000', stock='متوفر', status='active', colors='وردي ازرق',
               sizes='1 إلى 3 سنوات', store_id=baraah.STORE_ID)
CARRIER = 'هاي أجور التوصيل تابعة لشركة التوصيل مو إلنا'


class DeliveryInquiryTests(unittest.TestCase):
    def test_reported_inquiry_is_a_question_not_a_persistent_condition(self):
        self.assertTrue(checkout.delivery_fee_inquiry(INQUIRY))
        self.assertEqual(checkout.unresolved_purchase_terms(INQUIRY), 'unanswered_price_or_shipping')
        self.assertEqual(sales_recovery.policy_question(INQUIRY), 'delivery')
        history = [dict(direction='incoming', text=INQUIRY)]
        for next_text in ('وين مكانكم\nبي سنه', 'بي سنة', '07701234567 بغداد حي اور', 'ثبتي'):
            self.assertIsNone(checkout.unresolved_condition(history, next_text))

    def test_single_piece_questions_without_punctuation_are_not_conditions(self):
        for text in ('اذا قطعة وحدة التوصيل مجاني', 'يعني التوصيل مجاني', 'هل التوصيل مجاني؟'):
            self.assertTrue(checkout.delivery_fee_inquiry(text), text)
            self.assertIsNone(checkout.unresolved_condition([dict(direction='incoming', text=text)]))

    def test_explicit_free_shipping_or_arrival_conditions_remain_conditions(self):
        for text, kind in (('احجزي اذا التوصيل مجاني', 'free_delivery'),
                           ('بشرط التوصيل مجاني', 'free_delivery'),
                           ('ثبتيه اذا يوصل باجر فقط', 'arrival')):
            self.assertFalse(checkout.delivery_fee_inquiry(text))
            self.assertEqual(checkout.unresolved_condition([dict(direction='incoming', text=text)])['kind'], kind)

    def test_later_question_does_not_waive_an_explicit_condition(self):
        history = [dict(direction='incoming', text='احجزي بشرط التوصيل مجاني'),
                   dict(direction='incoming', text=INQUIRY)]
        self.assertEqual(checkout.unresolved_condition(history)['kind'], 'free_delivery')

    def test_compound_price_fabric_and_size_questions_are_not_swallowed(self):
        for text in ('هل سعر الفستان 13 الف مع التوصيل؟', INQUIRY + ' وشنو القماش؟',
                     INQUIRY + ' بي عمر سنة؟', 'كم السعر مع التوصيل؟'):
            self.assertEqual(sales_recovery.policy_question(text), '', text)

    def test_return_fees_are_distinct_from_ordinary_delivery(self):
        for text in ('اذا رفضت الاستلام ارجعه بدون ما ادفع توصيل',
                     'حتى اذا ما عجبني ارجع الطلب مجاني التوصيل؟'):
            self.assertFalse(checkout.delivery_fee_inquiry(text))
            self.assertEqual(sales_recovery.policy_question(text), '')


class CarrierDeliveryPipelineTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set(baraah.STORE_ID)
        self.sender = 'carrier-test'
        fixtures.AuditFixTests.customer(self, self.sender, baraah.STORE_ID)
        self.product = dict(PRODUCT)
        self.catalog = [self.product]
        self.m.save_delivery_settings(self.db, dict(baghdad_fee=5000, other_fee=5000,
            inspection_message='التوصيل متوفر لجميع المحافظات مع إمكانية الفحص عند الاستلام'))
        self.patches = [patch.object(self.m, 'load_products_from_file', return_value=self.catalog),
                        patch.object(self.m, 'load_active_products', return_value=self.catalog),
                        patch.object(self.m, 'send_order_to_telegram', return_value=True),
                        patch.object(self.m, 'save_booking_to_file'),
                        patch.object(self.m, 'send_telegram_message', return_value=False),
                        patch.object(self.m, 'is_store_feature_enabled', return_value=False)]
        for item in self.patches: item.start()

    def tearDown(self):
        for item in reversed(self.patches): item.stop()
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def save(self, direction, text, payload=None):
        self.m.save_message(self.db, self.sender, direction, 'text', text, None, None, None, payload or {})

    def call(self, text, history=(), customer=None, **event):
        return self.m.call_main_ai(dict(sender_id=self.sender, text=text, **event), 'text', customer or {},
                                  list(history), self.catalog, self.product, {}, '', [], customer_products=self.catalog)

    def visible(self, result):
        return self.m.format_customer_reply(result, result['reply'])[1]

    def test_approved_carrier_quote_ends_with_missing_age_question(self):
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.call(INQUIRY)
        model.assert_not_called()
        reply = self.visible(result)
        self.assertIn('5 آلاف', reply)
        self.assertNotIn(CARRIER, reply)
        self.assertTrue(reply.endswith('شكد العمر المطلوب؟'), reply)
        self.assertEqual(reply.count('؟'), 1)
        self.assertNotIn('حسم هالشرط', reply)
        self.assertNotIn('تحتاج تأكيد', reply)
        self.assertFalse(result['create_order'])

    def test_known_age_asks_color_without_repeating_age_or_fee_consent(self):
        self.save('incoming', 'عمر سنة')
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.call('شكد التوصيل؟')
        model.assert_not_called()
        reply = self.visible(result)
        self.assertNotIn(CARRIER, reply)
        self.assertTrue(reply.endswith(('شنو اللون اللي يعجبج؟', 'شنو اللون اللي يعجبك؟')))
        self.assertNotIn('توافق', reply)

    def test_known_variants_advance_to_booking_question(self):
        self.save('incoming', 'اريد وردي عمر سنة')
        reply = self.visible(self.call('شكد التوصيل؟'))
        self.assertNotIn(CARRIER, reply)
        self.assertTrue(reply.endswith('أثبت الحجز؟'), reply)
        self.assertEqual(reply.count('؟'), 1)

    def test_regional_actual_fee_and_product_free_shipping_are_preserved(self):
        self.m.save_delivery_settings(self.db, dict(baghdad_fee=5000, other_fee=7000))
        reply = self.visible(self.call('شكد التوصيل للبصرة؟'))
        self.assertIn('7 آلاف', reply)
        self.assertNotIn('5 آلاف', reply)
        self.assertNotIn(CARRIER, reply)
        self.product['offer'] = 'التوصيل مجاني'
        reply = self.visible(self.call(INQUIRY))
        self.assertIn('التوصيل مجاني حسب عرض المنتج', reply)
        self.assertNotIn(CARRIER, reply)
        self.assertNotIn('7 آلاف', reply)

    def test_zero_fee_does_not_claim_customer_owes_carrier(self):
        self.m.save_delivery_settings(self.db, dict(baghdad_fee=0, other_fee=0))
        reply = self.visible(self.call(INQUIRY))
        self.assertIn('التوصيل مجاني', reply)
        self.assertNotIn(CARRIER, reply)

    def test_other_store_keeps_its_existing_copy(self):
        self.m._current_store_id.set('al-fatena')
        reply = self.visible(self.m.store_policy_question_reply('شكد التوصيل؟', self.product))
        self.assertNotIn(CARRIER, reply)
        self.assertIn('وإلكم حق الفحص بوجود المندوب قبل الدفع', reply)

    def test_inspection_payment_and_total_survive_carrier_explanation(self):
        result = self.m.recover_factual_sales_reply(dict(sender_id=self.sender,
            text='شكد سعره وشكد التوصيل وشنو الدفع والفحص؟'), self.product)
        reply = self.visible(result)
        for value in ('13 ألف', '5 آلاف', '18 ألف', 'الفحص', 'الدفع يصير عند الاستلام'):
            self.assertEqual(reply.count(value), 1, reply)
        self.assertFalse(result['_needs_fact_review'])

    def test_reported_history_does_not_block_new_known_photo_and_age(self):
        history = [dict(direction='incoming', text=INQUIRY),
                   dict(direction='outgoing', text='هذا بكج 3 قطع والتوصيل مجاني.')]
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.call('بي سنه', history, image_url='https://images.test/new.jpg',
                               _image_matches=[dict(product_found=True, product_id='P015', image_index=1)])
        model.assert_not_called()
        self.assertIn('متوفر لعمر سنة', result['reply'])
        self.assertNotIn('شرط', result['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_place_and_age_question_keeps_complete_answer_path(self):
        text = 'السلام عليكم\nوين مكانكم\nبي سنه'
        self.assertIsNone(child_availability.reply(self.product, self.catalog, text))
        state = sales_context.advance({}, [dict(direction='incoming', text='اريد وردي ازرق عمر سنة')])
        self.assertIsNone(color_choices.reply(self.product, text, state))
        history = [dict(direction='incoming', text=INQUIRY)]
        generated = dict(reply='وعليكم السلام، عنوان المتجر حسب المعلومات المرفقة. فستان الاميرات متوفر لعمر سنة. شنو اللون اللي يعجبج؟',
                         create_order=False, order={}, requires_human=False)
        with patch.object(self.m, '_call_main_ai_once', return_value=generated) as model:
            result = self.call(text, history)
        self.assertTrue(model.called)
        self.assertIn('عنوان المتجر', result['reply'])
        self.assertIn('لعمر سنة', result['reply'])
        self.assertNotIn('حسم هالشرط', result['reply'])

    def test_complete_contact_after_quote_books_without_fee_confirmation(self):
        self.m.bind_customer_to_product(self.db, self.sender, self.product, source='image_recognition')
        self.save('incoming', INQUIRY)
        self.save('outgoing', 'التوصيل 5 آلاف، ' + CARRIER + ' 🌷 شكد العمر المطلوب؟')
        self.save('incoming', 'اريد وردي عمر سنة ثبتي')
        item = dict(product_id='P015', product_name='فستان الاميرات', color='وردي', size='سنة',
                    size_type='size', quantity=1)
        self.save('outgoing', 'دزيلي الرقم والمحافظة والعنوان', {'checkout_draft': {'items': [item]}})
        event = dict(sender_id=self.sender, page_id='page', platform='facebook', text='07701234567 بغداد حي اور قرب المدرسة',
                     image_url=None, attachments=[], store_id=baraah.STORE_ID, ref=None, ad_id=None,
                     timestamp=0, postback=None, quick_reply=None, referral_source=None, referral_type=None)
        with patch.object(self.m, 'extract_facebook_event', return_value=event), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'get_auto_product_settings', return_value=dict(enabled=False, product=None)), \
             patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        model.assert_not_called()
        self.assertEqual(result['reply'], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        orders = self.db.execute('SELECT * FROM orders').fetchall()
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]['delivery_fee'], 5000)
        self.assertEqual(orders[0]['total_amount'], 18000)
        self.assertEqual(json.loads(orders[0]['order_items'])[0]['color'], 'وردي')


if __name__ == '__main__':
    unittest.main()
