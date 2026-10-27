"""Policy questions must survive image review without guessing the product."""
import unittest
from unittest.mock import patch

from account_app import sales_recovery
from account_app.tests import test_audit_orders as fixtures


class ImagePolicyRecoveryTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def prepare(self, text):
        self.customer('image-policy')
        self.m.save_delivery_settings(self.db, {'baghdad_fee': 4000, 'other_fee': 7000,
                                               'delivery_time': 'يومين إلى ثلاثة أيام'})
        return dict(sender_id='image-policy', store_id='default', page_id='page',
                    platform='facebook', text=text, image_url='https://images.test/unknown.jpg',
                    attachments=[], ref=None, ad_id=None, timestamp=0,
                    postback=None, quick_reply=None, referral_source=None, referral_type=None)

    def test_failed_image_answers_province_fee_and_preserves_review(self):
        ev = self.prepare('شكد التوصيل الانبار الفلوجه\nبي لون جوزي!')
        with patch.object(self.m, 'extract_facebook_event', return_value=ev), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'load_active_products', return_value=[]), \
             patch.object(self.m, 'match_product', return_value=(None, '', {})), \
             patch.object(self.m, 'send_telegram_message', return_value=False), \
             patch.object(self.m, '_call_main_ai_once') as provider:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        provider.assert_not_called()
        self.assertIn('7,000', result['reply'])
        self.assertIn('الأنبار', result['reply'])
        self.assertNotIn('جوزي متوفر', result['reply'])
        self.assertTrue(result['meta']['image_unresolved'])
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, ev['sender_id']))
        self.assertTrue(self.m.has_pending_human_review(self.db, ev['sender_id']))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_partial_answer_does_not_erase_image_size_or_address_from_next_turn(self):
        ev = self.prepare('القياس 38\nكم يوم يوصل الطلب\nشكد سعر التوصيل')
        image = self.m.save_message(self.db, ev['sender_id'], 'incoming', 'image', ev['text'],
                                    ev['image_url'], None, None, {})
        result = self.m.pending_image_policy_reply(self.db, ev, {'human_review_id': 1})
        self.assertIn('يومين إلى ثلاثة', result['reply'])
        self.assertIn('4,000', result['reply'])
        self.assertIn('7,000', result['reply'])
        latest = self.m.save_message(self.db, ev['sender_id'], 'incoming', 'text', 'نفس اللون', None, None, None, {})
        collected = self.m.collect_unanswered_event(self.db, dict(ev, image_url=None, text='نفس اللون'), latest)
        self.assertIn('القياس 38', collected['text'])
        self.assertEqual(collected['image_url'], ev['image_url'])
        self.assertIn('نفس اللون', collected['text'])

    def test_service_failure_still_answers_policy_but_never_claims_product_offer(self):
        ev = self.prepare('وشكد توصيل')
        with patch.object(self.m, 'extract_facebook_event', return_value=ev), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'load_active_products', return_value=[]), \
             patch.object(self.m, 'match_product', return_value=(None, '', {'service_error': True, 'error_code': 'timeout'})), \
             patch.object(self.m, 'send_telegram_message', return_value=False):
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertIn('7,000', result['reply'])
        self.assertTrue(result['meta']['service_error'])
        self.assertNotIn('الصورة وصلت', result['reply'])
        self.assertNotIn('عرض القطعة', result['reply'])

    def test_saved_order_uses_its_own_service_flow(self):
        ev = self.prepare('شكد التوصيل')
        self.db.execute('INSERT INTO orders(sender_id,status) VALUES(?,?)', (ev['sender_id'], 'new'))
        self.db.commit()
        result = self.m.pending_image_policy_reply(self.db, ev, {})
        self.assertEqual(result['reply'], '')

    def test_policy_destination_is_not_saved_as_customer_address(self):
        self.prepare('شكد التوصيل لبغداد؟')
        for question, province, fee in [('شكد التوصيل لبغداد؟', 'بغداد', '4,000'),
                                        ('شكد التوصيل للبصرة؟', 'البصرة', '7,000')]:
            result = self.m.store_policy_question_reply(question)
            self.assertIn(province, result['reply'])
            self.assertIn(fee, result['reply'])
        self.assertIsNone(sales_recovery.policy_province('بغداد لو البصرة'))
        row = self.db.execute('SELECT address,province FROM customers WHERE sender_id=?', ('image-policy',)).fetchone()
        self.assertFalse(row['address'])
        self.assertFalse(row['province'])

    def test_payment_answer_uses_actual_inspection_policy(self):
        self.prepare('شلون الدفع')
        self.m.save_delivery_settings(self.db, {'inspection_message': 'الفحص غير متاح قبل الدفع.'})
        result = self.m.store_policy_question_reply('شلون الدفع')
        self.assertIn('الفحص غير متاح قبل الدفع', result['reply'])
        self.assertNotIn('تكدر تفحص', result['reply'])
        self.m.save_delivery_settings(self.db, {'inspection_message': ''})
        self.assertNotIn('فحص', self.m.store_policy_question_reply('شلون الدفع')['reply'])

    def test_concatenated_shipping_question_and_mixed_questions_stay_distinct(self):
        self.assertEqual(sales_recovery.policy_question('شكدالتوصيل'), 'delivery')
        for question in ['شكد سعره ويا التوصيل', 'قطعتين ويا التوصيل بشكد',
                         'اذا اطلب قطعتين التوصيل مجاني؟', 'ليش تاخر المندوب',
                         'شكد التوصيل اذا اخذ قطعتين', 'كم سعر القطعة؟']:
            self.assertEqual(sales_recovery.independent_policy_questions(question), [], question)
        self.assertTrue(sales_recovery.independent_policy_questions('سلام عليكم\nبي غير الوان؟\nوشكد توصيل'))


if __name__ == '__main__':
    unittest.main()
