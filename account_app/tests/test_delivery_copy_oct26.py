"""Replay the reported repeated policy and keep fees, conditions and payment distinct."""
import unittest
from unittest.mock import patch

from account_app import delivery_copy, customer_copy
from account_app.tests import test_audit_orders as fixtures

DEFAULT_INSPECTION = 'التوصيل متوفر لجميع المحافظات مع إمكانية الفحص عند الاستلام'
REPORTED = ('فستان دانتيل، التوصيل 5 آلاف لكل المحافظات. '
            + DEFAULT_INSPECTION + ' الدفع يصير عند الاستلام. '
            + DEFAULT_INSPECTION + ' التوصيل 5 آلاف لكل المحافظات.')
PRODUCT = dict(product_id='F1', product_name='فستان دانتيل', price='16000',
               stock='متوفر', status='active', sizes='38 إلى 52', colors='اسود')


class PolicyCopyTests(unittest.TestCase):
    def test_reported_reply_has_each_fact_once_without_losing_product_or_price(self):
        reply = delivery_copy.compact_repetition(REPORTED)
        self.assertEqual(reply.count('التوصيل'), 1)
        self.assertEqual(reply.count('5 آلاف'), 1)
        self.assertEqual(reply.count('الفحص'), 1)
        self.assertEqual(reply.count('الدفع'), 1)
        self.assertIn('فستان دانتيل', reply)
        self.assertIn('عند الاستلام', reply)
        self.assertNotIn('الاستلام الدفع', reply)
        self.assertEqual(delivery_copy.compact_repetition(reply), reply)

    def test_inspection_extraction_keeps_unique_conditions(self):
        self.assertEqual(delivery_copy.inspection_only(DEFAULT_INSPECTION), 'الفحص عند الاستلام')
        custom = 'الفحص بحضور المندوب قبل الدفع فقط؛ أجور التوصيل تدفع بحالة رفض الاستلام'
        self.assertEqual(delivery_copy.inspection_only(custom), custom)
        exceptions = 'التوصيل متوفر لجميع المحافظات عدا منطقة محددة، والفحص بحضور المندوب'
        self.assertEqual(delivery_copy.inspection_only(exceptions), exceptions)

    def test_qualified_fees_negation_and_distinct_regions_are_not_dropped(self):
        for text in ('التوصيل 5 آلاف لكل المحافظات. التوصيل 7 آلاف لكل المحافظات.',
                     'التوصيل 5 آلاف لكل المحافظات عدا المناطق البعيدة. التوصيل 5 آلاف لكل المحافظات.',
                     'التوصيل لبغداد 5 آلاف ولبقية المحافظات 7 آلاف. مدة التوصيل يومين.',
                     'لا يمكن الفحص عند الاستلام. الفحص عند الاستلام.',
                     'الدفع عند الاستلام بشرط تأكيد العنوان. الدفع عند الاستلام.'):
            self.assertEqual(delivery_copy.compact_repetition(text), text)

    def test_plain_payment_and_fee_spelling_variants_are_deduplicated(self):
        text = 'الدفع عند الاستلام. الدفع يصير عند الاستلام. التوصيل: 5 آلاف لجميع المحافظات. التوصيل 5 آلاف لكل المحافظات.'
        reply = delivery_copy.compact_repetition(text)
        self.assertEqual(reply.count('الدفع'), 1)
        self.assertEqual(reply.count('التوصيل'), 1)

    def test_unrelated_text_and_prices_are_untouched(self):
        for text in ('السعر 16,000 د.ع. القياس 38/40.', 'تم تثبيت الطلب.',
                     'سعره مع التوصيل 21 ألف؛ الفستان 16 والتوصيل 5 آلاف 🌷'):
            self.assertEqual(delivery_copy.compact_repetition(text), text)


class PolicyAssemblyTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def settings(self, **extra):
        self.m.save_delivery_settings(self.db, dict(baghdad_fee=5000, other_fee=5000,
            inspection_message=DEFAULT_INSPECTION, **extra))

    def visible(self, result):
        result, reply = self.m.format_customer_reply(result, result['reply'])
        return result, reply

    def test_payment_question_has_one_coverage_fee_inspection_and_payment(self):
        self.settings()
        result, reply = self.visible(self.m.store_policy_question_reply('كيفية الدفع والتوصيل؟', PRODUCT))
        for fact in ('التوصيل', '5 آلاف', 'الفحص', 'الدفع يصير عند الاستلام'):
            self.assertEqual(reply.count(fact), 1, reply)
        self.assertEqual(result['reply_parts'], [reply])
        self.assertTrue(result['_single_message'])

    def test_mixed_policy_recovery_does_not_append_nested_policy_answers(self):
        self.settings()
        result, reply = self.visible(self.m.recover_factual_sales_reply(
            {'text': 'شكد التوصيل وشلون الدفع واكدر افحص عند الاستلام؟'}, PRODUCT))
        self.assertEqual(reply.count('التوصيل'), 1)
        self.assertEqual(reply.count('الفحص'), 1)
        self.assertEqual(reply.count('الدفع يصير عند الاستلام'), 1)
        self.assertEqual(reply.count('5 آلاف'), 1)
        self.assertFalse(result['_needs_fact_review'])
        self.assertFalse(result['create_order'])

    def test_reported_refusal_fee_question_keeps_unresolved_cost_explicit(self):
        self.settings()
        question = 'توصيل شكد بغداد؟ في حال إذا ما عجبني الطلب ارجعة بدون ماادفع توصيل'
        result, reply = self.visible(self.m.recover_factual_sales_reply({'text': question}, PRODUCT))
        self.assertIn('بغداد', reply)
        self.assertEqual(reply.count('5 آلاف'), 1)
        self.assertEqual(reply.count('الفحص'), 1)
        self.assertEqual(reply.count('الدفع يصير عند الاستلام'), 1)
        self.assertIn('أجور التوصيل بحالة رفض الاستلام تحتاج تأكيد', reply)
        self.assertNotIn('التوصيل مجاني', reply)
        self.assertTrue(result['_needs_fact_review'])

    def test_recorded_refusal_cost_and_product_inspection_override_are_preserved(self):
        self.settings()
        policy = 'الفحص بحضور المندوب قبل الدفع فقط؛ أجور التوصيل تدفع بحالة رفض الاستلام'
        product = dict(PRODUCT, inspection_policy=policy)
        result, reply = self.visible(self.m.recover_factual_sales_reply(
            {'text': 'إذا رفضت الاستلام ادفع توصيل؟'}, product))
        self.assertIn(policy, reply)
        self.assertNotIn('تحتاج تأكيد', reply)
        self.assertFalse(result['_needs_fact_review'])

    def test_variable_region_fees_free_delivery_and_duration_are_correct(self):
        self.m.save_delivery_settings(self.db, dict(baghdad_fee=5000, other_fee=7000,
            inspection_message=DEFAULT_INSPECTION, delivery_time='من يومين إلى ثلاثة أيام'))
        _, reply = self.visible(self.m.store_policy_question_reply('شكد التوصيل للبصرة؟', PRODUCT))
        self.assertIn('البصرة', reply)
        self.assertIn('7 آلاف', reply)
        self.assertNotIn('5 آلاف', reply)
        _, free = self.visible(self.m.store_policy_question_reply('شلون الدفع والتوصيل؟', dict(PRODUCT, delivery='توصيل مجاني')))
        self.assertIn('التوصيل مجاني حسب عرض المنتج', free)
        self.assertNotIn('7 آلاف', free)
        _, duration = self.visible(self.m.store_policy_question_reply('متى التوصيل؟', PRODUCT))
        self.assertIn('يومين إلى ثلاثة أيام', duration)

    def test_zero_regional_fee_is_not_claimed_as_a_product_offer(self):
        self.m.save_delivery_settings(self.db, dict(baghdad_fee=0, other_fee=7000))
        _, reply = self.visible(self.m.store_policy_question_reply('شكد التوصيل لبغداد؟', PRODUCT))
        self.assertIn('مجاني', reply)
        self.assertNotIn('عرض المنتج', reply)

    def test_full_price_and_total_survive_policy_cleanup(self):
        self.settings()
        _, reply = self.visible(self.m.recover_factual_sales_reply(
            {'text': 'شكد سعره وشكد التوصيل وشنو الدفع والفحص؟'}, PRODUCT))
        for value in ('16 ألف', '5 آلاف', '21 ألف'):
            self.assertEqual(reply.count(value), 1, reply)
        self.assertEqual(reply.count('الفحص'), 1)
        self.assertEqual(reply.count('الدفع يصير عند الاستلام'), 1)

    def test_default_policy_builder_does_not_repeat_coverage_from_inspection(self):
        reply = self.m.build_delivery_policy_text(dict(baghdad_fee=5000, other_fee=5000,
                                                       inspection_message=DEFAULT_INSPECTION))
        self.assertEqual(reply.count('محافظات'), 1)
        self.assertEqual(reply.count('الفحص'), 1)

    def test_cleanup_precedes_storage_and_direct_delivery(self):
        self.customer('delivery-copy')
        event = dict(sender_id='delivery-copy', page_id='page', platform='facebook')
        result = self.m.saved_checkout_reply(self.db, event, REPORTED, {})
        self.assertEqual(' '.join(' '.join(result['reply_parts']).split()), ' '.join(result['reply'].split()))
        with patch.object(self.m, 'send_text_to_facebook', return_value=True) as send:
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual([call.args[1] for call in send.call_args_list], result['reply_parts'])
        rows = self.db.execute("SELECT text FROM messages WHERE direction='outgoing'").fetchall()
        self.assertEqual([row[0] for row in rows], result['reply_parts'])
        self.assertEqual(result['reply'].count('التوصيل'), 1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def delivery_reply(self, *, product=None, customer=None, history=(), text='شكد التوصيل؟'):
        self.settings()
        product = product or dict(PRODUCT, sizes='من عمر 3 إلى 7 سنوات', colors='اسود وردي')
        self.customer('policy-next')
        for item in history:
            self.m.save_message(self.db, 'policy-next', item['direction'], 'text', item['text'],
                                None, None, None, {})
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.call_main_ai(dict(sender_id='policy-next', text=text), 'text',
                customer or {}, list(history), [product], product, None, '', [], customer_products=[product])
        model.assert_not_called()
        self.assertEqual(result['reply_parts'], [result['reply']])
        self.assertFalse(result['create_order'])
        return result['reply']

    def test_approved_delivery_copy_continues_with_missing_child_age(self):
        reply = self.delivery_reply()
        self.assertIn('التوصيل 5,000 د.ع لكل المحافظات، وإلكم حق الفحص بوجود المندوب قبل الدفع', reply)
        self.assertIn('شكد العمر المطلوب؟', reply)
        self.assertEqual(reply.count('؟'), 1)
        self.assertEqual(reply.count('الفحص'), 1)

    def test_known_age_advances_to_missing_color(self):
        reply = self.delivery_reply(customer={'age': 4})
        self.assertNotIn('العمر المطلوب', reply)
        self.assertRegex(reply, 'شنو اللون اللي يعجب[جك]؟')
        self.assertEqual(reply.count('؟'), 1)

    def test_age_from_previous_customer_reply_is_not_requested_again(self):
        reply = self.delivery_reply(history=[dict(direction='outgoing', text='شكد عمر البنوتة؟'),
                                             dict(direction='incoming', text='4')])
        self.assertNotIn('العمر المطلوب', reply)
        self.assertRegex(reply, 'شنو اللون اللي يعجب[جك]؟')

    def test_adult_uses_missing_size_then_color(self):
        reply = self.delivery_reply(product=dict(PRODUCT, colors='اسود وردي'), customer={'size': '44'})
        self.assertNotIn('قياس تلبسين', reply)
        self.assertNotIn('العمر', reply)
        self.assertRegex(reply, 'شنو اللون اللي يعجب[جك]؟')

    def test_unavailable_product_or_unresolved_refusal_fee_has_no_sales_question(self):
        self.settings()
        product = dict(PRODUCT, stock='نفذ')
        result = self.m.store_policy_question_reply('شكد التوصيل؟', product,
                                                   ev={'text': 'شكد التوصيل؟'})
        self.assertNotIn('؟', result['reply'])
        result = self.m.recover_factual_sales_reply(
            {'text': 'إذا رفضت الاستلام ادفع توصيل؟'}, PRODUCT)
        self.assertTrue(result['_needs_fact_review'])
        self.assertNotIn('؟', result['reply'])

    def test_recovery_uses_approved_copy_and_known_answers(self):
        self.settings()
        self.customer('policy-recovery')
        self.m.save_message(self.db, 'policy-recovery', 'incoming', 'text', 'قياسي 44',
                            None, None, None, {})
        result = self.m.recover_factual_sales_reply(
            dict(sender_id='policy-recovery', text='شكد التوصيل وشلون الدفع؟'),
            dict(PRODUCT, colors='اسود وردي'))
        self.assertIn('وإلكم حق الفحص بوجود المندوب قبل الدفع', result['reply'])
        self.assertIn('شنو اللون اللي يعجبج؟', result['reply'])
        self.assertNotIn('قياس تلبسين', result['reply'])

    def test_custom_inspection_restriction_is_not_replaced_with_plain_right(self):
        self.settings()
        product = dict(PRODUCT, inspection_policy='الفحص غير مسموح قبل الدفع')
        result = self.m.store_policy_question_reply('شكد التوصيل؟', product)
        self.assertIn('الفحص غير مسموح قبل الدفع', result['reply'])
        self.assertNotIn('إلكم حق الفحص', result['reply'])

