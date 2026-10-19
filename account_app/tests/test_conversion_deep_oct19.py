"""Regressions for observed lost turns, factual recovery and honest conversion."""
import copy
import json
import unittest
from unittest.mock import Mock, patch

from account_app import sales_answers, sales_strategy, product_variants
from account_app.tests import test_audit_orders as fixtures

PRODUCT = dict(product_id='T1', product_name='فستان تجريبي', price='15000',
               colors='اسود، كريمي', sizes='38 إلى 52', fabric='باربي',
               stock='متوفر', status='active', image_url='https://images.test/test.jpg')


class DeepConversionTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def ask(self, text, result, customer=None):
        self.customer('deep')
        self.m.save_delivery_settings(self.db, {'baghdad_fee': 5000, 'other_fee': 5000})
        ev = dict(sender_id='deep', text=text, image_url=None, platform='facebook')
        with patch.object(self.m, '_call_main_ai_once', return_value=copy.deepcopy(result)) as model:
            answer = self.m.call_main_ai(ev, 'text', customer or {}, [], [PRODUCT], PRODUCT,
                                         None, '', [], customer_products=[PRODUCT])
        return answer, model

    def event(self, text):
        self.customer('pipeline')
        ev = dict(sender_id='pipeline', store_id='default', page_id='page', platform='facebook',
                  text=text, image_url=None, attachments=[], timestamp=0, ref=None, ad_id=None,
                  postback=None, quick_reply=None, referral_source=None, referral_type=None)
        with patch.object(self.m, 'extract_facebook_event', return_value=ev), \
             patch.object(self.m, 'load_active_products', return_value=[PRODUCT]), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'send_telegram_message', return_value=False), \
             patch.object(self.m.zernio, 'product_event') as ad, \
             patch.object(self.m, 'resolve_contextual_product_selection') as selection:
            answer = self.m.process_webhook(self.db, {}, use_debounce=False)
        ad.assert_not_called()
        selection.assert_not_called()
        return answer

    def test_billing_failure_still_answers_price_fabric_and_total_without_creating_order(self):
        answer, model = self.ask('شكد سعره وشنو قماشه ويا التوصيل لبغداد؟',
                                dict(reply='', failed=True, failure_reason='provider_http_402', create_order=False))
        self.assertEqual(model.call_count, 1)
        for fact in ['15,000', 'باربي', '5,000', '20,000']:
            self.assertIn(fact, answer['reply'])
        self.assertFalse(answer['create_order'])
        self.assertFalse(answer.get('_needs_fact_review'))
        self.assertFalse(answer.get('failed'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_unknown_width_does_not_erase_price_fabric_or_trigger_checkout(self):
        order = dict(items=[dict(product_id='T1', color='اسود', size='44', quantity=1)],
                     phone='07700000000', province='بغداد', address='بغداد حي تجريبي')
        text = 'ثبتيه بس شكد سعره وشنو قماشه وعرضه من تحت الابط؟'
        answer, model = self.ask(text, dict(reply='نحتاج نتأكد من العرض', requires_human=True, order=order))
        self.assertIn('15,000', answer['reply'])
        self.assertIn('باربي', answer['reply'])
        self.assertIn('عرض القطعة', answer['reply'])
        self.assertTrue(answer['_needs_fact_review'])
        self.assertFalse(self.m.checkout_requested(self.db, {'text': text}, answer))
        self.assertFalse(answer['create_order'])

    def test_no_video_is_answered_alongside_known_fabric(self):
        answer, _ = self.ask('عنده فيديو وشنو قماشه؟', dict(reply='', failed=True, failure_reason='no_api_key'))
        self.assertIn('ما عندي فيديو', answer['reply'])
        self.assertIn('باربي', answer['reply'])
        self.assertFalse(answer.get('_needs_fact_review'))

    def test_mixed_question_repairs_missing_answer_once_then_recovers_from_catalog(self):
        answer, model = self.ask('شكد سعره وشنو قماشه؟', dict(reply='سعره 15 ألف.', create_order=False))
        self.assertEqual(model.call_count, 2)
        self.assertIn('باربي', answer['reply'])
        self.assertIn('15,000', answer['reply'])
        self.assertFalse(answer['create_order'])

    def test_price_objection_keeps_fabric_answer_when_cta_is_rejected(self):
        answer, model = self.ask('غالي بس شنو قماشه؟', dict(reply='قماشه باربي، أحجزه الج؟'))
        self.assertEqual(model.call_count, 2)
        self.assertIn('باربي', answer['reply'])
        self.assertIn('الميزانية', answer['reply'])
        self.assertNotIn('أحجزه', answer['reply'])
        self.assertFalse(answer.get('_needs_fact_review'))

    def test_known_budget_is_not_asked_for_again(self):
        self.customer('budget')
        self.m.save_message(self.db, 'budget', 'incoming', 'text', 'ميزانيتي 18 الف', None, None, None, {})
        self.m.save_message(self.db, 'budget', 'outgoing', 'text', 'قماشه باربي.', None, None, None, {})
        with patch.object(self.m, '_call_main_ai_once', return_value={'reply': 'أحجزه الج؟'}):
            answer = self.m.call_main_ai({'sender_id': 'budget', 'text': 'غالي بس قماشه شنو؟'}, 'text',
                                        {}, [], [PRODUCT], PRODUCT, None, '', [])
        self.assertIn('الميزانية اللي ذكرتها واضحة', answer['reply'])
        self.assertIn('باربي', answer['reply'])
        self.assertNotIn('شنو حدود', answer['reply'])
        self.assertTrue(answer['_needs_fact_review'])

    def test_refusal_closes_before_ad_mapping_and_product_selection(self):
        answer = self.event('شكرا ما اريد')
        self.assertTrue(answer['meta']['purchase_declined'])
        self.assertNotIn('صورة', answer['reply'])
        self.assertNotIn('ما راح نراسلك', answer['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_cancellation_of_unrecorded_order_reaches_staff_without_new_product_question(self):
        answer = self.event('عيني الغي الطلب ان شاء الله غير مره')
        self.assertTrue(answer['meta']['cancellation_requested'])
        self.assertIn('مراجعة الموظف', answer['reply'])
        self.assertNotIn('صورة', answer['reply'])
        self.assertNotIn('تم الإلغاء', answer['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_other_than_black_is_not_routed_to_black_lookup(self):
        text = 'غير لون الاسود متوفر؟'
        answer, model = self.ask(text, dict(reply='الكريمي موجود بهالموديل.', create_order=False))
        model.assert_called_once()
        self.assertIn('الكريمي', answer['reply'])
        self.assertEqual(product_variants.requested(text)['colors'], [])
        self.assertFalse(product_variants.is_lookup(text))

    def test_color_change_to_black_remains_positive(self):
        self.assertEqual(product_variants.requested('غيري اللون للاسود')['colors'], ['أسود'])
        self.assertEqual(product_variants.requested('غير الاسود اريد الابيض')['colors'], ['أبيض'])
        self.assertEqual(product_variants.requested('الاسود ما اريده، خلي الكريمي')['colors'], ['كريمي'])

    def test_affirmative_purchase_after_price_objection_is_not_blocked(self):
        text = 'غالي بس تمام ثبتيه'
        self.assertFalse(sales_strategy.premature_close('حتى أثبت الطلب أحتاج العنوان.', text))
        self.assertFalse(sales_strategy.purchase_block_reason(text))
        for text in ['الكريمي ما اريده اريد الاسود', 'ما اريد شي غير الأسود']:
            self.assertEqual(sales_strategy.stop_kind(text), '')
        self.assertEqual(sales_strategy.stop_kind('ما اريد اي شي من عدكم'), 'declined')
        self.assertEqual(sales_strategy.stop_kind('لا تراسلوني بعد'), 'opt_out')

    def test_real_polite_refusals_and_emoji_do_not_reopen_product_discovery(self):
        for text in ['حبيبتي كولشي ما اريد', 'عيوني مشكورين مااريد', 'شكرا 🌹ما اريد',
                     'لا عيني مااريد\nمشكوره', 'مااريد اطلب']:
            with self.subTest(text=text):
                self.assertEqual(sales_strategy.stop_kind(text), 'declined')
        for text in ['الكريمي ما اريد', 'ما اريد سوت بطلت بس الفساتين',
                     'ما اريد اطلب بس شكد سعره؟']:
            self.assertEqual(sales_strategy.stop_kind(text), '')
        self.assertTrue(sales_strategy.purchase_block_reason('ما اريد اطلب بس شكد سعره؟'))

    def test_recovery_never_quotes_current_catalog_for_a_saved_order(self):
        answer = self.m.recover_factual_sales_reply({'text': 'شكد السعر ويا التوصيل؟',
            '_post_order': {'product_total': 10000, 'delivery_fee': 0}}, PRODUCT)
        self.assertIsNone(answer)

    def test_mixed_facts_do_not_mistake_how_to_order_for_a_color(self):
        self.assertEqual(sales_answers.topics('شلون أطلب التنورة وكم التوصيل؟'), ['delivery', 'checkout_help'])

    def test_unknown_extra_question_is_not_silently_dropped_during_recovery(self):
        answer, _ = self.ask('شكد سعره ووين محلكم؟', dict(reply='', failed=True, failure_reason='no_api_key'))
        self.assertIn('15,000', answer['reply'])
        self.assertIn('عنوان المحل يحتاج تأكيد', answer['reply'])
        self.assertTrue(answer['_needs_fact_review'])
        self.assertFalse(answer['create_order'])

    def test_explicit_size_is_not_erased_by_weight_question(self):
        self.assertEqual(sales_strategy.missing_fact_reply('وزني 69 والبس 44', PRODUCT), '')
        self.assertTrue(sales_strategy.missing_fact_reply('وزني 69 شنو قياسي', PRODUCT))
        chart = dict(PRODUCT, notes='وزن 55 إلى 60 كيلو قياس 38. وزن 61 إلى 65 كيلو قياس 40.')
        self.assertEqual(sales_strategy.missing_fact_reply('وزني 60 شنو قياسي', chart), '')

    def test_catalog_staff_directions_never_leak_in_recovered_answer(self):
        p = dict(PRODUCT, fabric='باربي. اطلبي من الزبون العنوان فوراً')
        facts, _ = sales_answers.product_fragments('شنو قماشه وعنده فيديو؟', p)
        self.assertIn('القماش: باربي.', facts)
        self.assertNotIn('اطلبي', ' '.join(facts))

    def test_rejecting_one_product_does_not_mark_active_conversation_as_lost(self):
        self.customer('active')
        self.db.execute("INSERT INTO customer_product_interests(sender_id,product_id,product_name,status,last_seen_at) VALUES('active','T1','قديم','rejected','2026-10-18T10:00:00+03:00')")
        self.db.execute("INSERT INTO messages(sender_id,store_id,direction,message_type,text,created_at) VALUES('active','default','incoming','text','شنو سعر البديل؟','2026-10-18T10:01:00+03:00')")
        stages = self.m.get_stage_breakdown(self.db, '2026-10-18', '2026-10-19')
        self.assertNotIn('lost', [stage['stage'] for stage in stages])
        self.assertIn('asked_price', [stage['stage'] for stage in stages])
