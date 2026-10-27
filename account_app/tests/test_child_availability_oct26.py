"""Noor's two-age question gets separate factual answers and real alternatives."""
import unittest
from unittest.mock import patch

from account_app import child_availability, product_variants as variants, baraah
from account_app.reply_layout import plan_sales_bubbles
from account_app.tests import test_audit_orders as fixtures

QUESTION = 'سلام عليكم خويه شكد سعر عمر ٦ سنوات\nوعمر سنتين'
PRODUCT = dict(product_id='A', product_name='فستان دانتيل', category='فستان', store_id='baraah-kids',
               sizes='3 إلى 7 سنوات', colors='وردي اسود', price='13000', stock='متوفر', status='active',
               image_url='https://images.test/a.jpg')
ALTERNATIVE = dict(PRODUCT, product_id='B', product_name='فستان وردة', sizes='1 إلى 3 سنوات', price='12000',
                   image_url='https://images.test/b.jpg')


class ChildAvailabilityFactsTests(unittest.TestCase):
    def test_collects_both_ages_without_splitting_year_and_month_parts(self):
        self.assertEqual(variants.child_ages(QUESTION), [6, 2])
        self.assertEqual(variants.child_ages('عمر 6 سنوات وعمر سنتين'), [6, 2])
        ages = variants.child_ages('عمر سنة و10 أشهر وعمر 6 سنوات')
        self.assertAlmostEqual(ages[0], 22 / 12)
        self.assertEqual(ages[1], 6)
        self.assertEqual(variants.child_ages('عمر 6 سنوات وعمر 6 سنوات'), [6])
        self.assertEqual(variants.child_ages('قياس 44 ووزن 74 كيلو'), [])

    def test_variant_assessment_checks_each_age_instead_of_only_the_first(self):
        checks = variants.assess(PRODUCT, QUESTION)
        self.assertEqual([(c['value'], c['supported']) for c in checks], [('6', True), ('2', False)])

    def test_both_ages_available_and_one_unit_price(self):
        product = dict(PRODUCT, sizes='1 إلى 7 سنوات')
        result = child_availability.reply(product, [product], QUESTION)
        self.assertIn('متوفر لعمر 6 سنوات ولعمر سنتين', result['reply'])
        self.assertEqual(result['reply'].count('13 ألف'), 1)
        self.assertNotIn('المجموع', result['reply'])
        self.assertNotIn('العمر المطلوب', result['reply'])
        self.assertFalse(result['create_order'])
        self.assertFalse(plan_sales_bubbles(result, result['reply'])['needs_rewrite'])

    def test_last_pieces_offer_keeps_approved_short_offer_first_style(self):
        product = dict(PRODUCT, sizes='1 إلى 7 سنوات', offer='عرض على آخر القطع')
        result = child_availability.reply(product, [product], QUESTION)
        self.assertTrue(result['reply'].startswith('عليه عرض على آخر قطع، وصار سعره 13 ألف'))
        self.assertEqual(result['reply'].count('13 ألف'), 1)

    def test_partial_availability_proposes_a_matching_alternative_and_its_price(self):
        result = child_availability.reply(PRODUCT, [PRODUCT, ALTERNATIVE], QUESTION)
        self.assertIn('لعمر 6 سنوات متوفر', result['reply'])
        self.assertIn('لعمر سنتين مو ضمن قياساته', result['reply'])
        self.assertIn('بديل فستان وردة متوفر لعمر سنتين', result['reply'])
        self.assertIn('13 ألف.', result['reply'])
        self.assertIn('12 ألف.', result['reply'])
        self.assertEqual(result['image_product_ids'], ['B'])
        self.assertEqual(result['reply'].count('؟'), 1)
        self.assertEqual(result['order'], {})

    def test_sold_out_model_has_one_unavailability_notice_and_verified_alternative(self):
        product = dict(PRODUCT, stock='نفذ')
        alternative = dict(ALTERNATIVE, sizes='1 إلى 7 سنوات')
        result = child_availability.reply(product, [product, alternative], QUESTION)
        self.assertEqual(result['reply'].count('مو متوفر عدنا حالياً'), 1)
        self.assertIn('بديل فستان وردة متوفر لعمر 6 سنوات ولعمر سنتين', result['reply'])
        self.assertNotIn('13 ألف', result['reply'])
        for changed in (dict(product, stock='متوفر', stock_quantity=0), dict(product, price='')):
            result = child_availability.reply(changed, [changed, alternative], QUESTION)
            self.assertEqual(result['reply'].count('مو متوفر عدنا حالياً'), 1)
            self.assertEqual(result['image_product_ids'], ['B'])

    def test_no_available_requested_ages_does_not_deny_entire_model_stock(self):
        product = dict(PRODUCT, sizes='8 إلى 12 سنوات')
        result = child_availability.reply(product, [product], QUESTION)
        self.assertIn('لعمر 6 سنوات مو ضمن قياساته', result['reply'])
        self.assertIn('لعمر سنتين مو ضمن قياساته', result['reply'])
        self.assertNotIn('مو متوفر عدنا حالياً', result['reply'])
        self.assertIn('ما عندنا بديل مؤكد', result['reply'])
        self.assertNotIn('؟', result['reply'])

    def test_two_missing_ages_can_have_two_separate_verified_alternatives(self):
        product = dict(PRODUCT, stock='نفذ')
        older = dict(ALTERNATIVE, product_id='C', product_name='فستان فراشة', sizes='5 إلى 7 سنوات', price='14000')
        result = child_availability.reply(product, [product, ALTERNATIVE, older], QUESTION)
        self.assertEqual(result['image_product_ids'], ['C', 'B'])
        self.assertIn('فستان فراشة متوفر لعمر 6 سنوات', result['reply'])
        self.assertIn('فستان وردة متوفر لعمر سنتين', result['reply'])
        self.assertIn('14 ألف', result['reply'])
        self.assertIn('12 ألف', result['reply'])

    def test_unknown_stock_or_age_table_requires_review_without_stock_claim(self):
        for product in (dict(PRODUCT, stock=''), dict(PRODUCT, sizes='')):
            result = child_availability.reply(product, [product], QUESTION)
            self.assertTrue(result['requires_human'])
            self.assertEqual(result['reply'], '')
        self.assertIsNone(child_availability.reply(None, [PRODUCT], QUESTION))

    def test_invalid_alternatives_are_not_proposed(self):
        for candidate in (dict(ALTERNATIVE, stock='نفذ'), dict(ALTERNATIVE, stock_quantity=0),
                          dict(ALTERNATIVE, store_id='al-fatena'), dict(ALTERNATIVE, category='طقم'),
                          dict(ALTERNATIVE, sizes='8 إلى 12 سنوات'), dict(ALTERNATIVE, price='غير معروف'),
                          dict(ALTERNATIVE, offer='بشرط شراء قطعتين')):
            result = child_availability.reply(PRODUCT, [PRODUCT, candidate], QUESTION)
            self.assertEqual(result['image_product_ids'], [], candidate)

    def test_known_color_is_not_reasked_or_ignored_in_alternative(self):
        result = child_availability.reply(dict(PRODUCT, sizes='1 إلى 7 سنوات'), [], QUESTION, known_color='وردي')
        self.assertNotIn('اللون اللي يعجبج', result['reply'])
        alternative = dict(ALTERNATIVE, colors='جوزي')
        result = child_availability.reply(PRODUCT, [PRODUCT, alternative], QUESTION, known_color='وردي')
        self.assertEqual(result['image_product_ids'], [])

    def test_compound_facts_conditions_and_bundle_prices_keep_their_full_flow(self):
        for question in (QUESTION + ' وشكد التوصيل؟', QUESTION + ' وشنو القماش؟',
                         'ثبتي لعمر 6 سنوات', QUESTION + ' إذا يوصل باجر', QUESTION + ' باللون الازرق'):
            self.assertIsNone(child_availability.reply(PRODUCT, [PRODUCT], question))
        self.assertIsNone(child_availability.reply(dict(PRODUCT, price='بكج 3 قطع سعره 25 الف'), [PRODUCT], QUESTION))
        self.assertIsNone(child_availability.reply(PRODUCT, [PRODUCT], 'عمره سنتين وجسمه خشن شوي'))


class ChildAvailabilityPipelineTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set(baraah.STORE_ID)
        fixtures.AuditFixTests.customer(self, 'two-ages', baraah.STORE_ID)
        self.catalog = [PRODUCT, ALTERNATIVE]
        self.patches = [patch.object(self.m, 'load_products_from_file', return_value=self.catalog),
                        patch.object(self.m, 'load_active_products', return_value=self.catalog),
                        patch.object(self.m, 'send_telegram_message', return_value=False),
                        patch.object(self.m, 'send_telegram_photo', return_value=False),
                        patch.object(self.m, 'is_store_feature_enabled', return_value=False)]
        for item in self.patches: item.start()

    def tearDown(self):
        for item in reversed(self.patches): item.stop()
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def event(self, text=QUESTION, image=None):
        return dict(sender_id='two-ages', text=text, image_url=image, attachments=[], page_id='page',
                    platform='facebook', store_id=baraah.STORE_ID, ref=None, ad_id=None,
                    timestamp=0, postback=None, quick_reply=None, referral_source=None, referral_type=None)

    def test_recognized_photo_answers_both_ages_and_only_sends_verified_alternative_image(self):
        event = self.event(image='https://images.test/customer.jpg')
        event['_image_matches'] = [dict(product_found=True, product_id='A', image_index=1)]
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.call_main_ai(event, 'image', {}, [], self.catalog, PRODUCT, {}, '', [], customer_products=[PRODUCT])
        model.assert_not_called()
        self.assertEqual([c['available'] for c in result['_child_age_results']], [True, False])
        self.assertEqual(self.m.requested_reply_images(result, self.catalog, PRODUCT, QUESTION, result['reply']),
                         ['https://images.test/b.jpg'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_unknown_photo_stays_for_review_without_generic_holding_or_fake_unavailability(self):
        result = self.m.pending_image_policy_reply(self.db, self.event(image='https://images.test/unknown.jpg'),
                                                  dict(human_review_id=123, image_unresolved=True))
        self.assertEqual(result['reply'], '')
        self.assertEqual(result['meta']['human_review_id'], 123)
        self.assertFalse(result.get('send_image'))

    def test_image_service_failure_does_not_become_out_of_stock(self):
        event = self.event(image='https://images.test/unknown.jpg')
        with patch.object(self.m, 'extract_facebook_event', return_value=event), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'get_auto_product_settings', return_value=dict(enabled=False, product=None)), \
             patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=False, service_error=True, error_code='billing')), \
             patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        model.assert_not_called()
        self.assertEqual(result['reply'], '')
        self.assertTrue(result['meta']['human_review_id'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_manual_resolution_after_photo_recovers_original_two_age_question(self):
        self.m.save_message(self.db, 'two-ages', 'incoming', 'text', QUESTION)
        self.m.save_message(self.db, 'two-ages', 'incoming', 'image', '', 'https://images.test/customer.jpg')
        self.m.complete_customer_product_link(self.db, 'two-ages', PRODUCT, 'manual', source='manual_admin')
        with patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'send_webhook_result_to_facebook', return_value=True) as send, \
             patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.auto_reply_after_product_link(self.db, 'two-ages', PRODUCT, staff_action=True)
        model.assert_not_called()
        self.assertTrue(result['sent'])
        self.assertIn('لعمر 6 سنوات متوفر', result['reply'])
        self.assertIn('لعمر سنتين مو ضمن قياساته', result['reply'])
        self.assertEqual(send.call_args.args[0]['image_urls'], ['https://images.test/b.jpg'])
        self.assertEqual(self.m.load_customer_products(self.db, 'two-ages')[0]['product_id'], 'A')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_manual_link_with_unknown_stock_creates_review_without_availability_claim(self):
        self.catalog[0] = dict(PRODUCT, stock='')
        self.m.save_message(self.db, 'two-ages', 'incoming', 'text', QUESTION)
        self.m.save_message(self.db, 'two-ages', 'incoming', 'image', '', 'https://images.test/customer.jpg')
        with patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'send_webhook_result_to_facebook') as send:
            result = self.m.auto_reply_after_product_link(self.db, 'two-ages', self.catalog[0], staff_action=True)
        self.assertFalse(result['sent'])
        self.assertEqual(result['reason'], 'child_availability_review')
        self.assertTrue(result['human_review_id'])
        send.assert_not_called()
