"""Decision-quality regressions with synthetic data, never conversion claims.

The pipeline cases use real routing and temporary SQLite persistence. Network
providers are stubbed so no customer, model or delivery endpoint is contacted.
"""
import json
import unittest
from unittest.mock import patch

from account_app import sales_answers, sales_context, sales_decision, sales_strategy, sales_engagement
from account_app.tests import test_audit_orders as fixtures


PRODUCT = dict(product_id='A', product_name='فستان الاختبار', category='فستان',
               price='20000', colors='اسود، كريمي', sizes='38 إلى 52',
               fabric='قطن', stock='متوفر', status='active',
               image_url='https://images.test/deep-sales.jpg')


def conversation(*turns):
    return sales_context.advance({}, [dict(direction=direction, text=text)
                                     for direction, text in turns])


class DecisionEvidenceTests(unittest.TestCase):
    def test_short_budget_answer_survives_intervening_turns(self):
        state = conversation(('outgoing', 'شنو حدود الميزانية المناسبة ويا التوصيل؟'),
                             ('incoming', '18 الف ويا التوصيل'),
                             ('outgoing', 'خل نتأكد من الخيارات المناسبة.'),
                             ('incoming', 'اريد ارخص'))
        self.assertEqual(sales_answers.known_budget('اريد ارخص', state), 18000)
        options = [dict(PRODUCT, product_id='B', price='12000'),
                   dict(PRODUCT, product_id='C', price='16000')]
        result = sales_decision.budget_alternatives('اريد ارخص', state, options, PRODUCT, 5000)
        self.assertEqual([item['product_id'] for item in result], ['B'])
        self.assertEqual(result[0]['budget_scope'], 'with_delivery')

    def test_later_budget_correction_wins(self):
        state = conversation(('incoming', 'ميزانيتي 25 الف'),
                             ('outgoing', 'شنو حدود الميزانية الحالية؟'),
                             ('incoming', '15 الف'))
        self.assertEqual(sales_answers.known_budget('اريد ارخص', state), 15000)
        self.assertEqual(sales_answers.known_budget('ميزانيتي 20 الف', state), 20000)

    def test_short_size_is_used_to_reject_smaller_alternative(self):
        state = conversation(('outgoing', 'شنو القياس المطلوب؟'), ('incoming', '48'),
                             ('outgoing', 'متوفر، وسعره عشرين ألف.'),
                             ('incoming', 'اريد ارخص'))
        options = [dict(PRODUCT, product_id='B', price='12000', sizes='38 إلى 42'),
                   dict(PRODUCT, product_id='C', price='14000', sizes='44 إلى 52')]
        self.assertEqual([item['product_id'] for item in sales_decision.budget_alternatives(
            'اريد ارخص', state, options, PRODUCT, 5000)], ['C'])

    def test_number_for_quantity_is_never_reinterpreted_as_size(self):
        item = dict(text='44', in_reply_to='شنو العدد المطلوب؟')
        self.assertEqual(sales_answers.evidence_text(item), '44')
        item['in_reply_to'] = 'شنو القياس والعمر؟'
        self.assertEqual(sales_answers.evidence_text(item), '44')

    def test_short_age_needs_explicit_year_unit(self):
        self.assertEqual(sales_answers.evidence_text(dict(text='6', in_reply_to='شنو العمر بالسنوات؟')), 'عمر 6 سنوات')
        self.assertEqual(sales_answers.evidence_text(dict(text='6', in_reply_to='شنو العمر؟')), '6')

    def test_rejected_color_does_not_survive_as_current_preference(self):
        state = conversation(('incoming', 'اريد الاسود قياس 44'),
                             ('outgoing', 'هذا المتوفر.'),
                             ('incoming', 'لا اريد الاسود اريد غيره وارخص'))
        options = [dict(PRODUCT, product_id='B', price='12000', colors='اسود'),
                   dict(PRODUCT, product_id='C', price='14000', colors='كريمي'),
                   dict(PRODUCT, product_id='D', price='15000', colors='اسود، كريمي')]
        result = sales_decision.budget_alternatives('اريد ارخص', state, options, PRODUCT, 5000)
        self.assertEqual([item['product_id'] for item in result], ['C', 'D'])
        for item in result:
            self.assertEqual(item['available_colors'], ['كريمي'])

    def test_rejected_size_needs_new_choice_before_verified_alternative(self):
        state = conversation(('incoming', 'قياس 44'), ('incoming', 'مو قياس 44 اريد ارخص'))
        self.assertEqual(sales_decision.budget_alternatives('اريد ارخص', state,
                         [dict(PRODUCT, product_id='B', price='12000')], PRODUCT, 5000), [])

    def test_corrected_size_does_not_require_both_old_and_new_sizes(self):
        state = conversation(('incoming', 'قياس 44'), ('incoming', 'مو قياس 44 اريد قياس 48'))
        result = sales_decision.budget_alternatives('اريد ارخص', state,
                 [dict(PRODUCT, product_id='B', price='12000', sizes='48 إلى 52')], PRODUCT, 5000)
        self.assertEqual([item['product_id'] for item in result], ['B'])

    def test_customer_height_does_not_discard_lining_and_closure_questions(self):
        state = conversation(('incoming', 'طولي 160، بيها بطانة لو لا؟ والإغلاق كبس؟'),
                             ('outgoing', 'القياس المطلوب؟'), ('incoming', '44'))
        self.assertEqual(state['open_details'], ['lining', 'closure'])

    def test_waived_length_does_not_discard_new_width_question(self):
        pending = sales_decision.advance_details(['length'], 'incoming', 'ما يهمني الطول، بس العرض شكد؟')
        self.assertEqual(pending, ['width'])

    def test_explicit_closure_answer_resolves_pending_decision(self):
        for answer in ['الإغلاق بسحاب.', 'الإغلاق: سحاب خلفي.', 'تجي بكبس.']:
            with self.subTest(answer=answer):
                self.assertEqual(sales_decision.advance_details(['closure'], 'outgoing', answer), [])
        self.assertEqual(sales_decision.advance_details(['closure'], 'outgoing', 'الإغلاق يحتاج تأكيد من المتجر.'), ['closure'])


class CompoundAnswerTests(unittest.TestCase):
    def test_every_decision_detail_is_checked_after_price_answer(self):
        cases = [('سعره وبيها بطانة؟', 'البطانة'),
                 ('سعره وينفتح بكبس؟', 'الإغلاق'),
                 ('سعره والعرض شكد؟', 'عرض القطعة'),
                 ('سعره وطولها شكد؟', 'طول القطعة'),
                 ('سعره ومطاط لو لا؟', 'المطاط'),
                 ('سعره ومتى يوصل؟', 'مدة الوصول'),
                 ('سعره وشلون الدفع؟', 'طريقة الدفع'),
                 ('سعره واكدر افحص؟', 'الفحص والإرجاع'),
                 ('سعره والوانه شنو؟', 'الألوان'),
                 ('سعره وشنو قياساته؟', 'القياسات')]
        for question, label in cases:
            with self.subTest(question=question):
                self.assertIn(label, sales_answers.missing_answers('سعره 20 ألف.', question, PRODUCT))

    def test_shipping_fee_does_not_answer_delivery_duration(self):
        missing = sales_answers.missing_answers('سعره 20 ألف، التوصيل 5 آلاف.',
                                                'سعره وشكد التوصيل ومتى يوصل؟', PRODUCT)
        self.assertEqual(missing, ['مدة الوصول'])

    def test_compound_answer_can_cover_all_details_without_another_question(self):
        reply = 'سعره 20 ألف، الألوان أسود وكريمي، القياسات 38 إلى 52. الإغلاق بسحاب وبيها بطانة. التوصيل 5 آلاف، خلال يومين، والدفع عند الاستلام.'
        question = 'شنو سعره والوانه وقياساته وبيها بطانة وإغلاق سحاب؟ شكد التوصيل ومتى يوصل وشلون الدفع؟'
        self.assertEqual(sales_answers.missing_answers(reply, question, PRODUCT), [])

    def test_declared_variant_is_not_mistaken_for_an_unanswered_options_question(self):
        self.assertEqual(sales_answers.missing_answers('قماشه قطن، سعره 20 ألف.',
                         'اريد الاسود قياس 44 وشنو سعره وقماشه؟', PRODUCT), [])
        self.assertEqual(sales_answers.missing_answers('سعره 20 ألف.',
                         'قياس 44 وزني 80 وسعره شكد؟', PRODUCT), [])

    def test_request_for_one_piece_is_not_a_question_about_composition(self):
        facts, gaps = sales_answers.product_fragments('اريد قطعة وحدة شكد سعرها وشنو قماشها؟', PRODUCT)
        self.assertNotIn('composition', sales_answers.topics('اريد قطعة وحدة شكد سعرها؟'))
        self.assertFalse(gaps)
        self.assertTrue(any('20,000' in fact for fact in facts))
        self.assertIn('composition', sales_answers.topics('هو قطعتين لو فستان؟'))

    def test_recorded_numerical_measurement_is_shared_by_answer_and_handoff_check(self):
        product = dict(PRODUCT, notes='عرض القطعة 54 سم، طولها 110 سم.')
        facts, gaps = sales_answers.product_fragments('عرضها وطولها شكد؟', product)
        self.assertTrue(any('54' in fact and '110' in fact for fact in facts))
        self.assertFalse(gaps)
        self.assertFalse(sales_strategy.fact_requires_human('عرضها وعنده فيديو؟', product))

    def test_unknown_video_does_not_turn_known_width_into_human_review(self):
        product = dict(PRODUCT, measurements='عرض القطعة 54 سم')
        self.assertIn('ما عندي فيديو', sales_strategy.missing_fact_reply('عرضها وعنده فيديو؟', product))
        self.assertFalse(sales_strategy.fact_requires_human('عرضها وعنده فيديو؟', product))
        self.assertTrue(sales_strategy.fact_requires_human('العرض شكد؟', PRODUCT))

    def test_adjective_or_staff_example_is_not_a_measured_fact(self):
        for value in ['عرض القطعة مناسب', 'عرض القطعة يحتاج تأكيد',
                      'قولي للزبون عرض القطعة 54 سم']:
            with self.subTest(value=value):
                product = dict(PRODUCT, measurements=value)
                facts, gaps = sales_answers.product_fragments('العرض شكد؟', product)
                self.assertFalse(facts)
                self.assertTrue(gaps)
                self.assertTrue(sales_strategy.fact_requires_human('العرض شكد؟', product))

    def test_explicit_size_is_not_reasked_because_weight_is_also_present(self):
        for text in ['قياس 44 وزني 80', 'اريد XL وزني 80', 'قياس XL وزني 80']:
            self.assertEqual(sales_strategy.missing_fact_reply(text, PRODUCT), '')

    def test_salary_received_and_purchase_confirmed_does_not_trigger_deferral_advice(self):
        selected = sales_strategy.signals('استلمت الراتب ثبتي الطلب قياس 44')
        self.assertIn('ready', selected)
        self.assertNotIn('deferred', selected)
        self.assertIn('deferred', sales_strategy.signals('خليني استلم الراتب وبعدين اطلب'))


class DeepSalesPipelineTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def test_failed_outgoing_does_not_answer_the_customers_pending_lining_question(self):
        self.customer('unsent-answer')
        self.m.save_message(self.db, 'unsent-answer', 'incoming', 'text', 'بيها بطانة؟', None, None, None, {})
        self.m.save_message(self.db, 'unsent-answer', 'outgoing', 'text', 'بيها بطانة قطن.', None, None, None,
                            {'_delivery_status': 'unconfirmed'})
        state = sales_context.load(self.db, self.m.current_store_id(), 'unsent-answer')
        self.assertEqual(state['open_details'], ['lining'])
        self.assertFalse(state['last_staff_turn'])

    def test_failed_outgoing_keeps_customer_waiting_for_an_answer_instead_of_followup(self):
        self.customer('unsent-followup')
        self.m.save_message(self.db, 'unsent-followup', 'incoming', 'text', 'شكد سعره؟', None, None, None, {})
        self.m.save_message(self.db, 'unsent-followup', 'outgoing', 'text', 'سعره عشرين ألف.', None, None, None,
                            {'_delivery_status': 'unconfirmed'})
        self.assertEqual(sales_engagement.followup_block_reason(self.db, 'unsent-followup', self.m.now_baghdad_iso()),
                         'customer_waiting_for_answer')

    def test_many_split_bubbles_do_not_hide_first_bubble_optout_from_followup(self):
        self.customer('split-optout')
        for direction, text in [('outgoing', 'السعر عشرين ألف.'),
                                ('incoming', 'ما اريد رسائل'),
                                *[('incoming', 'عيني') for _ in range(15)],
                                ('outgoing', 'حاضر.')]:
            self.m.save_message(self.db, 'split-optout', direction, 'text', text, None, None, None, {})
        self.assertEqual(sales_engagement.followup_block_reason(self.db, 'split-optout', self.m.now_baghdad_iso()),
                         'customer_declined_or_deferred')

    def test_old_cached_memory_is_rebuilt_to_recover_previously_missed_budget(self):
        self.customer('old-memory')
        for direction, text in [('outgoing', 'شنو حدود الميزانية؟'), ('incoming', '15 الف')]:
            self.m.save_message(self.db, 'old-memory', direction, 'text', text, None, None, None, {})
        state = sales_context.load(self.db, self.m.current_store_id(), 'old-memory')
        self.assertEqual(sales_answers.known_budget('اريد ارخص', state), 15000)
        state.pop('evidence_version')
        state['evidence'] = []
        self.db.execute('UPDATE sales_conversation_context SET data=? WHERE sender_id=?',
                        (json.dumps(state), 'old-memory'))
        self.db.commit()
        rebuilt = sales_context.load(self.db, self.m.current_store_id(), 'old-memory')
        self.assertEqual(sales_answers.known_budget('اريد ارخص', rebuilt), 15000)

    def test_omitted_lining_is_repaired_with_factual_answer_before_any_checkout(self):
        self.customer('lining')
        product = dict(PRODUCT, lining='بطانة قطن')
        responses = [dict(reply='سعره 20 ألف.', create_order=False, order={}),
                     dict(reply='سعره 20 ألف وبيه بطانة قطن.', create_order=False, order={})]
        with patch.object(self.m, '_call_main_ai_once', side_effect=responses) as provider:
            result = self.m.call_main_ai(dict(sender_id='lining', text='سعره وبيها بطانة؟'),
                      'text', {}, [], [product], product, None, '', [], customer_products=[product])
        self.assertEqual(provider.call_count, 2)
        self.assertIn('بطانة', result['reply'])
        self.assertFalse(result['create_order'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_repeated_omission_recovers_price_and_missing_detail_together(self):
        self.customer('fallback')
        with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='سعره 20 ألف.', create_order=False, order={})) as provider:
            result = self.m.call_main_ai(dict(sender_id='fallback', text='سعره وبيها بطانة؟'),
                      'text', {}, [], [PRODUCT], PRODUCT, None, '', [], customer_products=[PRODUCT])
        self.assertEqual(provider.call_count, 2)
        self.assertIn('20,000', result['reply'])
        self.assertIn('بطانة', result['reply'])
        self.assertTrue(result['_needs_fact_review'])
        self.assertFalse(result['create_order'])

    def test_height_and_size_answers_do_not_allow_bypassing_prior_lining_question(self):
        self.customer('pending')
        for direction, text in [('incoming', 'طولي 160 وبيها بطانة؟'),
                                ('outgoing', 'شنو القياس المطلوب؟'), ('incoming', '44')]:
            self.m.save_message(self.db, 'pending', direction, 'text', text, None, None, None, {})
        order = dict(items=[dict(product_id='A', size='44', color='اسود', quantity=1)])
        with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='نكمل الطلب', create_order=True, order=order)):
            result = self.m.call_main_ai(dict(sender_id='pending', text='ثبتيه'),
                      'text', {}, [], [PRODUCT], PRODUCT, None, '', [], customer_products=[PRODUCT])
        self.assertFalse(result['create_order'])
        self.assertTrue(result['_needs_fact_review'])
        self.assertIn('البطانة', result['reply'])
        self.assertEqual(result['order']['items'][0]['size'], '44')


if __name__ == '__main__':
    unittest.main()
