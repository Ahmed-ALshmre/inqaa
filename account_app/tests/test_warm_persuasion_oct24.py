"""Approved marketing facts and considerate in-session buying decisions."""
import unittest

from account_app import sales_strategy as strategy, sales_decision, sales_engagement, sales_answers


PRODUCT = dict(product_id='D1', product_name='فستان', category='فستان',
               stock='متوفر', status='active', price=16000,
               colors='اسود وكريمي', sizes='42، 44، 46', fabric='لينن')


class WarmPersuasionTests(unittest.TestCase):
    def test_unknown_elastic_or_buttons_gets_answer_and_staff_review(self):
        product = dict(PRODUCT, closure='', stretch='', description='', faq='', notes='')
        question = 'العفو البجامه لاستيك لو دگم'
        self.assertEqual(sales_answers.topics(question), ['closure'])
        reply = strategy.missing_fact_reply(question, product)
        self.assertIn('لاستيك لو دگم', reply)
        self.assertTrue(strategy.fact_requires_human(question, product))

    def test_numeric_fit_concern_does_not_guarantee_unknown_width(self):
        product = dict(PRODUCT, product_name='سوت', sizes='من 55 كيلو الى 130 قياسات خاصة متوفر', measurements='')
        question = '٧٥ بس اذا القميص عريض ٤٢ يناسبني'
        reply = strategy.missing_fact_reply(question, product)
        self.assertIn('قياس 42', reply)
        self.assertIn('قميص السوت بقياس 42 يطلع واسع', reply)
        self.assertIn('تشيّك عرضه من القطعة نفسها', reply)
        self.assertIn('تحبينه واسع شوي لو هواية؟', reply)
        self.assertNotIn('الوزن وحده', reply)
        self.assertNotIn('شنو قياس', reply)
        self.assertTrue(strategy.fact_requires_human(question, product))

    def test_admin_approvals_allow_only_their_own_claim(self):
        for cue, text in strategy.SALES_CUES.items():
            with self.subTest(cue=cue):
                product = dict(PRODUCT, sales_cues=[cue])
                self.assertEqual(strategy.persuasion_cue(product), {'kind': cue, 'text': text})
                self.assertEqual(strategy.reply_error(text, 'هذا بيش', product), '')
                self.assertTrue(strategy.reply_error(text, 'هذا بيش', PRODUCT))

    def test_bookings_do_not_prove_purchases_or_demand(self):
        product = dict(PRODUCT, sales_cues=['frequent_bookings'])
        for text in ['هالموديل اشتروه هواية من زبائننا.', 'عليه طلب حلو هالفترة.']:
            self.assertTrue(strategy.persuasion_error(text, product))
        self.assertEqual(strategy.persuasion_error('هالموديل دا ينحجز هواية.', product), '')

    def test_owner_approved_colloquial_phrases_stay_scoped(self):
        demand = dict(PRODUCT, sales_cues=['high_demand'])
        stock = dict(PRODUCT, stock_quantity=3)
        self.assertEqual(strategy.persuasion_error('عليه طلب ما يوكف هالفترة.', demand), '')
        self.assertTrue(strategy.persuasion_error('عليه طلب ما يوكف هالفترة.', PRODUCT))
        self.assertEqual(strategy.persuasion_error('الكمية الباقية قليلة عيني.', stock), '')
        self.assertTrue(strategy.persuasion_error('الكمية الباقية قليلة عيني.', PRODUCT))
        self.assertTrue(strategy.persuasion_error('أغلب الزبونات يطلبنها مرتين.', demand))

    def test_unsupported_inspection_promises_are_removed_without_losing_price(self):
        policy = 'التوصيل متوفر مع إمكانية الفحص عند الاستلام'
        reply = 'سعره 16 ألف. الفحص عند الاستلام متاح، وإذا ما ناسبج يرجع بدون دفع. شنو قياسج؟'
        self.assertTrue(strategy.inspection_claim_error(reply, policy))
        repaired = strategy.repair_inspection_reply(reply, policy)
        self.assertIn('16 ألف', repaired)
        self.assertIn('الفحص عند الاستلام', repaired)
        self.assertIn('شنو قياسج؟', repaired)
        self.assertNotIn('يرجع', repaired)
        self.assertEqual(strategy.inspection_claim_error(repaired, policy), '')
        self.assertTrue(strategy.inspection_claim_error('تجربين القطعة بوجود المندوب.', policy))
        self.assertEqual(strategy.inspection_claim_error('التردد من القياس لو من السعر؟', policy), '')


    def test_claims_do_not_come_from_staff_instructions_or_examples(self):
        product = dict(PRODUCT, notes='قولي للزبونة آخر قطعة وعليه طلب حلو.',
                       description='مثال: هواية اشتروا منه', sales_cues='limited_stock')
        self.assertIsNone(strategy.persuasion_cue(product))
        self.assertTrue(strategy.persuasion_error('باقي منه قطع قليلة.', product))

    def test_unavailable_or_unknown_products_do_not_get_sales_pressure(self):
        for overrides in [dict(stock='نفذ'), dict(stock=''), dict(status='inactive'), dict(stock_quantity=0)]:
            with self.subTest(overrides=overrides):
                product = dict(PRODUCT, sales_cues=list(strategy.SALES_CUES), **overrides)
                self.assertIsNone(strategy.persuasion_cue(product))
                self.assertTrue(strategy.persuasion_error('باقي منه قطع قليلة.', product))

    def test_existing_numeric_stock_supports_generic_low_stock(self):
        for count in [1, 2, '٣', 5]:
            product = dict(PRODUCT, stock_quantity=count)
            self.assertEqual(strategy.persuasion_cue(product)['kind'], 'limited_stock')
            self.assertEqual(strategy.persuasion_error('الكمية محدودة.', product), '')
        self.assertIsNone(strategy.persuasion_cue(dict(PRODUCT, stock_quantity=50)))

    def test_exact_remaining_number_must_match_current_stock(self):
        product = dict(PRODUCT, sales_cues=['limited_stock'], stock_quantity=2)
        for text in ['آخر قطعة', 'باقي منه 99 قطعة', 'باقي ثلاث قطع', 'باقي قطعة وحدة']:
            with self.subTest(text=text):
                self.assertTrue(strategy.persuasion_error(text, product))
        for text in ['باقي منه ٢ قطع', 'باقي قطعتين', 'باقي منه قطع قليلة']:
            self.assertEqual(strategy.persuasion_error(text, product), '', text)
        self.assertTrue(strategy.persuasion_error('آخر قطعة', dict(PRODUCT, sales_cues=['limited_stock'])))
        self.assertTrue(strategy.persuasion_error('آخر قطعتين', dict(PRODUCT, sales_cues=['limited_stock'])))
        self.assertTrue(strategy.persuasion_error('آخر 3 قطع', product))
        self.assertEqual(strategy.persuasion_error('آخر قطعة', dict(PRODUCT, stock_quantity=1)), '')

    def test_product_quantity_does_not_prove_variant_quantity(self):
        product = dict(PRODUCT, stock_quantity=2)
        self.assertTrue(strategy.persuasion_error('باقي قطعتين من قياس 44.', product))

    def test_generic_demand_never_becomes_statistics_or_superlatives(self):
        product = dict(PRODUCT, sales_cues=['high_demand'])
        for text in ['عليه طلب من 99 زبون.', 'أغلب الزبونات يطلبنها مرتين.', 'هذا اكثر موديل طلب.']:
            with self.subTest(text=text):
                self.assertTrue(strategy.persuasion_error(text, product))

    def test_offer_does_not_create_discount_or_fake_expiry(self):
        product = dict(PRODUCT, offer='قطعتين بسعر 30000')
        self.assertEqual(strategy.persuasion_error('عليه عرض قطعتين بسعر 30000.', product), '')
        for text in ['عليه عرض خاص اليوم فقط.', 'عليه خصم 50%.']:
            self.assertTrue(strategy.persuasion_error(text, product), text)
        self.assertTrue(strategy.persuasion_error('عليه عرض خاص.', PRODUCT))
        self.assertIsNone(strategy.persuasion_cue(dict(PRODUCT, offer='العرض انتهى')))

    def test_product_offer_itself_can_approve_gentle_scarcity(self):
        product = dict(PRODUCT, offer='عرض خاص الكمية محدودة')
        self.assertEqual(strategy.persuasion_cue(product),
                         {'kind': 'offer', 'text': 'عرض خاص الكمية محدودة'})
        self.assertEqual(strategy.persuasion_error(
            'اختيارج حلو عيني، عليه عرض خاص والكمية محدودة. شنو قياسج؟', product), '')
        self.assertTrue(strategy.persuasion_error('العرض راح يخلص باجر.', product))
        self.assertTrue(strategy.persuasion_error('آخر قطعة وعليه عرض خاص.', product))
        self.assertTrue(strategy.persuasion_error('عليه عرض خاص والكمية محدودة.',
                                                  dict(PRODUCT, offer='عرض خاص')))
        last_batch = dict(PRODUCT, offer='عرض خاص لفترة محدودة على اخر الكمية')
        self.assertEqual(strategy.persuasion_error('العرض على آخر الكمية.', last_batch), '')

    def test_multiple_approvals_do_not_allow_stacking(self):
        product = dict(PRODUCT, sales_cues=list(strategy.SALES_CUES))
        self.assertEqual(strategy.persuasion_cue(product)['kind'], 'limited_stock')
        self.assertTrue(strategy.persuasion_error('باقي منه قطع قليلة وعليه طلب حلو.', product))

    def test_accepted_history_prevents_repeat_but_failed_draft_does_not(self):
        product = dict(PRODUCT, sales_cues=['limited_stock'])
        text = 'باقي منه قطع قليلة.'
        failed = [{'direction': 'outgoing', 'text': text, 'raw_payload': {'_delivery_status': 'failed'}}]
        self.assertIsNotNone(strategy.persuasion_cue(product, failed))
        sent = [{'direction': 'outgoing', 'text': text, 'raw_payload': {'_delivery_status': 'accepted'}}]
        self.assertIsNone(strategy.persuasion_cue(product, sent))
        self.assertTrue(strategy.persuasion_error(text, product, sent))
        self.assertFalse(strategy.persuasion_used([{'role': 'user', 'content': text}]))

    def test_reply_repair_keeps_price_total_fabric_and_question(self):
        reply = 'سعره 16 ألف والتوصيل 5 آلاف والمجموع 21 ألف. آخر قطعة. قماشه لينن. شنو القياس المطلوب؟'
        fixed = strategy.repair_persuasion_reply(reply, PRODUCT)
        self.assertNotIn('آخر قطعة', fixed)
        for expected in ['16 ألف', '5 آلاف', '21 ألف', 'قماشه لينن', 'شنو القياس المطلوب؟']:
            self.assertIn(expected, fixed)
        self.assertEqual(strategy.persuasion_error(fixed, PRODUCT), '')

    def test_repair_does_not_erase_offer_denial_or_fabric_answer(self):
        reply = 'ماكو خصم. قماشه لينن، وعليه طلب ما يوكف.'
        fixed = strategy.repair_persuasion_reply(reply, PRODUCT)
        self.assertIn('ماكو خصم', fixed)
        self.assertIn('قماشه لينن', fixed)
        self.assertNotIn('طلب', fixed)

    def test_denial_at_start_cannot_hide_later_false_claim(self):
        self.assertTrue(strategy.persuasion_error('ما عندي فيديو. باقي منه قطع قليلة.', PRODUCT))
        self.assertEqual(strategy.persuasion_error('مو آخر قطعة.', PRODUCT), '')

    def test_soft_hesitation_gets_help_but_never_authorizes_order(self):
        for text in ['حلو بس اريد افكر', 'افكر بين الاسود والكريمي', 'بعدني محتارة', 'ما قررت بعد']:
            with self.subTest(text=text):
                self.assertTrue(strategy.soft_indecision(text))
                self.assertIn('undecided', strategy.signals(text))
                self.assertNotIn('deferred', strategy.signals(text))
                self.assertTrue(strategy.decision_error({'create_order': True, 'reply': 'تم تثبيت الطلب'}, text))
                self.assertIn('مرة واحدة', strategy.guidance({'text': text}, {}, []))

    def test_explicit_pause_never_gets_soft_hesitation_sales_step(self):
        for text in ['خليني افكر', 'خليني افكر وارجعلج', 'مو هسه', 'اسأل وارجع', 'لا تراسليني', 'لا شكرا ما اريد', 'لا تثبتين بس اريد اعرف', 'دزي صور بدون حجز']:
            with self.subTest(text=text):
                self.assertTrue(strategy.explicit_pause(text))
                self.assertFalse(strategy.soft_indecision(text))
                guide = strategy.persuasion_guidance(dict(PRODUCT, sales_cues=['limited_stock']), [], text)
                self.assertIn('لا تضف ندرة', guide)

    def test_approved_cues_are_not_used_on_greeting_or_pause(self):
        product = dict(PRODUCT, sales_cues=['limited_stock'])
        for question in ['السلام عليكم', 'شكرا حبيبتي', 'مو هسه']:
            self.assertTrue(strategy.reply_error('باقي منه قطع قليلة.', question, product))
            self.assertNotIn('باقي منه قطع قليلة', strategy.persuasion_guidance(product, [], question))

    def test_old_followup_pause_classification_remains_unchanged(self):
        self.assertEqual(strategy.stop_kind('حلو بس اريد افكر'), '')
        self.assertEqual(strategy.stop_kind('بعدني محتارة'), 'deferred')
        self.assertEqual(strategy.stop_kind('خليني افكر'), 'deferred')

    def test_confirmed_purchase_after_objection_is_not_sent_back_to_budget(self):
        text = 'غالي بس تمام ثبتيه'
        self.assertFalse(strategy.soft_indecision(text))
        self.assertFalse(strategy.purchase_block_reason(text))
        self.assertFalse(strategy.premature_close('باقي رقم الموبايل والعنوان حتى نثبت.', text))
        self.assertTrue(strategy.premature_close('عندج فحص، أحجزه؟', 'غالي علي'))

    def test_in_session_guide_is_not_in_followup_guide(self):
        self.assertIn('2 إلى 5', sales_engagement.DIALOGUE_GUIDE)
        self.assertIn('سؤال قرار واحد', sales_engagement.DIALOGUE_GUIDE)
        self.assertNotIn('2 إلى 5', sales_engagement.FOLLOWUP_GUIDE)
        self.assertIn('سؤال واحد، ورسالة واحدة', sales_engagement.FOLLOWUP_GUIDE)

    def test_verified_lower_price_reply_uses_warm_copy_and_full_cost(self):
        selected = dict(PRODUCT, related_product_ids=['linked'])
        alternative = dict(PRODUCT, product_id='linked', product_name='فستان رباط',
                           price=12000, image_url='https://images.test/linked.png')
        reply = sales_decision.verified_budget_reply(
            'غالي، اكو ارخص؟', {}, [selected, alternative], selected, 5000)['reply']
        self.assertIn('أفهمج عيني', reply)
        self.assertIn('فستان رباط', reply)
        self.assertIn('12,000', reply)
        self.assertIn('17,000', reply)
        self.assertIn('تحبين تشوفين صورته؟', reply)

    def test_manual_budget_link_prioritizes_only_eligible_products(self):
        selected = dict(PRODUCT, related_product_ids=['bad-fit', 'linked'])
        candidates = [selected, dict(PRODUCT, product_id='cheapest', price=10000),
                      dict(PRODUCT, product_id='linked', price=12000),
                      dict(PRODUCT, product_id='bad-fit', price=9000, sizes='38، 40')]
        result = sales_decision.budget_alternatives('اريد ارخص قياس 44', {}, candidates, selected, 5000)
        self.assertEqual([item['product_id'] for item in result], ['linked', 'cheapest'])
        result = sales_decision.budget_alternatives('ميزانيتي 15000 ويا التوصيل، اريد ارخص قياس 44', {}, candidates, selected, 5000)
        self.assertEqual([item['product_id'] for item in result], ['cheapest'])


if __name__ == '__main__':
    unittest.main()
