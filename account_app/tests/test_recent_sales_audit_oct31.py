"""Synthetic regressions derived from the 100-conversation audit; no customer data."""
import json
import unittest
from unittest.mock import patch

from account_app import product_variants, sales_answers, sales_context, sales_playbook, conversation_quality, reply_layout, sales_strategy, checkout, color_choices, sales_decision, owner_sales
from account_app.tests import test_stale_detail_checkout_oct28 as fixtures, test_post_order_service as service


class RecentSalesSelectionTests(unittest.TestCase):
    def test_ignored_size_questions_stop_without_losing_facts_or_other_questions(self):
        history = [dict(direction='outgoing', text='شكد قياس تلبسين؟'),
                   dict(direction='incoming', text='شنو خامته؟'),
                   dict(direction='outgoing', text='قماشه لينن. شنو القياس اللي يناسبج؟')]
        reply = conversation_quality.defer_repeated_choice_questions('سعره 16 ألف. شنو القياس المطلوب؟', history)
        self.assertEqual(reply, 'سعره 16 ألف.')
        self.assertIn('يا لون', conversation_quality.defer_repeated_choice_questions('متوفر. يا لون مطلوب؟', history))
        self.assertIn('قياسات', conversation_quality.defer_repeated_choice_questions('قياسات 38 إلى 52 متوفرة، شنو القياس المطلوب؟', history))
        history = [dict(direction='outgoing', text='شكد الكمية اللي تحتاجها للمحل؟'),
                   dict(direction='outgoing', text='شنو الكمية اللي تناسبك؟')]
        self.assertEqual(conversation_quality.defer_repeated_choice_questions('القماش فور وي. شنو الكمية والألوان المطلوبة؟', history), 'القماش فور وي.')

    def test_washing_claims_require_documented_facts(self):
        facts, gaps = sales_answers.product_fragments('يكش بالغسل لو يذب لون؟', dict(fabric='لينن'))
        self.assertFalse(gaps)
        self.assertTrue(any('مو موضحة' in fact for fact in facts))
        self.assertIn('لا يكش بالغسل', sales_answers.care_fact(dict(description='قماش لا يكش بالغسل.')))

    def test_male_photo_wording_and_known_province_survive_local_reply(self):
        reply = owner_sales.surface('هاي صور الفستان، شوفيهن براحتج. طلبج مسجل.', 'male', store_id='al-fatena')
        self.assertIn('شوفهن براحتك', reply)
        self.assertIn('طلبك', reply)
        neutral = owner_sales.surface('تفحصين القطعة وتتأكدين منها وما تدفعين شي إذا ترجعينها.', '')
        self.assertNotRegex(neutral, r'تفحصين|تتأكدين|تدفعين|ترجعين')
        context = sales_context.advance({}, [dict(direction='incoming', text='قياس 44 أسود')])
        step = owner_sales.next_step(dict(sizes='38 إلى 52', colors='أسود'), context, {},
                                    [dict(direction='incoming', text='شكد المجموع للنجف؟')])
        self.assertNotIn('محافظة', step)

    def selection(self, *texts):
        return sales_playbook.customer_selection(sales_context.advance({}, [
            dict(direction='incoming', text=text) for text in texts]))

    def test_size_aliases_work_for_customer_and_catalog(self):
        for text in ('2XL', '٢xl', 'XXL', '٢ اكس', 'اكسين'):
            with self.subTest(text=text):
                self.assertEqual(self.selection(text)['size'], 'XXL')
                self.assertTrue(product_variants.assess(dict(sizes='XXL'), text)[0]['supported'])
        self.assertEqual(product_variants.size_data('2XL XXXL')['labels'], ['XXL', '3XL'])

    def test_explicit_new_size_replaces_old_during_side_questions(self):
        for question in ('اني قياسي 44، متوفر عندكم؟', 'مقاسي 44 اكو؟'):
            self.assertEqual(self.selection(question)['size'], '44')
        for text in ('لا اعتمدي 46 بدل 44، نفس الاسود قطعة وحدة ثبتي',
                     'قياس ٤٦ مو ٤٤', 'غيري قياس 44 الى 46', '46 بدل 44'):
            with self.subTest(text=text):
                self.assertEqual(self.selection('قياس 44 اسود', 'شنو القماش؟', text)['size'], '46')
        self.assertIsNone(self.selection('قياس 42 لو 44')['size'])
        self.assertEqual(product_variants.requested('قياس 46 بدل 44 متوفر؟')['sizes'], ['46'])

    def test_age_correction_is_not_a_two_child_cart(self):
        for text in ('اقصد عمرها 4 سنوات مو 3 سنوات', 'عمر ٤ سنوات بدل ٣ سنوات'):
            with self.subTest(text=text):
                self.assertEqual(product_variants.child_age(text), 4)
                self.assertEqual(conversation_quality.child_ages(text), [4])
                self.assertEqual(self.selection('عمرها 3 سنوات', text)['ages'], [4])
        self.assertEqual(conversation_quality.child_ages('اريد عمر 3 سنوات وعمر 4 سنوات'), [3, 4])
        self.assertEqual(product_variants.child_age('عمرها سنة و3 اشهر'), 1.25)
        self.assertEqual(product_variants.child_age('خمس سنوات'), 5)
        self.assertEqual(conversation_quality.child_ages('اريد ست سنوات وثلاث سنوات'), [6, 3])
        self.assertEqual(conversation_quality.child_ages('اقصد عمرها 4 سنوات مو 3 سنوات وعندي طفل ثاني عمره 6 سنوات'), [4, 6])
        self.assertEqual(conversation_quality.child_ages('اريد سنة و3 اشهر وعمر 4 سنوات'), [1.25, 4])
        self.assertEqual(conversation_quality.child_ages('اريد عمر سنتين وعمر سنتين'), [2, 2])
        selected = self.selection('الاولى عمرها 3 سنوات والثانية 6 سنوات، موجود؟',
                                  'اريد للاصغر وردي وللكبيرة كريمي',
                                  'اقصد الاصغر عمرها 4 سنوات مو 3 سنوات')
        self.assertEqual(selected['ages'], [4, 6])

    def test_account_words_never_request_a_zipper_detail(self):
        for text in ('اذا اخذ قطعتين شكد يصير حسابهن؟', 'شكد الحساب النهائي ويا التوصيل؟',
                     'شكد حساب الدرزن؟', 'دفعت الحساب'):
            with self.subTest(text=text):
                self.assertNotIn('closure', sales_answers.topics(text))
        for text in ('السحاب ينفتح؟', 'اكو ساب من ورا؟', 'الدكم تنفتح؟'):
            self.assertIn('closure', sales_answers.topics(text))

    def test_understanding_the_offer_is_not_a_width_request(self):
        text = 'تمام فهمت العرض، شنو الالوان المتوفرة؟'
        product = dict(product_id='P', price='15000', colors='ماروني اخضر')
        self.assertNotIn('width', sales_answers.topics(text))
        self.assertFalse(sales_strategy.missing_fact_reply(text, product))
        self.assertFalse(sales_strategy.fact_requires_human(text, product))
        self.assertEqual(sales_decision.advance_details([], 'incoming', text), [])
        self.assertTrue(sales_strategy.fact_requires_human('العرض شكد؟', product))
        self.assertTrue(sales_strategy.fact_requires_human('شكد عرض القطعة بالسنتيمتر؟', product))

    def test_restock_promises_need_a_real_subscription_capability(self):
        for text in ('اول ما ينزل موديل يناسب قياسج ابلغج فوراً',
                     'من تتوفر وجبة جديدة ندزلج ونبلغج بيها أكيد'):
            self.assertTrue(conversation_quality.unsupported_claim(text))
        self.assertFalse(conversation_quality.unsupported_claim('تكدرين تراسلينا وتشوفين الموديلات المتوفرة وقتها.'))

    def test_sales_questions_keep_their_introduction_in_one_message(self):
        for intro in ('حتى أضبط لج القياس،', 'متوفرة موديلات لعمر خمس سنوات، بس يا ريت توضحلي'):
            text = intro + ' شنو الموديل المطلوب؟'
            plan = reply_layout.plan_sales_bubbles({}, text)
            self.assertEqual(plan['reply_parts'], [text])
        text = 'سعر القطعة 15 ألف، التوصيل 5 آلاف، المجموع 20 ألف. شنو اللون المطلوب؟'
        plan = reply_layout.plan_sales_bubbles({}, text)
        self.assertEqual(reply_layout.compact(' '.join(plan['reply_parts'])), text)
        self.assertTrue(any('15 ألف' in p and '20 ألف' in p for p in plan['reply_parts']))

    def test_response_objective_respects_purchase_stage(self):
        paused = sales_strategy.turn_plan({'text': 'خلي افكر وارجعلكم'})
        buying = sales_strategy.turn_plan({'text': 'ثبتي الاسود قياس 44'})
        repeated = sales_strategy.turn_plan({'text': 'عيني سالت عن السعر مو القياس'})
        self.assertIn('بلا سؤال بيع', paused)
        self.assertIn('اجمعي الناقص فقط', buying)
        self.assertIn('الجواب الناقص', repeated)

    def test_hypothetical_account_question_is_not_checkout_consent(self):
        for text in ('اذا اخذ قطعتين شكد يصير حسابهن؟', 'شكد حساب الدرزن؟'):
            self.assertEqual(checkout.unresolved_purchase_terms(text), 'unanswered_price_or_shipping')
        reply = 'القطعتين 25 ألف والتوصيل 5 آلاف، المجموع 30 ألف. شنو القياس المطلوب؟\nدزيلي رقم الموبايل والمحافظة والعنوان.'
        cleaned = conversation_quality.defer_contact_until_consent(reply)
        self.assertIn('30 ألف', cleaned)
        self.assertIn('شنو القياس', cleaned)
        self.assertNotIn('رقم الموبايل', cleaned)

    def test_iraqi_direct_booking_forms_are_not_lost(self):
        for text in ('اريد تحجزيلي قطعة اسود قياس 44', 'اريد تثبتينلي قطعة',
                     'احجزيلي قطعة', 'ثبتيلي الماروني', 'اريدج تحجزينلي هالموديل'):
            self.assertTrue(sales_strategy._explicit_purchase(text), text)
        for text in ('ما اريد تحجزينلي هسه', 'شلون احجز؟', 'اكدر احجز؟',
                     'اذا اريد احجز شنو اسوي؟', 'لا تحجزينلي، هسه بس اسأل'):
            self.assertFalse(sales_strategy._explicit_purchase(text), text)

    def test_explicit_non_booking_ack_is_not_pressure(self):
        self.assertFalse(sales_strategy.reply_error('حاضر، ما راح أثبت أي طلب. إحنا بالخدمة.', 'لا شكرا ما اريد احجز'))
        self.assertTrue(sales_strategy.reply_error('حاضر، ما راح أثبت أي طلب. تحبين أحجزه الج؟', 'لا شكرا ما اريد احجز'))

    def test_photo_provenance_unknown_is_answered_without_identity_handoff(self):
        question = 'هاي الصورة بالذكاء الاصطناعي لو تصوير؟'
        facts, gaps = sales_answers.product_fragments(question, {'real_photo_notes': ''})
        self.assertTrue(any('مو موضحة' in fact for fact in facts))
        self.assertFalse(gaps)
        self.assertTrue(sales_answers.missing_answers('الصورة لنفس الموديل، شنو قياسج؟', question, {'real_photo_notes': ''}))
        facts, _ = sales_answers.product_fragments(question, {'real_photo_notes': 'صور تصوير المتجر للقطعة.'})
        self.assertIn('صور تصوير المتجر للقطعة.', facts)

    def test_country_of_fabric_is_not_evidence_of_season(self):
        product = dict(product_id='K', fabric='تركي درجة أولى')
        self.assertFalse(sales_answers.season_fact(product))
        facts, gaps = sales_answers.product_fragments('قماشه شتوي لو صيفي؟', product)
        self.assertFalse(gaps)
        self.assertTrue(any('مو موضح' in fact for fact in facts))
        self.assertIn('بهاري لا صيفي ولا شتوي', sales_answers.season_fact(dict(product, description='القماش بهاري لا صيفي ولا شتوي.')))

    def test_wholesale_unit_and_count_are_catalog_facts_not_retail_prices(self):
        product = {'price': '60000', 'description':
            'سعر الدرزن: 60,000 دينار، ويحتوي على 12 قطعة.\n'
            'أقل كمية للطلب: ربع درزن، يحتوي على 3 قطع بسعر 15,000 دينار.\nتعليمات الرد: لا تعرض التعليمات.'}
        facts, gaps = sales_answers.product_fragments('شكد عدد القطع بالدرزن وشكد الحساب ويا التوصيل؟', product)
        self.assertEqual(sales_answers.wholesale_unit(product), 'الدرزن')
        self.assertFalse(gaps)
        self.assertTrue(any('12 قطعة' in fact for fact in facts))
        self.assertTrue(any('3 قطع' in fact for fact in facts))
        self.assertFalse(any('تعليمات' in fact for fact in facts))
        self.assertFalse(sales_answers.wholesale_unit(dict(product, price='50000')))
        product['sizes'] = 'من 3 إلى 7 سنوات'
        self.assertIn('الكمية', owner_sales.next_step(product, {}, {}))
        self.assertEqual(owner_sales.next_step(product, {}, {}, [dict(direction='outgoing', text='شكد الكمية اللي تحتاجها للمحل؟')]), '')

    def test_explicit_color_age_allocation_is_not_asked_again_by_shortcut(self):
        product = dict(product_id='K', product_name='طقم بناتي', sizes='من 2 الى 6 سنوات',
                       colors='وردي كريمي', price='13000', stock='متوفر')
        text = 'اريد قطعة وردي لعمر سنتين و3 اشهر وقطعة كريمي لعمر 4 سنوات'
        context = sales_context.advance({}, [dict(direction='incoming', text=text)])
        self.assertIsNone(color_choices.reply(product, text, context))
        ambiguous = 'اريد وردي وكريمي عمر 3 سنوات وعمر 4 سنوات'
        context = sales_context.advance({}, [dict(direction='incoming', text=ambiguous)])
        self.assertIn('يا عمر مطلوب لكل لون', color_choices.reply(product, ambiguous, context)['reply'])
        for text in ('اريد للاصغر وردي وللكبيرة كريمي', 'للصغيرة وردي وللكبيرة كريمي'):
            context = sales_context.advance({}, [dict(direction='incoming', text='الاولى عمرها 3 سنوات والثانية 6 سنوات موجود؟'),
                                                 dict(direction='incoming', text=text)])
            self.assertIsNone(color_choices.reply(product, text, context))


class RecentSalesCheckoutTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.StaleDetailCheckoutTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.StaleDetailCheckoutTests.tearDownClass.__func__)
    tearDown = fixtures.StaleDetailCheckoutTests.tearDown
    save = fixtures.StaleDetailCheckoutTests.save
    order_count = fixtures.StaleDetailCheckoutTests.order_count

    def setUp(self):
        fixtures.StaleDetailCheckoutTests.setUp(self)
        self.product.update(sizes='38 إلى 52', price='18000')

    def process(self, text):
        result, model = fixtures.StaleDetailCheckoutTests.process(self, text)
        with patch.object(self.m, 'send_text_to_facebook', return_value=True), \
             patch.object(self.m, 'send_image_to_facebook', return_value=True):
            self.m.send_webhook_result_to_facebook(result, self.sender)
        return result, model

    def test_corrected_size_survives_separate_contacts_and_books_once(self):
        self.save('outgoing', 'الأسود متوفر، شنو القياس المطلوب؟')
        self.save('incoming', 'اريد قياس 44 اسود')
        self.save('outgoing', 'قماشه تركي.')
        self.save('incoming', 'اعتمدي 46 بدل 44، الاسود قطعة وحدة ثبتي')
        self.save('outgoing', 'دزيلي رقم الموبايل والمحافظة والعنوان حتى أثبت الحجز.')
        self.process('07701234567')
        result, model = self.process('بغداد حي الاختبار قرب مدرسة التجربة')
        model.assert_not_called()
        self.assertEqual(self.order_count(), 1)
        row = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual(json.loads(row['order_items'])[0]['size'], '46')
        self.process('تمام')
        self.assertEqual(self.order_count(), 1)

    def test_two_xl_is_booked_from_weight_catalog_without_reasking_weight(self):
        self.product.update(sizes='من 55 كيلو الى 95', category='دشداشة نسائي')
        self.save('outgoing', 'شكد قياس تلبسين؟')
        self.save('incoming', 'قياس ٢xl اسود')
        self.save('outgoing', 'أثبتلج حجز؟')
        self.save('incoming', 'اي ثبتي قطعة وحدة')
        self.save('outgoing', 'دزيلي رقم الموبايل والمحافظة والعنوان.')
        result, model = self.process('07701234567 بغداد حي الاختبار قرب مدرسة التجربة')
        model.assert_not_called()
        self.assertEqual(self.order_count(), 1)
        row = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual(json.loads(row['order_items'])[0]['size'], 'XXL')
        self.assertNotIn('وزن', result['reply'])

    def test_wholesale_total_does_not_treat_a_quarter_dozen_as_a_full_dozen(self):
        self.product.update(price='60000', description='سعر الدرزن: 60,000 دينار، ويحتوي على 12 قطعة.\n'
            'أقل كمية للطلب: ربع درزن، يحتوي على 3 قطع بسعر 15,000 دينار.')
        whole = self.m.recover_factual_sales_reply(dict(self.ev, text='شكد عدد القطع بالدرزن وشكد الحساب ويا التوصيل؟'), self.product)
        self.assertIn('65,000', whole['reply'])
        self.assertIn('للدرزن', whole['reply'])
        self.assertNotIn('للقطعة', whole['reply'])
        quarter = self.m.recover_factual_sales_reply(dict(self.ev, text='شكد حساب ربع درزن ويا التوصيل؟'), self.product)
        self.assertNotIn('65,000', quarter['reply'])

    def test_want_you_to_book_survives_side_question_and_split_contacts(self):
        self.save('outgoing', 'شكد قياس تلبسين؟')
        text = 'تمام عيني خوش سعر، اريد تحجزيلي قطعة وحدة باللون الأسود قياس 44 فدوه.'
        self.assertTrue(self.m.checkout_requested(self.db, dict(self.ev, text=text), {'order': {'items': []}}))
        self.process(text)
        self.process('07701234567')
        self.process('بغداد حي الاختبار قرب مدرسة التجربة')
        self.assertEqual(self.order_count(), 1)
        self.process('تسلمين شكرا')
        self.assertEqual(self.order_count(), 1)

    def test_checkout_shows_selected_size_and_total_once_before_contact(self):
        self.m.set_store_setting(self.db, 'delivery_baghdad_fee', '5000')
        self.m.set_store_setting(self.db, 'delivery_other_fee', '5000')
        data = {'items': [dict(product_id='W1', color='اسود', size='44', quantity=1)]}
        first = self.m.checkout_contact_prompt(self.db, self.sender, data, self.product, 'رقم الموبايل والمحافظة والعنوان')
        self.assertIn('قياس 44', first)
        self.assertIn('23 ألف', first)
        self.assertNotIn('تم تثبيت', first)
        self.save('outgoing', first)
        second = self.m.checkout_contact_prompt(self.db, self.sender, data, self.product, 'المحافظة والعنوان')
        self.assertNotIn('رقم الموبايل', second)
        self.assertNotIn('23 ألف', second)
        self.assertIn('العنوان', second)
    def test_offer_acknowledgement_does_not_create_a_silent_review(self):
        self.save('incoming', 'اكو مجال تخفيض؟')
        self.save('outgoing', 'السعر ثابت وعليه عرض، شنو القياس المطلوب؟')
        result, _ = self.process('تمام فهمت العرض، شنو الالوان المتوفرة؟')
        self.assertTrue(result['reply'])
        self.assertIsNone(self.m.has_pending_human_review(self.db, self.sender))
        self.assertTrue(self.m.is_customer_ai_enabled(self.db, self.sender))

    def test_unknown_season_uses_product_facts_even_with_multiple_child_choices(self):
        self.product.update(fabric='تركي درجة أولى', sizes='من 2 الى 6 سنوات', colors='وردي كريمي')
        history = [dict(direction='incoming', text='الاولى عمرها 3 سنوات والثانية 6 سنوات، موجود؟'),
                   dict(direction='outgoing', text='متوفر. شنو اللون المطلوب؟')]
        with patch.object(self.m, '_call_main_ai_once', return_value={'reply': 'صيفي وبارد'}) as model:
            result = self.m.call_main_ai(dict(self.ev, text='قماشه شتوي لو صيفي؟'), 'text', {}, history,
                [self.product], None, None, '', [], customer_products=[self.product])
        model.assert_not_called()
        self.assertIn('مو موضح', result['reply'])
        self.assertIn('تركي', result['reply'])

    def test_unknown_photo_source_never_becomes_our_photography(self):
        with patch.object(self.m, '_call_main_ai_once', return_value={'reply': 'الصورة من تصويرنا'}) as model:
            result = self.m.call_main_ai(dict(self.ev, text='هاي الصورة بالذكاء لو تصوير؟'), 'text', {}, [],
                [self.product], None, None, '', [], customer_products=[self.product])
        model.assert_not_called()
        self.assertIn('مو موضحة', result['reply'])
        self.assertNotIn('من تصويرنا', result['reply'])

    def test_advice_request_is_not_purchase_consent(self):
        self.assertFalse(self.m.checkout_requested(self.db, dict(self.ev,
            text='محتاجة فستان للزيارة شنو تنصحيني اخذ؟'), {'order': {'items': []}}))

    def test_total_question_is_not_replaced_with_a_delivery_only_template(self):
        result = self.m.store_policy_question_reply('شكد يصير المجموع الكلي ويا التوصيل للنجف؟', self.product,
                                                    ev=self.ev, customer={}, history=[])
        self.assertIsNone(result)

    def test_fear_of_mismatch_never_rejects_the_chosen_model(self):
        text = 'زين عيني اكدر افتح الطلب واشوفه قبل ما انطي الفلوس للمندوب؟ خاف يطلع مو نفس الصورة'
        self.assertTrue(self.m.is_conditional_return_question(text))
        self.assertFalse(self.m.is_product_objection(text))
        self.assertIsNone(self.m.classify_customer_problem(text))
        self.assertFalse(self.m.is_conditional_return_question('استلمت وطلع مو نفس الصورة'))

    def test_offered_cheaper_product_does_not_replace_customer_binding(self):
        alternative = dict(self.product, product_id='W2', product_name='سوت بديل', price='15000')
        catalog = [self.product, alternative]
        with patch.object(self.m, 'load_active_products', return_value=catalog), \
             patch.object(self.m, 'load_products_from_file', return_value=catalog):
            result, _ = self.process('غالي على ميزانيتي، اريد ارخص')
            self.assertIn('سوت بديل', result['reply'])
            self.assertEqual(self.m.get_active_product_binding(self.db, self.sender)['product_id'], 'W1')
        self.assertEqual(self.order_count(), 0)

    def test_total_can_precede_shipping_without_becoming_a_discount(self):
        text = 'يعني المجموع واصل للنجف هم يصير 21 ألف لو يختلف التوصيل؟'
        self.assertTrue(checkout.delivered_total_inquiry(text))
        self.assertFalse(owner_sales.negotiation_kind(text))

    def test_fabric_name_is_preserved_but_undocumented_feel_is_not_promised(self):
        self.assertIn('قديفة ثقيل', sales_answers.product_fragments('شنو القماش؟', dict(fabric='قديفة ثقيل'))[0][0])
        product = dict(self.product, fabric='لينن درجة أولى')
        with patch.object(self.m, '_call_main_ai_once', return_value={'reply': 'خفيف وبارد'}) as model:
            result = self.m.call_main_ai(dict(self.ev, text='قماشه ثقيل لو خفيف وبارد؟'), 'text', {}, [],
                [product], product, None, '', [], customer_products=[product])
        model.assert_not_called()
        self.assertIn('مو موضحين', result['reply'])

    def test_legacy_image_url_list_does_not_crash_after_model_selection(self):
        images = [dict(image_index=1, product_found=True, product_id='W1',
                       image_url=['https://image.test/one.jpg', 'https://image.test/two.jpg'])]
        positions = self.m.customer_image_positions(self.db, self.sender, images)
        self.assertEqual(positions[0]['image_url'], 'https://image.test/one.jpg')
        self.assertEqual(len(positions[0]['image_urls']), 2)
        self.save('incoming', 'اريده اسود قياس 44')
        identity = self.db.execute('SELECT MAX(id) FROM messages WHERE sender_id=?', (self.sender,)).fetchone()[0]
        event = self.m.collect_unanswered_event(self.db, dict(self.ev, text='اريده اسود قياس 44'), identity)
        self.assertIn('قياس 44', event['text'])


class RecentSalesPostOrderTests(unittest.TestCase):
    setUpClass = classmethod(service.PostOrderServiceTests.setUpClass.__func__)
    tearDownClass = classmethod(service.PostOrderServiceTests.tearDownClass.__func__)
    setUp = service.PostOrderServiceTests.setUp
    tearDown = service.PostOrderServiceTests.tearDown
    handle = service.PostOrderServiceTests.handle

    def test_booking_status_is_answered_from_saved_order_without_model(self):
        for question in ('يعني صار الحجز؟', 'صار الحجز؟', 'ثبتتوا طلبي؟'):
            response, calls = self.handle({'reply': 'unused', 'requires_human': True}, text=question)
            self.assertEqual(calls, 0)
            self.assertIn('مسجل', response['reply'])
            self.assertFalse(response['meta']['needs_human'])
        self.assertFalse(conversation_quality.booking_status_question('صار الحجز لو بعد ما حجزتوا وشوكت يوصل؟'))

    def test_accepting_addition_never_becomes_duplicate_booking_ack(self):
        for question in ('تحبين أضيف القديفة لطلبج؟', 'تحبين نعدل قياس التنورة لقياس أكبر؟'):
            response, calls = self.handle({'reply': 'unused'}, text='اي', history=[
                {'direction': 'incoming', 'text': 'اريدها ويا الطلب'},
                {'direction': 'outgoing', 'text': question},
                {'direction': 'incoming', 'text': 'اي'}])
            self.assertEqual(calls, 0)
            self.assertEqual(response['reply'], '')
            self.assertTrue(response['meta']['human_review_id'])
            self.assertTrue(self.m.is_customer_ai_enabled(self.db, self.sender))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 1)

    def test_ordinary_thanks_never_reuses_a_stale_action_question(self):
        response, calls = self.handle({'reply': 'unused'}, text='تمام', history=[
            {'direction': 'outgoing', 'text': 'تحبين أضيف قطعة؟'},
            {'direction': 'incoming', 'text': 'لا، شكد المدة؟'},
            {'direction': 'outgoing', 'text': 'المدة من يوم ليومين.'},
            {'direction': 'incoming', 'text': 'تمام'}])
        self.assertEqual(calls, 0)
        self.assertFalse(response['meta']['needs_human'])
