"""Synthetic regressions for checkout failures found in the Nov 1 backup."""
import json
import unittest
from unittest.mock import patch

from account_app import pricing, product_variants, sales_answers, conversation_quality, owner_sales, child_availability
from account_app.tests import test_baraah_checkout_continuity_oct28 as kids


class BookingFactsNov01Tests(unittest.TestCase):
    def test_leading_single_price_and_explicit_pair_offer_are_not_ambiguous(self):
        for text in ('12000 او قطعتين ب 20000', '١٢ الف أو قطعتين بـ٢٠ الف',
                     '12000 والقطعتين ب20000'):
            with self.subTest(text=text):
                product = dict(product_id='K', price=text)
                self.assertEqual(pricing.terms(product)[:2], (1, 12000))
                self.assertEqual(pricing.offer_tiers(product), [(2, 20000)])
                self.assertEqual(pricing.quote([dict(product_id='K', quantity=2)], [product], 5000)['product_total'], 20000)
        for text in ('قطعتين ب20000', '12000 او قطعتين ب20000 و15000',
                     '3 قطع 12000 او قطعتين ب20000'):
            with self.subTest(ambiguous=text), self.assertRaises(ValueError):
                pricing.terms(dict(price=text))

    def test_unit_first_ages_stay_two_children_not_one_and_three(self):
        for text in ('سنه 3\nسنه 5', 'سنة٣، سنة٥', 'اريد قطعتين سنه 3 وسنه 5'):
            with self.subTest(text=text):
                self.assertEqual(product_variants.child_ages(text), [3, 5])
        self.assertEqual(product_variants.child_age('عمرها سنة و3 أشهر'), 1.25)
        self.assertEqual(product_variants.child_ages('3 سنوات 5 سنوات'), [3, 5])
        self.assertEqual(product_variants.child_ages('عمر 3 ونص وعمر 5 سنوات'), [3.5, 5])
        self.assertEqual(product_variants.child_age('عمر 4 وربع'), 4.25)
        for text, ages in (('عندي طفلين الاول 3 ونص والثاني 5', [3.5, 5]),
                           ('حتى واحد 5 ونص والثاني 6 سنوات', [5.5, 6])):
            self.assertEqual(product_variants.child_ages(text), ages)
        self.assertFalse(product_variants.child_ages('سعر الاول 12 الف والثاني 20 الف'))

    def test_definite_arabic_size_is_retained_without_reasking_weight(self):
        for text in ('بالاكسين لارج', 'الإكسين لارج', 'الاكس لارج'):
            expected = 'XXL' if 'كسين' in text else 'XL'
            self.assertIn(expected, product_variants.normalize_size_words(text))

    def test_local_shortcuts_do_not_drop_independent_questions(self):
        product = dict(product_id='K', product_name='طقم', price='12000', stock='متوفر', sizes='من 2 الى 7 سنوات')
        question = 'كم يوم لتوصيل الطلبية؟ هل يوجد خصم إضافي؟ ما هي طريقة الدفع؟'
        self.assertIsNone(owner_sales.reply({'text': question}, product, {}, {}, []))
        self.assertIsNone(child_availability.reply(product, [product], 'سنتين و9 شهور، شوكت يوصل؟'))
        facts, gaps = sales_answers.product_fragments('شنو المتوفر؟', product)
        self.assertFalse(gaps)
        self.assertTrue(any('متوفر' in fact for fact in facts))

    def test_shipping_and_checkout_questions_are_not_unknown_catalog_facts(self):
        self.assertIn('delivery', sales_answers.topics('كيف توصلون الطلب؟'))
        self.assertIn('checkout_help', sales_answers.topics('الا تحتاجون للعنوان؟'))
        product = dict(product_name='طقم اختبار', price='12000')
        for text in ('شنو قصدك؟', 'يعني شنو؟'):
            self.assertFalse(sales_answers.product_fragments(text, product)[1])
        self.assertTrue(sales_answers.product_fragments('شنو بلد المنشأ؟', product)[1])

    def test_contact_guard_does_not_leave_an_unfinished_but(self):
        reply = conversation_quality.defer_contact_until_consent('تدللين 🌷 بس دزيلي رقم الموبايل والعنوان.')
        self.assertNotIn('بس', reply)
        self.assertNotIn('الموبايل', reply)
        for text in ('يا ريت تدزيلي رقم الموبايل والمحافظة والعنوان بالتفصيل',
                     'ممكن الرقم والعنوان؟', 'محتاجة منج الرقم والمحافظة'):
            self.assertEqual(conversation_quality.defer_contact_until_consent(text), '')


class BookingPipelineNov01Tests(unittest.TestCase):
    setUpClass = classmethod(kids.BaraahCheckoutContinuityTests.setUpClass.__func__)
    tearDownClass = classmethod(kids.BaraahCheckoutContinuityTests.tearDownClass.__func__)
    setUp = kids.BaraahCheckoutContinuityTests.setUp
    tearDown = kids.BaraahCheckoutContinuityTests.tearDown
    save = kids.BaraahCheckoutContinuityTests.save
    process = kids.BaraahCheckoutContinuityTests.process
    count = kids.BaraahCheckoutContinuityTests.count

    def test_actual_price_wording_books_pair_with_split_contacts_once(self):
        self.product.update(price='12000 او قطعتين ب 20000', colors='أخضر', sizes='من 2 الى 7 سنوات')
        self.save('incoming', 'اريد قطعتين أخضر لعمر 4 سنوات ثبتي')
        draft = dict(items=[dict(product_id='K1', color='أخضر', size='4 سنوات', quantity=2)])
        self.m.save_message(self.db, self.sender, 'outgoing', 'text',
            'القطعتين ب20 ألف والتوصيل5 آلاف، دزيلي رقم الموبايل والعنوان.', None, None, None,
            {'checkout_draft': draft})
        self.process('07701234567')
        result, model = self.process('بغداد حي الاختبار قرب مدرسة التجربة')
        model.assert_not_called()
        self.assertIn('تم تثبيت', result['reply'])
        self.assertEqual(self.count(), 1)
        order = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual((order['product_total'], order['total_amount']), (20000, 25000))
        self.assertEqual(sum(i['quantity'] for i in json.loads(order['order_items'])), 2)
        self.process('تمام')
        self.assertEqual(self.count(), 1)

    def test_unit_first_age_question_answers_both_ages_without_provider(self):
        with patch.object(self.m, '_call_main_ai_once') as model:
            reply = self.m.call_main_ai(dict(self.ev, text='سنه 3\nسنه 5'), 'text', {}, [],
                [self.product], self.product, None, '', [], customer_products=[self.product])
        model.assert_not_called()
        self.assertIn('3 سنوات', reply['reply'])
        self.assertIn('5 سنوات', reply['reply'])
        self.assertNotIn('لعمر سنة ', reply['reply'])

    def test_soft_answer_gets_a_complete_consent_question_without_booking(self):
        self.product.update(colors='أخضر')
        self.save('incoming', 'أخضر لعمر 4 سنوات قطعة وحدة')
        self.save('outgoing', 'تحبون نثبت الحجز هسه؟')
        generated = dict(reply='تدللين روحي، حتى نثبت طلبج يا ريت تدزيلي رقم الموبايل والعنوان.', create_order=False,
                         order=dict(items=[dict(product_id='K1', color='أخضر', size='4 سنوات', quantity=1)]))
        for prefix in ('تدللين روحي، حتى نثبت طلبج', 'تدللين روحي 🌷 حتى نثبتلج القطعة', 'تدللين 🌷 بس'):
            generated['reply'] = prefix + ' يا ريت تدزيلي رقم الموبايل والعنوان.'
            with self.subTest(prefix=prefix), patch.object(self.m, '_call_main_ai_once', return_value=generated):
                result = self.m.call_main_ai(dict(self.ev, text='ان شاء الله'), 'text', {}, [],
                    [self.product], self.product, None, '', [], customer_products=[self.product])
            self.assertIn('؟', result['reply'])
            self.assertNotIn('الموبايل', result['reply'])
            self.assertFalse(result.get('create_order'))
        self.assertEqual(self.count(), 0)

    def test_invalid_catalog_price_is_internal_and_never_sent_to_customer(self):
        self.product.update(price='قطعتين ب20000')
        self.save('incoming', 'ثبتي أسود لعمر 4 سنوات قطعة وحدة')
        self.save('outgoing', 'دزيلي رقم الموبايل والعنوان.')
        result, _ = self.process('07701234567 بغداد حي الاختبار قرب مدرسة التجربة')
        self.assertEqual(result['reply'], '')
        self.assertTrue(result['meta']['human_review_id'])
        question=self.db.execute("SELECT topic FROM product_questions WHERE sender_id=? AND status='pending'",(self.sender,)).fetchone()
        self.assertIsNotNone(question)
        self.assertEqual(question['topic'],'price')
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertEqual(self.count(), 0)
        with patch.object(self.m, 'send_text_to_facebook') as send:
            self.m.send_webhook_result_to_facebook(result, self.sender)
        send.assert_not_called()

    def test_months_in_cart_match_bare_fraction_in_purchase(self):
        self.product['colors'] = 'أخضر برتقالي'
        self.save('incoming', 'ثبتي أخضر عمر 3 ونص وبرتقالي عمر 5 سنوات قطعتين')
        draft = dict(items=[dict(product_id='K1', color='أخضر', size='3 سنوات و6 أشهر', quantity=1),
                            dict(product_id='K1', color='برتقالي', size='5 سنوات', quantity=1)])
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'باقي الرقم والعنوان حتى أثبت الطلب.',
                            None, None, None, {'checkout_draft': draft})
        self.process('07701234567')
        result, _ = self.process('بغداد حي الاختبار قرب مدرسة التجربة')
        self.assertEqual(self.count(), 1, result)

    def test_age_shortcut_preserves_arrival_question(self):
        generated = dict(reply='التوصيل خلال 24 ساعة إلى يومين.', create_order=False, order={})
        with patch.object(self.m, '_call_main_ai_once', return_value=generated) as model:
            result = self.m.call_main_ai(dict(self.ev, text='سنتين و9 شهور، شوكت يوصل؟'), 'text', {}, [],
                [self.product], self.product, None, '', [], customer_products=[self.product])
        self.assertRegex(result['reply'],r'24|يوم|ساعة')

    def test_factual_followup_uses_single_catalog_card_not_previous_product(self):
        alternative = dict(self.product, product_id='K2', product_name='طقم جديد', fabric='قطن صيفي')
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'هذا طقم جديد.', None, None, None,
            {'catalog_page': {'product_ids': ['K2'], 'browsing_only': True}})
        event = dict(self.ev, text='شنو قماشه؟')
        self.assertEqual(self.m.single_catalog_discussion_product(self.db, event, [self.product, alternative])['product_id'], 'K2')
        self.assertIsNone(self.m.single_catalog_discussion_product(self.db, dict(event, text='شنو قماش طقم اختبار أطفال؟'), [self.product, alternative]))
        self.assertIsNone(self.m.single_catalog_discussion_product(self.db, dict(event, image_url='https://images.test/new.jpg'), [self.product, alternative]))
        with patch.object(self.m, 'load_products_from_file', return_value=[self.product, alternative]), \
             patch.object(self.m, 'load_active_products', return_value=[self.product, alternative]):
            result, _ = self.process('شنو قماشه؟')
            binding = self.m.get_active_product_binding(self.db, self.sender)
        self.assertIsNotNone(binding, result)
        self.assertEqual(binding['product_id'], 'K2')
        self.assertIn('قطن صيفي', result['reply'])
        self.assertEqual(self.count(), 0)

    def test_unknown_specific_age_opening_does_not_claim_inventory(self):
        self.db.execute('DELETE FROM customer_product_interests WHERE sender_id=?', (self.sender,))
        self.db.commit()
        with patch.object(self.m.requests, 'post') as provider:
            reply, _ = self.m.generate_first_message_reply(self.db, dict(self.ev, text='بيها عمر 4 سنوات؟'),
                                                          [self.product], '', [])
        provider.assert_not_called()
        self.assertNotIn('متوفر', reply)
        result, model = self.process('بيها عمر 4 سنوات؟')
        model.assert_not_called()
        self.assertNotIn('متوفر', result['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM customer_product_interests WHERE sender_id=?',
                                         (self.sender,)).fetchone()[0], 0)
