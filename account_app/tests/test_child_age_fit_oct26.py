"""Inclusive year sizes and owner-approved conditional fits for ages 2 and 8."""
import unittest
from unittest.mock import patch

from account_app import baraah, conversation_quality, product_albums, product_variants as variants, sales_playbook
from account_app.checkout import order_line_error
from account_app.tests import test_audit_orders as fixtures

PRODUCT = dict(product_id='K1', product_name='فستان الأميرات', store_id=baraah.STORE_ID,
               sizes='من 3 إلى 7 سنوات', colors='وردي بنفسجي', price='13000', stock='متوفر', status='active')


class ChildAgeFactsTests(unittest.TestCase):
    def test_every_year_including_both_endpoints_is_available(self):
        for age in range(3, 8):
            for question in (f'قياس {age} متوفر؟', f'عمر {age} سنوات', f'عمره {age}'):
                with self.subTest(question=question):
                    check = variants.assess(PRODUCT, question)[0]
                    self.assertIs(check['supported'], True)
                    self.assertTrue(check['age_based'])
                    reply = product_albums.response([dict(product_found=True, product_id='K1')]*2, [PRODUCT], question)
                    self.assertFalse(reply['_needs_fact_review'])
                    self.assertNotIn('يحتاج تأكيد', reply['reply'])
                    self.assertIn(f'عمر {age} سنوات متوفر', reply['reply'])
                    self.assertEqual(reply['reply'].count('فستان الأميرات'), 1)
                    self.assertEqual(reply['reply_parts'], [reply['reply']])

    def test_arabic_digits_and_year_units_survive_catalog_parsing(self):
        product = dict(PRODUCT, sizes='من عمر ٣ إلى ٧ سنوات')
        self.assertIs(variants.assess(product, 'قياس ٣ متوفر؟')[0]['supported'], True)
        self.assertFalse(variants.size_data(product['sizes'])['known'])
        self.assertEqual(variants.size_data(product['sizes'])['years'], [(3, 7)])

    def test_age_is_not_confused_with_weight_months_or_a_year_range(self):
        for text, expected in [('عمر2سنه', 2), ('عمره 8', 8), ('عمره 36 شهر', 3),
                               ('عمره 3 اشهر', .25), ('عمره سنتين', 2),
                               ('عمره 2 وقياس 3 سنوات', 2), ('قياس 44', None),
                               ('وزنه 8 كيلو', None), ('من عمر 3 الى 7 سنوات', None)]:
            self.assertEqual(conversation_quality.child_age(text), expected, text)

    def test_outside_ages_are_not_new_stock_sizes(self):
        for age in (1, 2, 8, 9):
            self.assertIs(variants.assess(PRODUCT, f'قياس {age} متوفر؟')[0]['supported'], False)
        for size in ('3', '4 سنة', '5 سنوات', '6', '7 سنوات'):
            self.assertEqual(variants.order_choice_error(PRODUCT, 'size', size), '')
        for size in ('2', '8', 'سنتين', '3 شهر', '9 سنوات'):
            self.assertIn('مو موجود', variants.order_choice_error(PRODUCT, 'size', size))

    def test_two_year_old_large_build_can_be_offered_size_three_conditionally(self):
        result = baraah.age_answer(PRODUCT, 2, 'عمره سنتين وجسمه خشن شويه')
        self.assertEqual(result['suggested_size'], '3')
        self.assertIn('ممكن يناسبه', result['reply'])
        self.assertIn('قياس 3 سنوات متوفر', result['reply'])
        self.assertNotIn('قياس 2 سنوات متوفر', result['reply'])
        self.assertIn('شكد طوله', result['reply'])

    def test_eight_year_old_slim_build_can_be_offered_size_seven_conditionally(self):
        result = baraah.age_answer(PRODUCT, 8, 'عمره 8 سنوات وجسمه ناعم')
        self.assertEqual(result['suggested_size'], '7')
        self.assertIn('ممكن يناسبه', result['reply'])
        self.assertIn('قياس 7 سنوات متوفر', result['reply'])
        self.assertNotIn('قياس 8 سنوات متوفر', result['reply'])

    def test_unknown_build_asks_one_question_and_opposite_build_does_not_get_recommendation(self):
        for age in (2, 8):
            result = baraah.age_answer(PRODUCT, age)
            self.assertIn('جسمه خشن شوي لو ناعم؟', result['reply'])
            self.assertEqual(result['reply'].count('؟'), 1)
        for age, text in [(2, 'جسمه ناعم'), (8, 'جسمه خشن شوي')]:
            result = baraah.age_answer(PRODUCT, age, text)
            self.assertNotIn('suggested_size', result)
            self.assertIn('ما أرشح', result['reply'])

    def test_body_description_does_not_come_from_fabric_or_negation(self):
        for text in ('قماشه ناعم', 'فستان ناعم', 'عمره 8 والفستان ناعم', 'عمره 8 وقماش الفستان ناعم', 'مو ناعم', 'مو خشن', 'خشن لو ناعم؟'):
            self.assertEqual(variants.child_build(text), '', text)
        self.assertEqual(variants.child_build('بنتي ناعمه'), 'slim')
        self.assertEqual(variants.child_build('ابني خشن شويه'), 'large')

    def test_album_fit_advice_respects_the_build_already_given(self):
        for text in ('عمره سنتين وجسمه ناعم', 'عمره 8 وجسمه خشن'):
            result = product_albums.response([dict(product_found=True, product_id='K1')]*2, [PRODUCT], text)
            self.assertNotIn('ممكن يناسبه', result['reply'])
        result = product_albums.response([dict(product_found=True, product_id='K1')]*2,
                                         [PRODUCT], 'عمره سنتين وجسمه خشن')
        self.assertIn('ممكن يناسبه قياس 3 سنوات', result['reply'])

    def test_sold_out_or_unknown_stock_is_never_offered_as_available(self):
        for overrides in (dict(stock='نفذ'), dict(stock_quantity=0), dict(status='disabled'), dict(stock='')):
            for age in (2, 3, 7, 8):
                result = baraah.age_answer(dict(PRODUCT, **overrides), age, 'جسمه ناعم')
                self.assertFalse(result['supported'])
                self.assertNotIn('suggested_size', result)
                self.assertNotIn('سنوات متوفر', result['reply'])

    def test_boundary_advice_stays_with_the_approved_endpoints(self):
        for age in (1, 9):
            self.assertIsNone(variants.boundary_fit(PRODUCT, age))
        self.assertIsNone(variants.boundary_fit(dict(PRODUCT, sizes='من 4 إلى 6 سنوات'), 3))

    def test_checkout_uses_chosen_garment_size_instead_of_outside_child_age(self):
        item = dict(product_id='K1', color='وردي', size='3 سنوات', quantity=1)
        self.assertEqual(order_line_error([item], [PRODUCT]), '')
        self.assertIn('مو موجود', order_line_error([dict(item, size='2 سنة')], [PRODUCT]))
        self.assertEqual(order_line_error([dict(item, size='7 سنوات')], [PRODUCT]), '')
        self.assertIn('مو موجود', order_line_error([dict(item, size='8 سنوات')], [PRODUCT]))

    def test_selected_endpoint_size_does_not_reask_child_age_or_remain_a_conflict(self):
        context = {'evidence': [{'text': 'عمره 2 سنة'}, {'text': 'اختار قياس 3 سنوات وردي'}]}
        selected = sales_playbook.customer_selection(context)
        self.assertEqual(selected['age'], 2)
        self.assertEqual(selected['size'], '3')
        self.assertFalse(sales_playbook.selection_conflicts(PRODUCT, context))
        self.assertNotIn('العمر', sales_playbook.price_next_step(PRODUCT, context))
        self.assertNotIn('العمر', sales_playbook.price_next_step(PRODUCT, {}, {'size': '3', 'color': 'وردي'}))


class ChildAgeReplyTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set(baraah.STORE_ID)
        fixtures.AuditFixTests.customer(self, 'kids-age', baraah.STORE_ID)

    def tearDown(self):
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def reply(self, text):
        event = dict(sender_id='kids-age', text=text, image_url=None, attachments=[])
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.call_main_ai(event, 'text', {}, [], [PRODUCT], PRODUCT, None, '', [], customer_products=[PRODUCT])
        model.assert_not_called()
        self.assertFalse(result['create_order'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
        return result

    def test_direct_size_and_age_questions_answer_without_provider_or_confirmation(self):
        for text in ('قياس 3 متوفر؟', 'عمره 4 سنوات', 'عمر 5', 'قياس 6', 'عمره 7 سنوات'):
            result = self.reply(text)
            self.assertIn('متوفر لعمر', result['reply'])
            self.assertNotIn('يحتاج تأكيد', result['reply'])

    def test_two_and_eight_year_fit_questions_use_conditional_local_reply(self):
        for text, size in [('عمره سنتين خشن شوي', '3'), ('عمرها 8 سنوات ناعمه', '7')]:
            result = self.reply(text)
            self.assertEqual(result['_fit_recommendation_size'], size)
            self.assertIn('ممكن يناسبه', result['reply'])

    def test_short_build_answer_uses_previously_given_age(self):
        self.m.save_message(self.db, 'kids-age', 'incoming', 'text', 'عمره 8 سنوات')
        self.m.save_message(self.db, 'kids-age', 'outgoing', 'text', 'جسمه خشن شوي لو ناعم؟')
        result = self.reply('ناعم')
        self.assertEqual(result['_fit_recommendation_size'], '7')
        self.assertNotIn('شكد العمر', result['reply'])

