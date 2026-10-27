"""Read Zahraa's year-and-ten-month age and keep adult sizing in its own units."""
import re
import unittest
from unittest.mock import patch

from account_app import baraah, customer_copy, product_albums, product_variants as variants, sales_playbook
from account_app.tests import test_audit_orders as fixtures

CHILD = dict(product_id='P009', product_name='فستان', store_id='baraah-kids',
             sizes='من 1 إلى 2 سنوات', colors='جوزي وردي', price='10000', stock='متوفر', status='active')
ADULT = dict(product_id='F1', product_name='فستان دانتيل', sizes='38 إلى 52', colors='اسود',
             price='16000', stock='متوفر', status='active')


class AgeMonthFactsTests(unittest.TestCase):
    def test_complete_year_and_month_expression_is_added(self):
        for text in ('متوفر عمر سنه و١٠ اشهر', 'عمرها ١ سنة و١٠ أشهر', 'عمره1سنةو10اشهر',
                     'سنة و10 أشهر', 'عمرها 22 شهر'):
            self.assertAlmostEqual(variants.child_age(text), 22 / 12, msg=text)
            self.assertEqual(variants.age_text(variants.child_age(text)), 'سنة و10 أشهر')
        for text, age in [('سنة ونص', 1.5), ('سنتين او نص', 2.5), ('سنتين وشهرين', 2 + 2 / 12),
                          ('عمره 2 وقياس 3 سنوات', 2), ('عمره 10 اشهر', 10 / 12)]:
            self.assertAlmostEqual(variants.child_age(text), age, msg=text)

    def test_age_ranges_and_adult_size_or_weight_are_not_single_child_ages(self):
        for text in ('قياس 44', 'وزنه 74.5 كيلو', '6 إلى 12 أشهر', 'من سنة إلى سنتين', 'من عمر 3 إلى 7 سنوات'):
            self.assertIsNone(variants.child_age(text), text)

    def test_year_and_month_age_is_inside_one_to_two_year_table(self):
        check = variants.assess(CHILD, 'متوفر عمر سنه و١٠ اشهر')[0]
        self.assertTrue(check['supported'])
        self.assertTrue(check['age_based'])
        self.assertFalse(variants.assess(CHILD, 'عمره 10 أشهر')[0]['supported'])
        self.assertTrue(variants.assess(CHILD, 'عمره سنتين')[0]['supported'])
        self.assertFalse(variants.assess(CHILD, 'سنتين وشهر')[0]['supported'])

    def test_reply_uses_months_and_correct_year_grammar(self):
        for age, expected in [(1, 'سنة'), (2, 'سنتين'), (3, '3 سنوات'), (10 / 12, '10 أشهر'),
                              (22 / 12, 'سنة و10 أشهر'), (2.5, 'سنتين و6 أشهر')]:
            self.assertEqual(variants.age_text(age), expected)
            result = baraah.age_answer(CHILD, age)
            self.assertNotRegex(result['reply'], r'\d+\.\d+|1 سنوات|2 سنوات')
        self.assertTrue(baraah.age_answer(CHILD, 22 / 12)['supported'])
        self.assertFalse(baraah.age_answer(CHILD, 10 / 12)['supported'])

    def test_month_catalog_is_not_misread_as_adult_size_ten(self):
        product = dict(CHILD, sizes='6 إلى 12 أشهر')
        facts = customer_copy.fact(product['sizes'], 'sizes')
        self.assertFalse(variants.size_data(facts)['known'])
        self.assertEqual(variants.size_data(facts)['years'], [(.5, 1)])
        self.assertTrue(variants.assess(product, 'قياس 10 أشهر')[0]['supported'])
        self.assertFalse(variants.assess(product, 'عمر 10 سنوات')[0]['supported'])
        self.assertEqual(variants.order_choice_error(product, 'size', '10 أشهر'), '')
        self.assertTrue(variants.order_choice_error(product, 'size', '10 سنوات'))
        self.assertEqual(sales_playbook.price_next_step(product, {}, {'size': '10 أشهر'}), 'شنو اللون اللي يعجبج؟')

    def test_album_uses_whole_age_once_without_decimal_years(self):
        result = product_albums.response([dict(product_found=True, product_id='P009')] * 2,
                                        [CHILD], 'متوفر عمر سنه و١٠ اشهر')
        self.assertIn('سنة و10 أشهر متوفر', result['reply'])
        self.assertNotIn('مو ضمن', result['reply'])
        self.assertNotRegex(result['reply'], r'\d+\.\d+')
        self.assertEqual(result['reply'].count('سنة و10 أشهر'), 1)

    def test_display_roundtrip_preserves_age_table_and_weight_table(self):
        for sizes in ('1 إلى 2 سنوات', '6 إلى 12 أشهر', '38 إلى 52', '70 إلى 90 كيلو'):
            once = customer_copy.fact(sizes, 'sizes')
            twice = customer_copy.fact(once, 'sizes')
            self.assertEqual(variants.size_data(once), variants.size_data(twice))
        self.assertEqual(variants.size_data('38 إلى 52')['ranges'], [(38, 52)])
        self.assertEqual(variants.size_data('70 إلى 90 كيلو')['weights'], [(70, 90)])

    def test_decimal_age_cleanup_does_not_change_money_or_weight(self):
        text = 'عمر 0.833333 سنة، الوزن 74.5 كيلو، السعر 16.5 ألف.'
        reply = variants.age_wording(text)
        self.assertIn('عمر 10 أشهر', reply)
        self.assertIn('74.5 كيلو', reply)
        self.assertIn('16.5 ألف', reply)
        self.assertEqual(variants.age_wording('يبدي من عمر 1 سنوات.'), 'يبدي من عمر سنة.')

    def test_variant_guard_understands_age_wording_with_months(self):
        self.assertTrue(variants.reply_error('عمر 10 أشهر متوفر', CHILD, 'عمره 10 أشهر'))
        self.assertEqual(variants.reply_error('عمر 10 أشهر مو متوفر', CHILD, 'عمره 10 أشهر'), '')


class AgeMonthPipelineTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set('baraah-kids')
        fixtures.AuditFixTests.customer(self, 'age-months', 'baraah-kids')

    def tearDown(self):
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def test_reported_question_answers_available_without_provider_or_false_month_only_age(self):
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.call_main_ai(dict(sender_id='age-months', text='متوفر عمر سنه و١٠ اشهر'),
                'text', {}, [], [CHILD], CHILD, None, '', [], customer_products=[CHILD])
        model.assert_not_called()
        self.assertIn('متوفر لعمر سنة و10 أشهر', result['reply'])
        self.assertNotIn('مو متوفر', result['reply'])
        self.assertNotRegex(result['reply'], r'\d+\.\d+|1 سنوات')
        self.assertFalse(result['create_order'])

    def test_surface_cleanup_keeps_reply_parts_consistent(self):
        original = 'عمر 0.833333 سنة مو متوفر.'
        result, reply = self.m.format_customer_reply(dict(_single_message=True, reply_parts=[original]), original)
        self.assertEqual(reply, 'عمر 10 أشهر مو متوفر.')
        self.assertEqual(result['reply_parts'], [reply])

    def test_other_stores_use_clothing_size_and_weight_instead_of_age(self):
        for store in ('al-fatena', 'khuyoot', 'golden-threads', 'default'):
            token = self.m._current_store_id.set(store)
            try:
                check = variants.assess(ADULT, 'قياس 44 متوفر؟')[0]
                self.assertTrue(check['supported'])
                self.assertFalse(check.get('age_based'))
                self.assertIsNone(variants.requested_age(ADULT, 'عمره سنة و10 أشهر'))
                self.assertIn('قياس', sales_playbook.price_next_step(ADULT, {}, {}))
                self.assertNotIn('عمر', sales_playbook.price_next_step(ADULT, {}, {}))
                result, reply = self.m.format_customer_reply({}, 'وزن 74.5 كيلو وقياس 44')
                self.assertEqual(reply, 'وزن 74.5 كيلو وقياس 44')
            finally:
                self.m._current_store_id.reset(token)
