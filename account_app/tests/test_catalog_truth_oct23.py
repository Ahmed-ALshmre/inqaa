"""Sanitized regressions from the October 23 backup; no customer identifiers."""
import copy
import json
import unittest
from unittest.mock import Mock, patch

from account_app import catalog_truth, product_albums
from account_app.tests import test_audit_orders as fixtures


DRESS = dict(product_id='D', product_name='فستان دانتيل', category='فستان',
             price='16000', stock='متوفر', status='active', colors='اسود',
             sizes='38 إلى 52', fabric='لينن',
             description='السعر: 20 ألف دينار. الموديل طويل وخصره محدد وتنورته واسعة.',
             faq='عبارة عن فستان قطعة وحده وليس تنورة و بلوز',
             image_url='https://images.test/dress.jpg')


class CatalogTruthTests(unittest.TestCase):
    def test_prompt_view_drops_old_prices_and_preserves_current_fields(self):
        original = copy.deepcopy(DRESS)
        view = catalog_truth.product_view(DRESS)
        self.assertEqual(DRESS, original)
        self.assertEqual(view['price'], '16000')
        self.assertNotIn('20', view['description'])
        self.assertNotIn('تنور', view['description'])
        self.assertIn('واسعة من الأسفل', view['description'])
        self.assertEqual(view['faq'], DRESS['faq'])

    def test_dress_cut_is_clear_and_skirt_products_are_preserved(self):
        self.assertIn('واسع من الأسفل', catalog_truth.dress_wording('فستان بخصر محدد وتنورة واسعة', DRESS))
        self.assertEqual(catalog_truth.dress_wording('تنورة واسعة', dict(DRESS, product_name='تنورة')), 'تنورة واسعة')
        self.assertTrue(catalog_truth.identity_error('هذا تنورة وقميص', DRESS))
        self.assertFalse(catalog_truth.identity_error('هذا فستان مو تنورة وقميص', DRESS))
        self.assertFalse(catalog_truth.identity_error('هو قطعتين', dict(DRESS, bundle_contents='فستان مع كاب، قطعتين')))

    def test_price_checks_unit_delivery_and_total_separately(self):
        for reply in ['سعره 20 ألف', 'سعر الفستان 21 ألف',
                      'التوصيل 3 آلاف', 'المجموع 16 ألف ويا التوصيل',
                      'سعره ١٨,٠٠٠ دينار']:
            with self.subTest(reply=reply):
                self.assertTrue(catalog_truth.price_error(reply, DRESS, [5000]))
        for reply in ['سعره 16 ألف والتوصيل 5 آلاف والمجموع 21 ألف',
                      'سعره 21 ألف ويا التوصيل', 'سعره ١٦,٠٠٠ د.ع',
                      'قياساته 38 إلى 52، يناسب تقريباً وزن 55 إلى 100 كيلو',
                      'كان سعره 20 ألف، هسه سعره 16 ألف']:
            with self.subTest(reply=reply):
                self.assertFalse(catalog_truth.price_error(reply, DRESS, [5000]))

    def test_actual_quantity_offer_and_regional_delivery_are_allowed(self):
        product = dict(DRESS, offer='عرض قطعتين ب 28 الف')
        self.assertFalse(catalog_truth.price_error('سعر قطعتين 28 ألف، المجموع 33 ألف', product, [5000], 'شكد قطعتين'))
        self.assertTrue(catalog_truth.price_error('سعر قطعتين 32 ألف، المجموع 37 ألف', product, [5000], 'شكد قطعتين'))
        self.assertFalse(catalog_truth.price_error('التوصيل لبغداد 5 آلاف، التوصيل لبقية المحافظات 7 آلاف', product, [5000,7000]))
        self.assertFalse(catalog_truth.price_error('المجموع 16 ألف', dict(DRESS, delivery='التوصيل مجاني'), [5000]))
        self.assertFalse(catalog_truth.price_error('سعره 16 ألف، عرض قطعتين بـ28 ألف', product, [5000]))
        self.assertTrue(catalog_truth.price_error('سعره 16 ألف، عرض قطعتين بـ25 ألف', product, [5000]))

    def test_color_in_catalog_name_does_not_hide_false_stock_claim(self):
        product = dict(DRESS, product_name='عباء لون وردي')
        self.assertTrue(product_albums.availability_error('عباء لون وردي غير متوفر', product, [product]))
        self.assertFalse(product_albums.availability_error('عباء لون وردي متوفر، اللون الأسود غير متوفر', product, [product]))
        for reply in ['القطعة غير متوفرة', 'الفستان مو موجوده', 'ما عندنا هذا الموديل']:
            self.assertTrue(product_albums.availability_error(reply, DRESS, [DRESS]), reply)


class CatalogTruthPipelineTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def run_reply(self, question, answer, product=DRESS):
        self.customer('truth')
        with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply=answer, create_order=False, order={})):
            return self.m.call_main_ai(dict(sender_id='truth',text=question), 'text', {}, [],
                                       [product], product, None, '', [])

    def test_punctuation_and_arabic_spelling_identify_the_named_product(self):
        for question in ['سعر فستان الدانتيل؟', 'عندكم فُستان دانتيل؟', 'دانتيل؟']:
            with self.subTest(question=question):
                self.assertEqual(self.m._text_match_product(question, [DRESS])['product_id'], 'D')
        self.assertIsNone(self.m._text_match_product('دانتيل؟', [DRESS,dict(DRESS,product_id='E')]))
        other = dict(DRESS, product_id='E', product_name='سوت')
        self.assertEqual(self.m._text_match_product('D مو سوت', [DRESS, other])['product_id'], 'D')

    def test_named_product_is_answered_even_without_saved_interest(self):
        self.customer('named')
        with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='سعره 16 ألف', create_order=False, order={})) as provider:
            self.m.call_main_ai(dict(sender_id='named',text='سعر فستان الدانتيل؟'), 'text', {}, [],
                                [DRESS], None, None, '', [])
        self.assertEqual(provider.call_args.args[5]['product_id'], 'D')

    def test_repeated_wrong_price_is_replaced_by_calculated_facts(self):
        result = self.run_reply('شنو مواصفاته وسعره ويا التوصيل؟', 'سعر الفستان 20 ألف وقماشه لينن')
        self.assertIn('16,000', result['reply'])
        self.assertIn('21,000', result['reply'])
        self.assertNotIn('20 ألف', result['reply'])
        self.assertFalse(result['create_order'])

    def test_dress_reply_does_not_reintroduce_skirt_wording(self):
        result = self.run_reply('شنو مواصفاته؟', 'الفستان طويل وتنورته واسعة')
        self.assertNotIn('تنور', result['reply'])
        self.assertIn('واسعة من الأسفل', result['reply'])
        self.assertEqual(result['reply_parts'], [result['reply']])

    def test_repeated_wrong_identity_is_corrected(self):
        result = self.run_reply('هو فستان لو قطعتين؟', 'هذا تنورة وقميص')
        self.assertIn('هذا فستان دانتيل', result['reply'])
        self.assertNotIn('هذا تنورة', result['reply'])

    def test_available_named_model_is_not_reported_out(self):
        product = dict(DRESS, product_name='فستان أسود دانتيل')
        result = self.run_reply('هذا موجود؟', 'فستان أسود دانتيل غير متوفر', product)
        self.assertNotIn('غير متوفر', result['reply'])
        self.assertIn('متوفر', result['reply'])

    def test_prompt_has_current_price_math_and_clear_description(self):
        self.customer('prompt')
        response = Mock(); response.raise_for_status.return_value = None
        response.json.return_value = {'choices':[{'message':{'content':json.dumps(dict(reply='سعره 16 ألف',create_order=False))}}]}
        with patch.object(self.m,'OPENROUTER_KEY','test-key'), patch.object(self.m.ai_transport,'post',return_value=response) as provider:
            self.m._call_main_ai_once(dict(sender_id='prompt',text='شنو مواصفاته'), 'text', {}, [],
                                      [DRESS], DRESS, None, '', [])
        payload = provider.call_args.kwargs['json']['messages']
        self.assertIn('حقل price الحالي', payload[0]['content'])
        self.assertIn('21,000', payload[1]['content'])
        self.assertNotIn('20 ألف', payload[1]['content'])
        self.assertNotIn('تنورته واسعة', payload[1]['content'])

    def test_post_order_keeps_historical_order_price(self):
        ev = dict(text='شكد كان حسابي', _post_order={'total_amount':20000})
        self.assertEqual(self.m.catalog_reply_error('المجموع 20 ألف', ev, DRESS, {}, [], []), '')

    def test_fallback_answers_known_specifications_without_generic_gap(self):
        result = self.run_reply('ما هي مواصفات الفستان؟', '')
        self.assertIn('قطعة وحدة', result['reply'])
        for fact in ['16,000', 'لينن', 'أسود', '38 إلى 52']:
            self.assertIn(fact, result['reply'])
        self.assertNotIn('بقية التفاصيل', result['reply'])
        self.assertFalse(result.get('_needs_fact_review'))

    def test_composition_fallback_does_not_invent_single_piece(self):
        result = self.run_reply('هو قطعتين يعني تنورة لو فستان؟', '', dict(DRESS, faq=''))
        self.assertNotIn('الفستان قطعة وحدة', result['reply'])
        self.assertTrue(result.get('_needs_fact_review'))
