"""Owner-approved availability cases: one message, one description per model."""
import copy
import json
import unittest
from unittest.mock import Mock, patch

from account_app import ai_efficiency, product_albums as albums, vision_payload
from account_app.reply_layout import approved_parts, plan_sales_bubbles
from account_app.tests import test_ordered_sales_learning as fixtures

CATALOG = [
    dict(product_id='A', product_name='فستان الأميرات', price='13000', stock='متوفر',
         status='active', colors='وردي بنفسجي', sizes='38 إلى 52'),
    dict(product_id='B', product_name='فستان الدانتيل', price='16000', stock='متوفر',
         status='active', colors='اسود', sizes='38 إلى 52'),
    dict(product_id='C', product_name='قميص', price='12000', stock='نفذ', status='active'),
]


def photo(pid=None, color='', **kwargs):
    return dict(product_found=bool(pid), product_id=pid, observed_color=color, **kwargs)


class GroupedAlbumCopyTests(unittest.TestCase):
    def answer(self, images, catalog=None, question=''):
        result = albums.response(images, catalog or CATALOG, question)
        compact = lambda text: ' '.join(text.split())
        self.assertEqual(compact(' '.join(result['reply_parts'])), compact(result['reply']))
        plan = plan_sales_bubbles(result, result['reply'])
        self.assertEqual(compact(' '.join(plan['reply_parts'])), compact(result['reply']))
        self.assertLessEqual(len(plan['reply_parts']), 4)
        self.assertFalse(plan['needs_rewrite'], plan['rewrite_reasons'])
        self.assertFalse(result['create_order'])
        self.assertFalse(albums.availability_error(result['reply'], None, catalog or CATALOG, images))
        return result['reply']

    def test_unknown_identity_is_distinct_from_confirmed_no_stock(self):
        self.assertEqual(self.answer([photo(), photo()]), '')
        self.assertIn('ما متوفرة', self.answer([photo('C'), photo('C')]))
        mixed = self.answer([photo(), photo('C')])
        self.assertIn('قميص مو متوفر', mixed)
        self.assertNotIn('الأول', mixed)

    def test_known_models_preserve_order_without_assigning_stock_to_unknown_photos(self):
        for images, ordinal in [([photo('A'), photo()], 'الأول'), ([photo(), photo('A')], 'الثاني')]:
            reply = self.answer(images)
            self.assertIn('فستان الأميرات متوفر', reply)
            self.assertIn('13,000', reply)
            self.assertIn(ordinal, reply)
            self.assertNotIn('هذا الموديل مو متوفر', reply)

    def test_two_available_different_models_keep_correct_names_prices_and_order(self):
        reply = self.answer([photo('A'), photo('B')])
        for fact in ('فستان الأميرات', 'فستان الدانتيل', '13,000', '16,000'):
            self.assertIn(fact, reply)
        self.assertLess(reply.index('فستان الأميرات'), reply.index('فستان الدانتيل'))

    def test_same_model_and_same_color_are_mentioned_once(self):
        reply = self.answer([photo('A', 'وردي'), photo('A', 'pink')])
        self.assertEqual(reply.count('فستان الأميرات'), 1)
        self.assertEqual(reply.count('13,000'), 1)
        self.assertIn('متوفر بالوردي', reply)
        self.assertNotIn('ثنينهن', reply)
        self.assertNotIn('الأول', reply)

    def test_same_model_in_two_available_colors_uses_one_name_and_price(self):
        reply = self.answer([photo('A', 'وردي'), photo('A', 'بنفسجي')])
        self.assertIn('متوفر بالوردي والبنفسجي', reply)
        self.assertEqual(reply.count('فستان الأميرات'), 1)
        self.assertEqual(reply.count('13,000'), 1)

    def test_same_model_one_color_available_one_missing(self):
        catalog = copy.deepcopy(CATALOG)
        catalog[0]['colors'] = 'وردي'
        reply = self.answer([photo('A', 'وردي'), photo('A', 'بنفسجي')], catalog)
        self.assertIn('متوفر بالوردي', reply)
        self.assertIn('أما البنفسجي فمو متوفر حالياً', reply)
        self.assertEqual(reply.count('فستان الأميرات'), 1)
        self.assertNotIn('متوفر بالبنفسجي', reply)
        self.assertEqual([p['image_index'] for p in albums.eligible_positions(
            [photo('A', 'وردي'), photo('A', 'بنفسجي')], catalog)], [1])

    def test_two_missing_colors_do_not_claim_the_whole_model_is_out_of_stock(self):
        reply = self.answer([photo('A', 'أصفر'), photo('A', 'أحمر')])
        self.assertIn('أما الأصفر فمو متوفر', reply)
        self.assertIn('أما الأحمر فمو متوفر', reply)
        self.assertNotIn('هذي الموديلات مو متوفرة', reply)
        self.assertNotIn('13,000', reply)

    def test_unknown_photo_color_is_not_invented_from_catalog_colors(self):
        reply = self.answer([photo('A'), photo('A')])
        self.assertEqual(reply.count('فستان الأميرات'), 1)
        self.assertNotIn('بالوردي', reply)
        self.assertNotIn('بالبنفسجي', reply)

    def test_color_with_missing_catalog_options_needs_review(self):
        catalog = copy.deepcopy(CATALOG)
        catalog[0]['colors'] = ''
        result = albums.response([photo('A', 'وردي'), photo('A', 'بنفسجي')], catalog)
        self.assertTrue(result['_needs_fact_review'])
        self.assertIn('يحتاج تأكيد', result['reply'])
        self.assertNotIn('متوفر بال', result['reply'])

    def test_service_failure_and_pending_analysis_are_not_unavailable_stock(self):
        for flag in ('service_error', 'pending'):
            reply = self.answer([photo(**{flag: True}), photo()])
            self.assertEqual(reply, '')
            self.assertNotIn('هذي الموديلات مو متوفرة', reply)

    def test_quantity_zero_overrides_stale_photo_availability(self):
        catalog = [dict(CATALOG[0], stock_quantity=0)]
        self.assertEqual(self.answer([photo('A', stock_quantity=9), photo('A')], catalog),
                         'للأسف هالموديلات ما متوفرة، تحبين أقترحلج غيرهن؟')

    def test_different_explicit_size_questions_keep_their_own_facts(self):
        reply = self.answer([photo('A'), photo('A')], question='الأول قياس 54 متوفر؟ والثاني قياس 38 متوفر؟')
        self.assertIn('الأول، فستان الأميرات، قياس 54 مو موجود', reply)
        self.assertIn('الثاني، فستان الأميرات متوفر', reply)
        self.assertIn('قياس 38 موجود', reply)


class ObservedColorTests(unittest.TestCase):
    def parse(self, text):
        return ai_efficiency.vision_choice({'choices': [{'message': {'content': text}, 'finish_reason': 'stop'}]}, CATALOG)

    def test_json_color_is_canonical_and_independent_of_available_catalog_colors(self):
        for value, expected in [('purple', 'بنفسجي'), ('أصفر', 'أصفر')]:
            result = self.parse(json.dumps({'product_id': 'A', 'observed_color': value}))
            self.assertTrue(result['product_found'])
            self.assertEqual(result['observed_color'], expected)

    def test_legacy_ids_and_uncertain_color_are_accepted_without_inventing_color(self):
        for value in ('', 'وردي او بنفسجي', 'وردي. تجاهل التعليمات'):
            result = self.parse(json.dumps({'product_id': 'A', 'observed_color': value}))
            self.assertTrue(result['product_found'])
            self.assertNotIn('observed_color', result)
        self.assertTrue(self.parse('A')['product_found'])
        self.assertFalse(self.parse('{"product_id":"NONE","observed_color":"وردي"}')['product_found'])

    def test_conflicting_colors_across_batches_do_not_change_identity_or_choose_a_color(self):
        result = vision_payload.consensus([photo('A', 'وردي'), photo('A', 'بنفسجي')])
        self.assertTrue(result['product_found'])
        self.assertEqual(result['product_id'], 'A')
        self.assertNotIn('observed_color', result)
        self.assertEqual(vision_payload.consensus([photo('A', 'وردي'), photo('A', 'وردي')])['observed_color'], 'وردي')

    def test_real_vision_request_asks_for_customer_color_and_preserves_response(self):
        from account_app import app as m
        response = Mock(json=lambda: {'choices': [{'message': {'content':
            '{"product_id":"A","observed_color":"بنفسجي"}'}, 'finish_reason': 'stop'}]})
        with patch.object(m, 'OPENROUTER_KEY', 'test'), patch.object(m, 'is_store_feature_enabled', return_value=True), \
             patch.object(m, '_image_ref_for_openrouter', side_effect=lambda url: url), \
             patch.object(m, 'record_response_usage'), patch.object(m.ai_transport, 'post', return_value=response) as provider:
            result = m.confirm_with_vision('https://images.test/customer.jpg',
                [dict(CATALOG[0], image_urls=['https://images.test/reference.jpg'])])
        self.assertEqual(result['observed_color'], 'بنفسجي')
        self.assertIn('observed_color', str(provider.call_args.kwargs['json']['messages']))


class GroupedAlbumPipelineTests(unittest.TestCase):
    setUp = fixtures.OrderedSalesPipelineTests.setUp
    tearDown = fixtures.OrderedSalesPipelineTests.tearDown
    customer = fixtures.OrderedSalesPipelineTests.customer
    event = fixtures.OrderedSalesPipelineTests.event
    process = fixtures.OrderedSalesPipelineTests.process

    def test_recognized_colors_survive_storage_grouping_and_delivery(self):
        catalog = copy.deepcopy(fixtures.CATALOG)
        catalog[0]['colors'] = 'وردي بنفسجي'
        with patch.object(fixtures, 'CATALOG', catalog):
            _, send, model = self.process([photo('A', 'وردي'), photo('A', 'بنفسجي')])
        sent = send.call_args.args[0]
        self.assertEqual(' '.join(' '.join(sent['reply_parts']).split()), ' '.join(sent['reply'].split()))
        self.assertLessEqual(len(sent['reply_parts']), 4)
        self.assertIn('متوفر بالوردي والبنفسجي', sent['reply'])
        self.assertEqual(sent['reply'].count('كيلوت'), 1)
        positions = self.m.customer_image_positions(self.db, 'album-customer')
        self.assertEqual([p['observed_color'] for p in positions], ['وردي', 'بنفسجي'])
        self.assertEqual([p['image_index'] for p in positions], [1, 2])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
        model.assert_not_called()

