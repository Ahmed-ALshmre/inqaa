import unittest
from unittest.mock import patch

from account_app import product_questions, product_variants
from account_app.sales_runtime import intent as sales_intent
from account_app.sales_runtime.contracts import clean_plan
from account_app.sales_runtime.response import Responses
from account_app.tests import test_sales_runtime_linking as linking


class UnbeatableSalesEngineTests(unittest.TestCase):
    def test_requested_parses_albas_size_with_and_without_space(self):
        self.assertEqual(['46'], product_variants.requested('إني البس46')['sizes'])
        self.assertEqual(['46'], product_variants.requested('انا البس 46')['sizes'])
        self.assertEqual(['XL'], product_variants.requested('اني البس xl')['sizes'])

    def test_apply_intent_does_not_withdraw_on_no_plus_color_or_size_question(self):
        state = {
            'cart': [{'line_id': 'L1', 'product_id': 'P001', 'color': 'نيلي', 'size': '40', 'quantity': 1}],
            'contact': {},
            'conditions': [],
            'focus_product_id': 'P001',
        }
        plan = clean_plan({'intent': 'withdraw', 'changes': [], 'contact': {'phone': None, 'address': None},
                           'answer': {'facts': [], 'question_field': None, 'goal': 'withdraw', 'catalog_photos': None}})
        evidence = {'payload': {'text': 'لا\nاريد اسود\nياقيس متوفر بلاسود'}}
        withdrawn = sales_intent.apply_intent(
            state, plan, 'ev-1', evidence, last_reply={'text': 'تحبين لون ثاني؟'}
        )
        self.assertFalse(withdrawn)

    def test_dashboard_gap_surfaces_unpunctuated_missing_cut_question(self):
        product = {
            'store_id': 'khuyoot',
            'product_id': 'P007',
            'name': 'فستان كلوش',
            'status': 'active',
            'price': 35000,
            'colors': 'أسود، نيلي',
            'sizes': '38-46',
            'cut': '',
            'closure': '',
        }
        row = {
            'id': 352,
            'store_id': 'khuyoot',
            'product_id': 'P007',
            'topic': 'cut',
            'question': 'كلوش عيني\nكسرات لو كبس\nواذا كسرات من الخلف كذلك نفس الموديل لو لاستيك',
        }
        gap = product_questions.dashboard_gap(row, product)
        self.assertIsNotNone(gap)
        self.assertIn('cut', gap[1])


class UnbeatableSalesResponseTests(unittest.TestCase):
    setUp = linking.LinkingTests.setUp
    tearDown = linking.LinkingTests.tearDown
    event = linking.LinkingTests.event
    version = linking.LinkingTests.version
    run_turn = linking.LinkingTests.run_turn
    select = linking.LinkingTests.select
    prompt = linking.LinkingTests.prompt

    def test_out_of_range_fit_with_matching_alternative_sets_catalog_show_option(self):
        self.products[0].update(name='فستان قصير', colors='أسود', sizes='38, 40, 42', selection_mode='clothing')
        self.products.append({
            'store_id': self.scope.store, 'product_id': 'P013', 'name': 'فستان طويل', 'status': 'active',
            'price': 40000, 'colors': 'أسود', 'sizes': '44, 46, 48', 'availability': 'متوفر',
            'selection_mode': 'clothing',
        })
        self.run_turn('أسود قياس 40', changes=[dict(line_id='one', values=dict(product_id='P1', color='أسود', size='40', quantity=1))])
        event = self.event('غير موديل قياس 46')
        out = self.runtime.turn(self.scope, event)
        seen_context = {}

        def fake_writer(context):
            seen_context.update(context)
            return [dict(type='text', text='عدنا فستان طويل متوفر بقياس 46، تحبين أدزلج صورته؟')]

        with patch('account_app.sales_runtime.response.sales_intelligence.relevant', return_value={
            'requested': False,
            'opportunity': 'out_of_range_fit',
            'offer_kind': None,
            'browse': False,
            'matches': [{'product_id': 'P013', 'name': 'فستان طويل', 'price': 40000, 'matched_size': '46', 'colors': ['أسود']}],
        }):
            prepared = Responses(
                self.store,
                self.loader,
                fake_writer,
                product_photos=lambda _p, _c: [{'type': 'image', 'url': 'https://example.com/p13.jpg'}],
            ).prepare(self.scope, event, out)

        self.assertIn(seen_context.get('question_field'), {'catalog.show_option', 'alternative.P1'})
        self.assertTrue(seen_context.get('recommendations', {}).get('matches'))
        self.assertEqual('queued', prepared['action'])

    def test_warm_persuasive_bubble_over_145_chars_and_hi_greeting_recovery(self):
        self.select()
        event = self.event('شكد سعره')
        out = self.runtime.turn(self.scope, event)
        long_warm = (
            'حبيبتي الغالية سعر الفستان 35,000 دينار وقماشه بارد ومريح جداً باللبس اليومي '
            'وعندج حق الفحص قبل الدفع حتى تتأكدين من الجودة والقياس بنفسج، تحبين يا لون نجهزه إلج؟'
        )
        self.assertGreater(len(long_warm), 145)
        self.assertLess(len(long_warm), 240)
        prepared = Responses(self.store, self.loader, lambda _ctx: [dict(type='text', text=long_warm)]).prepare(
            self.scope, event, out
        )
        self.assertEqual('queued', prepared['action'])
        self.assertEqual(1, prepared['parts'])

        ctx = {
            'mode': 'continue',
            'customer_message': 'hi',
            'progress': {'product_unresolved': True},
            'policies': {},
        }
        recovered = Responses.greeting_recovery(ctx)
        self.assertIsNotNone(recovered)
        self.assertIn('يا هلا بيج', recovered[0]['text'])


if __name__ == '__main__':
    unittest.main()
