import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from account_app import checkout
from account_app.sales_runtime import live as sales_live
from account_app.sales_runtime.contracts import ServiceRequired
from account_app.sales_runtime.copy_guard import validate_fulfillment
from account_app.sales_runtime.response import Responses
from account_app.tests import test_sales_runtime_linking as linking


class SalesBeastV2UnitTests(unittest.TestCase):
    def test_phone_number_extracts_bidi_spaced_and_attached_arabic_digits(self):
        self.assertEqual('07750534133', checkout.phone_number('\u202d0775 053 4133\u202c'))
        self.assertEqual('07730993156', checkout.phone_number('العنوان دورة زراعي ابو الطيارة الهاتف٠٧٧٣٠٩٩٣١٥٦'))
        self.assertEqual('07714529701', checkout.phone_number('تلفون / 07714 529701'))

    def test_is_confirmation_accepts_thumbs_up_and_iraqi_affirmative_only(self):
        for text in ('👍', '👍🏻', '👌', 'تمام عفيه', 'اي عفية'):
            with self.subTest(text=text):
                self.assertTrue(checkout.is_confirmation(text))
        for text in ('ان شاء الله باجر اطلبه', 'لا اريد اسود', 'شكد سعره'):
            with self.subTest(text=text):
                self.assertFalse(checkout.is_confirmation(text))

    def test_copy_guard_blocks_premature_delivery_or_celebration_without_saved_order(self):
        for bad in (
            'راح نجهزه الج حتى يوصلج بأقرب وقت.',
            'تدللين عيني، يوصلج نفس الموديل قياس 48.',
            'ملبوس العافية مقدماً حبيبتي.',
            'إن شاء الله تتهنين بيه.',
        ):
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(ValueError, r'reply\.(?:action|booking)'):
                    validate_fulfillment([dict(type='text', text=bad)], {'order_saved': False})
        # General delivery window answer remains valid without an order
        validate_fulfillment(
            [dict(type='text', text='التوصيل يوصلج خلال 24 ساعة إلى يومين عيوني.')],
            {'order_saved': False, 'delivery_time': 'خلال 24 ساعة إلى يومين'},
        )

    def test_split_short_bubbles_breaks_long_reply_into_human_short_bubbles(self):
        long_warm = (
            'حبيبتي الغالية سعر الفستان 35 ألف وقماشه بارد ومريح جداً باللبس اليومي. '
            'وعندج حق الفحص قبل الدفع حتى تتأكدين من الجودة والقياس بنفسج، تحبين يا لون؟'
        )
        parts = Responses.split_short_bubbles([long_warm], max_len=135, max_parts=4)
        self.assertEqual(2, len(parts))
        self.assertTrue(all(len(p) <= 135 for p in parts))

    def test_schedule_live_followup_wires_stage_and_respects_order_or_withdraw(self):
        calls = []

        def fake_scheduler(db, sender_id, stage='conversation', product=None, delay_minutes=None, meta=None):
            calls.append((sender_id, stage, (product or {}).get('product_id'), meta))
            return 99

        module = SimpleNamespace(schedule_followup_if_needed=fake_scheduler)
        scope = SimpleNamespace(store='khuyoot', sender='khuyoot::cw-1')

        def make_entry(state_data):
            entry = Mock()
            entry.store.snapshot.return_value = {'data': state_data}
            entry.catalog.return_value = SimpleNamespace(products={'P012': {'product_id': 'P012', 'product_name': 'كيلوت'}})
            return entry

        # Checkout stage
        sales_live.schedule_live_followup(module, None, scope, make_entry({'purchase_event': 5, 'focus_product_id': 'P012', 'cart': [{'product_id': 'P012'}]}), 'continue')
        # Selection stage
        sales_live.schedule_live_followup(module, None, scope, make_entry({'focus_product_id': 'P012', 'cart': []}), 'continue')
        # Already ordered -> skipped
        sales_live.schedule_live_followup(module, None, scope, make_entry({'order_id': 123, 'focus_product_id': 'P012'}), 'continue')
        # Withdrawn -> skipped
        sales_live.schedule_live_followup(module, None, scope, make_entry({'focus_product_id': 'P012'}), 'withdrawn')

        self.assertEqual(2, len(calls))
        self.assertEqual(('khuyoot::cw-1', 'checkout', 'P012'), calls[0][:3])
        self.assertEqual(('khuyoot::cw-1', 'selection', 'P012'), calls[1][:3])

    def test_courtesy_recovery_prevents_handoff_on_simple_thanks_or_ok(self):
        for msg in ('تمام', 'شكراً حبيبتي', 'تسلمين', 'اوكي'):
            with self.subTest(msg=msg):
                recovered = Responses.courtesy_recovery({
                    'mode': 'continue',
                    'question_field': None,
                    'customer_message': msg,
                })
                self.assertIsNotNone(recovered)
                self.assertIn('تدللين', recovered[0]['text'])
        self.assertIsNone(Responses.courtesy_recovery({
            'mode': 'continue',
            'question_field': 'product.P1.color',
            'customer_message': 'تمام',
        }))


class SalesBeastV2RuntimeTests(unittest.TestCase):
    setUp = linking.LinkingTests.setUp
    tearDown = linking.LinkingTests.tearDown
    event = linking.LinkingTests.event
    version = linking.LinkingTests.version
    run_turn = linking.LinkingTests.run_turn
    select = linking.LinkingTests.select
    prompt = linking.LinkingTests.prompt

    def test_draft_cart_with_inactive_product_auto_prunes_instead_of_freezing(self):
        # Draft inquiry (no purchase_event) on P1, then P1 becomes inactive
        self.run_turn('شكد سعره قياس 44', changes=[dict(values=dict(product_id='P1', size='44'))])
        self.assertIsNone(self.store.snapshot(self.scope)['data'].get('purchase_event'))
        self.products[0]['status'] = 'inactive'
        _, out = self.run_turn('هلا')
        self.assertEqual('continue', out['action'])
        self.assertEqual([], self.store.snapshot(self.scope)['data']['cart'])

    def test_confirmed_cart_with_inactive_product_still_raises_cart_catalog_changed(self):
        self.select()
        self.products[0]['status'] = 'inactive'
        with self.assertRaisesRegex(ServiceRequired, 'cart_catalog_changed'):
            self.run_turn('هلا')


if __name__ == '__main__':
    unittest.main()
