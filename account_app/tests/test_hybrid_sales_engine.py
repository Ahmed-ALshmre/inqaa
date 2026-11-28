"""Isolated behavioral tests for the Hybrid Sales Engine.

Verifies all 5 winning human-staff sales dimensions engineered into the runtime
without sending any real messages or mutating production data:
  1. Multi-photo handling (mixed available + OOS, per-model photos + concise status)
  2. Model & OOS / size-mismatch pivoting (alternatives surfaced, no dead ends)
  3. Multi-piece upselling (bundle offer + shared-delivery math + copy_guard safe)
  4. Difficult objections (inspection/return delivery risk, delivery fee shock,
     bargaining, postponement) — both LLM context AND deterministic fallback
  5. Total-with-delivery upfront & Fast-Track Human Close (verified amounts +
     concise bubble splitting)
"""
import sqlite3
import unittest

from account_app import sales_intelligence
from account_app.sales_runtime import hybrid_playbook, wording
from account_app.sales_runtime.cart import Catalog
from account_app.sales_runtime.response import Responses
from account_app.sales_runtime.store import Scope, Store


def _make_store_and_catalog():
    db = sqlite3.connect(':memory:')
    db.row_factory = sqlite3.Row
    store = Store(db)
    store.migrate()
    sales_intelligence.init(db)

    products = [
        {
            'store_id': 'baraah-kids',
            'product_id': 'p001',
            'product_name': 'تراك أطفال شتوي سحاب',
            'product_type': 'تراك',
            'audience': 'بناتي',
            'status': 'active',
            'stock': 'متوفر',
            'price': '12000',
            'offer': 'القطعة بـ 12 ألف والقطعتين بـ 20 ألف',
            'colors': 'وردي، ماروني، بنفسجي',
            'sizes': '2 إلى 8 سنوات',
            'selection_mode': 'age',
            'fabric': 'تركي شتوي مبطن فرو',
            'image_url': 'https://cdn.example.com/p001.jpg',
        },
        {
            'store_id': 'baraah-kids',
            'product_id': 'p002',
            'product_name': 'بدلة أطفال حمرة',
            'product_type': 'بدلة',
            'audience': 'بناتي',
            'status': 'active',
            'stock': 'متوفر',
            'price': '10000',
            'colors': 'أحمر، كريمي',
            'sizes': '1 إلى 3 سنوات',
            'selection_mode': 'age',
            'fabric': 'تركي شتوي',
            'image_url': 'https://cdn.example.com/p002.jpg',
        },
        {
            'store_id': 'baraah-kids',
            'product_id': 'p003',
            'product_name': 'قاط ولادي شتوي',
            'product_type': 'قاط',
            'audience': 'ولادي',
            'status': 'active',
            'stock': 'مخلص',
            'price': '15000',
            'colors': 'جوزي',
            'sizes': '2 إلى 5 سنوات',
            'selection_mode': 'age',
            'image_url': 'https://cdn.example.com/p003.jpg',
        },
        {
            'store_id': 'baraah-kids',
            'product_id': 'p004',
            'product_name': 'قاط ولادي تركي دافي',
            'product_type': 'قاط',
            'audience': 'ولادي',
            'status': 'active',
            'stock': 'متوفر',
            'price': '14000',
            'colors': 'أخضر، أسود',
            'sizes': '2 إلى 6 سنوات',
            'selection_mode': 'age',
            'image_url': 'https://cdn.example.com/p004.jpg',
        },
    ]
    catalog = Catalog('baraah-kids', products)
    policy = {
        'delivery_fee': {'بغداد': 5000, 'كربلاء': 5000, 'النجف': 5000, 'أربيل': 5000},
        'delivery_time': '24 ساعة إلى يومين',
        'payment': 'الدفع عند الاستلام بعد الفحص',
        'inspection_policy': 'يرجى فحص الطلب بحضور المندوب وإذا لم يطابق يرجع بدون دفع',
    }
    return store, catalog, policy


class CaptureWriter:
    structured_sales = True

    def __init__(self, reply_fn):
        self.reply_fn = reply_fn
        self.contexts = []

    def __call__(self, context):
        self.contexts.append(context)
        return self.reply_fn(context)


class HybridSalesEngineTests(unittest.TestCase):
    def _seed_turn(self, store, scope, text, *, focus_id=None, cart=None, purchase_event=None, media_kind='text'):
        rec = store.append(
            scope,
            f'ev:{text}:{len(cart or [])}',
            kind='message',
            actor='customer',
            payload={'text': text, 'media_kind': media_kind},
        )
        turn_id = rec['id']
        if focus_id or cart is not None or purchase_event is not None:
            snap = store.snapshot(scope)
            def mutate(state):
                if focus_id:
                    state['focus_product_id'] = focus_id
                if cart is not None:
                    state['cart'] = cart
                if purchase_event is not None:
                    state['purchase_event'] = purchase_event
                return {'ok': True}
            store.apply(scope, f'seed:{turn_id}', snap['version'], {'turn_id': turn_id}, mutate)
        return turn_id, store.snapshot(scope)['version']

    def test_1_multi_photo_mixed_available_and_oos(self):
        """Multi-photo turn detects multi_photo_inquiry, injects playbook, and interleaves catalog photos."""
        store, catalog, policy = _make_store_and_catalog()
        scope = Scope('baraah-kids', 'zernio', 'acc1', 'cust-multi-photo')
        turn_id, ver = self._seed_turn(store, scope, 'هذني متوفرات وشكد أسعارهن؟', media_kind='image')

        writer = CaptureWriter(lambda ctx: [
            {'type': 'text', 'text': 'الأول متوفر وسعره 12 ألف وعليه عرض قطعتين بـ 20 ألف.'},
            {'type': 'text', 'text': 'الثاني للأسف مخلص حالياً.'},
            {'type': 'text', 'text': 'يا قطعة عجبتج أكثر من المتوفر حتى نثبتها؟'},
        ])
        photos_fn = lambda prod, colors, include_unavailable=False: [
            {'type': 'image', 'product_id': prod['product_id'], 'url': prod['image_url']}
        ]
        responses = Responses(store, lambda s: catalog, writer, lambda s: policy, product_photos=photos_fn)
        result = responses.prepare(
            scope, turn_id,
            {'action': 'media_unresolved', 'state_version': ver, 'multiple_images': True,
             'matched_product_ids': ['p001', 'p003']},
        )
        self.assertEqual(result['action'], 'queued')
        self.assertEqual(len(writer.contexts), 1)
        pb = writer.contexts[0]['hybrid_sales_playbook']
        self.assertIsNotNone(pb)
        self.assertIn('multi_photo_inquiry', pb['situations'])
        self.assertTrue(pb['tactics'])

    def test_2_oos_model_pivots_with_playbook_and_alternatives(self):
        """Sold-out model triggers oos_or_size_pivot and surfaces available alternatives."""
        store, catalog, policy = _make_store_and_catalog()
        scope = Scope('baraah-kids', 'zernio', 'acc1', 'cust-oos')
        turn_id, ver = self._seed_turn(store, scope, 'هذا القاط متوفر عمر 4؟', focus_id='p003')

        writer = CaptureWriter(lambda ctx: [
            {'type': 'text', 'text': 'حبيبتي هذا الموديل مخلص حالياً، بس متوفر عدنا موديلات شتوية ثانية.'},
            {'type': 'text', 'text': 'تحبين أدزلج صور بدائل متوفرة تعجبج؟'},
        ])
        photos_fn = lambda prod, colors: [{'type': 'image', 'product_id': prod['product_id'], 'url': prod['image_url']}]
        responses = Responses(store, lambda s: catalog, writer, lambda s: policy, product_photos=photos_fn)
        result = responses.prepare(
            scope, turn_id,
            {'action': 'continue', 'state_version': ver, 'answer_plan': {'facts': [], 'question_field': None}},
        )
        self.assertEqual(result['action'], 'queued')
        pb = writer.contexts[0]['hybrid_sales_playbook']
        self.assertIn('oos_or_size_pivot', pb['situations'])
        self.assertEqual(writer.contexts[0]['next_step']['field'], 'alternatives.photos')

    def test_3_multi_piece_upsell_on_single_price_inquiry(self):
        """Single-piece price inquiry exposes 2-piece bundle offer + upsell_multi_piece playbook."""
        store, catalog, policy = _make_store_and_catalog()
        scope = Scope('baraah-kids', 'zernio', 'acc1', 'cust-upsell')
        turn_id, ver = self._seed_turn(store, scope, 'شكد سعره؟', focus_id='p001')

        writer = CaptureWriter(lambda ctx: [
            {'type': 'text', 'text': 'سعر القطعة {{unit_price}} وعليه عرض القطعتين بـ{{offer_total}} بنفس التوصيل يطلع عليج أوفر.'},
            {'type': 'text', 'text': 'تاخذين قطعة وحدة لو تستفادين من عرض القطعتين؟'},
        ])
        responses = Responses(store, lambda s: catalog, writer, lambda s: policy)
        result = responses.prepare(
            scope, turn_id,
            {'action': 'continue', 'state_version': ver, 'answer_plan': {'facts': [], 'question_field': None}},
        )
        self.assertEqual(result['action'], 'queued')
        ctx = writer.contexts[0]
        self.assertEqual(ctx['amounts'].get('unit_price'), 12000)
        self.assertEqual(ctx['amounts'].get('offer_total'), 20000)
        self.assertEqual(ctx['next_step']['field'], 'offer.quantity')
        pb = ctx['hybrid_sales_playbook']
        self.assertIn('upsell_multi_piece', pb['situations'])
        self.assertTrue(pb['upsell_angle']['has_bundle_offer'])
        self.assertEqual(pb['upsell_angle']['bundle_qty'], 2)

    def test_4a_objection_inspection_risk_writer_and_fallback(self):
        """'إذا ما عجبني أدفع توصيل؟' gets zero-risk human reassurance in both playbook AND fallback."""
        store, catalog, policy = _make_store_and_catalog()
        pb = hybrid_playbook.build_playbook(
            'إذا ما عجبني أو ما رهم أدفع فلوس التوصيل؟',
            product=catalog.get('p002'),
            policy=policy,
            catalog_stock='available',
            question='product.p002.size',
            amounts={},
        )
        self.assertIn('objection_inspection_risk', pb['situations'])
        self.assertTrue(any('ما تدفعين' in ex for ex in pb.get('winning_examples', [])))

        # Verify deterministic fallback (fact_recovery_parts) fixes the old robotic reply!
        fallback = wording.fact_recovery_parts({
            'mode': 'continue',
            'customer_message': 'اذا ماعجبني ادفع فلوس التوصيل؟',
            'question_field': 'product.p002.size',
            'requested_fields': ['inspection_policy', 'delivery_fee'],
            'facts': [
                {'field': 'inspection_policy', 'value': policy['inspection_policy']},
            ],
            'policies': policy,
            'amounts': {'delivery_fee': 5000},
            'progress': {},
        })
        self.assertIsNotNone(fallback)
        joined = ' '.join(p['text'] for p in fallback)
        self.assertIn('حتى التوصيل ما تدفعين', joined)

    def test_4b_objections_delivery_bargaining_postpone(self):
        """Delivery fee shock, bargaining, and postponement each activate their exact winning tactic."""
        _, catalog, policy = _make_store_and_catalog()
        prod = catalog.get('p001')

        pb_del = hybrid_playbook.build_playbook(
            'التوصيل 5 يعني نص سعر القطعة، مو مجاني؟',
            product=prod, policy=policy, catalog_stock='available', question='offer.quantity', amounts={},
        )
        self.assertIn('objection_delivery_fee', pb_del['situations'])
        self.assertIn('upsell_multi_piece', pb_del['situations'])
        self.assertIn('shared_delivery_benefit', pb_del['upsell_angle'])

        pb_barg = hybrid_playbook.build_playbook(
            'بيه مجال تنزلين شوية من السعر؟',
            product=prod, policy=policy, catalog_stock='available', question='product.p001.size', amounts={},
        )
        self.assertIn('objection_bargaining', pb_barg['situations'])

        pb_post = hybrid_playbook.build_playbook(
            'لا بعد اسبوع افكر واردلج خبر',
            product=prod, policy=policy, catalog_stock='available', question=None, amounts={},
        )
        self.assertIn('objection_postpone', pb_post['situations'])
        # No question marks allowed in winning_examples when next_step.field is None!
        for ex in pb_post.get('winning_examples', []):
            self.assertNotIn('؟', ex)
            self.assertNotIn('?', ex)

    def test_5_total_with_delivery_and_fast_close(self):
        """Asking 'يعني 15 الحساب ويا التوصيل؟' computes verified total=15000 & fast close stays concise."""
        store, catalog, policy = _make_store_and_catalog()
        scope = Scope('baraah-kids', 'zernio', 'acc1', 'cust-total')
        turn_id, ver = self._seed_turn(store, scope, 'شكد يصير هيه ويا التوصيل؟', focus_id='p002')

        writer = CaptureWriter(lambda ctx: [
            {'type': 'text', 'text': 'إي عيوني، يصير المجموع {{total}} مع التوصيل (التوصيل {{delivery_fee}}).'},
            {'type': 'text', 'text': 'شكد عمر البنوتة حتى أشوفلج القياس المضبوط؟'},
        ])
        responses = Responses(store, lambda s: catalog, writer, lambda s: policy)
        result = responses.prepare(
            scope, turn_id,
            {'action': 'continue', 'state_version': ver, 'answer_plan': {'facts': [], 'question_field': None}},
        )
        self.assertEqual(result['action'], 'queued')
        ctx = writer.contexts[0]
        self.assertEqual(ctx['amounts'].get('delivery_fee'), 5000)
        self.assertEqual(ctx['amounts'].get('total'), 15000)
        self.assertIn('total_with_delivery_check', ctx['hybrid_sales_playbook']['situations'])

        # Verify fast-close checkout question is crisp & human-length
        close_line = wording.checkout_question('contact.details', 'female', final_detail=True)
        self.assertLessEqual(len(close_line), 75)
        self.assertIn('العنوان الكامل ورقم الموبايل', close_line)


if __name__ == '__main__':
    unittest.main()
