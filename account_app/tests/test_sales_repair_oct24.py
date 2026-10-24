"""Sanitized October 24 incidents; all provider and delivery calls are blocked."""
import base64
import io
import json
import unittest
from unittest.mock import Mock, patch

from PIL import Image
from account_app import vision_payload, conversation_quality, order_actions, sales_recovery, sales_strategy
from account_app.tests import test_audit_orders as fixtures


PRODUCT = dict(product_id='A', product_name='فستان دانتيل', category='فستان',
               price='16000', fabric='لينن', colors='اسود', sizes='38 إلى 52',
               stock='متوفر', status='active', image_url='https://images.test/a.jpg')


def inline_image():
    output = io.BytesIO()
    Image.effect_noise((1800, 1800), 80).convert('RGB').save(output, format='PNG')
    return 'data:image/png;base64,' + base64.b64encode(output.getvalue()).decode('ascii')


class PayloadTests(unittest.TestCase):
    def test_large_inline_image_fits_and_original_is_unchanged(self):
        original = inline_image()
        result = vision_payload.image_url(original)
        self.assertLess(len(base64.b64decode(result.split(',')[1])), 350_001)
        self.assertTrue(original.startswith('data:image/png;'))
        with Image.open(io.BytesIO(base64.b64decode(result.split(',')[1]))) as picture:
            self.assertLessEqual(max(picture.size), 1600)
            self.assertGreater(min(picture.size), 500)

    def test_batches_keep_every_reference_and_customer_photo_under_byte_limit(self):
        prefix = [{'type': 'image_url', 'image_url': {'url': 'customer'}}]
        groups = [[{'type': 'text', 'text': str(i)},
                   {'type': 'image_url', 'image_url': {'url': 'x' * 200_000}}] for i in range(40)]
        requests = vision_payload.request_batches({'model': 'test'}, prefix, groups)
        self.assertGreater(len(requests), 1)
        labels = []
        for request in requests:
            self.assertLessEqual(len(json.dumps(request).encode()), vision_payload.MAX_REQUEST_BYTES)
            self.assertEqual(request['messages'][-1]['content'][0], prefix[0])
            labels += [x['text'] for x in request['messages'][-1]['content'] if x.get('type') == 'text']
        self.assertEqual(labels, list(map(str, range(40))))

    def test_consensus_rejects_conflicting_products_and_partial_failure(self):
        a = dict(product_found=True, product_id='A')
        b = dict(product_found=True, product_id='B')
        self.assertFalse(vision_payload.consensus([a, b])['product_found'])
        failure = dict(service_error=True, error_code='billing', product_found=False)
        self.assertTrue(vision_payload.consensus([a, failure])['service_error'])
        self.assertEqual(vision_payload.consensus([a, {'product_found': False}, a])['product_id'], 'A')

    def test_invalid_inline_image_is_not_passed_to_provider(self):
        with self.assertRaises(Exception):
            vision_payload.image_url('data:image/png;base64,TEST')


class DialogueTests(unittest.TestCase):
    customer = fixtures.AuditFixTests.customer

    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set('default')
        catalog = [PRODUCT, dict(PRODUCT, product_id='B', product_name='سوت')]
        self.catalogue = patch.object(self.m, 'load_products_from_file', return_value=catalog)
        self.catalogue.start()
        self.active = patch.object(self.m, 'load_active_products', return_value=catalog)
        self.active.start()
        self.auto = patch.object(self.m, 'get_auto_product_settings', return_value=dict(enabled=False, product=None, product_id=''))
        self.auto.start()

    def tearDown(self):
        self.auto.stop(); self.active.stop(); self.catalogue.stop()
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def test_classifier_failure_does_not_reask_one_known_model(self):
        self.customer('known')
        self.m.complete_customer_product_link(self.db, 'known', PRODUCT, 'manual', source='manual_admin')
        for text in ['ماادري بس احسة مامناسب', 'اريده يناسب جلسة التخرج', 'لا من يتعدل يخرب شكرا']:
            with self.subTest(text=text), patch.object(self.m, 'classify_contextual_selection', return_value=None):
                result = self.m.resolve_contextual_product_selection(self.db, dict(sender_id='known', text=text), [PRODUCT])
            self.assertIsNone(result)
        self.assertEqual(self.m.get_active_product_binding(self.db, 'known')['product_id'], 'A')
        self.assertEqual(self.db.execute('select count(*) from orders').fetchone()[0], 0)

    def test_ambiguous_multiple_products_still_need_clarification(self):
        self.customer('many')
        for product in [PRODUCT, dict(PRODUCT, product_id='B', product_name='سوت')]:
            self.m.complete_customer_product_link(self.db, 'many', product, 'manual', preserve_existing=True)
        with patch.object(self.m, 'classify_contextual_selection', return_value=None):
            result = self.m.resolve_contextual_product_selection(self.db, dict(sender_id='many', text='هذا مو ذوقي'), [PRODUCT, dict(PRODUCT, product_id='B', product_name='سوت')])
        self.assertIn('تقصدين', result['reply'])
        self.assertEqual(len(self.m.load_customer_products(self.db, 'many')), 2)

    def test_first_customer_with_age_needs_catalogue_not_greeting(self):
        self.customer('discovery')
        event = dict(sender_id='discovery', store_id='default', page_id='page', platform='facebook',
                     text='عدكم ملابس بناتي عمر 9 اشهر', image_url=None, attachments=[], timestamp=0,
                     ref=None, ad_id=None, postback=None, quick_reply=None, referral_source=None, referral_type=None)
        with patch.object(self.m, 'extract_facebook_event', return_value=event), \
             patch.object(self.m, 'load_active_products', return_value=[PRODUCT]), \
             patch.object(self.m, 'load_products_from_file', return_value=[PRODUCT]), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'generate_first_message_reply') as greeting, \
             patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='ما عدنا قياس لهذا العمر حالياً عيني.', create_order=False, order={})) as model:
            self.m.process_webhook(self.db, {}, use_debounce=False)
        greeting.assert_not_called()
        model.assert_called_once()

    def test_price_gives_documented_fabric_total_and_useful_next_step(self):
        self.customer('price')
        self.m.save_delivery_settings(self.db, dict(baghdad_fee=5000, other_fee=5000))
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.call_main_ai(dict(sender_id='price', text='شكد سعره'), 'text', {}, [], [PRODUCT], PRODUCT, None, '', [])
        model.assert_not_called()
        for text in ['عيني', 'لينن', '16,000', '21,000', 'القياس المطلوب']:
            self.assertIn(text, result['reply'])
        self.assertFalse(result['create_order'])

    def test_delivery_question_uses_saved_duration(self):
        self.m.save_delivery_settings(self.db, dict(baghdad_fee=5000, other_fee=5000, delivery_time='من يوم إلى يومين'))
        self.assertEqual(sales_recovery.policy_question('متى يتم التسليم؟'), 'delivery_time')
        result = self.m.store_policy_question_reply('متى يتم التسليم؟')
        self.assertIn('من يوم إلى يومين', result['reply'])
        self.assertNotIn('صورة', result['reply'])

    def test_child_size_edit_is_durable_until_order_actually_changes(self):
        self.customer('child')
        action = conversation_quality.operational_request('عدلي الطلب عمر2سنه')
        self.assertEqual(action, dict(kind='size', value='2 سنة'))
        self.db.execute("insert into orders(sender_id,size,order_items) values('child','سنة','[]')")
        order = dict(self.db.execute("select * from orders where sender_id='child'").fetchone())
        order_actions.record(self.db, 'child', order['id'], action, self.m.now_baghdad_iso())
        self.assertTrue(order_actions.pending(self.db, 'child'))
        self.db.execute("update orders set size='سنتين' where id=?", (order['id'],)); self.db.commit()
        self.assertFalse(order_actions.pending(self.db, 'child'))

    def test_child_edit_does_not_confuse_months_years_or_conditional_question(self):
        self.assertNotEqual(order_actions.size_key('2 سنة'), order_actions.size_key('2 شهر'))
        action = conversation_quality.operational_request('غيري القياس عمر 6 اشهر')
        self.assertEqual(action['value'], '6 شهر')
        self.assertNotEqual(conversation_quality.operational_request('اذا اغيره عمر2سنه؟'), dict(kind='size', value='2 سنة'))
        self.assertEqual(order_actions.single_size(dict(size='44', order_items=json.dumps([dict(size='40'), dict(size='44')]))), '')

    def test_vision_multiple_batches_validate_ids_against_shown_references(self):
        response = dict(choices=[dict(message=dict(content='A'), finish_reason='stop')])
        with patch.object(self.m, 'OPENROUTER_KEY', 'test'), \
             patch.object(self.m, 'is_store_feature_enabled', return_value=True), \
             patch.object(vision_payload, 'MAX_REQUEST_BYTES', 20_000), \
             patch.object(self.m.ai_transport, 'post', return_value=Mock(json=lambda: response)) as provider:
            references = ['https://images.test/' + str(i) + '/' + 'x' * 6000 for i in range(6)]
            candidates = [dict(PRODUCT, image_urls=references)]
            result = self.m.confirm_with_vision('https://images.test/customer', candidates)
        self.assertEqual(result['product_id'], 'A')
        self.assertGreater(provider.call_count, 1)
        sent = [part['image_url']['url'] for call in provider.call_args_list
                for part in call.kwargs['json']['messages'][-1]['content']
                if part.get('type') == 'image_url' and part['image_url']['url'] != 'https://images.test/customer']
        self.assertEqual(sent, references)

    def test_saved_legacy_word_limit_is_replaced(self):
        text = sales_strategy.upgrade_legacy_rules('الرد يجب أن يكون قصيراً جداً (جملة إلى جملتين، لا يتجاوز 30 كلمة).')
        self.assertNotIn('لا يتجاوز 30', text)


if __name__ == '__main__':
    unittest.main()
