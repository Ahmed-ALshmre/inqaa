"""Owner-approved Iraqi endearments, with facts, context and reply boundaries intact."""
import json
import unittest
from unittest.mock import Mock, patch

from account_app import sales_engagement as engagement
from account_app.tests import test_audit_orders as fixtures


class WarmAddressTests(unittest.TestCase):
    def test_local_female_reply_can_use_each_approved_word_without_changing_price(self):
        history = []
        for word in ('حبيبتي', 'عمري', 'حياتي', 'يا روحي', 'عيني'):
            reply = engagement.warm_local_address('عليه عرض على آخر قطع، وصار سعره 16 ألف 🌷 شنو اللون؟',
                                                  'female', history)
            self.assertIn(word, reply)
            self.assertTrue(reply.startswith('عليه عرض على آخر قطع، وصار سعره 16 ألف'))
            self.assertEqual(reply.count('؟'), 1)
            self.assertEqual(reply.count('16 ألف'), 1)
            self.assertEqual(len(engagement._ADDRESS.findall(reply)), 1)
            history += [dict(direction='outgoing', text=reply),
                        dict(direction='outgoing', text='الوردي موجود.'),
                        dict(direction='outgoing', text='قياس 44 متوفر.')]

    def test_recent_affection_does_not_force_another_address(self):
        text = 'سعره 16 ألف 🌷'
        history = [dict(direction='outgoing', text='تدللين حياتي 🌷')]
        self.assertEqual(engagement.warm_local_address(text, 'female', history), text)

    def test_repeated_local_neutral_template_leaves_plain_turns_then_varies(self):
        history, replies = [], []
        for _ in range(4):
            reply = engagement.warm_local_address('قياس 44 متوفر عيني 🌷', 'female', history)
            replies.append(reply)
            history.append(dict(direction='outgoing', text=reply))
        self.assertIn('حبيبتي', replies[0])
        self.assertFalse(engagement._ADDRESS.search(replies[1]))
        self.assertFalse(engagement._ADDRESS.search(replies[2]))
        self.assertIn('عمري', replies[3])
        self.assertTrue(all('قياس 44 متوفر' in reply for reply in replies))

    def test_failed_delivery_does_not_count_as_affection_the_customer_saw(self):
        history = [dict(direction='outgoing', text='حبيبتي', raw_payload=json.dumps({'_delivery_status': 'failed'}))]
        self.assertIn('حبيبتي', engagement.warm_local_address('سعره 16 ألف 🌷', 'female', history))

    def test_neutral_address_is_varied_and_existing_affection_is_not_stacked(self):
        self.assertEqual(engagement.warm_local_address('قياس 44 متوفر عيني 🌷', 'female'),
                         'قياس 44 متوفر حبيبتي 🌷')
        for text in ('متوفر حياتي 🌷', 'تدللين يا روحي، اللون الوردي موجود.', 'هلا حبيبتي، شنو تحتاجين؟'):
            self.assertEqual(engagement.warm_local_address(text, 'female'), text)

    def test_unknown_or_male_gender_is_not_guessed(self):
        for gender in ('unknown', '', None, 'male'):
            text = 'القياس 38/40 متوفر عيني، سعره 12,500 د.ع.'
            self.assertEqual(engagement.warm_local_address(text, gender), text)

    def test_customer_formality_and_distress_override_added_affection(self):
        text = 'التوصيل 5 آلاف.'
        for request in ('خاطبيني رسمي', 'بدون ألقاب', 'لا تكولين حبيبتي', 'عندي شكوى ومتضايقة'):
            history = [dict(direction='incoming', text=request)]
            self.assertEqual(engagement.warm_local_address(text, 'female', history), text)

    def test_spelling_cleanup_keeps_sizes_prices_and_regular_affection(self):
        self.assertEqual(engagement.natural_address('حبيبيتي، قياس 38/40 وسعره 16,000'),
                         'حبيبتي، قياس 38/40 وسعره 16,000')
        self.assertEqual(engagement.natural_address('حياتي، قياس 44'), 'حياتي، قياس 44')


class WarmAddressPipelineTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def test_rendered_local_reply_and_parts_have_one_consistent_address(self):
        result = dict(_local_reply=True, _single_message=True, _compact_offer_price=True,
                      reply_parts=['سعره 16,000 د.ع 🌷 شنو اللون؟'])
        formatted, reply = self.m.format_customer_reply(result, result['reply_parts'][0], customer={'gender': 'female'})
        self.assertIn('حبيبتي', reply)
        self.assertIn('16 ألف', reply)
        self.assertEqual(formatted['reply_parts'], [reply])
        plan = self.m.plan_sales_bubbles(formatted, reply)
        self.assertEqual(plan['reply_parts'], [reply])
        self.assertFalse(plan['needs_rewrite'])

    def test_confirmation_unavailable_review_and_wholesale_are_not_reworded(self):
        for text, overrides in [('تم تثبيت الطلب بنجاح 🌸', {}),
                                ('هذي الموديلات مو متوفرة عدنا حالياً.', {}),
                                ('القياس يحتاج تأكيد.', {'_needs_fact_review': True})]:
            result = dict(_local_reply=True, **overrides)
            _, reply = self.m.format_customer_reply(result, text, customer={'gender': 'female'})
            self.assertEqual(reply, text)
        token = self.m._current_store_id.set('golden-threads')
        try:
            _, reply = self.m.format_customer_reply({'_local_reply': True}, 'متوفر.', customer={'gender': 'female'})
            self.assertEqual(reply, 'متوفر.')
        finally:
            self.m._current_store_id.reset(token)

    def test_main_and_first_message_receive_the_same_owner_approved_address_guide(self):
        self.customer('warm-prompt')
        self.db.execute("UPDATE customers SET gender='female' WHERE sender_id='warm-prompt'")
        self.db.commit()
        product = dict(product_id='A', product_name='فستان دانتيل', price='16000', stock='متوفر', status='active')
        response = Mock(json=lambda: {'choices': [{'message': {'content':
            json.dumps({'reply': 'تدللين حياتي، هذا فستان دانتيل.', 'create_order': False, 'order': {}})},
            'finish_reason': 'stop'}]})
        event = dict(sender_id='warm-prompt', text='شنو تنصحيني', image_url=None, attachments=[])
        with patch.object(self.m, 'OPENROUTER_KEY', 'test'), \
             patch.object(self.m.ai_transport, 'post', return_value=response) as provider:
            self.m._call_main_ai_once(event, 'text', {'gender': 'female'}, [], [product], product, None, '', [])
            main_system = provider.call_args.kwargs['json']['messages'][0]['content']
            self.assertIn(engagement.ADDRESS_GUIDE, main_system)
            self.m.generate_first_message_reply(self.db, dict(event, text='مرحبا'), [product], '', [])
            first_system = provider.call_args.kwargs['json']['messages'][0]['content']
            self.assertIn(engagement.ADDRESS_GUIDE, first_system)

