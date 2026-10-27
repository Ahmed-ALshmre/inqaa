"""Turn-level sales behavior: new opportunities, truthful cues and real grouping."""
import unittest

from account_app import customer_copy, sales_playbook, sales_strategy, sales_decision
from account_app.tests import test_sales_playbook as fixtures

PRODUCT = fixtures.PRODUCT


class SalesTurnSelectionTests(unittest.TestCase):
    def test_province_is_needed_for_regional_quote_but_not_a_fixed_fee(self):
        product = dict(PRODUCT, colors='اسود')
        self.assertEqual(sales_playbook.price_next_step(product, {}, {'size': '44'}), 'أثبتلج حجز؟')
        regional = sales_playbook.price_next_step(product, {}, {'size': '44'}, delivery_requires_province=True)
        self.assertIn('حتى أحسبلج أجور التوصيل', regional)
        initial = sales_playbook.price_next_step(product, {})
        next_turn = sales_playbook.price_next_step(product, {}, history=[{'direction': 'outgoing', 'text': initial}])
        self.assertNotEqual(initial, next_turn)
        self.assertNotIn('محافظ', next_turn)
        self.assertNotIn('اللون', next_turn)

    def test_second_close_needs_new_information_and_stops_after_refusal(self):
        history = [{'direction': 'outgoing', 'text': 'أثبتلج حجز؟'}]
        context = {'evidence': [{'text': 'سعره'}, {'text': 'متوفر؟'}]}
        self.assertIn('أثبتلج الحجز', sales_playbook.booking_invitation(context, history))
        for text in ['لا ما اريد', 'خليني افكر وارجعلج', 'شكرا', 'حلو بس اريد افكر']:
            with self.subTest(text=text):
                self.assertEqual(sales_playbook.booking_invitation({'evidence': [{'text': text}]}, history), '')
        self.assertEqual(sales_playbook.booking_invitation({'evidence': [{'text': 'سعره'}, {'text': 'سعره'}]}, history), '')
        history.append({'direction': 'outgoing', 'text': 'تحبين أثبتلج الحجز هسه؟'})
        self.assertEqual(sales_playbook.booking_invitation(context, history), '')

    def test_documented_offer_and_scarcity_use_distinct_delivered_turns(self):
        product = dict(PRODUCT, offer='عرض خاص الكمية محدودة')
        first = sales_playbook.next_sales_message(product)
        self.assertIn('وعليه عرض هالفترة', first)
        self.assertNotIn('الكمية', first)
        history = [{'direction': 'outgoing', 'text': first}]
        second = sales_playbook.next_sales_message(product, history)
        self.assertIn('الكمية محدودة', second)
        self.assertNotIn('عرض', second)
        self.assertEqual(sales_strategy.persuasion_error(second, product, history), '')
        history.append({'direction': 'outgoing', 'text': second})
        self.assertEqual(sales_playbook.next_sales_message(product, history), '')
        self.assertEqual(sales_playbook.next_sales_message(dict(PRODUCT, offer='عرض خاص'), history[:1]), '')

    def test_unanswered_construction_detail_blocks_booking(self):
        pending = sales_decision.advance_details([], 'incoming', 'لاستيك لو دگم؟')
        self.assertIn('closure', pending)
        product = dict(PRODUCT, colors='اسود')
        self.assertEqual(sales_playbook.price_next_step(product, {'open_details': pending}, {'size': '44'}), '')

    def test_customer_contact_wording_does_not_change_amounts(self):
        text = customer_copy.chat_wording('رقم الهاتف، سعره 18,000 د.ع والتوصيل 5,000 د.ع')
        self.assertIn('رقم الموبايل', text)
        self.assertIn('18 ألف', text)
        self.assertIn('5 آلاف', text)
        self.assertNotIn('الهاتف', text)
        self.assertEqual(customer_copy.chat_money('بـ17,000 د.ع. وبـ١٢٬٠٠٠ د.ع.'), 'بـ17 ألف وبـ12 ألف')

    def test_delivered_total_uses_actual_fee_and_keeps_exact_amounts(self):
        product = dict(PRODUCT, product_name='سوت', price='18000')
        self.assertEqual(sales_playbook.delivered_price_line(product, [('', 5000)]),
                         'سعره مع التوصيل 23 ألف؛ السوت 18 والتوصيل 5 آلاف 🌷')
        self.assertEqual(sales_playbook.delivered_price_line(product, [(' بغداد', 5000), (' غيرها', 6000)]), '')
        self.assertEqual(sales_playbook.delivered_price_line(product, [('', 5000)], quantity=2), '')
        precise = sales_playbook.delivered_price_line(dict(product, price='18500'), [('', 5500)])
        self.assertIn('24 ألف', precise)
        self.assertIn('18 ألف و500', precise)
        self.assertIn('5 آلاف و500', precise)
        self.assertEqual(sales_playbook.size_range_question(dict(product, sizes='من 70 الى 130 كيلو')),
                         'قياساته من 70 الى 130 كيلو، شكد تلبسين؟')


class SalesTurnPipelineTests(unittest.TestCase):
    setUp = fixtures.ReviewedReplyPipelineTests.setUp
    tearDown = fixtures.ReviewedReplyPipelineTests.tearDown
    customer = fixtures.ReviewedReplyPipelineTests.customer
    message = fixtures.ReviewedReplyPipelineTests.message
    ask = fixtures.ReviewedReplyPipelineTests.ask

    def test_customer_price_corrections_and_baghdad_sizes(self):
        product = dict(PRODUCT, product_name='سوت', price='18000', sizes='من 70 الى 130 كيلو')
        for index, text in enumerate(['شكد تصير مع التوصيل؟', 'شكد المبلغ مع التوصيل؟',
                     'السلام عليكم شكد سعر السوت مع التوصيل لبغداد وشكد بي قياسات؟']):
            with self.subTest(text=text):
                result, provider = self.ask(text, product=product, sender='price-correction-' + str(index))
                provider.assert_not_called()
                self.assertIn('سعره مع التوصيل 23 ألف؛ السوت 18 والتوصيل 5 آلاف 🌷', result['reply'])
                self.assertNotIn('نكمل اختيارج', result['reply'])
                self.assertNotIn('شامل', result['reply'])
                if 'قياسات' in text:
                    self.assertIn('قياساته من 70 الى 130 كيلو، شكد تلبس؟', result['reply'])
                    self.assertIn('وعليكم السلام', result['reply'])
                else:
                    self.assertIn('شنو القياس اللي يناسبك؟', result['reply'])
                self.assertFalse(result['create_order'])
        self.m.save_delivery_settings(self.db, {'baghdad_fee': 4000, 'other_fee': 7000})
        known_province = self.m.transparent_price_reply({'sender_id': 'reply', 'text':
            'السلام عليكم شكد سعر السوت مع التوصيل لبغداد وشكد بي قياسات؟'},
            product, {'province': 'البصرة'}, [product], [])
        self.assertIn('سعره مع التوصيل 22 ألف', known_province['reply'])
        self.assertNotIn('25 ألف', known_province['reply'])

    def test_unknown_detail_routes_entire_compound_question_to_human(self):
        for index, text in enumerate(['السعر وشنو نوع القماش وشكد طول القميص؟', 'شكد سعره ولاستيك لو دگم؟']):
            with self.subTest(text=text):
                result, provider = self.ask(text, product=PRODUCT, sender='unknown-compound-' + str(index))
                provider.assert_not_called()
                self.assertTrue(result['requires_human'])
                self.assertTrue(result['_direct_fact_handoff'])
                self.assertEqual(result['reply'], '')
                self.assertFalse(result['create_order'])

    def test_local_composer_preserves_price_group_and_later_scarcity(self):
        product = dict(PRODUCT, offer='عرض خاص لفترة محدودة على اخر الكمية')
        result, provider = self.ask('سعره', product=product)
        provider.assert_not_called()
        self.assertNotIn('اختيارج فد شي حلو', result['reply'])
        self.assertNotIn('الكمية', result['reply'])
        plan = self.m.plan_sales_bubbles(result, result['reply'])
        self.assertEqual(len(plan['reply_parts']), 3)
        self.assertIn('عرض هالفترة', plan['reply_parts'][0])
        for amount in ['15 ألف', '5 آلاف', '20 ألف']:
            self.assertIn(amount, plan['reply_parts'][1])
        self.message('reply', 'outgoing', result['reply'])
        context = self.m.sales_context.load(self.db, 'default', 'reply')
        later = self.m.transparent_price_reply({'sender_id': 'reply', 'text': 'سعره', '_sales_context': context},
                                              product, {}, [product], [])
        later_plan = self.m.plan_sales_bubbles(later, later['reply'])
        self.assertTrue(any('الكمية الباقية' in part and 'عرض' not in part for part in later_plan['reply_parts']))
        self.assertFalse(any(part.endswith('،') for part in later_plan['reply_parts']))

    def test_availability_can_close_on_known_size_without_forcing_province(self):
        product = dict(PRODUCT, colors='اسود')
        result, provider = self.ask('متوفر', product=product, customer={'size': '44'})
        provider.assert_not_called()
        self.assertIn('أثبتلك حجز؟', result['reply'])
        self.assertNotIn('محافظ', result['reply'])
        self.assertNotIn('اختيارج فد شي حلو', result['reply'])
        self.assertEqual(len(self.m.plan_sales_bubbles(result, result['reply'])['reply_parts']), 2)
        self.assertFalse(result['create_order'])

    def test_checkout_after_consent_requests_mobile_and_only_missing_fields(self):
        self.customer('consented')
        self.db.execute('UPDATE customers SET province=? WHERE sender_id=?', ('بغداد', 'consented'))
        self.db.commit()
        _, message = self.m.create_order_if_valid(self.db, 'consented', {'order': {'items': [
            dict(product_id=PRODUCT['product_id'], size='44', color='اسود', quantity=1)]}}, PRODUCT)
        self.assertIn('رقم الموبايل', message)
        self.assertIn('العنوان', message)
        self.assertIn('حتى أثبتلج الحجز', message)
        self.assertNotIn('المحافظة', message)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_simple_size_confirmation_uses_owner_fit_and_next_missing_step(self):
        product = dict(PRODUCT, colors='اسود', offer='عرض خاص')
        result, provider = self.ask('قياسي 44', product=product)
        provider.assert_not_called()
        self.assertIn('متوفر', result['reply'])
        self.assertIn('فيت', result['reply'])
        self.assertIn('محافظة', result['reply'])
        self.assertNotIn('وزن', result['reply'])
        self.assertFalse(result['create_order'])
        parts = self.m.plan_sales_bubbles(result, result['reply'])['reply_parts']
        self.assertTrue(1 <= len(parts) <= 4)
        for text in ['قياسي 56', 'قياسي 44؟', 'قياسي 44 بس اخاف يضيق', 'قياسي 44 لو 46', 'قياسي 44 واحجزيه']:
            with self.subTest(text=text):
                self.assertIsNone(self.m.guided_size_choice_reply(
                    {'sender_id': 'reply', 'text': text}, product, {}, [product], []))
        self.assertIsNone(self.m.guided_size_choice_reply(
            {'sender_id': 'reply', 'text': 'قياسي 44', '_sales_context': {'open_details': ['closure']}},
            product, {}, [product], []))


if __name__ == '__main__':
    unittest.main()
