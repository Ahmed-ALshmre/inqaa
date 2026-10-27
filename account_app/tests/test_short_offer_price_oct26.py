"""Short Iraqi offer-first prices, with requested costs and known choices intact."""
import unittest
from unittest.mock import patch

from account_app import sales_playbook as playbook, sales_strategy
from account_app.reply_layout import plan_sales_bubbles
from account_app.tests import test_audit_orders as fixtures

PRODUCT = dict(product_id='F1', product_name='فستان دانتيل', price='16000', stock='متوفر',
               status='active', sizes='38 إلى 52', colors='اسود وردي', fabric='باربي',
               offer='عرض خاص لفترة محدودة على اخر الكمية')


class ShortOfferFactsTests(unittest.TestCase):
    def test_owner_style_uses_current_price_without_a_fake_old_price_or_count(self):
        line = playbook.last_pieces_price_line(PRODUCT)
        self.assertEqual(line, 'عليه عرض على آخر قطع، وصار سعره 16 ألف 🌷')
        self.assertEqual(sales_strategy.persuasion_error(line, PRODUCT), '')
        alternate = dict(PRODUCT, offer='عرض على آخر القطع')
        self.assertEqual(playbook.last_pieces_price_line(alternate), line)
        self.assertEqual(sales_strategy.persuasion_error(line.replace('آخر قطع', 'آخر القطع'), alternate), '')
        self.assertIn('18 ألف و500', playbook.last_pieces_price_line(dict(PRODUCT, price='18500')))
        for claim in ('خصم', '%', 'قبل', 'اليوم فقط', 'آخر قطعة', 'باقي 2'):
            self.assertNotIn(claim, line)

    def test_missing_or_inactive_offer_does_not_invent_last_pieces(self):
        for offer in ('', 'عرض خاص', 'عرض خاص الكمية محدودة', 'العرض انتهى',
                      'قطعتين بسعر 30000', 'عرض خاص على اخر الكمية بشرط شراء قطعتين'):
            self.assertEqual(playbook.last_pieces_price_line(dict(PRODUCT, offer=offer)), '', offer)
        for overrides in (dict(stock='نفذ'), dict(status='disabled'), dict(stock_quantity=0)):
            self.assertEqual(playbook.last_pieces_price_line(dict(PRODUCT, **overrides)), '')

    def test_quantity_and_bundle_terms_are_not_shortened_into_wrong_unit_prices(self):
        self.assertEqual(playbook.last_pieces_price_line(PRODUCT, quantity=2), '')
        self.assertEqual(playbook.last_pieces_price_line(dict(PRODUCT, price='بكج 3 قطع سعره 25 الف')), '')
        self.assertEqual(playbook.last_pieces_price_line(dict(PRODUCT, notes='قطعتين بسعر 30000')), '')

    def test_only_accepted_history_suppresses_repeating_the_offer(self):
        text = 'عليه عرض على آخر قطع، وصار سعره 16 ألف 🌷'
        for status, expected in [('accepted', 'سعره 16 ألف 🌷'), ('failed', text)]:
            history = [dict(direction='outgoing', text=text, raw_payload={'_delivery_status': status})]
            self.assertEqual(playbook.last_pieces_price_line(PRODUCT, history), expected)
        self.assertEqual(playbook.last_pieces_price_line(PRODUCT, allow_offer=False), 'سعره 16 ألف 🌷')

    def test_main_guidance_allows_same_style_without_conflicting_split_instruction(self):
        guide = playbook.guidance({'text': 'شكد سعره'}, PRODUCT)
        self.assertIn('الصياغة مرنة', guide)
        self.assertIn('أجب عن المطلوب كله', guide)
        guidance = sales_strategy.persuasion_guidance(PRODUCT, (), 'شكد سعره')
        self.assertIn('وصار سعره', guidance)
        self.assertNotIn('لا تجمعهما في جملة واحدة', guidance)


class ShortOfferReplyTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set('default')
        fixtures.AuditFixTests.customer(self, 'short-offer')
        self.m.save_delivery_settings(self.db, {'baghdad_fee': 5000, 'other_fee': 5000})

    def tearDown(self):
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def reply(self, text='شكد سعره؟', product=None, customer=None):
        product = product or PRODUCT
        with patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.call_main_ai(dict(sender_id='short-offer', text=text), 'text', customer or {}, [],
                                        [product], product, None, '', [], customer_products=[product])
        model.assert_not_called()
        self.assertFalse(result['create_order'])
        self.assertTrue(result['_compact_offer_price'])
        self.assertEqual(result['reply_parts'], [result['reply']])
        plan = plan_sales_bubbles(result, result['reply'])
        self.assertEqual(len(plan['reply_parts']), 1)
        self.assertFalse(plan['needs_rewrite'], plan['rewrite_reasons'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
        return result

    def test_simple_price_is_offer_then_price_then_one_natural_question(self):
        result = self.reply()
        reply = result['reply']
        self.assertTrue(reply.startswith('عليه عرض على آخر قطع، وصار سعره 16 ألف 🌷'))
        self.assertEqual(reply.count('؟'), 1)
        self.assertIn('قياس', reply)
        for unsolicited in ('التوصيل', 'المجموع', 'الإجمالي', 'باربي', 'قماشه', 'قليلة كلش'):
            self.assertNotIn(unsolicited, reply)

    def test_known_size_moves_to_missing_color_instead_of_repeating_size(self):
        reply = self.reply(customer={'size': '44'})['reply']
        self.assertIn('اللون', reply)
        self.assertNotIn('شكد قياس', reply)
        self.assertEqual(reply.count('؟'), 1)

    def test_known_child_age_is_not_reasked_after_offer(self):
        product = dict(PRODUCT, sizes='من 3 إلى 7 سنوات')
        reply = self.reply(product=product, customer={'age': 5})['reply']
        self.assertIn('اللون', reply)
        self.assertNotIn('شكد العمر', reply)

    def test_requested_delivery_and_total_are_kept_without_repeating_unit_price(self):
        reply = self.reply('شكد سعره ويا التوصيل')['reply']
        for fact in ('16 ألف', '5 آلاف', '21 ألف'):
            self.assertEqual(reply.count(fact), 1, reply)
        self.assertLess(reply.index('عرض'), reply.index('16 ألف'))
        self.assertLess(reply.index('16 ألف'), reply.index('التوصيل'))

    def test_region_fees_and_free_delivery_remain_correct_when_requested(self):
        self.m.save_delivery_settings(self.db, {'baghdad_fee': 5000, 'other_fee': 7000})
        reply = self.reply('شكد سعره ويا التوصيل', customer={'province': 'البصرة'})['reply']
        self.assertIn('البصرة', reply)
        self.assertIn('7 آلاف', reply)
        self.assertIn('23 ألف', reply)
        free = self.reply('شكد سعره ويا التوصيل', product=dict(PRODUCT, delivery='توصيل مجاني'))['reply']
        self.assertIn('التوصيل مجاني', free)
        self.assertNotIn('23 ألف', free)

    def test_already_delivered_offer_is_not_repeated_on_new_price_question(self):
        self.m.save_message(self.db, 'short-offer', 'outgoing', 'text',
            'عليه عرض على آخر قطع، وصار سعره 16 ألف 🌷', None, None, None, {'_delivery_status': 'accepted'})
        reply = self.reply()['reply']
        self.assertTrue(reply.startswith('سعره 16 ألف 🌷'))
        self.assertNotIn('عرض', reply)
        self.assertNotIn('آخر', reply)

    def test_incompatible_size_keeps_price_without_marketing_pressure(self):
        reply = self.reply(customer={'size': '56'})['reply']
        self.assertIn('16 ألف', reply)
        self.assertNotIn('عرض', reply)
        self.assertNotIn('آخر', reply)
        self.assertNotIn('حجز', reply)

    def test_compound_questions_are_not_consumed_by_simple_price_route(self):
        for text in ('شكد سعره وشنو قماشه؟', 'شكد سعره وطوله؟'):
            self.assertIsNone(self.m.transparent_price_reply(
                dict(sender_id='short-offer', text=text), PRODUCT, {}, [PRODUCT], []))

    def test_sender_delivers_the_complete_offer_and_followup_once(self):
        result = dict(self.reply(), sender_id='short-offer')
        with patch.object(self.m, 'send_text_to_facebook', return_value=True) as send:
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        send.assert_called_once()
        self.assertEqual(send.call_args.args[1], result['reply'])

