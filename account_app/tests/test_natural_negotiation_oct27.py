"""Haggling gets a truthful refusal and a next step, not a permanent hold."""
import unittest
from unittest.mock import patch

from account_app import checkout, owner_sales
from account_app.tests import test_checkout_completion_oct25 as completion


PRODUCT = dict(product_id='N', product_name='فستان', sizes='38 إلى 52', colors='اسود وردي',
               price='15000', offer='عرض حالي', stock='متوفر', status='active', fabric='باربي')


class NegotiationReplyTests(unittest.TestCase):
    def answer(self, text, product=None, **kwargs):
        return owner_sales.negotiation_reply(dict(text=text), product or PRODUCT, {}, {}, [], **kwargs)

    def test_discount_request_gets_actual_price_and_a_natural_refusal(self):
        for text in ('اكو تخفيض؟', 'ممكن خصم', 'تنزلين السعر', 'يصير اقل', 'خليها ب10'):
            result = self.answer(text)
            self.assertIsNotNone(result, text)
            self.assertIn('15 ألف', result['reply'])
            self.assertIn('ما نكدر ننزل أكثر', result['reply'])
            self.assertIn('القطعة تستاهل', result['reply'])
            self.assertFalse(result['create_order'])
            self.assertFalse(result['requires_human'])
            for word in ('حسم', 'شرط', 'موافقة المتجر'):
                self.assertNotIn(word, result['reply'])

    def test_no_recorded_offer_never_claims_a_discount(self):
        result = self.answer('اكو تخفيض؟', dict(PRODUCT, offer=''))
        self.assertIn('السعر النهائي', result['reply'])
        self.assertIn('15 ألف', result['reply'])
        self.assertNotIn('عليه عرض', result['reply'])

    def test_recorded_reduced_price_counts_as_an_offer_without_inventing_a_new_discount(self):
        result = self.answer('ممكن تخفيض؟', dict(PRODUCT, price='13000 بدل 15 الف', offer=''))
        self.assertIn('13 ألف', result['reply'])
        self.assertIn('عليه عرض', result['reply'])
        self.assertNotIn('سعره 15', result['reply'])

    def test_known_size_moves_to_color_without_reasking_size(self):
        result = owner_sales.negotiation_reply(dict(text='ممكن تخفيض؟'), PRODUCT,
            {'size': '44'}, {'evidence': [{'text': 'قياسي 44'}]}, [])
        self.assertIn('اللون', result['reply'])
        self.assertNotIn('قياس تلبسين', result['reply'])

    def test_mixed_fabric_question_keeps_the_fabric_answer(self):
        result = self.answer('ممكن تخفيض وشنو القماش؟')
        self.assertIn('باربي', result['reply'])
        self.assertIn('15 ألف', result['reply'])

    def test_free_shipping_request_uses_actual_regional_fee(self):
        result = self.answer('خلي التوصيل مجاني', delivery_options=[('', 5000)])
        self.assertIn('ما نكدر نخلي التوصيل مجاني', result['reply'])
        self.assertIn('5 آلاف', result['reply'])
        self.assertFalse(result['create_order'])

    def test_actual_free_shipping_offer_is_honored(self):
        product = dict(PRODUCT, delivery='توصيل مجاني')
        result = self.answer('خلي التوصيل مجاني', product, delivery_options=[('', 5000)])
        self.assertIn('التوصيل مجاني بهذا العرض', result['reply'])
        self.assertNotIn('5 آلاف', result['reply'])

    def test_budget_alternative_and_post_order_keep_their_own_routes(self):
        self.assertIsNone(self.answer('ميزانيتي 10 اريد ارخص'))
        self.assertIsNone(owner_sales.negotiation_reply(dict(text='ممكن تخفيض', _post_order={'id': 1}),
                                                       PRODUCT, {}, {}, []))


class NegotiationMemoryTests(unittest.TestCase):
    def test_answered_discount_is_not_a_permanent_condition(self):
        for condition, reply in [
            ('اذا السعر 10 الف ثبتي', 'هذا السعر النهائي، ما عدنا تخفيض حاليًا، سعره 15 ألف.'),
            ('احجزي بشرط التوصيل مجاني', 'ما نكدر نخلي التوصيل مجاني، التوصيل 5 آلاف. سعره 15 ألف.')]:
            history = [dict(direction='incoming', text=condition), dict(direction='outgoing', text=reply)]
            self.assertIsNone(checkout.unresolved_condition(history, 'ثبتي'))
            self.assertIsNone(checkout.unresolved_condition(history, '07701234567 بغداد حي الاختبار'))

    def test_no_answer_and_reasserted_discount_are_still_not_consent(self):
        history = [dict(direction='incoming', text='اذا السعر 10 الف ثبتي')]
        self.assertIsNotNone(checkout.unresolved_condition(history, '07701234567'))
        history.append(dict(direction='outgoing', text='هذا السعر النهائي، سعره 15 ألف.'))
        self.assertIsNotNone(checkout.unresolved_condition(history, 'لا اذا السعر 10 الف فقط'))

    def test_baghdad_is_not_the_word_for_tomorrow(self):
        history = [dict(direction='incoming', text='اذا السعر 10 الف ثبتي 07701234567 بغداد حي الاختبار')]
        self.assertEqual(checkout.unresolved_condition(history)['kind'], 'price')

    def test_arrival_guarantee_is_never_cleared_by_a_price_refusal(self):
        history = [dict(direction='incoming', text='ثبتي اذا يوصل باجر فقط'),
                   dict(direction='outgoing', text='ما نكدر ننزل أكثر، سعره 15 ألف.')]
        self.assertEqual(checkout.unresolved_condition(history, 'ثبتي')['kind'], 'arrival')

    def test_customer_quoted_refusal_does_not_resolve_their_own_condition(self):
        history = [dict(direction='incoming', text='اذا السعر 10 الف ثبتي'),
                   dict(direction='incoming', text='تكولين هذا السعر النهائي سعره 15 ألف')]
        self.assertIsNotNone(checkout.unresolved_condition(history, '07701234567'))


class NegotiationCheckoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        completion.CheckoutCompletionTests.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        completion.CheckoutCompletionTests.tearDownClass.__func__(cls)

    setUp = completion.CheckoutCompletionTests.setUp
    tearDown = completion.CheckoutCompletionTests.tearDown
    data = completion.CheckoutCompletionTests.data
    incoming = completion.CheckoutCompletionTests.incoming
    link_cart = completion.CheckoutCompletionTests.link_cart
    assert_confirmation = completion.CheckoutCompletionTests.assert_confirmation

    def process(self, text):
        result, model = completion.CheckoutCompletionTests.process(self, text)
        # A generated refusal is context only after messaging accepts it.
        with patch.object(self.m, 'send_text_to_facebook', return_value=True), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True):
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        return result, model

    def test_reject_discount_then_complete_the_saved_cart_without_repeated_questions(self):
        self.link_cart()
        refusal, model = self.process('احجزي اذا يصير تخفيض بالسعر')
        model.assert_not_called()
        self.assertIn('16 ألف', refusal['reply'])
        self.assertNotIn('شرط', refusal['reply'])
        self.assertNotIn('قياس تلبس', refusal['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)
        history = self.m.load_history(self.db, self.sender, limit=100)
        self.assertIsNone(checkout.unresolved_condition(history), str([(r['direction'], r['text']) for r in history]))
        result, model = self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called()
        self.assert_confirmation(result)
        saved = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual(saved['total_amount'], 21000)

    def test_discount_request_with_full_contact_does_not_book_the_requested_lower_price(self):
        self.link_cart()
        refusal, model = self.process('اذا السعر 10 الف ثبتي 07701234567 بغداد حي الاختبار')
        model.assert_not_called()
        self.assertIn('16 ألف', refusal['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)
        result, model = self.process('تمام ثبتي')
        model.assert_not_called()
        self.assert_confirmation(result)
        saved = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual(saved['address'], 'بغداد حي الاختبار')
