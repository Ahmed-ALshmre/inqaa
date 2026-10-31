"""A published single-piece price and its delivered total must survive checkout."""
import json
import unittest
from unittest.mock import patch

from account_app import checkout, pricing, sales_playbook
from account_app.tests import test_stale_detail_checkout_oct28 as fixtures


PRICE = 'عرض خاص سعر القطعتين ب 25 الف سعر القطعة 15 الف'
TOTAL_QUESTION = 'يعني اذا اطلب قطعه ويه التوصيل يصير ٢٠'
AUDIO_TEXT = ('عيني اني أريد قطعة وحدة. انتو قلتو المفرد ب15، يعني إذا القطعة 15 '
              'ويه التوصيل ح يصير 20، إذا هيج اني احجز. ليش المبلغ يحتاج تأكيد؟')


class SinglePiecePricingTests(unittest.TestCase):
    def test_explicit_single_and_offer_prices_work_in_either_order_and_digit_style(self):
        for value in (PRICE, 'سعر القطعه الواحده ١٥ ألف، سعر القطعتين ب ٢٥ ألف',
                      'القطعة المفردة ب 15,000 والقطعتين ب 25,000',
                      'عرض 2 قطع ب 25 الف سعر القطعة 15 الف'):
            with self.subTest(value=value):
                product = dict(product_id='P', price=value, offer=value)
                self.assertEqual(pricing.terms(product), (1, 15000, False))
                self.assertEqual(pricing.offer_tiers(product), [(2, 25000)])
                for quantity, amount in ((1, 15000), (2, 25000), (3, 40000), (4, 50000)):
                    result = pricing.quote([dict(product_id='P', quantity=quantity)], [product], 5000)
                    self.assertEqual(result['product_total'], amount)
                    self.assertEqual(result['delivery_fee'], 5000)
                    self.assertEqual(sum(i['quantity'] * i['unit_price'] for i in result['items']), amount)

    def test_offer_in_price_field_is_retained_without_duplicate_offer_field(self):
        product = dict(product_id='P', price=PRICE)
        self.assertEqual(pricing.offer_tiers(product), [(2, 25000)])

    def test_ambiguous_conflicting_and_nonpositive_prices_stay_blocked(self):
        for value in ('15 الف و25 الف', 'سعر القطعتين ب25 الف', 'عرض 2 قطعة ب25 الف',
                      PRICE + ' 12000', PRICE + ' سعر القطعة 14 الف',
                      'سعر القطعة 0 سعر القطعتين ب25 الف',
                      'سعر القطعة -15 الف سعر القطعتين ب25 الف'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                pricing.terms(dict(price=value))
        product = dict(product_id='P', price=PRICE, offer='القطعتين ب24 الف')
        with self.assertRaisesRegex(ValueError, 'متعارضة'):
            pricing.quote([dict(product_id='P', quantity=2)], [product], 5000)


class DeliveredTotalInquiryTests(unittest.TestCase):
    def test_total_questions_are_not_persistent_discount_conditions_or_consent(self):
        for text in (TOTAL_QUESTION, AUDIO_TEXT,
                     'يعني القطعة ويا التوصيل يصير 20 الف؟',
                     'اذا اطلب قطعتين ويا التوصيل يصير ٣٠؟',
                     'المجموع ويه التوصيل يطلع 20000؟'):
            with self.subTest(text=text):
                self.assertTrue(checkout.delivered_total_inquiry(text))
                self.assertTrue(sales_playbook.delivery_price_question(text))
                self.assertEqual(checkout.unresolved_purchase_terms(text), 'unanswered_price_or_shipping')
                self.assertIsNone(checkout.unresolved_condition([dict(direction='incoming', text=text)]))
                self.assertFalse(checkout.is_confirmation(text))

    def test_actual_discount_shipping_and_arrival_conditions_remain_blocking(self):
        for text, kind in (
            ('يعني اذا السعر 10 الف ثبتي ويا التوصيل يصير 15', 'price'),
            ('اذا اطلب قطعة ويا التوصيل يصير 10 فقط احجزي', 'price'),
            ('يعني اذا التوصيل يصير 10 لازم، غير هذا ما اريد', 'price'),
            ('احجزي بشرط التوصيل مجاني', 'free_delivery'),
            ('ثبتي اذا يوصل باجر فقط', 'arrival'),
        ):
            with self.subTest(text=text):
                self.assertFalse(checkout.delivered_total_inquiry(text))
                self.assertEqual(checkout.unresolved_condition([], text)['kind'], kind)


class SinglePieceCheckoutTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.StaleDetailCheckoutTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.StaleDetailCheckoutTests.tearDownClass.__func__)
    tearDown = fixtures.StaleDetailCheckoutTests.tearDown
    save = fixtures.StaleDetailCheckoutTests.save
    order_count = fixtures.StaleDetailCheckoutTests.order_count
    assert_confirmation = fixtures.StaleDetailCheckoutTests.assert_confirmation

    def setUp(self):
        fixtures.StaleDetailCheckoutTests.setUp(self)
        self.product.update(product_name='دشداشة قديفة تطريز', price=PRICE, offer=PRICE,
                            colors='ماروني اخضر', sizes='من 55 كيلو الى 95')

    def process(self, text):
        result, model = fixtures.StaleDetailCheckoutTests.process(self, text)
        with patch.object(self.m, 'send_text_to_facebook', return_value=True), \
             patch.object(self.m, 'send_image_to_facebook', return_value=True):
            self.m.send_webhook_result_to_facebook(result, self.sender)
        return result, model

    def assert_total_answer(self, text):
        result, model = self.process(text)
        model.assert_not_called()
        self.assertIn('20 ألف', result['reply'])
        self.assertNotIn('السعر الأقل', result['reply'])
        self.assertNotIn('ما ثبتت', result['reply'])
        self.assertNotIn('سعر القطعتين', result['reply'])
        self.assertFalse(result.get('requires_human'))
        self.assertEqual(self.order_count(), 0)
        return result

    def test_initial_price_separates_single_piece_from_two_piece_offer(self):
        result, model = self.process('السعر')
        model.assert_not_called()
        self.assertIn('15 ألف', result['reply'])
        self.assertIn('25 ألف', result['reply'])
        self.assertIn('20 ألف', result['reply'])
        self.assertEqual(self.order_count(), 0)

    def test_reported_question_and_audio_recover_after_previous_bad_refusals_then_book_once(self):
        self.save('incoming', 'هذا اللون بي قياس XXL')
        self.save('outgoing', 'سعر القطعة 15 ألف، والتوصيل 5 آلاف، المجموع 20 ألف.')
        self.save('incoming', TOTAL_QUESTION)
        self.save('outgoing', 'السعر الأقل يحتاج تأكيد، ما أگدر أوعدج بيه هسه.')
        self.save('outgoing', 'ما ثبتت طلب على هالاتفاق.')
        self.assert_total_answer(AUDIO_TEXT)
        self.assert_total_answer('يعني القطعة ويا التوصيل يصير 20 الف؟')
        self.process('اريد قطعة وحدة ماروني XXL')
        first, _ = self.process('07701234567')
        self.assertNotIn('رقم الموبايل', first['reply'])
        final, model = self.process('بغداد حي الاختبار قرب المدرسة')
        model.assert_not_called()
        self.assert_confirmation(final)
        order = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual(order['total_amount'], 20000)
        items = json.loads(order['order_items'])
        self.assertEqual(sum(i['quantity'] for i in items), 1)
        self.assertEqual(items[0]['color'], 'ماروني')
        self.assertEqual(items[0]['size'], 'XXL')
        self.process('07701234567')
        self.assertEqual(self.order_count(), 1)

    def test_incorrect_total_is_corrected_without_accepting_a_lower_price(self):
        self.assert_total_answer('يعني القطعة ويا التوصيل يصير 10 الف؟')

    def test_compound_total_question_does_not_hide_fabric_or_inspection_questions(self):
        for suffix in ('وشنو قماشه؟', 'واكدر افحص قبل الدفع؟', 'وشنو الوانه؟'):
            text = TOTAL_QUESTION + ' ' + suffix
            self.assertTrue(checkout.delivered_total_inquiry(text))
            self.assertIsNone(self.m.transparent_price_reply(
                dict(self.ev, text=text), self.product, {}, [self.product], []))

    def test_two_piece_total_uses_the_offer_and_does_not_create_order(self):
        result, model = self.process('اذا اطلب قطعتين ويا التوصيل يصير ٣٠؟')
        model.assert_not_called()
        self.assertIn('30 ألف', result['reply'])
        self.assertEqual(self.order_count(), 0)

    def test_two_piece_purchase_saves_offer_total_and_shipping_once(self):
        self.process('اريد قطعتين ماروني XXL ثبتي')
        result, model = self.process('07701234567 بغداد حي الاختبار قرب المدرسة')
        model.assert_not_called()
        self.assert_confirmation(result)
        order = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        self.assertEqual(order['total_amount'], 30000)
        items = json.loads(order['order_items'])
        self.assertEqual(sum(i['quantity'] for i in items), 2)
        self.assertEqual(sum(i['quantity'] * i['unit_price'] for i in items), 25000)

    def test_real_lower_price_condition_still_gets_a_refusal_at_the_actual_single_price(self):
        result, model = self.process('اذا السعر 10 الف ثبتي 07701234567 بغداد حي الاختبار')
        model.assert_not_called()
        self.assertIn('15 ألف', result['reply'])
        self.assertIn('ما نكدر ننزل أكثر', result['reply'])
        self.assertEqual(self.order_count(), 0)
