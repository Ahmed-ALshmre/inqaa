import unittest

from account_app import dual_sales


class NegotiatedPricingTests(unittest.TestCase):
    def setUp(self):
        self.products = [dict(product_id='T1', price='24000', colors='اسود نيلي', sizes='44 46')]
        self.items = [dict(product_id='T1', color='اسود', size='44', quantity=2)]

    def price(self, **kw):
        return dual_sales.negotiated_quote(self.items, self.products, 5000, **kw)

    def test_no_discount_without_request(self):
        self.assertEqual(self.price(requested_discount=2000)['total_amount'], 53000)

    def test_requested_reductions_and_counteroffer(self):
        for requested, expected in [(500, 52500), (1000, 52000), (2000, 51000), (3000, 51000), (90000, 51000)]:
            with self.subTest(requested=requested):
                q = self.price(requested_discount=requested, customer_requested=True)
                self.assertEqual(q['total_amount'], expected)
                self.assertEqual(q['delivery_fee'], 5000)

    def test_does_not_accumulate_each_turn_or_each_piece(self):
        q = self.price(previous_discount=2000, requested_discount=2000, customer_requested=True)
        self.assertEqual(q['negotiated_discount'], 2000)
        self.items[0]['quantity'] = 5
        self.assertEqual(self.price(previous_discount=2000)['negotiated_discount'], 2000)

    def test_single_piece_revokes_previous_concession(self):
        self.items[0]['quantity'] = 1
        self.assertEqual(self.price(previous_discount=2000, requested_discount=2000, customer_requested=True)['total_amount'], 29000)

    def test_catalog_offer_and_shipping_remain_separate(self):
        self.products[0]['offer'] = 'قطعتين ب40000'
        q = self.price(requested_discount=1000, customer_requested=True)
        self.assertEqual((q['product_total'], q['negotiated_discount'], q['total_amount']), (40000, 1000, 44000))
        self.products[0]['delivery'] = 'توصيل مجاني'
        self.assertEqual(self.price(previous_discount=1000)['total_amount'], 39000)

    def test_fingerprint_rejects_changed_price_quantity_or_variant(self):
        accepted = self.price(previous_discount=2000)['quote_fingerprint']
        self.assertTrue(dual_sales.offer_matches(accepted, self.price(previous_discount=2000)))
        self.items[0]['color'] = 'نيلي'
        self.assertFalse(dual_sales.offer_matches(accepted, self.price(previous_discount=2000)))
        self.items[0]['color'] = 'اسود'
        self.products[0]['price'] = '25000'
        self.assertFalse(dual_sales.offer_matches(accepted, self.price(previous_discount=2000)))

    def test_equivalent_cart_line_merge_keeps_agreement(self):
        before=self.price(previous_discount=2000)['quote_fingerprint']
        self.items[0]['quantity']=1
        self.items.append(dict(self.items[0]))
        self.assertTrue(dual_sales.offer_matches(before,self.price(previous_discount=2000)))

    def test_bad_monetary_or_quantity_types(self):
        for value in [-1, 2.5, True, '2000']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.price(requested_discount=value, customer_requested=True)
        for quantity in [0, -1, 2.5, True, 21]:
            self.items[0]['quantity'] = quantity
            with self.subTest(quantity=quantity), self.assertRaises(ValueError):
                self.price()


class WriterGuardTests(unittest.TestCase):
    def check(self, text, **kw):
        return dual_sales.validate_parts([text], **kw)

    def test_save_does_not_mean_fulfillment(self):
        self.assertIn('unsupported_fulfilled', self.check('طلبج مثبت وكلشي جاهز', effects={'saved'}))
        self.assertEqual(self.check('إي عيوني طلبج مثبت', effects={'saved'}), [])
        self.assertEqual(self.check('الطلب بعده ما انشحن'), [])

    def test_unsupported_save_cancel_update_and_images(self):
        for text, issue in [('ثبتتلج الطلب', 'unsupported_saved'), ('تم إلغاء الطلب', 'unsupported_cancelled'),
                            ('عدلتلج الطلب', 'unsupported_updated'), ('هذن صور الأسود', 'unsupported_images')]:
            with self.subTest(text=text):
                self.assertIn(issue, self.check(text))

    def test_negated_customer_fear_is_not_a_shipping_claim(self):
        self.assertEqual(self.check('تفحصينهن بوجود المندوب قبل الدفع، وإذا ما طابق يرجع وياه بدون دفع.'), [])

    def test_malformed_dialect_and_unapproved_prices(self):
        self.assertIn('unnatural_phrase', self.check('صار تدللين'))
        self.assertIn('unapproved_number', self.check('أخليهن بـ50', allowed_numbers={'51'}))
        self.assertEqual(self.check('تدللين، أخليهن بـ٥١ واصلات', allowed_numbers={'51'}), [])
        self.assertIn('observed_dialect_typo',self.check('واصل لباب بيتد'))
        self.assertIn('observed_dialect_typo',self.check('أرشح الج الأسود'))

    def test_no_question_or_known_contact_repetition(self):
        self.assertIn('unplanned_question', self.check('يالون تحبين؟'))
        self.assertIn('repeated_phone', self.check('دزيلي رقمج وعنوانج', next_question='العنوان', known_contact={'phone'}))
        self.assertEqual(self.check('باقي بس العنوان عيوني', next_question='العنوان', known_contact={'phone'}), [])
        self.assertEqual(self.check('دزيلي عنوانج الجديد', next_question='العنوان الجديد', known_contact={'address'}), [])
        self.assertEqual(self.check('تدللين، بأي وقت تحبين تنورين الصفحة.'), [])

    def test_message_count_and_dry_refusal(self):
        self.assertIn('message_count', dual_sales.validate_parts(['أ','ب'], message_count=1))
        self.assertIn('dry_negotiation', self.check('السعر ثابت حبيبتي'))
        self.assertIn('dry_negotiation', self.check('ما أكدر بأقل من51',allowed_numbers={'51'}))
        self.assertIn('defensive_negotiation', self.check('يا ريت بيدي والله'))

    def test_repeat_and_internal_leak(self):
        self.assertIn('repeated_reply', self.check('صار بعيوني', previous_replies=['صار بعيوني']))
        self.assertIn('internal_details', self.check('commit_order'))

    def test_tool_outcomes_override_writer_control(self):
        handoff=[dict(name='handoff_to_staff',result=dict(ok=True,status='pending'))]
        self.assertEqual(dual_sales.controlled_reply(handoff)['parts'],[])
        failed=[dict(name='commit_order',result=dict(ok=False))]
        self.assertEqual(dual_sales.controlled_reply(failed)['mode'],'writer')
        created=[dict(name='commit_order',result=dict(ok=True,action='created',confirmation_text='OWNER RECEIPT'))]
        self.assertEqual(dual_sales.controlled_reply(created)['parts'],['OWNER RECEIPT'])


if __name__ == '__main__':
    unittest.main()
