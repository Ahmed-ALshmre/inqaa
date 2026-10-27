"""Complete the reviewed Fatena booking without reviving an old lining question."""
import json
import unittest

from account_app import sales_context, sales_decision
from account_app.tests import test_checkout_completion_oct25 as completion


class RequiredDetailTests(unittest.TestCase):
    def test_information_question_is_distinct_from_purchase_condition(self):
        for text in ('بيها بطانة لو لا؟', 'طول الفستان لطفا', 'لون اسود شرط، بيها بطانة؟'):
            self.assertEqual(sales_decision.required_details([dict(direction='incoming', text=text)]), [])
        for text in ('إذا مبطن ثبتي', 'البطانة شرط', 'لا تثبتي قبل تأكيد البطانة'):
            self.assertEqual(sales_decision.required_details([dict(direction='incoming', text=text)]), ['lining'])

    def test_answer_waiver_and_model_change_clear_condition(self):
        for direction, text in [('outgoing', 'بيها بطانة كاملة.'),
                                ('incoming', 'ما يهمني البطانة ثبتي'),
                                ('incoming', 'بدلي الموديل')]:
            rows = [dict(direction='incoming', text='شرط يكون مبطن'), dict(direction=direction, text=text)]
            self.assertEqual(sales_decision.required_details(rows), [])
        rows = [dict(direction='incoming', text='شرط يكون مبطن'),
                dict(direction='outgoing', text='البطانة تحتاج تأكيد من المتجر.'),
                dict(direction='incoming', text='07701234567 صلاح الدين قرية الفياض')]
        self.assertEqual(sales_decision.required_details(rows), ['lining'])


class BookingContactTests(unittest.TestCase):
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
    process = completion.CheckoutCompletionTests.process
    assert_confirmation = completion.CheckoutCompletionTests.assert_confirmation

    def previous_question(self, text='بي بطانة لولاا', *, draft=True):
        self.product = completion.CATALOG[0]
        self.m.bind_customer_to_product(self.db, self.sender, self.product, source='image_recognition')
        self.incoming(text)
        order = self.data()
        order['items'][0]['size'] = '46'
        payload = {'checkout_draft': order} if draft else {}
        self.m.save_message(self.db, self.sender, 'outgoing', 'text',
                            'البطانة تحتاج تأكيد من المتجر.', None, None, None, payload)

    def candidate(self, text, customer=None):
        context = sales_context.load(self.db, self.m.current_store_id(), self.sender)
        context = sales_context.advance(context, [dict(direction='incoming', text=text)])
        return self.m.resume_known_checkout(self.db, dict(self.ev, text=text), self.product,
                                            customer or {}, [self.product], context)

    def test_reported_complete_contact_and_chosen_size_book_without_model(self):
        self.previous_question()
        result, model = self.process('صلاح الدين\nقرية الفياض\nرقم الهاتف\n07701234567\nتمام\nقياس46')
        model.assert_not_called()
        self.assert_confirmation(result)
        saved = self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        items = json.loads(saved['order_items'])
        self.assertEqual((items[0]['size'], items[0]['color'], items[0]['quantity']), ('46', 'أسود', 1))
        self.assertEqual(saved['phone'], '07701234567')
        self.assertEqual(saved['province'], 'صلاح الدين')
        self.assertIn('قرية الفياض', saved['address'])
        self.assertEqual(saved['total_amount'], 21000)
        self.assertNotIn('بطانة', result['reply'])

    def test_complete_contact_resumes_saved_cart_past_old_lining_question(self):
        self.previous_question()
        result, model = self.process('07701234567 صلاح الدين قرية الفياض')
        model.assert_not_called()
        self.assert_confirmation(result)

    def test_saved_contact_and_thabti_book_without_another_confirmation(self):
        self.previous_question()
        self.m.capture_customer_contact(self.db, self.sender, '07701234567 صلاح الدين قرية الفياض')
        result, model = self.process('اي\nثبتي')
        model.assert_not_called()
        self.assert_confirmation(result)

    def test_known_selection_and_full_contact_work_without_old_ai_cart(self):
        self.previous_question(draft=False)
        self.incoming('قياس46')
        result, model = self.process('07701234567 صلاح الدين قرية الفياض')
        model.assert_not_called()
        self.assert_confirmation(result)

    def test_new_question_and_prior_explicit_condition_are_still_answered(self):
        self.previous_question('إذا مبطن ثبتي')
        self.assertIsNone(self.candidate('07701234567 صلاح الدين قرية الفياض'))
        self.assertIsNone(self.candidate('ثبتي بس بيها بطانة؟ 07701234567 صلاح الدين قرية الفياض'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',
                                        (self.sender,)).fetchone()[0], 0)

    def test_fresh_detail_in_contact_turn_is_answered_before_booking(self):
        self.previous_question()
        self.assertIsNone(self.candidate('ثبتي بس بيها بطانة؟ 07701234567 صلاح الدين قرية الفياض'))

    def test_incomplete_contact_does_not_skip_the_old_question(self):
        self.previous_question(draft=False)
        self.incoming('قياس46')
        self.assertIsNone(self.candidate('07701234567'))

    def test_duplicate_submission_keeps_one_saved_order(self):
        self.previous_question()
        result, model = self.process('07701234567 صلاح الدين قرية الفياض')
        model.assert_not_called()
        self.assert_confirmation(result)
        candidate = {'order': self.data()}
        candidate['order']['items'][0]['size'] = '46'
        created, reply = self.m.create_order_if_valid(self.db, self.sender, candidate, self.product)
        self.assertTrue(created)
        self.assertIsNone(reply)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',
                                        (self.sender,)).fetchone()[0], 1)
