"""A selected two-child cart advances to contacts instead of a courtesy dead end."""
import json
import unittest
from unittest.mock import patch

from account_app import product_variants
from account_app.tests import test_baraah_checkout_continuity_oct28 as fixtures


class ChildCheckoutProgressTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.BaraahCheckoutContinuityTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.BaraahCheckoutContinuityTests.tearDownClass.__func__)
    tearDown = fixtures.BaraahCheckoutContinuityTests.tearDown
    save = fixtures.BaraahCheckoutContinuityTests.save
    process = fixtures.BaraahCheckoutContinuityTests.process
    count = fixtures.BaraahCheckoutContinuityTests.count

    def setUp(self):
        fixtures.BaraahCheckoutContinuityTests.setUp(self)
        self.product.update(colors='أخضر برتقالي', price='12000', offer='قطعتين ب 20 الف')

    def choice_history(self, summary=True, paired=False):
        turns = [
            ('incoming', 'ثنينهن اريد'),
            ('incoming', 'بس اذا مو نفس الصورة و الحجي ارفضة'),
            ('outgoing', 'تفحص القطعة قبل الدفع وإذا بيها إشكال ترجعها مجاناً.'),
            ('incoming', 'عمر اربعة سنين واحد'),
            ('incoming', 'عمر ثلاث سنين واحد'),
            ('incoming', 'واحد برتقالي واحد اخضر'),
            ('outgoing', 'حددلي يا لون لعمر 3 ويا لون لعمر 4؟'),
            ('incoming', 'اربعة سنين برتقالي'),
        ]
        if paired:
            turns.append(('incoming', 'ثلاث سنين أخضر'))
        if summary:
            turns.append(('outgoing', 'تدلل، يعني عمر 4 سنين برتقالي وعمر 3 سنين أخضر 🌷'))
        for direction, text in turns:
            self.save(direction, text)

    def accept_generated_reply(self):
        rows = self.db.execute("SELECT id FROM messages WHERE sender_id=? AND direction='outgoing' "
                               "AND raw_payload LIKE '%generated%'", (self.sender,)).fetchall()
        self.m.delivery_state.update(self.db, [dict(row) for row in rows], 'accepted')

    def test_yes_to_confirmed_two_child_summary_requests_contacts_and_books_saved_cart(self):
        self.choice_history()
        result, model = self.process('نعم')
        model.assert_not_called()
        self.assertIn('رقم', result['reply'])
        self.assertIn('العنوان', result['reply'])
        self.assertNotIn('شكد العمر', result['reply'])
        self.assertEqual(self.count(), 0)
        self.accept_generated_reply()
        phone, model = self.process('07701234567')
        model.assert_not_called()
        self.assertIn('العنوان', phone['reply'])
        self.assertNotIn('رقم الموبايل', phone['reply'])
        self.accept_generated_reply()
        result, model = self.process('بغداد حي الاختبار قرب المدرسة')
        model.assert_not_called()
        self.assertIn('تم تثبيت', result['reply'])
        order = self.db.execute('SELECT order_items FROM orders WHERE sender_id=?', (self.sender,)).fetchone()
        items = json.loads(order['order_items'])
        self.assertEqual({(product_variants.child_age(i['size']), i['color'], i['quantity']) for i in items},
                         {(4.0, 'برتقالي', 1), (3.0, 'أخضر', 1)})
        self.assertEqual(self.count(), 1)

    def test_customer_allocated_both_children_need_no_further_choice_confirmation(self):
        self.choice_history(summary=False, paired=True)
        result, model = self.process('نعم')
        model.assert_not_called()
        self.assertIn('رقم', result['reply'])
        self.assertIn('العنوان', result['reply'])
        self.assertEqual(self.count(), 0)

    def test_unallocated_second_colour_is_not_invented_from_yes(self):
        self.choice_history(summary=False)
        ev = dict(self.ev, text='نعم')
        self.assertIsNone(self.m.selected_child_cart(self.db, ev, self.product))
        self.assertEqual(self.count(), 0)

    def test_summary_cannot_swap_an_explicit_customer_allocation(self):
        self.choice_history(summary=False, paired=True)
        self.save('outgoing', 'عمر 4 سنين أخضر وعمر 3 سنين برتقالي؟')
        self.assertIsNone(self.m.selected_child_cart(self.db, dict(self.ev, text='نعم'), self.product))
        self.assertEqual(self.count(), 0)

    def test_yes_to_photo_offer_remains_browsing(self):
        self.save('incoming', 'اريد قطعتين أخضر لعمر 3 سنوات وبرتقالي لعمر 4 سنوات')
        self.save('outgoing', 'تحب أرسل صور الموديل؟')
        ev = dict(self.ev, text='نعم')
        self.assertIsNone(self.m.resume_known_checkout(self.db, ev, self.product, {}, [self.product], {}))
        self.assertEqual(self.count(), 0)

    def test_later_reduction_to_one_piece_does_not_restore_two_children(self):
        self.choice_history(paired=True)
        self.save('incoming', 'بس وحدة أريد')
        self.save('outgoing', 'تمام عيني')
        self.assertIsNone(self.m.selected_child_cart(self.db, dict(self.ev, text='نعم'), self.product))
        self.assertEqual(self.count(), 0)

    def test_new_question_and_delivery_condition_still_need_resolution(self):
        self.choice_history()
        for text in ('نعم بس شكد طوله؟', 'اذا التوصيل مجاني ثبت'):
            with self.subTest(text=text):
                self.assertIsNone(self.m.selected_child_cart(self.db, dict(self.ev, text=text), self.product))
        self.assertEqual(self.count(), 0)

    def test_fabric_answer_does_not_repeat_the_unanswered_size_question(self):
        history = [dict(direction='incoming', text='ممكن القياسات'),
                   dict(direction='outgoing', text='القياسات 38 إلى 52. شكد قياس تلبسين؟'),
                   dict(direction='incoming', text='شنو نوع القماش')]
        product = dict(self.product, sizes='38 إلى 52', fabric='خام تركي درجة اولى')
        event = dict(self.ev, text='شنو نوع القماش', _reply_product=product)
        result = self.m.owner_result_surface(dict(reply='القماش خام تركي درجة اولى. شكد قياس تلبسين؟',
                                                  _local_reply=True), event, {}, history)
        self.assertIn('تركي', result['reply'])
        self.assertNotIn('قياس تلبسين', result['reply'])

    def test_written_age_with_taa_marbuta_is_recognized(self):
        self.assertEqual(product_variants.child_ages('عمر اربعة سنين واحد'), [4.0])
        self.assertEqual(product_variants.child_ages('واحد لعمر خمسة سنوات'), [5.0])


if __name__ == '__main__':
    unittest.main()
