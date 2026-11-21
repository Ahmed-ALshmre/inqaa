"""De-identified checkout follow-up after a staff delivery answer; no live sends."""
import json
import unittest
from unittest.mock import Mock

from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.tests import test_sales_runtime_linking as linking


class CheckoutFollowupTests(unittest.TestCase):
    setUp = linking.LinkingTests.setUp
    tearDown = linking.LinkingTests.tearDown
    event = linking.LinkingTests.event
    version = linking.LinkingTests.version
    run_turn = linking.LinkingTests.run_turn

    def select_skirt(self):
        self.products[0].update(product_name='تنورة', colors='أسود', sizes='46', price='16000')
        self.plan = dict(intent='buy', changes=[dict(values=dict(product_id='P1', color='أسود', size='46'))])
        self.runtime.turn(self.scope, self.event('اريد لون اسود قياس ٤٦'))
        self.event('تمام')
        self.event('شوكت توصل')
        self.event('باجر', actor='staff')
        self.event('تمام')

    def receive(self, entry, text, key):
        return entry.receive(store_id=self.scope.store, channel=self.scope.channel,
                             account_id=self.scope.account, sender_id=self.scope.sender,
                             message_id=key, text=text)

    def test_contact_after_staff_delivery_answer_asks_only_missing_province_then_books(self):
        self.select_skirt()
        writer = Mock(return_value=[dict(type='text', text='حبيبتي باقي بس المحافظة حتى أثبتلج الطلب.')])
        decide = Mock(side_effect=AssertionError('Existing selection and received contacts need no new choice.'))
        entry = Entrypoint(self.db, lambda _: self.products, lambda *_: 5000, decide, writer=writer)
        result = self.receive(entry, 'العنوان حي الاختبار الهاتف٠٧٧٠١٢٣٤٥٦٧', 'contacts')
        self.assertEqual('continue', result['action'], result)
        context = writer.call_args.args[0]
        self.assertEqual('contact.province', context['question_field'])
        self.assertNotIn('delivery_time', context['requested_fields'])
        state = entry.store.snapshot(self.scope)['data']
        self.assertEqual('07701234567', state['contact']['phone'])
        self.assertEqual(('أسود', '46'), (state['cart'][0]['color'], state['cart'][0]['size']))
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        result = self.receive(entry, 'بغداد', 'province')
        self.assertEqual('booked', result['action'], result)
        order = self.db.execute('SELECT phone,order_items FROM orders').fetchone()
        self.assertEqual('07701234567', order[0])
        self.assertEqual('46', json.loads(order[1])[0]['size'])
        decide.assert_not_called()

    def test_complete_contacts_after_staff_answer_book_without_repeating_delivery_reply(self):
        self.select_skirt()
        writer = Mock()
        decide = Mock(side_effect=AssertionError('The selected cart is already complete.'))
        entry = Entrypoint(self.db, lambda _: self.products, lambda *_: 5000, decide, writer=writer)
        text = 'العنوان بغداد الدورة حي الاختبار الهاتف٠٧٧٠١٢٣٤٥٦٧'
        result = self.receive(entry, text, 'complete')
        self.assertEqual('booked', result['action'], result)
        writer.assert_not_called()
        decide.assert_not_called()
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_dora_address_resolves_baghdad_and_books_with_arabic_phone_digits(self):
        self.select_skirt()
        writer, decide = Mock(), Mock(side_effect=AssertionError('Do not ask for a saved selection.'))
        entry = Entrypoint(self.db, lambda _: self.products, lambda *_: 5000, decide, writer=writer)
        result = self.receive(entry, 'العنوان الدورة حي الاختبار الهاتف٠٧٧٠١٢٣٤٥٦٧', 'dora')
        self.assertEqual('booked', result['action'], result)
        self.assertEqual('بغداد', entry.store.snapshot(self.scope)['data']['contact']['province'])
        writer.assert_not_called()
        decide.assert_not_called()


if __name__ == '__main__':
    unittest.main()
