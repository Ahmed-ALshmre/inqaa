"""Offline behavioral tests for the new engine's persistence boundary."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from account_app.sales_runtime import Conflict, Scope, Store
from account_app.sales_runtime.cart import CartService, Catalog


class RuntimeFoundationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temp.name) / 'runtime.db')
        self.db = sqlite3.connect(self.path)
        self.store = Store(self.db)
        self.store.migrate()
        self.scope = Scope('khuyoot', 'chatwoot', 'account-1', 'test-customer')
        self.products = [dict(product_id='P1', store_id='khuyoot', product_name='قميص',
                              colors='ماروني، أزرق', sizes='38، 40، 44', price='12000'),
                         dict(product_id='B1', store_id='khuyoot', product_name='حقيبة',
                              colors='أسود', sizes='', size_system='none', price='10000'),
                         dict(product_id='P9', store_id='other', price='1')]
        self.cart = CartService(self.store, Catalog('khuyoot', self.products))

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def event(self, text='اريد الأزرق', external=None, actor='customer'):
        return self.store.append(self.scope, external or str(self.version()), kind='message',
                                 actor=actor, payload={'text': text})['id']

    def version(self):
        return self.store.snapshot(self.scope)['version']

    def add(self, event, key='add', **values):
        return self.cart.change(self.scope, key, self.version(), event, line_id='one',
                                values=dict(product_id='P1', color='ماروني', size='44', **values))

    def test_repeated_webhook_does_not_invalidate_or_duplicate(self):
        event = self.event(external='m1')
        version = self.version()
        self.assertTrue(self.store.append(self.scope, 'm1', kind='message', actor='customer',
                                         payload={'text': 'اريد الأزرق'})['duplicate'])
        self.assertEqual(version, self.version())
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM sales_v2_events').fetchone()[0])
        with self.assertRaises(Conflict):
            self.store.append(self.scope, 'm1', kind='message', actor='customer', payload={'text': 'different'})
        self.assertIsInstance(event, int)

    def test_same_sender_and_event_id_are_isolated_by_account_and_store(self):
        self.event(external='m1')
        for scope in [Scope('other', 'chatwoot', 'account-1', 'test-customer'),
                      Scope('khuyoot', 'chatwoot', 'account-2', 'test-customer')]:
            self.assertFalse(self.store.append(scope, 'm1', kind='message', actor='customer', payload={})['duplicate'])
            self.assertEqual([], self.store.snapshot(scope)['data']['cart'])

    def test_latest_correction_preserves_size_and_quantity(self):
        self.add(self.event('ماروني 44 قطعتين'), quantity=2)
        correction = self.event('بدلي الماروني أزرق')
        self.cart.change(self.scope, 'correct', self.version(), correction,
                         line_id='one', values={'color': 'أزرق'})
        line = self.store.snapshot(self.scope)['data']['cart'][0]
        self.assertEqual(('أزرق', '44', 2), (line['color'], line['size'], line['quantity']))
        evidence = self.db.execute('SELECT event_id,field FROM sales_v2_evidence ORDER BY id DESC').fetchone()
        self.assertEqual((correction, 'color'), evidence)

    def test_replayed_operation_after_new_event_does_not_restore_old_color(self):
        event = self.event()
        original_version = self.version()
        first = self.cart.change(self.scope, 'one-op', original_version, event, line_id='one',
                                 values={'product_id': 'P1', 'color': 'ماروني', 'size': '44'})
        correction = self.event('أزرق بدل الماروني')
        self.cart.change(self.scope, 'correct', self.version(), correction, line_id='one', values={'color': 'أزرق'})
        replay = self.cart.change(self.scope, 'one-op', original_version, event, line_id='one',
                                  values={'product_id': 'P1', 'color': 'ماروني', 'size': '44'})
        self.assertTrue(replay['replayed'])
        self.assertEqual(first['result'], replay['result'])
        self.assertEqual('أزرق', self.store.snapshot(self.scope)['data']['cart'][0]['color'])

    def test_old_worker_cannot_overwrite_new_message(self):
        event = self.event()
        version = self.version()
        other = sqlite3.connect(self.path)
        try:
            Store(other).append(self.scope, 'staff-message', kind='message', actor='staff', payload={'text': 'new'})
            with self.assertRaisesRegex(Conflict, 'state_version_changed'):
                self.cart.change(self.scope, 'stale', version, event, line_id='one', values={'product_id': 'P1'})
        finally:
            other.close()

    def test_cross_store_catalog_and_evidence_rejected(self):
        event = self.event()
        with self.assertRaises(Conflict):
            self.cart.change(self.scope, 'bad-product', self.version(), event,
                             line_id='one', values={'product_id': 'P9'})
        other = Scope('khuyoot', 'chatwoot', 'account-1', 'other-customer')
        with self.assertRaises(Conflict):
            self.cart.change(other, 'bad-evidence', 0, event, line_id='one', values={'product_id': 'P1'})
        self.assertEqual([], self.store.snapshot(other)['data']['cart'])

    def test_sizeless_bag_needs_no_measurement(self):
        event = self.event('الحقيبة السوداء')
        self.cart.change(self.scope, 'bag', self.version(), event, line_id='bag',
                         values={'product_id': 'B1', 'color': 'أسود'})
        readiness = self.cart.readiness(self.scope)
        self.assertFalse(any('.size' in field for field in readiness['missing_fields']))
        with self.assertRaises(Conflict):
            self.cart.change(self.scope, 'bad-size', self.version(), event,
                             line_id='bag', values={'size': '40'})

    def test_price_comes_from_catalog_and_quantity_does_not_truncate(self):
        self.add(self.event(), quantity=2)
        quote = self.cart.quote(self.scope, 5000)
        self.assertEqual(29000, quote['total'])
        with self.assertRaises(ValueError):
            self.cart.change(self.scope, 'fake-price', self.version(), self.event(),
                             line_id='one', values={'price': 1})

    def test_failed_handler_rolls_back_external_local_writes_and_state(self):
        self.event()
        self.db.execute('CREATE TABLE test_orders (id INTEGER PRIMARY KEY)')
        self.db.commit()
        before = self.version()
        def failed(state):
            state['cart'].append({'wrong': True})
            self.db.execute('INSERT INTO test_orders DEFAULT VALUES')
            raise RuntimeError('simulated process failure before commit')
        with self.assertRaises(RuntimeError):
            self.store.apply(self.scope, 'save', before, {'cart': 1}, failed)
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM test_orders').fetchone()[0])
        self.assertEqual(before, self.version())
        self.assertEqual([], self.store.snapshot(self.scope)['data']['cart'])

    def test_restart_replays_success_without_second_local_order(self):
        self.event()
        self.db.execute('CREATE TABLE test_orders (id INTEGER PRIMARY KEY)')
        self.db.commit()
        before = self.version()
        def save(state):
            return {'order_id': self.db.execute('INSERT INTO test_orders DEFAULT VALUES').lastrowid}
        original = self.store.apply(self.scope, 'save', before, {'cart': 1}, save)
        self.db.close()
        self.db = sqlite3.connect(self.path)
        self.store = Store(self.db)
        self.store.migrate()
        replay = self.store.apply(self.scope, 'save', before, {'cart': 1}, save)
        self.assertTrue(replay['replayed'])
        self.assertEqual(original['result'], replay['result'])
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM test_orders').fetchone()[0])
        with self.assertRaises(Conflict):
            self.store.apply(self.scope, 'save', self.version(), {'cart': 2}, save)

    def test_outbox_orders_images_and_text_and_deduplicates(self):
        self.event()
        parts = [{'text': 'هاي الصور'}, {'image': 'asset-1'}, {'text': 'يالون عجبج؟'}]
        self.store.enqueue_reply(self.scope, 'r1', self.version(), parts)
        self.store.enqueue_reply(self.scope, 'r1', self.version(), parts)
        for part in parts:
            claimed = self.store.claim_reply(self.scope)
            self.assertEqual(part, claimed['payload'])
            self.assertIsNone(self.store.claim_reply(self.scope))
            self.store.settle_reply(self.scope, claimed['id'], 'accepted', 'provider-test-id')
        self.assertIsNone(self.store.claim_reply(self.scope))
        self.assertEqual(3, self.db.execute('SELECT COUNT(*) FROM sales_v2_outbox').fetchone()[0])

    def test_crash_during_send_blocks_blind_replay_and_later_parts(self):
        self.event()
        self.store.enqueue_reply(self.scope, 'r1', self.version(), [{'text': 'saved'}, {'text': 'followup'}])
        self.store.claim_reply(self.scope)
        self.store.recover()
        self.assertIsNone(self.store.claim_reply(self.scope))
        self.assertEqual('unknown', self.db.execute('SELECT status FROM sales_v2_outbox ORDER BY id').fetchone()[0])

    def test_new_message_invalidates_old_draft_but_receipt_does_not(self):
        self.event()
        version = self.version()
        self.store.append(self.scope, 'receipt', kind='receipt', actor='system', payload={})
        self.assertEqual(version, self.version())
        self.store.enqueue_reply(self.scope, 'old', version, [{'text': 'maroon'}])
        self.event('أزرق', external='correction')
        self.assertIsNone(self.store.claim_reply(self.scope))
        with self.assertRaises(Conflict):
            self.store.enqueue_reply(self.scope, 'stale', version, [{'text': 'maroon'}])

    def test_partial_contact_does_not_remove_previous_data(self):
        phone_event = self.event('07700000000')
        self.cart.contact(self.scope, 'phone', self.version(), phone_event, phone='07700000000')
        address_event = self.event('بغداد حي الاختبار قرب مدرسة التجربة')
        self.cart.contact(self.scope, 'address', self.version(), address_event,
                          address='بغداد حي الاختبار قرب مدرسة التجربة')
        self.assertEqual('07700000000', self.store.snapshot(self.scope)['data']['contact']['phone'])
        self.assertNotIn('contact.phone', self.cart.readiness(self.scope)['missing_fields'])

    def test_complete_checkout_needs_no_second_confirmation(self):
        event = self.event('ثبتيلي ماروني 44')
        self.add(event)
        self.cart.purchase(self.scope, 'buy', self.version(), event)
        contact = self.event('07700000000 بغداد حي الاختبار قرب مدرسة التجربة')
        self.cart.contact(self.scope, 'contacts', self.version(), contact, phone='07700000000',
                          address='بغداد حي الاختبار قرب مدرسة التجربة')
        self.assertTrue(self.cart.readiness(self.scope)['ready'])
        self.assertEqual([], self.cart.readiness(self.scope)['missing_fields'])

    def test_price_question_or_staff_message_is_not_customer_purchase(self):
        for actor in ('customer', 'staff'):
            event = self.event('شكد قطعتين واصلات؟' if actor == 'customer' else 'ثبتيلي', actor=actor)
            with self.assertRaises(Conflict):
                self.cart.purchase(self.scope, 'buy-' + actor, self.version(), event)
        self.assertIsNone(self.store.snapshot(self.scope)['data']['purchase_event'])

    def test_ordering_and_address_do_not_bypass_missing_color(self):
        event = self.event('ثبتيلي')
        self.cart.change(self.scope, 'draft', self.version(), event, line_id='one',
                         values={'product_id': 'P1', 'size': '44'})
        self.cart.purchase(self.scope, 'buy', self.version(), event)
        contact = self.event('07700000000 بغداد حي الاختبار')
        self.cart.contact(self.scope, 'contact', self.version(), contact,
                          phone='07700000000', address='بغداد حي الاختبار')
        self.assertEqual(['cart.one.color'], self.cart.readiness(self.scope)['missing_fields'])

    def test_age_weight_and_clothing_are_independent_types(self):
        products = [dict(product_id='kids', store_id='khuyoot', product_name='طقم',
                         size_system='age', age_min_months=24, age_max_months=84, price='12000'),
                    dict(product_id='weight', store_id='khuyoot', product_name='دشداشة',
                         size_system='weight', weight_min_kg=55, weight_max_kg=140, price='12000')]
        cart = CartService(self.store, Catalog('khuyoot', products))
        event = self.event('عمر خمس سنوات ووزن 130')
        cart.change(self.scope, 'age', self.version(), event, line_id='child',
                    values={'product_id': 'kids', 'age_months': 60})
        cart.change(self.scope, 'weight', self.version(), event, line_id='adult',
                    values={'product_id': 'weight', 'weight_kg': 130})
        self.assertEqual(130, self.store.snapshot(self.scope)['data']['cart'][1]['weight_kg'])
        with self.assertRaises(Conflict):
            cart.change(self.scope, 'wrong-type', self.version(), event, line_id='child', values={'weight_kg': 85})
        with self.assertRaises(Conflict):
            cart.change(self.scope, 'wrong-range', self.version(), event, line_id='child', values={'age_months': 1020})

    def test_invalid_phone_does_not_discard_valid_address_in_same_message(self):
        event = self.event('0770000 بغداد حي الاختبار')
        result = self.cart.contact(self.scope, 'invalid', self.version(), event,
                                   phone='0770000', address='بغداد حي الاختبار')
        self.assertEqual(['contact.phone'], result['result']['invalid_fields'])
        self.assertEqual('بغداد حي الاختبار', self.store.snapshot(self.scope)['data']['contact']['address'])
        self.assertNotIn('contact.address', self.cart.readiness(self.scope)['missing_fields'])

    def test_contact_requires_actual_source_and_not_model_invention(self):
        event = self.event('شنو الالوان؟')
        with self.assertRaises(Conflict):
            self.cart.contact(self.scope, 'invented', self.version(), event, phone='07700000000')

    def test_staff_takeover_invalidates_draft_and_blocks_new_sends(self):
        self.event()
        self.store.enqueue_reply(self.scope, 'draft', self.version(), [{'text': 'old'}])
        event = self.event('سأتولى الرد', actor='staff')
        self.store.set_control(self.scope, 'pause', self.version(), event, 'human')
        self.assertIsNone(self.store.claim_reply(self.scope))
        with self.assertRaises(Conflict):
            self.store.enqueue_reply(self.scope, 'new', self.version(), [{'text': 'new'}])
        self.store.set_control(self.scope, 'resume', self.version(), event, 'active')
        self.assertIsNone(self.store.claim_reply(self.scope))

    def test_new_product_cannot_inherit_existing_line_size(self):
        self.add(self.event())
        event = self.event('أريد الحقيبة')
        with self.assertRaises(Conflict):
            self.cart.change(self.scope, 'switch', self.version(), event, line_id='one',
                             values={'product_id': 'B1'})

    def test_rejected_delivery_blocks_followup_and_cannot_be_claimed_as_success(self):
        self.event()
        self.store.enqueue_reply(self.scope, 'r1', self.version(), [{'image': 'missing'}, {'text': 'شوفيها'}])
        part = self.store.claim_reply(self.scope)
        self.store.settle_reply(self.scope, part['id'], 'rejected')
        self.assertIsNone(self.store.claim_reply(self.scope))
        with self.assertRaises(Conflict):
            self.store.settle_reply(self.scope, part['id'], 'accepted')

    def test_newborn_age_zero_is_valid_when_catalog_explicitly_allows_it(self):
        catalog = Catalog('khuyoot', [dict(product_id='baby', store_id='khuyoot',
                          price='10000', size_system='age', age_min_months=0, age_max_months=12)])
        event = self.event('حديث ولادة')
        CartService(self.store, catalog).change(self.scope, 'baby', self.version(), event,
                                               line_id='baby', values={'product_id': 'baby', 'age_months': 0})
        self.assertEqual(0, self.store.snapshot(self.scope)['data']['cart'][0]['age_months'])

    def test_two_colors_need_distinct_lines(self):
        event = self.event('ماروني وأزرق')
        with self.assertRaisesRegex(Conflict, 'one_color_per_cart_line'):
            self.cart.change(self.scope, 'ambiguous', self.version(), event, line_id='one',
                             values={'product_id': 'P1', 'color': 'ماروني أزرق', 'size': '44'})

    def test_old_engine_refuses_to_write_newer_state_schema(self):
        self.event()
        self.db.execute('UPDATE sales_v2_scopes SET data=? WHERE scope=?',
                        ('{"schema_version":2}', self.scope.key))
        self.db.commit()
        with self.assertRaisesRegex(Conflict, 'unsupported_state_schema'):
            self.store.apply(self.scope, 'old-engine', 1, {}, lambda state: {})
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM sales_v2_operations').fetchone()[0])

    def test_conditional_price_is_not_unconditional_purchase(self):
        event = self.event('إذا بعشرين ثبتيلي')
        with self.assertRaisesRegex(Conflict, 'purchase_condition_needs_resolution'):
            self.cart.purchase(self.scope, 'conditional', self.version(), event)
        self.assertIsNone(self.store.snapshot(self.scope)['data']['purchase_event'])


if __name__ == '__main__':
    unittest.main()
