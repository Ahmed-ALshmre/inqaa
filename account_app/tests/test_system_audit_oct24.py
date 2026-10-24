"""Stateful regression checks for the October 24 reliability audit.

These tests execute real SQLite state changes and delivery guards. Provider
responses are controlled fixtures; no live customers or networks are involved.
"""
import json
import sqlite3
import unittest
from unittest.mock import Mock

from account_app import ai_jobs, checkout, menger, order_actions, vision_payload


class DeliveryRecoveryAudit(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        ai_jobs.init_db(self.db)

    def tearDown(self):
        self.db.close()

    def failed_job(self):
        job = ai_jobs.enqueue(self.db, 'store', 'customer', 'webhook', {'id': 1}, 'incoming-1')
        ai_jobs.finish(self.db, job['id'], 'failed', {'ok': False})
        return job

    def test_known_rejection_before_acceptance_can_be_retried(self):
        for rejected in ({'ok': False, 'status': 'missing_key', 'status_code': 0},
                         {'ok': False, 'status': 'error', 'status_code': 429}):
            with self.subTest(rejected=rejected):
                self.db.execute('DELETE FROM ai_jobs')
                self.db.execute('DELETE FROM ai_deliveries')
                self.db.commit()
                job = self.failed_job()
                ai_jobs.deliver(self.db, job['id'], ['first'], lambda: rejected)
                retried = ai_jobs.enqueue(self.db, 'store', 'customer', 'webhook', {'id': 1}, 'incoming-1')
                self.assertEqual(retried['id'], job['id'])
                self.assertEqual(retried['status'], 'queued')
                sender = Mock(return_value={'ok': True})
                self.assertTrue(ai_jobs.deliver(self.db, job['id'], ['first'], sender)['ok'])
                sender.assert_called_once()

    def test_partial_acceptance_does_not_requeue_business_work(self):
        job = self.failed_job()
        ai_jobs.deliver(self.db, job['id'], ['first'], lambda: {'ok': True})
        ai_jobs.deliver(self.db, job['id'], ['second'], lambda: {'ok': False, 'status_code': 429})
        retried = ai_jobs.enqueue(self.db, 'store', 'customer', 'webhook', {'id': 1}, 'incoming-1')
        self.assertEqual(retried['status'], 'failed')
        sender = Mock(return_value={'ok': True})
        self.assertTrue(ai_jobs.deliver(self.db, job['id'], ['first'], sender)['ok'])
        sender.assert_not_called()

    def test_timeout_or_in_progress_never_replays_an_uncertain_send(self):
        job = self.failed_job()
        sender = Mock(side_effect=TimeoutError)
        with self.assertRaises(TimeoutError):
            ai_jobs.deliver(self.db, job['id'], ['first'], sender)
        retried = ai_jobs.enqueue(self.db, 'store', 'customer', 'webhook', {'id': 1}, 'incoming-1')
        self.assertEqual(retried['status'], 'failed')
        sender = Mock(return_value={'ok': True})
        self.assertEqual(ai_jobs.deliver(self.db, job['id'], ['first'], sender)['status'], 'delivery_uncertain')
        sender.assert_not_called()


class RepeatedOrderActionAudit(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.execute('CREATE TABLE orders(id INTEGER PRIMARY KEY, sender_id TEXT, status TEXT, size TEXT, order_items TEXT)')
        self.db.execute("INSERT INTO orders VALUES(1,'customer','new','44','[{\"size\":\"44\"}]')")
        self.db.commit()
        order_actions.init(self.db)

    def tearDown(self):
        self.db.close()

    def test_same_size_requested_again_after_a_later_change_reopens_review(self):
        action = {'kind': 'size', 'value': '46'}
        request_id = order_actions.record(self.db, 'customer', 1, action, '2026-10-24T12:00:00+03:00')
        self.db.execute('UPDATE orders SET size=?,order_items=? WHERE id=1', ('46', json.dumps([{'size': '46'}])))
        self.db.commit()
        self.assertFalse(order_actions.pending(self.db, 'customer'))
        self.db.execute('UPDATE orders SET size=?,order_items=? WHERE id=1', ('48', json.dumps([{'size': '48'}])))
        self.db.commit()
        repeated = order_actions.record(self.db, 'customer', 1, action, '2026-10-24T13:00:00+03:00')
        self.assertEqual(repeated, request_id)
        self.assertTrue(order_actions.pending(self.db, 'customer'))
        row = self.db.execute('SELECT * FROM order_action_requests WHERE id=?', (request_id,)).fetchone()
        self.assertIsNone(row['resolved_at'])
        self.assertEqual(row['created_at'], '2026-10-24T13:00:00+03:00')

    def test_duplicate_pending_request_keeps_its_first_timestamp(self):
        action = {'kind': 'cancel', 'value': 'cancelled'}
        request_id = order_actions.record(self.db, 'customer', 1, action, 'first')
        self.assertEqual(order_actions.record(self.db, 'customer', 1, action, 'second'), request_id)
        self.assertEqual(self.db.execute('SELECT created_at FROM order_action_requests').fetchone()[0], 'first')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM order_action_requests').fetchone()[0], 1)


class VisionBatchAudit(unittest.TestCase):
    def test_all_reference_groups_are_sent_once_with_customer_in_every_batch(self):
        from unittest.mock import patch
        prefix = [{'type': 'text', 'text': 'customer-image'}]
        groups = [[{'type': 'text', 'text': str(i) + ('x' * 80)}] for i in range(5)]
        with patch.object(vision_payload, 'MAX_REQUEST_BYTES', 320):
            batches = vision_payload.request_batches({'model': 'fixture'}, prefix, groups)
        self.assertGreater(len(batches), 1)
        actual = []
        for batch in batches:
            content = batch['messages'][-1]['content']
            self.assertEqual(content[0], prefix[0])
            actual.extend(content[1:])
        self.assertEqual(actual, [group[0] for group in groups])

    def test_partial_failure_or_conflicting_models_cannot_become_a_confirmed_match(self):
        match = {'product_found': True, 'product_id': 'P1'}
        failure = {'product_found': False, 'service_error': True, 'error_code': 'timeout'}
        self.assertTrue(vision_payload.consensus([match, failure])['service_error'])
        self.assertFalse(vision_payload.consensus([match, {'product_found': True, 'product_id': 'P2'}])['product_found'])
        self.assertEqual(vision_payload.consensus([match, dict(match)])['product_id'], 'P1')


class CheckoutConsentAudit(unittest.TestCase):
    """A model flag must never turn a browsing question into an actual order."""
    def setUp(self):
        from account_app.tests.test_audit_orders import AuditFixTests
        from unittest.mock import patch
        AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set('default')
        self.sender = 'consent-audit'
        AuditFixTests.customer(self, self.sender)
        self.product = dict(product_id='CONSENT1', product_name='فستان دانتيل',
                            price='16000', colors='اسود', sizes='38 إلى 52',
                            stock='متوفر', status='active', fabric='لينن')
        self.items = [dict(product_id='CONSENT1', product_name='فستان دانتيل',
                           color='اسود', size='44', quantity=1)]
        self.patches = []
        for target, value in [('load_products_from_file', [self.product]),
                              ('load_active_products', [self.product]),
                              ('is_ai_enabled', True), ('is_customer_ai_enabled', True),
                              ('is_store_feature_enabled', False),
                              ('send_order_to_telegram', True), ('save_booking_to_file', None),
                              ('check_reply', {'approved': True}),
                              ('get_auto_product_settings', {'enabled': False, 'product': None, 'product_id': ''})]:
            mocked = patch.object(self.m, target, return_value=value)
            mocked.start()
            self.patches.append(mocked)
        self.m.complete_customer_product_link(self.db, self.sender, self.product, 'manual', source='manual_admin')
        self.db.execute('UPDATE customers SET phone=?,province=?,address=? WHERE sender_id=?',
                        ('07701234567', 'بغداد', 'بغداد حي اور', self.sender))
        self.db.commit()
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'تدللين عيني، شنو تحبين تعرفين عنه؟', None, None, None, {})

    def tearDown(self):
        from account_app.tests.test_audit_orders import AuditFixTests
        for mocked in reversed(self.patches):
            mocked.stop()
        self.m._current_store_id.reset(self.token)
        AuditFixTests.tearDown(self)

    def process(self, text, *, create_order=True):
        from unittest.mock import patch
        event = dict(sender_id=self.sender, store_id='default', page_id='page', platform='facebook',
                     text=text, image_url=None, attachments=[], timestamp=0, ref=None, ad_id=None,
                     postback=None, quick_reply=None, referral_source=None, referral_type=None)
        result = dict(reply='سعره 16,000 د.ع عيني، شنو القياس المطلوب؟',
                      create_order=create_order, order={'items': self.items})
        with patch.object(self.m, 'extract_facebook_event', return_value=event), \
             patch.object(self.m, 'call_main_ai', return_value=result):
            return self.m.process_webhook(self.db, {}, use_debounce=False)

    def test_price_question_cannot_book_even_if_model_sets_create_order(self):
        response = self.process('شكد سعره؟')
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 0)
        self.assertFalse(response.get('meta', {}).get('order_created'))

    def test_photo_request_cannot_book_even_if_model_sets_create_order(self):
        self.process('دزيلي صورته')
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 0)

    def test_want_this_with_unanswered_price_cannot_book(self):
        self.process('اريد هذا بس شكد سعره؟')
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 0)

    def test_free_shipping_condition_cannot_book_before_agreement(self):
        self.process('اريد هذا اذا التوصيل مجاني')
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 0)

    def test_discount_condition_cannot_book_before_agreement(self):
        self.process('احجزيه اذا تحسبيه ب10000')
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 0)

    def test_confirmation_after_total_creates_one_order(self):
        self.m.save_message(self.db, self.sender, 'outgoing', 'text',
                            'سعره 16000 والتوصيل 5000 والمجموع 21000. تحبين أثبته؟', None, None, None, {})
        self.process('اي ثبتي', create_order=False)
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 1)

    def test_iraqi_want_it_after_quoted_total_completes_booking(self):
        self.m.save_message(self.db, self.sender, 'outgoing', 'text',
                            'سعره 16000 والتوصيل 5000 والمجموع 21000.', None, None, None, {})
        self.process('اريده', create_order=False)
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 1)

    def test_confirming_with_documented_inspection_question_can_book(self):
        self.process('ثبتيه، وافحصه بحضور المندوب قبل الدفع؟')
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 1)

    def test_want_this_then_split_contact_data_completes_one_booking(self):
        self.db.execute("UPDATE customers SET phone='',province='',address='' WHERE sender_id=?", (self.sender,))
        self.db.commit()
        first = self.process('اريد هذا')
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 0)
        self.assertIn('الهاتف', first['reply'])
        self.process('07701234567', create_order=False)
        final = self.process('بغداد حي اور', create_order=False)
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 1)
        self.assertEqual(final['reply_parts'], [self.m.DEFAULT_ORDER_CONFIRMATION_TEXT])


class MengerTransientAudit(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        menger.init_db(self.db)
        menger.save_connection(self.db, {'api_url': 'https://receiver.test/api/v1/orders',
                                        'api_key': 'offline-synthetic-key', 'source': 'audit-source'})
        menger.save_settings(self.db, 'default', {'store_id': 'original-store', 'store_name': 'لمسة ستور'})
        self.order = dict(created_at='2026-10-24T12:00:00+03:00', sender_id='synthetic-customer',
                          phone='07701234567', province='بغداد', address='بغداد حي اور',
                          items=[dict(product_id='P1', product_name='فستان', quantity=1,
                                      color='اسود', size='44', unit_price=16000)],
                          product_total=16000, delivery_fee=5000)
        menger.enqueue(self.db, 19, self.order, [{'product_id': 'P1', 'price': 16000}])
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def delivery(self):
        return self.db.execute('SELECT * FROM menger_deliveries WHERE order_id=19').fetchone()

    def test_rate_limit_and_request_timeout_retry_same_identity_then_stop_after_acceptance(self):
        for code in (429, 408):
            with self.subTest(code=code):
                self.db.execute('DELETE FROM menger_deliveries')
                menger.save_connection(self.db, {'source': 'audit-source'})
                menger.enqueue(self.db, 19, self.order, [{'product_id': 'P1', 'price': 16000}])
                menger.save_settings(self.db, 'default', {'store_id': 'original-store', 'store_name': 'لمسة ستور'})
                self.db.commit()
                limited = Mock(status_code=code, headers={'Retry-After': '180'}, json=Mock(return_value={'error': 'temporary'}))
                accepted = Mock(status_code=200, headers={}, json=Mock(return_value={'ok': True, 'order_id': 'remote-19', 'duplicate': True}))
                post = Mock(side_effect=[limited, accepted])
                menger.deliver_due(self.db, post, now=100)
                self.assertEqual(self.delivery()['status'], 'retry')
                self.assertEqual(self.delivery()['next_attempt'], 280)
                menger.save_connection(self.db, {'source': 'changed-source'})
                menger.save_settings(self.db, 'default', {'store_id': 'changed-store', 'store_name': 'متجر آخر'})
                self.db.commit()
                menger.deliver_due(self.db, post, now=279)
                self.assertEqual(post.call_count, 1)
                menger.deliver_due(self.db, post, now=280)
                first = post.call_args_list[0].kwargs['json']
                second = post.call_args_list[1].kwargs['json']
                self.assertEqual(first, second)
                self.assertEqual((second['source'], second['external_order_id'], second['store_id']),
                                 ('audit-source', '19', 'original-store'))
                self.assertEqual(self.delivery()['remote_order_id'], 'remote-19')
                menger.deliver_due(self.db, post, now=10000)
                self.assertEqual(post.call_count, 2)

    def test_retry_after_is_bounded_and_invalid_values_keep_backoff(self):
        for raw, expected_delay in [('999999999', 3600), ('invalid', 60), ('-2', 60), ('0', 60),
                                    ('Thu, 01 Jan 1970 00:06:40 GMT', 300)]:
            with self.subTest(raw=raw):
                self.db.execute("UPDATE menger_deliveries SET status='pending',attempts=0,next_attempt=0")
                self.db.commit()
                response = Mock(status_code=429, headers={'Retry-After': raw}, json=Mock(return_value={}))
                menger.deliver_due(self.db, Mock(return_value=response), now=100)
                self.assertEqual(self.delivery()['status'], 'retry')
                self.assertEqual(self.delivery()['next_attempt'], 100 + expected_delay)


class PurchaseTermsGuardAudit(unittest.TestCase):
    def test_unresolved_financial_or_arrival_condition_is_not_purchase_consent(self):
        for text in ['اريد هذا بس شكد سعره؟', 'اريد هذا اذا التوصيل مجاني',
                     'احجزيه اذا تحسبيه ب10000', 'ثبتيه بشرط يوصل باجر',
                     'اريده بيش هذا؟', 'اريد هذا شنو سعره؟']:
            with self.subTest(text=text):
                self.assertTrue(checkout.unresolved_purchase_terms(text))

    def test_inspection_question_does_not_revoke_an_unconditional_instruction(self):
        for text in ['ثبتيه، وافحصه بحضور المندوب قبل الدفع؟', 'ثبتي واذا مو نفس الصورة يرجع؟',
                     'اي ثبتي', 'اريد هذا', 'بغداد حي اور', '07701234567']:
            with self.subTest(text=text):
                self.assertEqual(checkout.unresolved_purchase_terms(text), '')


class FailedReplyCheckoutAudit(unittest.TestCase):
    setUp = CheckoutConsentAudit.setUp
    tearDown = CheckoutConsentAudit.tearDown

    def rejected_reply(self, text, **metadata):
        return self.m.save_message(self.db, self.sender, 'outgoing', 'text', text, None, None, None,
                                   dict(metadata, _delivery_status='rejected'))

    def incoming(self, text):
        self.m.save_message(self.db, self.sender, 'incoming', 'text', text, None, None, None, {})
        return dict(sender_id=self.sender, text=text)

    def test_bare_yes_cannot_confirm_a_rejected_cart_summary(self):
        result = {'order': {'items': self.items}, 'require_cart_confirmation': True}
        created, summary = self.m.create_order_if_valid(self.db, self.sender, result, self.product)
        self.assertFalse(created)
        self.rejected_reply(summary, checkout_proposal=result['_checkout_proposal'])
        event = self.incoming('اي')
        accepted = self.m.accept_checkout_proposal(self.db, event, [], [self.product])
        self.assertIsNone(accepted)
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0], 0)

    def test_contact_data_does_not_answer_a_rejected_booking_question(self):
        self.rejected_reply('دزيلي الموبايل والعنوان حتى أثبت الحجز')
        event = self.incoming('07701234567')
        self.assertFalse(self.m.checkout_requested(self.db, event, {'order': {'items': self.items}}))

    def test_unseen_draft_cannot_supply_customer_selected_variants(self):
        self.rejected_reply('دزيلي الرقم للحجز', checkout_draft={'items': self.items})
        event = self.incoming('07701234567')
        result = {'order': {}}
        self.m.restore_checkout_draft(self.db, event, result, self.product)
        self.assertFalse(result['order'].get('items'))

    def test_rejected_product_clarification_is_not_treated_as_a_repeated_question(self):
        self.rejected_reply('تقصدين الفستان لو السوت؟', product_selection_clarification=True)
        self.assertFalse(self.m.previous_product_clarification(self.db, self.sender))

    def test_rejected_pending_image_reply_does_not_suppress_the_next_attempt(self):
        self.rejected_reply('الصورة تحتاج تأكيد', human_review_id=19, pending_image_reply=True)
        self.assertFalse(self.m.has_sent_pending_image_reply(self.db, self.sender, 19))
