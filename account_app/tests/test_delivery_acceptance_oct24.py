"""Delivery acceptance regressions with real SQLite and actual send/layout code.

Only the HTTP provider boundary is simulated. No live model, subscriber, key,
customer data, or network is used. Acceptance is distinct from read/receipt.
"""
import contextlib
import copy
import json
import unittest
from unittest.mock import patch

import requests

from account_app import delivery_state
from account_app.tests import test_audit_orders as fixtures


class DeliveryAcceptanceOct24Tests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set('default')
        self.sender = 'default::synthetic-delivery-acceptance'
        self.m.get_or_create_customer(self.db, self.sender, 'synthetic-page', 'facebook')
        self.boundaries = contextlib.ExitStack()
        self.boundaries.enter_context(patch.object(self.m.zernio, 'enabled_for_store', return_value=False))
        self.boundaries.enter_context(patch.object(self.m.chatwoot, 'enabled', return_value=False))
        self.boundaries.enter_context(patch.object(self.m, 'manychat_api_key_for_page', return_value='synthetic-offline-key'))
        self.boundaries.enter_context(patch.object(self.m, 'is_ai_enabled', return_value=True))
        self.boundaries.enter_context(patch.object(self.m, 'is_customer_ai_enabled', return_value=True))

    def tearDown(self):
        self.boundaries.close()
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    @staticmethod
    def response(status=200):
        response = requests.Response()
        response.status_code = status
        response._content = json.dumps({'status': 'success' if status == 200 else 'error'}).encode()
        response.headers['Content-Type'] = 'application/json'
        return response

    def incoming(self, text):
        message_id = self.m.save_message(self.db, self.sender, 'incoming', 'text', text, None, None, None, {})
        self.m.save_conversation_message(self.db, self.sender, 'user', text)
        return message_id

    def prepare_result(self, *, album=False, image=False):
        if album:
            parts = [f'الصورة {index}: ' + ('القطعة متوفرة بالأسود وقياسها 44. ' * 25).strip()
                     for index in range(1, 4)]
        else:
            parts = ['الفستان سعره 16,000 د.ع وقياساته من 38 إلى 52.']
        result = dict(sender_id=self.sender, page_id='synthetic-page', platform='facebook',
                      reply='\n\n'.join(parts), reply_parts=parts, _ordered_album=album,
                      send_image=image, image_urls=['https://images.test/acceptance.jpg'] if image else [])
        approved = self.m.approved_reply_parts(result, result['reply'])
        self.assertEqual(len(approved), 3 if album else 1)
        for text in approved:
            self.m.save_message(self.db, self.sender, 'outgoing', 'text', text, None, None, None, {})
            self.m.save_conversation_message(self.db, self.sender, 'assistant', text)
        for url in result['image_urls']:
            self.m.save_message(self.db, self.sender, 'outgoing', 'image', None, url, None, None, {})
        return result, approved

    def saved_outgoing(self):
        return [dict(row, metadata=json.loads(row['raw_payload'])) for row in self.db.execute(
            "SELECT * FROM messages WHERE sender_id=? AND direction='outgoing' ORDER BY id", (self.sender,))]

    @staticmethod
    def sent_messages(post):
        return [message for call in post.call_args_list
                for message in call.kwargs['json']['data']['content']['messages']]

    def assert_question_pending(self, question):
        latest_id = self.incoming('ها عيني؟')
        collected = self.m.collect_unanswered_event(self.db, dict(sender_id=self.sender, text='ها عيني؟'), latest_id)
        self.assertIn(question, collected['text'])

    def test_failure_of_first_part_keeps_original_question_in_collect_history_and_memory(self):
        question = 'شكد سعره وشنو قياساته؟'
        self.incoming(question)
        result, _ = self.prepare_result(image=True)
        with patch.object(self.m.requests, 'post', return_value=self.response(503)) as post:
            self.assertFalse(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual(post.call_count, 1)
        self.assertEqual([message['type'] for message in self.sent_messages(post)], ['text'])
        rows = self.saved_outgoing()
        self.assertEqual([row['metadata']['_delivery_status'] for row in rows], ['unconfirmed', 'skipped'])
        self.assertTrue(all(row['metadata']['_partial_delivery_reply'] for row in rows))
        history = self.m.load_history(self.db, self.sender)
        self.assertEqual([row['text'] for row in history if row['direction'] == 'incoming'], [question])
        self.assertFalse(any(row['direction'] == 'outgoing' for row in history))
        self.assertEqual(self.m.get_conversation_history(self.db, self.sender), [{'role': 'user', 'content': question}])
        self.assert_question_pending(question)

    def test_partial_acceptance_keeps_question_pending_and_does_not_send_the_images(self):
        question = 'شنو تفاصيل الصور الثلاث وشكد سعرهن؟'
        self.incoming(question)
        result, parts = self.prepare_result(album=True, image=True)
        with patch.object(self.m.requests, 'post', side_effect=[self.response(), self.response(503)]) as post:
            self.assertFalse(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual(self.sent_messages(post), [{'type': 'text', 'text': part} for part in parts[:2]])
        rows = self.saved_outgoing()
        self.assertEqual([row['metadata']['_delivery_status'] for row in rows],
                         ['accepted', 'unconfirmed', 'skipped', 'skipped'])
        self.assertTrue(all(row['metadata']['_partial_delivery_reply'] for row in rows))
        self.assertEqual([row['text'] for row in self.m.load_history(self.db, self.sender) if row['direction'] == 'outgoing'], parts[:1])
        self.assertEqual([row['content'] for row in self.m.get_conversation_history(self.db, self.sender) if row['role'] == 'assistant'], parts[:1])
        self.assert_question_pending(question)

    def test_repeating_result_does_not_resend_accepted_or_uncertain_parts_even_without_job_token(self):
        self.incoming('تفاصيل الصور الثلاث؟')
        result, parts = self.prepare_result(album=True, image=True)
        with patch.object(self.m.requests, 'post', side_effect=[self.response(), self.response(503)]) as post:
            self.assertFalse(self.m.send_webhook_result_to_facebook(result))
            self.assertFalse(self.m.send_webhook_result_to_facebook(copy.deepcopy(result)))
        self.assertEqual(self.sent_messages(post), [{'type': 'text', 'text': part} for part in parts[:2]])
        self.assertEqual(post.call_count, 2)

    def test_definite_rejection_allows_safe_retry_until_all_parts_are_accepted(self):
        self.incoming('تفاصيل الصور الثلاث؟')
        result, parts = self.prepare_result(album=True, image=True)
        with patch.object(self.m.requests, 'post', side_effect=[
                self.response(), self.response(400), self.response(), self.response(), self.response()]) as post:
            self.assertFalse(self.m.send_webhook_result_to_facebook(result))
            self.assertEqual([row['metadata']['_delivery_status'] for row in self.saved_outgoing()],
                             ['accepted', 'rejected', 'skipped', 'skipped'])
            self.assertTrue(self.m.send_webhook_result_to_facebook(copy.deepcopy(result)))
        sent = self.sent_messages(post)
        self.assertEqual([message['text'] for message in sent if message['type'] == 'text'],
                         [parts[0], parts[1], parts[1], parts[2]])
        self.assertEqual([message['type'] for message in sent], ['text', 'text', 'text', 'text', 'image'])
        self.assertTrue(all(row['metadata']['_delivery_status'] == 'accepted' for row in self.saved_outgoing()))
        self.assertTrue(all(not row['metadata']['_partial_delivery_reply'] for row in self.saved_outgoing()))
        latest_id = self.incoming('شكرا')
        event = self.m.collect_unanswered_event(self.db, dict(sender_id=self.sender, text='شكرا'), latest_id)
        self.assertEqual(event['text'], 'شكرا')

    def test_correction_after_normalization_never_resurrects_stale_parts(self):
        self.incoming('شكد السعر الصحيح؟')
        original = {'reply': 'السعر 10,000 د.ع.', 'reply_parts': ['السعر 10,000 د.ع.', 'تم الحجز']}
        result = self.m.normalize_ai_reply_parts(original)
        corrected = 'السعر الصحيح 16,000 د.ع والتوصيل 5,000 والمجموع 21,000 د.ع.'
        result.update(sender_id=self.sender, page_id='synthetic-page', platform='facebook', reply=corrected)
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', corrected, None, None, None, {})
        with patch.object(self.m.requests, 'post', return_value=self.response()) as post:
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual(self.sent_messages(post), [{'type': 'text', 'text': corrected}])
        self.assertNotIn('تم الحجز', str(self.sent_messages(post)))
        self.assertNotIn('10,000', str(self.sent_messages(post)))

    def test_manychat_async_path_tracks_partial_acceptance_in_the_same_saved_rows(self):
        question = 'تفاصيل الصور الثلاث؟'
        self.incoming(question)
        result, _ = self.prepare_result(album=True, image=True)
        body = {'entry': [{'id': 'synthetic-page', '_store_id': 'default', 'messaging': [{
            'sender': {'id': self.sender}, 'message': {'text': question, 'mid': 'synthetic-async-mid'}}]}]}
        with patch.object(self.m, 'process_webhook', return_value=result), \
             patch.object(self.m.requests, 'post', side_effect=[self.response(), self.response(503)]) as post:
            delivery = self.m._process_manychat_webhook_async_locked(body, self.sender, 'facebook')
        self.assertFalse(delivery['ok'])
        self.assertEqual(post.call_count, 2)
        self.assertEqual([row['metadata'].get('_delivery_status') for row in self.saved_outgoing()],
                         ['accepted', 'unconfirmed', 'skipped', 'skipped'])
        self.assert_question_pending(question)

    def test_new_incoming_between_reply_save_and_send_cannot_make_a_failed_reply_look_accepted(self):
        question = 'شكد سعره وشنو قياساته؟'
        self.incoming(question)
        result, parts = self.prepare_result()
        # Webhook intake persists the next bubble before the previous job has
        # reached the sender. Its row ID is newer than the prepared answer.
        latest_id = self.incoming('وشكد التوصيل؟')
        with patch.object(self.m.requests, 'post', return_value=self.response(503)) as post:
            self.assertFalse(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.saved_outgoing()[0]['metadata'].get('_delivery_status'), 'unconfirmed')
        self.assertFalse(any(row['text'] in parts for row in self.m.load_history(self.db, self.sender)
                             if row['direction'] == 'outgoing'))
        event = self.m.collect_unanswered_event(self.db,
            dict(sender_id=self.sender, text='وشكد التوصيل؟'), latest_id)
        self.assertIn(question, event['text'])
        self.assertIn('وشكد التوصيل؟', event['text'])

    def test_identical_answer_to_a_new_customer_turn_is_a_new_delivery(self):
        self.incoming('شكد السعر؟')
        first, _ = self.prepare_result()
        with patch.object(self.m.requests, 'post', return_value=self.response()) as post:
            self.assertTrue(self.m.send_webhook_result_to_facebook(first))
            self.incoming('عيدي السعر عيني؟')
            second, _ = self.prepare_result()
            self.assertTrue(self.m.send_webhook_result_to_facebook(second))
            self.assertTrue(self.m.send_webhook_result_to_facebook(copy.deepcopy(second)))
        self.assertEqual(post.call_count, 2)

    def test_explicit_new_request_can_receive_the_same_answer_after_an_uncertain_prior_turn(self):
        self.incoming('شكد السعر؟')
        first, _ = self.prepare_result()
        with patch.object(self.m.requests, 'post', side_effect=[self.response(503), self.response()]) as post:
            self.assertFalse(self.m.send_webhook_result_to_facebook(first))
            self.incoming('ما ظهر عندي الرد، دزي السعر مرة ثانية')
            second, _ = self.prepare_result()
            self.assertTrue(self.m.send_webhook_result_to_facebook(second))
        self.assertEqual(post.call_count, 2)
        self.assertEqual([row['metadata'].get('_delivery_status') for row in self.saved_outgoing()],
                         ['unconfirmed', 'accepted'])

    def test_explicit_message_ids_never_include_a_foreign_sender_or_staff_row(self):
        self.incoming('شكد السعر؟')
        result, parts = self.prepare_result()
        own_id = self.saved_outgoing()[0]['id']
        self.m.get_or_create_customer(self.db, 'foreign-synthetic', 'page', 'facebook')
        foreign_id = self.m.save_message(self.db, 'foreign-synthetic', 'outgoing', 'text',
                                         parts[0], None, None, None, {})
        staff_id = self.m.save_message(self.db, self.sender, 'outgoing', 'text',
                                       parts[0], None, None, None, {'manual_reply': True})
        rows = delivery_state.prepare(self.db, self.sender, parts, [], [own_id, foreign_id, staff_id])
        self.assertEqual([row['id'] for row in rows], [own_id])
        for message_id in [foreign_id, staff_id]:
            raw = self.db.execute('SELECT raw_payload FROM messages WHERE id=?', (message_id,)).fetchone()[0]
            self.assertNotIn('_delivery_status', json.loads(raw))

    def test_explicit_ids_select_the_older_generation_without_touching_the_new_identical_answer(self):
        self.incoming('شكد السعر؟')
        _, parts = self.prepare_result()
        older_id = self.saved_outgoing()[0]['id']
        self.incoming('دزي السعر مرة ثانية')
        self.prepare_result()
        newer_id = self.saved_outgoing()[-1]['id']
        rows = delivery_state.prepare(self.db, self.sender, parts, [], [older_id])
        self.assertEqual([row['id'] for row in rows], [older_id])
        delivery_state.update(self.db, rows, 'unconfirmed')
        raw = self.db.execute('SELECT raw_payload FROM messages WHERE id=?', (newer_id,)).fetchone()[0]
        self.assertNotIn('_delivery_status', json.loads(raw))

    def test_empty_explicit_ids_do_not_fall_back_to_another_generation(self):
        self.incoming('شكد السعر؟')
        _, parts = self.prepare_result()
        self.assertEqual(delivery_state.prepare(self.db, self.sender, parts, [], []), [])
        self.assertNotIn('_delivery_status', self.saved_outgoing()[0]['metadata'])

    def test_generated_reply_is_invisible_until_provider_acceptance(self):
        message_id = self.incoming('شكد السعر؟')
        state = {'sender_id': self.sender, 'incoming_id': message_id, 'outgoing_ids': []}
        token = self.m._reply_generation.set(state)
        try:
            result, _ = self.prepare_result()
        finally:
            self.m._reply_generation.reset(token)
        self.assertEqual(self.saved_outgoing()[0]['metadata']['_delivery_status'], 'generated')
        self.assertFalse(any(row['direction'] == 'outgoing' for row in self.m.load_history(self.db, self.sender)))
        result.update(_reply_to_message_id=message_id, _outgoing_message_ids=state['outgoing_ids'])
        with patch.object(self.m.requests, 'post', return_value=self.response()) as post:
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual(post.call_count, 1)
        self.assertTrue(any(row['direction'] == 'outgoing' for row in self.m.load_history(self.db, self.sender)))

    def test_tracked_stale_reply_is_skipped_without_losing_either_incoming_question(self):
        first_id = self.incoming('شكد السعر؟')
        result, _ = self.prepare_result()
        result.update(_reply_to_message_id=first_id,
                      _outgoing_message_ids=[row['id'] for row in self.saved_outgoing()])
        last_id = self.incoming('وشكد التوصيل؟')
        with patch.object(self.m.requests, 'post', return_value=self.response()) as post:
            self.assertFalse(self.m.send_webhook_result_to_facebook(result))
        post.assert_not_called()
        self.assertEqual(self.saved_outgoing()[0]['metadata'].get('_delivery_status'), 'skipped')
        event = self.m.collect_unanswered_event(self.db, dict(sender_id=self.sender, text='وشكد التوصيل؟'), last_id)
        self.assertIn('شكد السعر؟', event['text'])
        self.assertIn('وشكد التوصيل؟', event['text'])

    def test_stale_tracked_result_cannot_mark_a_fresh_identical_answer_as_skipped(self):
        first_id = self.incoming('شكد السعر؟')
        older, _ = self.prepare_result()
        old_row_id = self.saved_outgoing()[0]['id']
        older.update(_reply_to_message_id=first_id, _outgoing_message_ids=[old_row_id])
        second_id = self.incoming('عيدي السعر عيني؟')
        newer, _ = self.prepare_result()
        new_row_id = self.saved_outgoing()[-1]['id']
        newer.update(_reply_to_message_id=second_id, _outgoing_message_ids=[new_row_id])
        with patch.object(self.m.requests, 'post', return_value=self.response()) as post:
            self.assertFalse(self.m.send_webhook_result_to_facebook(older))
            self.assertTrue(self.m.send_webhook_result_to_facebook(newer))
        self.assertEqual(post.call_count, 1)
        self.assertEqual([row['metadata'].get('_delivery_status') for row in self.saved_outgoing()],
                         ['skipped', 'accepted'])

    def test_new_customer_correction_during_generation_prevents_creating_a_stale_booking(self):
        product = dict(product_id='STALE1', product_name='فستان الاختبار', price='16000',
                       sizes='38 إلى 52', colors='اسود', status='active', stock='متوفر')
        self.m.complete_customer_product_link(self.db, self.sender, product, 'manual', source='manual_admin')
        self.db.execute('UPDATE customers SET phone=?,province=?,address=? WHERE sender_id=?',
                        ('07701234567', 'بغداد', 'بغداد حي اور', self.sender))
        self.db.commit()
        body = {'entry': [{'id': 'synthetic-page', '_store_id': 'default', 'messaging': [{
            'sender': {'id': self.sender}, 'message': {'text': 'ثبتي الطلب', 'mid': 'synthetic-stale-order'}}]}]}

        def late_provider(*args, **kwargs):
            # Simulate a different intake worker: direct persistence deliberately
            # avoids inheriting this generation's ContextVar tracking callback.
            self.db.execute('''INSERT INTO messages(sender_id,direction,message_type,text,raw_payload,created_at,store_id)
                VALUES(?,'incoming','text',?,'{}',?,'default')''',
                (self.sender, 'لا تثبتين، اريد اغير القياس', self.m.now_baghdad_iso()))
            self.db.commit()
            return {'reply': 'حاضر عيني.', 'create_order': True,
                    'order': {'items': [dict(product_id='STALE1', product_name='فستان الاختبار',
                                             size='44', color='اسود', quantity=1)]}}

        with patch.object(self.m, 'load_products_from_file', return_value=[product]), \
             patch.object(self.m, 'load_active_products', return_value=[product]), \
             patch.object(self.m, 'get_auto_product_settings', return_value={'enabled': False}), \
             patch.object(self.m, 'is_store_feature_enabled', return_value=False), \
             patch.object(self.m, 'check_reply', return_value={'approved': True}), \
             patch.object(self.m, 'call_main_ai', side_effect=late_provider), \
             patch.object(self.m, 'save_booking_to_file') as booking_file, \
             patch.object(self.m, 'send_order_to_telegram') as notification:
            self.m.process_webhook(self.db, body, use_debounce=False)
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)
        booking_file.assert_not_called()
        notification.assert_not_called()
