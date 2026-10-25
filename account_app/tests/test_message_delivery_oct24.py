"""Exercise the real layout/senders with HTTP replaced at the provider boundary.

These tests use a disposable database and fixture subscribers. They do not
contact customers or measure live conversion; the provider accepts/rejects
deterministic test payloads only.
"""
import contextlib
import copy
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

from account_app.reply_layout import approved_parts, compact


class MessageDeliveryOct24Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault('ENABLE_BACKGROUND_JOBS', '0')
        os.environ.setdefault('DISABLE_CLIP', '1')
        import account_app.app as module
        cls.m = module
        cls.temp = tempfile.TemporaryDirectory()
        cls.old_path = module.DB_PATH
        module.DB_PATH = str(Path(cls.temp.name) / 'delivery.db')
        module.init_db()

    @classmethod
    def tearDownClass(cls):
        cls.m.DB_PATH = cls.old_path
        cls.temp.cleanup()

    def setUp(self):
        self.context = self.m.app.app_context()
        self.context.push()
        self.store_token = self.m._current_store_id.set('default')
        self.sender = 'default::delivery-fixture-' + uuid.uuid4().hex
        self.m.get_or_create_customer(self.m.get_db(), self.sender, 'fixture-page', 'facebook')
        self.no_network = patch('requests.sessions.Session.request',
                                side_effect=AssertionError('Unexpected external network'))
        self.no_network.start()

    def tearDown(self):
        self.no_network.stop()
        self.m._current_store_id.reset(self.store_token)
        self.context.pop()

    @staticmethod
    def response(ok=True, status=200):
        response = Mock(ok=ok, status_code=status)
        response.json.return_value = {'status': 'success' if ok else 'error'}
        return response

    @contextlib.contextmanager
    def provider(self, responses=None):
        with patch.object(self.m.zernio, 'enabled_for_store', return_value=False), \
             patch.object(self.m.chatwoot, 'enabled', return_value=False), \
             patch.object(self.m, 'manychat_api_key_for_page', return_value='fixture-key'), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m.requests, 'post', side_effect=responses,
                          return_value=self.response()) as post:
            yield post

    def texts(self, post):
        return [message['text'] for call in post.call_args_list
                for message in call.kwargs['json']['data']['content']['messages']
                if message['type'] == 'text']

    def result(self, reply, **kwargs):
        return dict(sender_id=self.sender, platform='facebook', page_id='fixture-page',
                    reply=reply, **kwargs)

    def body(self, text='مواصفات'):
        return {'entry': [{'id': 'fixture-page', '_store_id': 'default', 'messaging': [{
            'sender': {'id': self.sender},
            'message': {'mid': 'fixture-' + uuid.uuid4().hex, 'text': text},
        }]}]}

    def test_real_sender_delivers_one_connected_reply_for_every_store(self):
        facts = ['هلا عيني 🌸', 'الفستان متوفر بالأسود.', 'السعر 17,000 د.ع.',
                 'التوصيل 5,000 د.ع. والمجموع 22,000 د.ع.', 'شنو القياس اللي تلبسينه؟']
        reply = '\n\n'.join(facts)
        for store in ['default', 'khuyoot', 'golden-threads', 'al-fatena', 'baraah-kids']:
            token = self.m._current_store_id.set(store)
            try:
                with self.provider() as post:
                    self.assertTrue(self.m.send_webhook_result_to_facebook(
                        self.result(reply, reply_parts=facts)))
                self.assertEqual(self.texts(post), [reply])
                self.assertEqual(post.call_count, 1)
            finally:
                self.m._current_store_id.reset(token)

    def test_mismatched_parts_cannot_resurrect_uncorrected_claims(self):
        reply = 'اللون الأسود مو متوفر حالياً. تحبين أشوفلج اللون المسجل للموديل؟'
        with self.provider() as post:
            self.assertTrue(self.m.send_webhook_result_to_facebook(self.result(
                reply, reply_parts=['باقي قطعتين بالأسود', 'تم الحجز'])))
        self.assertEqual(self.texts(post), [reply])

    def test_normalization_keeps_authoritative_reply_and_discards_duplicate_stale_parts(self):
        reply = 'السعر 17,000 د.ع. حسب الموديل، شنو المحافظة حتى أحسبلج التوصيل؟'
        result = self.m.normalize_ai_reply_parts(dict(
            reply=reply, reply_parts=['السعر 10,000 د.ع.', 'السعر 10,000 د.ع.']))
        self.assertEqual(result['reply'], reply)
        self.assertEqual(result['reply_parts'], [reply])

    def test_async_sender_uses_same_plan_as_direct_sender_and_sends_no_full_copy_afterwards(self):
        descriptions = [f'الصورة رقم {index}: ' + 'تفاصيل القطعة حسب الكتالوج. ' * 9
                        for index in range(1, 8)]
        reply = '\n\n'.join(descriptions)
        result = self.result(reply, reply_parts=descriptions, _ordered_album=True, send_image=False)
        expected = approved_parts(result, reply)
        with self.provider() as direct:
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        with self.provider() as asynchronous, patch.object(self.m, 'process_webhook', return_value=result):
            delivered = self.m._process_manychat_webhook_async_locked(self.body(), self.sender, 'facebook')
        self.assertTrue(delivered['ok'])
        self.assertEqual(self.texts(direct), expected)
        self.assertEqual(self.texts(asynchronous), expected)
        self.assertTrue(1 <= len(expected) <= 4)
        self.assertEqual(compact(' '.join(self.texts(asynchronous))), compact(reply))
        for description in descriptions:
            self.assertEqual(sum(description.strip() in text for text in self.texts(asynchronous)), 1)

    def test_same_job_replay_does_not_send_the_reply_twice(self):
        result = self.result('الفستان متوفر بالأسود. شنو قياسج؟')
        token = self.m.ai_jobs.active_job.set('delivery-test-' + uuid.uuid4().hex)
        try:
            with self.provider() as post:
                self.assertTrue(self.m.send_webhook_result_to_facebook(result))
                self.assertTrue(self.m.send_webhook_result_to_facebook(copy.deepcopy(result)))
            self.assertEqual(post.call_count, 1)
        finally:
            self.m.ai_jobs.active_job.reset(token)

    def test_uncertain_failure_is_not_retried_and_images_are_not_sent_after_failed_text(self):
        result = self.result('الموديل متوفر وهذه صورته.', send_image=True,
                             image_urls=['https://images.test/fixture.jpg'])
        token = self.m.ai_jobs.active_job.set('uncertain-test-' + uuid.uuid4().hex)
        try:
            with self.provider([self.response(False, 503)]) as post:
                self.assertFalse(self.m.send_webhook_result_to_facebook(result))
                self.assertFalse(self.m.send_webhook_result_to_facebook(result))
            self.assertEqual(post.call_count, 1)
            self.assertEqual(self.texts(post), [result['reply']])
        finally:
            self.m.ai_jobs.active_job.reset(token)

    def test_partial_delivery_replay_does_not_repeat_successful_first_part(self):
        descriptions = [f'الصورة {index}: ' + 'تفاصيل ' * 75 for index in range(1, 4)]
        result = self.result('\n\n'.join(descriptions), reply_parts=descriptions, _ordered_album=True)
        expected = approved_parts(result, result['reply'])
        self.assertEqual(len(expected), 3)
        token = self.m.ai_jobs.active_job.set('partial-test-' + uuid.uuid4().hex)
        try:
            with self.provider([self.response(), self.response(False, 503)]) as post:
                self.assertFalse(self.m.send_webhook_result_to_facebook(result))
                self.assertFalse(self.m.send_webhook_result_to_facebook(result))
            self.assertEqual(self.texts(post), expected[:2])
            self.assertEqual(post.call_count, 2)
        finally:
            self.m.ai_jobs.active_job.reset(token)

    def test_receipt_stays_one_message_even_with_many_structured_lines(self):
        reply = 'تم تثبيت الحجز بنجاح 🌸\n\n' + '\n'.join(
            f'تفصيل محفوظ {index}: ' + ('معلومة ' * 15).strip() for index in range(12))
        with self.provider() as post:
            self.assertTrue(self.m.send_webhook_result_to_facebook(self.result(
                reply, reply_parts=reply.splitlines())))
        self.assertEqual(self.texts(post), [reply])

    def test_full_webhook_prepares_requested_photo_and_final_sender_delivers_it_once(self):
        product = dict(product_id='PHOTO-FIXTURE', product_name='فستان صورة الاختبار',
                       price='17000', colors='اسود', sizes='38 إلى 52',
                       status='active', stock='متوفر', image_url='https://images.test/requested.jpg')
        db = self.m.get_db()
        self.m.complete_customer_product_link(db, self.sender, product, 'manual')
        with self.provider() as post, \
             patch.object(self.m, 'load_products_from_file', return_value=[product]), \
             patch.object(self.m, 'load_active_products', return_value=[product]), \
             patch.object(self.m, 'get_auto_product_settings', return_value={'enabled': False}), \
             patch.object(self.m, 'schedule_followup_if_needed'), \
             patch.object(self.m, '_call_main_ai_once', return_value={
                 'reply': 'تدللين، هاي صورة الموديل.', 'intent': 'product_photo',
                 'create_order': False, 'order': {}, 'requires_human': False}) as model:
            result = self.m.process_webhook(db, self.body('دزي صورة الموديل'), use_debounce=False)
            self.assertTrue(result['send_image'], result.get('meta'))
            self.assertEqual(result['product_image_urls'], [product['image_url']])
            # Processing prepares the full response; only the final sender owns
            # network delivery, so a photo cannot be sent here and sent again.
            post.assert_not_called()
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        model.assert_called_once()
        messages = [message for call in post.call_args_list
                    for message in call.kwargs['json']['data']['content']['messages']]
        self.assertEqual([message['type'] for message in messages], ['text', 'image'])
        self.assertEqual([message['url'] for message in messages if message['type'] == 'image'],
                         [product['image_url']])


if __name__ == '__main__':
    unittest.main()
