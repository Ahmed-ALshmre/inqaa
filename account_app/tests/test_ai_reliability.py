import json
import sqlite3
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import requests
from account_app import ai_efficiency, ai_jobs, ai_transport, rewrite_guard


def response(status=200):
    result = requests.Response()
    result.status_code = status
    result._content = b'{}'
    return result


class ProviderReliabilityTests(unittest.TestCase):
    def test_timeout_then_success_and_billing_not_retried(self):
        with patch.object(ai_transport.requests, 'post', side_effect=[requests.ReadTimeout(), response()]) as post, \
             patch.object(ai_transport.time, 'sleep'):
            self.assertEqual(ai_transport.post('https://example.test').status_code, 200)
            self.assertEqual(post.call_count, 2)
        for status in (400, 401, 402, 403, 404):
            with patch.object(ai_transport.requests, 'post', return_value=response(status)) as post:
                self.assertEqual(ai_transport.post('https://example.test').status_code, status)
                post.assert_called_once()

    def test_retry_after_is_respected_and_long_delay_not_ignored(self):
        first = response(429); first.headers['Retry-After'] = '2'
        with patch.object(ai_transport.requests, 'post', side_effect=[first, response()]), patch.object(ai_transport.time, 'sleep') as sleep:
            ai_transport.post('https://example.test')
            sleep.assert_called_once_with(2)
        first = response(429); first.headers['Retry-After'] = '3600'
        with patch.object(ai_transport.requests, 'post', return_value=first) as post:
            self.assertEqual(ai_transport.post('https://example.test').status_code, 429)
            post.assert_called_once()

    def test_eight_requests_enter_provider_concurrently(self):
        entered = threading.Barrier(8)
        def provider(*args, **kwargs):
            entered.wait(timeout=5)
            return response()
        with patch.object(ai_transport.requests, 'post', side_effect=provider), \
             patch.object(ai_transport, '_slots', threading.BoundedSemaphore(8)):
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda _: ai_transport.post('https://example.test'), range(8)))
        self.assertTrue(all(r.status_code == 200 for r in results))

    def test_vision_truncation_unknown_and_ambiguity_are_not_matches(self):
        def payload(content, reason='stop'):
            return {'choices': [{'message': {'content': content}, 'finish_reason': reason}]}
        products = [{'product_id': 'P005'}]
        self.assertTrue(ai_efficiency.vision_choice(payload('P005'), products)['product_found'])
        self.assertFalse(ai_efficiency.vision_choice(payload('NONE'), products).get('service_error'))
        for content, reason in [('P005', 'length'), ('', 'stop'), ('P005 or P006', 'stop'), ('P999', 'stop')]:
            self.assertTrue(ai_efficiency.vision_choice(payload(content, reason), products)['service_error'])

    def test_rewrite_cannot_change_price_color_negation_or_create_booking(self):
        for source, changed in [('السعر 15000', 'السعر 25000'), ('اللون وردي', 'اللون أسود'),
                                ('مو متوفر', 'متوفر'), ('متوفر', 'تم حجز المنتج')]:
            self.assertFalse(rewrite_guard.valid_rewrite(source, changed))
        self.assertTrue(rewrite_guard.valid_rewrite('القماش قطن', 'من عيوني، القماش قطن.'))


class PersistentQueueTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:'); self.db.row_factory = sqlite3.Row
        ai_jobs.init_db(self.db); ai_efficiency.init_db(self.db); self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_parallel_customers_but_same_customer_serial_and_deduped(self):
        first = ai_jobs.enqueue(self.db, 'a', 'u', 'preview', {}, 'one')
        again = ai_jobs.enqueue(self.db, 'a', 'u', 'preview', {}, 'one')
        self.assertEqual(first['id'], again['id'])
        second = ai_jobs.enqueue(self.db, 'a', 'u', 'preview', {}, 'two')
        other = ai_jobs.enqueue(self.db, 'b', 'u', 'preview', {}, 'one')
        self.db.execute('UPDATE ai_jobs SET created_at=1'); self.db.commit()
        a = ai_jobs.claim(self.db, 'worker1'); b = ai_jobs.claim(self.db, 'worker2')
        self.assertEqual({a['id'], b['id']}, {first['id'], other['id']})
        self.assertIsNone(ai_jobs.claim(self.db, 'worker3'))
        ai_jobs.finish(self.db, first['id'], 'done', {})
        self.assertEqual(ai_jobs.claim(self.db, 'worker3')['id'], second['id'])

    def test_restart_only_retries_pure_previews_and_keeps_queued_messages(self):
        for sender, kind in [('p', 'preview'), ('s', 'link')]:
            ai_jobs.enqueue(self.db, 'a', sender, kind, {}, sender)
            ai_jobs.claim(self.db, sender)
        queued = ai_jobs.enqueue(self.db, 'a', 'q', 'webhook', {}, 'queued')
        ai_jobs.recover_interrupted(self.db)
        statuses = dict(self.db.execute('SELECT sender_id,status FROM ai_jobs'))
        self.assertEqual(statuses, {'p': 'queued', 's': 'failed', 'q': 'queued'})

    def test_partial_delivery_never_repeats_confirmed_or_uncertain_parts(self):
        send = Mock(return_value={'ok': True})
        for _ in range(2):
            self.assertTrue(ai_jobs.deliver(self.db, 'job', ['first'], send)['ok'])
        send.assert_called_once()
        failure = Mock(side_effect=requests.ReadTimeout)
        with self.assertRaises(requests.ReadTimeout):
            ai_jobs.deliver(self.db, 'job', ['second'], failure)
        self.assertFalse(ai_jobs.deliver(self.db, 'job', ['second'], send)['ok'])
        send.assert_called_once()

    def test_transient_image_cache_recovers_after_cooldown_not_on_duplicate(self):
        callback = Mock(side_effect=[{'service_error': True, 'error_code': 'timeout'},
                                    {'product_found': True, 'product_id': 'P005'}])
        for _ in range(2):
            ai_efficiency.recognize_once(self.db, 's', 'u', 'key', [], callback)
        callback.assert_called_once()
        self.db.execute('UPDATE image_recognition_attempts SET updated_epoch=?', (time.time() - 61,)); self.db.commit()
        self.assertTrue(ai_efficiency.recognize_once(self.db, 's', 'u', 'key', [], callback)['product_found'])
        self.assertEqual(callback.call_count, 2)

    def test_payment_error_cache_can_recover_after_configuration_is_fixed(self):
        callback = Mock(side_effect=[{'service_error': True, 'error_code': 'billing'},
                                    {'product_found': True, 'product_id': 'P005'}])
        ai_efficiency.recognize_once(self.db, 's', 'u', 'payment', [], callback)
        self.db.execute('UPDATE image_recognition_attempts SET updated_epoch=?', (time.time() - 61,)); self.db.commit()
        self.assertTrue(ai_efficiency.recognize_once(self.db, 's', 'u', 'payment', [], callback)['service_error'])
        self.db.execute('UPDATE image_recognition_attempts SET updated_epoch=?', (time.time() - 301,)); self.db.commit()
        self.assertTrue(ai_efficiency.recognize_once(self.db, 's', 'u', 'payment', [], callback)['product_found'])
        self.assertEqual(callback.call_count, 2)


class DashboardReliabilityTests(unittest.TestCase):
    def setUp(self):
        import uuid
        from account_app import app
        self.m = app
        self.ctx = app.app.app_context(); self.ctx.push()
        self.db = app.get_db(); self.sender = 'reliability-' + uuid.uuid4().hex
        self.token = app._current_store_id.set('default')
        app.get_or_create_customer(self.db, self.sender, 'page')
        self.client = app.app.test_client()
        with self.client.session_transaction() as session: session['dashboard_authenticated'] = True
        self.product = {'product_id': 'R1', 'product_name': 'فستان', 'status': 'active'}

    def tearDown(self):
        self.m._current_store_id.reset(self.token); self.ctx.pop()

    def test_manual_preview_never_downloads_or_matches_unanswered_image(self):
        m = self.m
        m.save_message(self.db, self.sender, 'incoming', 'image', '', 'https://image.test/photo.jpg', None, None, {})
        with patch.object(m, 'find_product_by_id', return_value=self.product), \
             patch.object(m, 'load_active_products', return_value=[self.product]), \
             patch.object(m, 'match_product') as vision, \
             patch.object(m, '_download_image_to_data_url') as download, \
             patch.object(m, 'call_main_ai', return_value={'reply': 'شنو القياس المطلوب؟'}) as main:
            result = self.client.post(f'/api/conversations/{self.sender}/ask_ai', json={'product_id': 'R1', 'allow_empty': True})
        self.assertEqual(result.status_code, 200)
        vision.assert_not_called(); download.assert_not_called()
        self.assertIsNone(main.call_args.args[0]['image_url'])
        self.assertEqual(main.call_args.args[5]['product_id'], 'R1')

    def test_manual_link_retains_review_on_failure_and_resumes_only_automatic_pause(self):
        m = self.m
        with patch.object(m, 'send_telegram_message'):
            m.create_human_review(self.db, {'sender_id': self.sender}, 'image unresolved')
        m.set_store_setting(self.db, 'ai_enabled', '1', 'default')
        m.set_customer_ai_enabled(self.db, self.sender, False, reason='image_unresolved')
        with patch.object(m, 'find_product_by_id', return_value=self.product), \
             patch.object(m, 'auto_reply_after_product_link', return_value={'sent': False}):
            self.client.post(f'/api/conversations/{self.sender}/link_product', json={'product_id': 'R1'})
        self.assertFalse(m.is_customer_ai_enabled(self.db, self.sender))
        self.assertTrue(m.has_pending_human_review(self.db, self.sender))
        with patch.object(m, 'find_product_by_id', return_value=self.product), \
             patch.object(m, 'auto_reply_after_product_link', return_value={'sent': True}) as reply:
            self.client.post(f'/api/conversations/{self.sender}/link_product', json={'product_id': 'R1'})
        self.assertTrue(reply.call_args.kwargs['staff_action'])
        self.assertTrue(m.is_customer_ai_enabled(self.db, self.sender))
        m.set_customer_ai_enabled(self.db, self.sender, False, reason='manual')
        with patch.object(m, 'find_product_by_id', return_value=self.product), \
             patch.object(m, 'auto_reply_after_product_link', return_value={'sent': True}):
            self.client.post(f'/api/conversations/{self.sender}/link_product', json={'product_id': 'R1'})
        self.assertFalse(m.is_customer_ai_enabled(self.db, self.sender))

    def test_async_preview_returns_job_without_calling_provider(self):
        with patch.object(self.m, 'call_main_ai') as model:
            result = self.client.post(f'/api/conversations/{self.sender}/ask_ai', json={'async': True, 'allow_empty': True})
        self.assertEqual(result.status_code, 202); model.assert_not_called()
        job_id = result.json['job_id']
        self.assertEqual(self.client.get(f'/api/conversations/{self.sender}/ai_job/{job_id}').json['status'], 'queued')
        self.assertEqual(self.client.get(f'/api/conversations/other/ai_job/{job_id}').status_code, 404)

    def test_worker_preview_runs_with_correct_store_and_returns_result(self):
        from account_app import app as m
        queued = self.client.post(f'/api/conversations/{self.sender}/ask_ai', json={'async': True, 'allow_empty': True})
        job = dict(self.db.execute('SELECT * FROM ai_jobs WHERE id=?', (queued.json['job_id'],)).fetchone())
        def model(*args, **kwargs):
            self.assertEqual(m.current_store_id(), 'default')
            return {'reply': 'رد تجريبي'}
        with patch.object(m, 'call_main_ai', side_effect=model):
            result = m.run_ai_job(job, json.loads(job['payload']))
        self.assertEqual(result['reply'], 'رد تجريبي')

    def test_manual_image_resolution_survives_next_matching_without_download(self):
        m = self.m
        m.save_message(self.db, self.sender, 'incoming', 'image', '', 'https://image.test/item.jpg', None, None, {})
        with patch.object(m, 'find_product_by_id', return_value=self.product):
            self.client.post(f'/api/conversations/{self.sender}/link_product', json={'product_id': 'R1', 'silent': True})
        with patch.object(m, '_download_image_to_data_url') as download, patch.object(m, 'match_customer_image_with_catalog') as vision:
            matched, method, result = m._match_single_product(self.db, {'sender_id': self.sender, 'image_url': 'https://image.test/item.jpg'}, [self.product])
        self.assertEqual(matched['product_id'], 'R1'); self.assertEqual(method, 'manual')
        download.assert_not_called(); vision.assert_not_called()

    def test_product_link_stops_if_customer_changes_context_during_generation(self):
        m = self.m
        m.save_message(self.db, self.sender, 'incoming', 'text', 'السعر؟', None, None, None, {})
        def model(*args, **kwargs):
            m.save_message(self.db, self.sender, 'incoming', 'text', 'لا أريد هذا الموديل', None, None, None, {})
            return {'reply': 'متوفر', 'create_order': False}
        with patch.object(m, 'call_main_ai', side_effect=model), patch.object(m, 'send_webhook_result_to_facebook') as send:
            result = m.auto_reply_after_product_link(self.db, self.sender, self.product, staff_action=True)
        self.assertFalse(result['sent']); self.assertEqual(result['reason'], 'conversation_changed'); send.assert_not_called()

    def test_partial_delivery_is_not_reported_as_success(self):
        reply = 'تفاصيل المنتج والسعر حسب البيانات المسجلة لدى المتجر.\n\nشنو القياس المطلوب؟'
        with patch.object(self.m, 'send_text_to_facebook', side_effect=[True, False]) as send:
            result = self.m.send_webhook_result_to_facebook({'sender_id': self.sender, 'reply': reply})
        self.assertFalse(result); self.assertEqual(send.call_count, 2)

    def test_dashboard_sends_real_bubbles_and_records_only_confirmed_parts(self):
        text = 'القياس متوفر\n\nشنو اللون المطلوب؟'
        with patch.object(self.m, 'send_text_via_manychat_detailed', side_effect=[{'ok': True}, {'ok': False, 'status_code': 400}]) as send:
            result = self.client.post(f'/api/conversations/{self.sender}/send', json={'text': text})
        self.assertFalse(result.json['ok'])
        self.assertEqual([call.args[1] for call in send.call_args_list], ['القياس متوفر', 'شنو اللون المطلوب؟'])
        rows = self.db.execute("SELECT text FROM messages WHERE sender_id=? AND direction='outgoing'", (self.sender,)).fetchall()
        self.assertEqual([row[0] for row in rows], ['القياس متوفر'])

    def test_facebook_batch_is_persisted_and_duplicate_mid_not_requeued(self):
        m = self.m
        events = [{'sender': {'id': self.sender}, 'message': {'mid': self.sender + '-1', 'text': 'السعر؟'}},
                  {'sender': {'id': self.sender + '-other'}, 'message': {'mid': self.sender + '-2', 'text': 'القياسات؟'}}]
        body = {'entry': [{'id': 'page', '_store_id': 'default', 'messaging': events}]}
        m.process_webhook_in_background(body)
        m.process_webhook_in_background(body)
        count = self.db.execute('SELECT count(*) FROM ai_jobs WHERE sender_id IN (?,?)',
                                (self.sender, self.sender + '-other')).fetchone()[0]
        self.assertEqual(count, 2)


class TelegramRoutingTests(unittest.TestCase):
    def test_notifications_and_problems_use_separate_group(self):
        import account_app.app as m
        with patch.multiple(m, TELEGRAM_NOTIFICATIONS_CHAT_ID='-100alerts', TELEGRAM_ORDERS_CHAT_ID='-100orders', TELEGRAM_CHAT_ID='-100legacy', TELEGRAM_BOT_TOKEN='test-token'), patch.object(m.requests, 'post', return_value=Mock(ok=True)) as post:
            m.send_telegram_message('test')
            self.assertEqual(post.call_args.kwargs['json']['chat_id'], '-100alerts')
            m.send_problem_to_telegram({'reason': 'test'})
            self.assertEqual(post.call_args.kwargs['json']['chat_id'], '-100alerts')
            m.send_telegram_photo('https://images.test/photo.jpg')
            self.assertEqual(post.call_args.kwargs['json']['chat_id'], '-100alerts')

    def test_order_channel_and_legacy_order_fallback_unchanged(self):
        import account_app.app as m
        with patch.multiple(m, TELEGRAM_NOTIFICATIONS_CHAT_ID='-100alerts', TELEGRAM_ORDERS_CHAT_ID='-100orders', TELEGRAM_CHAT_ID='-100legacy'), patch.object(m, 'send_telegram_message', return_value=True) as send:
            m.send_order_to_telegram({})
            self.assertEqual(send.call_args.kwargs['chat_id'], '-100orders')
            with patch.object(m, 'TELEGRAM_ORDERS_CHAT_ID', ''):
                m.send_order_to_telegram({})
                self.assertEqual(send.call_args.kwargs['chat_id'], '-100legacy')

    def test_webhook_commands_only_accept_notifications_group(self):
        import account_app.app as m
        with patch.multiple(m, TELEGRAM_NOTIFICATIONS_CHAT_ID='-100alerts', TELEGRAM_CHAT_ID='-100legacy'), patch.object(m, 'handle_human_product_selection', return_value={'ok': True}) as action:
            client=m.app.test_client()
            response=client.post('/telegram/webhook', json={'message': {'chat': {'id': '-100legacy'}, 'text': '/product 1 P001'}})
            self.assertTrue(response.json['ignored'])
            action.assert_not_called()
            client.post('/telegram/webhook', json={'message': {'chat': {'id': '-100alerts'}, 'text': '/product 1 P001'}})
            action.assert_called_once()


class TelegramAlertFormatTests(unittest.TestCase):
    def test_timeout_is_short_arabic_with_action_and_reference(self):
        import account_app.app as m
        with patch.object(m, 'get_store_name', return_value='لمسة ستور'):
            text=m.format_telegram_alert('ReadTimeout: HTTPSConnectionPool ' + 'details '*200, review_id=42)
        self.assertIn('انتهت مهلة', text)
        self.assertIn('#42', text)
        self.assertIn('المطلوب:', text)
        self.assertNotIn('HTTPSConnectionPool', text)
        self.assertLess(len(text), 250)

    def test_problem_preview_is_bounded_and_preserves_order_reference(self):
        import account_app.app as m
        with patch.object(m, 'get_store_name', return_value='المتجر'):
            text=m.format_telegram_alert('تأخر التوصيل', order_id=17, customer='زبون', message='وين الطلب؟\n'*100)
        self.assertIn('#17', text)
        self.assertEqual(len(text.splitlines()), 6)
        self.assertLess(len(text), 300)


class BookingDetailTests(unittest.TestCase):
    def test_booking_keeps_weight_size_color_price_notes_and_store(self):
        import account_app.app as m
        items=m.normalize_order_items({'items':[{'product_id':'P1','color':'أسود','size':'44','size_type':'size','weight':'77','notes':'اتصال قبل الوصول'}]},products=[{'product_id':'P1','product_name':'سوت','stock':'متوفر'}])
        items[0]['unit_price']=15000
        text=m.format_order_for_telegram({'store_name':'المتجر المطلوب','items':items,'total_amount':20000,'product_total':15000,'delivery_fee':5000,'notes':'بعد العصر'})
        for expected in ['المتجر: المتجر المطلوب','الوزن: 77','القياس: 44','اللون: أسود','سعر القطعة: 15000','ملاحظات القطعة: اتصال قبل الوصول','ملاحظات: بعد العصر','20000 مع التوصيل']:
            self.assertIn(expected,text)

    def test_size_does_not_invent_weight_and_empty_notes_are_explicit(self):
        import account_app.app as m
        with patch.object(m,'get_store_name',return_value='المتجر'):
            text=m.format_order_for_telegram({'items':[{'product_name':'سوت','size':'44','size_type':'size'}]})
        self.assertIn('الوزن: غير مذكور',text)
        self.assertNotIn('الوزن: 44',text)
        self.assertIn('ملاحظات: لا توجد',text)
