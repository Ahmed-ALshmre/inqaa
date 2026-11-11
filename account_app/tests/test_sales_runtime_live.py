"""Exercise the production queue with signed test events and simulated providers."""
import json
import os
import unittest
from unittest.mock import Mock, patch

from account_app import app as m
from account_app.sales_runtime import live
from account_app.sales_runtime.gateway import BACKUP_MODEL
from account_app.sales_runtime.orders import CONFIRMATION
from account_app.tests import test_chatwoot as fixtures


class LiveSalesRuntimeTests(unittest.TestCase):
    sender = 'khuyoot::cw-1-3'
    phone = '07701234567'
    address = 'بغداد حي التجربة قرب المدرسة'

    def setUp(self):
        fixtures.ChatwootTests.setUp(self)
        self.addCleanup(fixtures.ChatwootTests.tearDown, self)
        self.start_patch(patch.dict(os.environ, CHATWOOT_INBOX_STORES='{"1:2":"khuyoot"}',
                                    ENABLE_BACKGROUND_JOBS='1', SALES_RUNTIME_KHUYOOT_PERCENT='100'))
        self.start_patch(patch.object(m, 'OPENROUTER_KEY', 'synthetic-test-only'))
        self.start_patch(patch.object(m, 'DEBOUNCE_DELAY', 0))
        self.start_patch(patch.object(m.ai_jobs, 'start'))
        self.start_patch(patch.object(m, 'load_products_from_file', return_value=[dict(
            product_id='P1', store_id='khuyoot', product_name='قميص اختبار', stock='متوفر',
            colors='ماروني، أزرق', sizes='40 إلى 48', price='15000', status='active', ad_ids=['1234'])]))
        self.notify = self.start_patch(patch.object(m, 'send_order_to_telegram', return_value=True))
        self.start_patch(patch.object(m, 'save_booking_to_file'))
        self.transport = self.start_patch(patch.object(m.chatwoot, 'send',
            return_value={'ok': True, 'status_code': 200, 'provider_id': 'synthetic-receipt'}))
        self.payload['conversation']['additional_attributes'] = {'referral': {'ad_id': '1234'}}
        with self.client.session_transaction() as session:
            session.update(dashboard_authenticated=True, csrf_token='test-csrf')

    post = fixtures.ChatwootTests.post

    def start_patch(self, patcher):
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def incoming(self, mid, text):
        response = self.post(dict(self.payload, id=mid, content=text))
        self.assertEqual(200, response.status_code, response.get_json())

    def work(self):
        with m.app.app_context():
            db = m.get_db()
            db.execute("UPDATE ai_jobs SET available_at=0 WHERE status='queued'")
            db.commit()
            job = m.ai_jobs.claim(db, 'synthetic-worker')
            if job:
                result = m.run_ai_job(job, json.loads(job['payload']))
                m.ai_jobs.finish_result(db, job['id'], result)
                return job['kind'], result
        return None

    def model_plan(self, *, intent='buy', color='أزرق'):
        return dict(intent=intent, changes=[dict(values=dict(product_id='P1', size='44', color=color))],
                    contact={'phone': self.phone, 'address': self.address})

    @staticmethod
    def model_response(plan, status=200):
        response = Mock(status_code=status)
        response.json.return_value = {'choices': [{'message': {'parsed': plan}}]}
        return response

    def test_signed_burst_books_last_choice_and_records_confirmation_and_fulfillment_once(self):
        for mid, text in enumerate(('أريد ماروني قياس 44', self.phone, 'لا بدلي الماروني أزرق', self.address), 10):
            self.incoming(mid, text)
        self.incoming(13, self.address)
        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan())) as model:
            for _ in range(3):
                self.assertEqual((live.JOB_KIND, {'ok': True, 'reason': 'conversation_changed'}), self.work())
            kind, result = self.work()
        self.assertEqual((live.JOB_KIND, 'booked'), (kind, result.get('action')), result)
        self.assertEqual(1, model.call_count)
        request = json.loads(model.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertIn(self.phone, request['event']['text'])
        self.assertIn('بدلي الماروني أزرق', request['event']['text'])
        self.assertEqual('P1', request['reference']['product_id'])
        self.transport.assert_called_once()
        self.assertEqual(CONFIRMATION, self.transport.call_args.args[2][0]['text'])
        self.notify.assert_called_once()
        with m.app.app_context():
            db = m.get_db()
            order = db.execute('SELECT store_id,color,size,customer_name,phone FROM orders').fetchone()
            self.assertEqual(('khuyoot', 'أزرق', '44', 'Test Customer', self.phone), tuple(order))
            self.assertEqual(1, db.execute('SELECT COUNT(*) FROM menger_deliveries').fetchone()[0])
            self.assertEqual(4, db.execute("SELECT COUNT(*) FROM messages WHERE direction='incoming'").fetchone()[0])
            outgoing = db.execute("SELECT text,raw_payload FROM messages WHERE direction='outgoing'").fetchall()
            self.assertEqual(1, len(outgoing))
            self.assertEqual(CONFIRMATION, outgoing[0]['text'])
            self.assertEqual('accepted', json.loads(outgoing[0]['raw_payload'])['_delivery_status'])
            db.execute("UPDATE ai_jobs SET status='running' WHERE kind=? AND result LIKE '%booked%'", (live.JOB_KIND,))
            db.commit()
            m.ai_jobs.recover_interrupted(db)
        self.assertEqual('already_handled', self.work()[1]['reason'])
        self.transport.assert_called_once()
        self.notify.assert_called_once()

    def test_argon_fails_once_and_leaves_silent_dashboard_review(self):
        self.incoming(10, 'شكد سعره؟')
        failure = self.model_response(None, status=503)
        with patch.object(m.ai_transport, 'post', return_value=failure) as post:
            kind, result = self.work()
        self.assertEqual(live.JOB_KIND, kind)
        self.assertTrue(result.get('needs_human'), result)
        models = [call.kwargs['json']['model'] for call in post.call_args_list]
        self.assertEqual([m.MAIN_MODEL, BACKUP_MODEL], models)
        self.assertTrue(all(call.kwargs['max_attempts'] == 1 for call in post.call_args_list))
        self.transport.assert_not_called()
        with m.app.app_context():
            db = m.get_db()
            self.assertFalse(m.is_customer_ai_enabled(db, self.sender))
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])

    def test_interrupted_worker_sends_committed_booking_without_creating_another_order(self):
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')
        apply = live.Store.apply

        def interrupt_before_turn_marker(store, scope, key, version, command, handler):
            if key.endswith(':done'):
                raise SystemExit('synthetic worker interruption')
            return apply(store, scope, key, version, command, handler)

        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan())) as model:
            with patch.object(live.Store, 'apply', interrupt_before_turn_marker), self.assertRaises(SystemExit):
                self.work()
            self.transport.assert_not_called()
            self.notify.assert_not_called()
            with m.app.app_context():
                db = m.get_db()
                self.assertEqual(1, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
                self.assertEqual(1, db.execute('SELECT COUNT(*) FROM menger_deliveries').fetchone()[0])
                m.ai_jobs.recover_interrupted(db)
            self.assertEqual('booked', self.work()[1]['action'])
            self.assertEqual(1, model.call_count)
        self.transport.assert_called_once()
        self.notify.assert_called_once()
        with m.app.app_context():
            self.assertEqual(1, m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_argon_recovers_booking_and_uncertain_confirmation_is_never_resent(self):
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')
        self.transport.return_value = {'ok': False, 'status': 'transport_error', 'status_code': 0}
        with patch.object(m.ai_transport, 'post', side_effect=[
            self.model_response(None, status=503), self.model_response(self.model_plan()),
        ]) as post:
            kind, result = self.work()
        self.assertEqual(live.JOB_KIND, kind)
        self.assertTrue(result.get('needs_human'), result)
        self.assertEqual([m.MAIN_MODEL, BACKUP_MODEL], [call.kwargs['json']['model'] for call in post.call_args_list])
        self.assertNotIn(self.phone, post.call_args.kwargs['json']['messages'][-1]['content'])
        self.transport.assert_called_once()
        with m.app.app_context():
            db = m.get_db()
            order = db.execute('SELECT id,phone FROM orders').fetchone()
            self.assertEqual(self.phone, order['phone'])
            self.assertEqual('unknown', db.execute("SELECT status FROM sales_v2_order_tasks WHERE kind='confirmation'").fetchone()[0])
        response = self.client.post(f'/api/orders/{order["id"]}/resend_confirmation?store_id=khuyoot',
                                    headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(409, response.status_code, response.get_json())
        self.transport.assert_called_once()

    def test_store_switch_supersedes_inflight_booking_even_if_enabled_again(self):
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')

        def toggle(*args, **kwargs):
            with m.app.app_context():
                db = m.get_db()
                m.set_store_ai_enabled(db, 'khuyoot', False)
                m.set_store_ai_enabled(db, 'khuyoot', True)
            return self.model_response(self.model_plan())

        with patch.object(m.ai_transport, 'post', side_effect=toggle) as post:
            self.assertEqual('conversation_changed', self.work()[1]['reason'])
        self.assertEqual(1, post.call_count)
        self.transport.assert_not_called()
        with m.app.app_context():
            self.assertEqual(0, m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_identity_in_burst_stops_before_provider_and_conversation_switch_resumes_new_messages(self):
        self.incoming(10, 'إنتِ AI؟')
        self.incoming(11, 'زين شكد سعره؟')
        with patch.object(m.ai_transport, 'post') as post:
            self.work()
            self.assertTrue(self.work()[1]['needs_human'])
            self.incoming(12, 'متوفر؟')
            self.assertEqual('ai_disabled', self.work()[1]['reason'])
        post.assert_not_called()
        self.transport.assert_not_called()
        response = self.client.post(f'/api/conversations/{self.sender}/ai?store_id=khuyoot',
                                    json={'enabled': True}, headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(202, response.status_code, response.get_json())
        with m.app.app_context():
            db = m.get_db()
            self.assertTrue(m.is_customer_ai_enabled(db, self.sender))
            self.assertEqual('active', live.Store(db).snapshot(live.scope_for(live.route(db, self.sender)))['data']['control_mode'])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])

    def test_preexisting_conversation_keeps_legacy_queue_after_activation(self):
        with patch.dict(os.environ, SALES_RUNTIME_KHUYOOT_PERCENT='0'):
            self.incoming(10, 'هلو')
        self.incoming(11, 'شكد السعر؟')
        with m.app.app_context():
            db = m.get_db()
            self.assertIsNone(live.route(db, self.sender))
            self.assertEqual(['webhook', 'webhook'], [row[0] for row in db.execute('SELECT kind FROM ai_jobs ORDER BY created_at')])
