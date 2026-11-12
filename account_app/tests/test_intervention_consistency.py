import json
from pathlib import Path
import unittest
from unittest.mock import patch
from account_app import app as m
from account_app.sales_runtime.store import Scope, Store
from account_app.tests import test_close_human_attention as fixtures


class InterventionConsistencyTests(unittest.TestCase):
    setUp = fixtures.CloseHumanAttentionTests.setUp
    tearDown = fixtures.CloseHumanAttentionTests.tearDown

    def test_delete_cleans_problems_and_jobs_preserves_orders(self):
        self.db.execute("INSERT INTO orders(sender_id,product_name) VALUES('old','test')")
        m.ai_jobs.enqueue(self.db,'default','old','preview',{'text':'test'},'intervention-test')
        self.db.commit()
        response = self.client.delete('/api/conversations/old')
        self.assertEqual(response.status_code,200)
        for table in ['human_reviews','problem_reports','messages','customers','ai_jobs']:
            self.assertEqual(self.db.execute(f"SELECT count(*) FROM {table} WHERE sender_id='old'").fetchone()[0],0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id='old'").fetchone()[0],1)
        self.assertNotIn('old',[r['sender_id'] for r in self.client.get('/api/problems').json['problems']])

    def test_delete_removes_runtime_and_indirect_records_without_touching_other_threads(self):
        scope = Scope('default', 'chatwoot', '1', 'old')
        other_scope = Scope('al-fatena', 'chatwoot', '1', 'other')
        store = Store(self.db)
        for target in (scope, other_scope):
            event = store.append(target, 'provider-1', kind='message', actor='customer',
                                 payload={'text': 'temporary conversation'})
            updated = store.apply(target, 'remember', store.snapshot(target)['version'],
                                  {'event_id': event['id']},
                                  lambda state: state.update(focus_product_id='P1') or {})
            store.enqueue_reply(target, 'answer', updated['result']['state_version'], [
                {'type': 'text', 'text': 'queued reply'}])
        self.db.execute("""INSERT INTO sales_v2_live_routes(sender_id,store_id,account_id,first_incoming_id)
                           VALUES('old','default','1',1)""")
        msg = self.db.execute("""INSERT INTO messages(sender_id,store_id,direction,message_type)
                                 VALUES('old','default','incoming','audio')""").lastrowid
        self.db.execute("INSERT INTO voice_interpretations VALUES(?,'default','test','[]','now')", (msg,))
        self.db.execute("""INSERT INTO verified_image_memory(store_id,fingerprint,product_id,product_signature,source,source_message_id)
                           VALUES('default','test-hash','P1','test-signature','manual_admin',?)""", (msg,))
        self.db.execute("INSERT INTO customer_tags(sender_id,tag) VALUES('old','test'),('other','keep')")
        job = m.ai_jobs.enqueue(self.db, 'default', 'old', 'preview', {}, 'delete-dependents', commit=False)
        self.db.execute("INSERT INTO ai_deliveries VALUES('delivery-key',?,'accepted',NULL,0)", (job['id'],))
        self.db.execute("INSERT INTO ai_request_events(job_id,created_at) VALUES(?,0)", (job['id'],))
        self.db.execute("INSERT INTO ai_deliveries VALUES('manual-key','manual:test','sent',NULL,0)")
        self.db.execute("""INSERT INTO messages(sender_id,direction,raw_payload)
                           VALUES('old','outgoing','{"delivery_id":"manual:test"}')""")
        order = self.db.execute("INSERT INTO orders(sender_id,product_name) VALUES('old','retained')").lastrowid
        for kind in ('confirmation', 'fulfillment'):
            self.db.execute("INSERT INTO sales_v2_order_tasks(scope,order_id,kind,payload) VALUES(?,?,?,'{}')",
                            (scope.key, order, kind))
        self.db.commit()

        response = self.client.delete('/api/conversations/old')
        self.assertEqual(200, response.status_code, response.get_json())
        for table in ('sales_v2_scopes', 'sales_v2_events', 'sales_v2_operations', 'sales_v2_outbox'):
            self.assertEqual([(other_scope.key,)], [tuple(row) for row in self.db.execute(f'SELECT scope FROM {table}')])
        for table in ('sales_v2_live_routes', 'voice_interpretations', 'verified_image_memory',
                      'ai_deliveries', 'ai_request_events', 'conversation_pause_state'):
            self.assertEqual(0, self.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0], table)
        self.assertEqual('other', self.db.execute('SELECT sender_id FROM customer_tags').fetchone()[0])
        self.assertEqual('fulfillment', self.db.execute('SELECT kind FROM sales_v2_order_tasks').fetchone()[0])
        self.assertEqual(order, self.db.execute('SELECT id FROM orders').fetchone()[0])
        self.assertEqual(200, self.client.delete('/api/conversations/old').status_code)
        store.append(scope, 'provider-2', kind='message', actor='customer', payload={'text': 'new conversation'})
        state = store.snapshot(scope)['data']
        self.assertFalse(state.get('focus_product_id'))
        self.assertEqual([], state['cart'])

    def test_delete_cleans_private_media_and_keeps_shared_media(self):
        root = Path(self.temp.name) / 'incoming'
        root.mkdir()
        private, shared = ('a' * 32 + '.jpg', 'b' * 32 + '.jpg')
        for name in (private, shared):
            (root / name).write_bytes(b'test')
        for sender, files in (('old', (private, shared)), ('other', (shared,))):
            media = [{'url': 'https://test.invalid/fixture.jpg', 'type': 'image', 'local_file': name}
                     for name in files]
            self.db.execute("""INSERT INTO messages(sender_id,direction,message_type,media_json)
                               VALUES(?,'incoming','image',?)""", (sender, json.dumps(media)))
        self.db.commit()
        with patch.object(m, 'INCOMING_MEDIA_DIR', str(root)):
            response = self.client.delete('/api/conversations/old')
        self.assertEqual(200, response.status_code, response.get_json())
        self.assertEqual(0, response.json['media_cleanup_pending'])
        self.assertFalse((root / private).exists())
        self.assertTrue((root / shared).is_file())
        self.assertEqual(0, self.db.execute('SELECT count(*) FROM incoming_media_deletions').fetchone()[0])

    def test_archive_cleanup_retries_after_a_filesystem_failure(self):
        root = Path(self.temp.name)
        file = root / ('a' * 32 + '.jpg')
        file.write_bytes(b'test')
        media = [{'url': 'https://test.invalid/fixture.jpg', 'type': 'image', 'local_file': file.name}]
        self.db.execute("""INSERT INTO messages(sender_id,direction,message_type,media_json)
                           VALUES('old','incoming','image',?)""", (json.dumps(media),))
        self.db.commit()
        with patch.object(m, 'INCOMING_MEDIA_DIR', str(root)), patch.object(Path, 'unlink', side_effect=PermissionError):
            response = self.client.delete('/api/conversations/old')
        self.assertEqual(200, response.status_code, response.get_json())
        self.assertEqual(1, response.json['media_cleanup_pending'])
        self.assertFalse(self.db.execute("SELECT 1 FROM messages WHERE sender_id='old'").fetchone())
        self.assertEqual(file.name, self.db.execute('SELECT filename FROM incoming_media_deletions').fetchone()[0])
        self.assertEqual(0, m.storage_maintenance.finish_incoming_deletions(self.db, root))
        self.assertFalse(file.exists())

    def test_old_orphan_problem_does_not_appear(self):
        self.db.execute("INSERT INTO problem_reports(sender_id,status) VALUES('deleted','open')")
        self.db.commit()
        self.assertNotIn('deleted',[r['sender_id'] for r in self.client.get('/api/problems').json['problems']])

    def test_nontechnical_review_is_silent_but_saved(self):
        with patch.object(m,'send_telegram_message') as send:
            review=m.create_human_review(self.db,{'sender_id':'old'},'تحتاج قياساً فعلياً')
        send.assert_not_called()
        self.assertIsNotNone(review)

    def test_credit_failure_notifies_once_with_simple_reference(self):
        with patch.object(m,'send_telegram_message') as send:
            first=m.create_human_review(self.db,{'sender_id':'old'},'exception: provider_http_402')
            second=m.create_human_review(self.db,{'sender_id':'old'},'exception: provider_http_402')
        self.assertEqual(first,second)
        send.assert_called_once()
        text=send.call_args.args[0]
        self.assertIn('تدخل بشري',text)
        self.assertIn(f'#{first}',text)
        self.assertNotIn('provider_http',text)
        self.assertNotIn('المشكلة',text)

    def test_ordinary_fact_questions_do_not_need_staff(self):
        product={'product_name':'سوت','sizes':'42,44'}
        self.assertFalse(m.sales_strategy.fact_requires_human('وزني 69 شنو قياسي',product))
        self.assertFalse(m.sales_strategy.fact_requires_human('اريد فيديو',product))
        self.assertTrue(m.sales_strategy.fact_requires_human('شكد محيط الصدر',product))

    def test_running_job_prevents_partial_delete(self):
        job=m.ai_jobs.enqueue(self.db,'default','old','preview',{},'running-delete')
        self.db.execute("UPDATE ai_jobs SET status='running' WHERE id=?",(job['id'],))
        self.db.commit()
        response=self.client.delete('/api/conversations/old')
        self.assertEqual(response.status_code,409)
        self.assertIsNotNone(self.db.execute("SELECT 1 FROM customers WHERE sender_id='old'").fetchone())
        self.assertTrue(m.has_pending_human_review(self.db,'old'))
