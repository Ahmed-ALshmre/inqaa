import unittest
from unittest.mock import patch
from account_app import app as m
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
