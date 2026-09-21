import unittest
from datetime import datetime, timedelta
from unittest.mock import patch
from account_app import app as m, rewards
from account_app.tests import test_staff_access as staff_tests


class RewardTests(unittest.TestCase):
    setUp = staff_tests.StaffAccessTests.setUp
    tearDown = staff_tests.StaffAccessTests.tearDown
    employee = staff_tests.StaffAccessTests.employee

    def review(self, sender, seconds=600):
        db = m.get_db()
        m.get_or_create_customer(db, sender, 'page', 'facebook')
        db.execute("INSERT INTO human_reviews(sender_id,status,created_at) VALUES(?,'pending',?)",
                   (sender,(datetime.now(rewards.BAGHDAD)-timedelta(seconds=seconds)).isoformat()))
        db.commit()

    def close(self, sender):
        return self.client.post(f'/api/conversations/{sender}/mark_reviewed',json={},headers=self.headers)

    def test_credit_speed_repeat_and_privacy(self):
        self.employee()
        self.review('slow',1200)
        self.assertEqual(self.close('slow').status_code,200)
        self.review('fast',60)
        self.assertEqual(self.close('fast').status_code,200)
        self.close('fast')
        result=self.client.get('/api/rewards?staff_id=999').json
        self.assertEqual((result['balance'],result['solved'],result['bonus']),(110,2,10))
        self.assertEqual(result['history'][0]['multiplier'],1.2)
        self.assertEqual(result['today']['solved'],2)
        self.assertEqual(self.client.get('/rewards').status_code,200)

    def test_withdrawal_reserves_balance_and_owner_settles_once(self):
        self.employee()
        self.assertEqual(self.client.post('/api/rewards/withdraw',json={},headers=self.headers).status_code,409)
        self.review('earned'); self.close('earned')
        self.assertEqual(self.client.post('/api/rewards/withdraw',json={}).status_code,403)
        response=self.client.post('/api/rewards/withdraw',json={'amount':999999,'staff_id':999},headers=self.headers)
        self.assertEqual(response.json['amount'],50)
        self.assertEqual(self.client.post('/api/rewards/withdraw',json={},headers=self.headers).status_code,409)
        data=self.client.get('/api/rewards').json
        self.assertEqual((data['available'],data['reserved'],data['paid']),(0,50,0))
        wid=data['withdrawals'][0]['id']
        self.assertEqual(self.client.post(f'/api/rewards/withdrawals/{wid}',json={'status':'paid'},headers=self.headers).status_code,403)
        with self.client.session_transaction() as session:
            session.pop('staff_id',None);session['csrf_token']='test-token'
        self.assertEqual(self.client.post(f'/api/rewards/withdrawals/{wid}',json={'status':'paid'}).status_code,403)
        self.assertEqual(self.client.post(f'/api/rewards/withdrawals/{wid}',json={'status':'paid'},headers={'X-CSRF-Token':'test-token'}).status_code,200)
        self.assertEqual(self.client.post(f'/api/rewards/withdrawals/{wid}',json={'status':'paid'},headers={'X-CSRF-Token':'test-token'}).status_code,409)
        data=self.client.get('/api/rewards').json['employees'][0]
        self.assertEqual((data['balance'],data['available'],data['reserved'],data['paid']),(50,0,0,50))

    def test_rejected_withdrawal_returns_credit(self):
        self.employee();self.review('return');self.close('return')
        self.client.post('/api/rewards/withdraw',json={},headers=self.headers)
        wid=self.client.get('/api/rewards').json['withdrawals'][0]['id']
        with self.client.session_transaction() as session:
            session.pop('staff_id',None);session['csrf_token']='test-token'
        self.assertEqual(self.client.post(f'/api/rewards/withdrawals/{wid}',json={'status':'rejected'},headers={'X-CSRF-Token':'test-token'}).status_code,200)
        data=self.client.get('/api/rewards').json['employees'][0]
        self.assertEqual((data['available'],data['reserved'],data['paid']),(50,0,0))

    def test_owner_bulk_and_unauthenticated_do_not_earn(self):
        self.review('owner')
        self.close('owner')
        self.review('bulk')
        self.client.post('/api/maintenance/close_human_reviews',headers=self.headers)
        result=self.client.get('/api/rewards').json
        self.assertTrue(result['owner'])
        self.assertEqual(result['employees'][0]['balance'],0)
        self.assertIn(m.app.test_client().get('/api/rewards').status_code,[302,401])

    def test_link_earns_once_and_invalid_link_does_not(self):
        self.employee()
        self.review('link')
        with patch.object(m,'find_product_by_id',return_value=None):
            self.assertEqual(self.client.post('/api/conversations/link/link_product',json={'product_id':'bad','silent':True},headers=self.headers).status_code,404)
        self.assertEqual(self.client.get('/api/rewards').json['balance'],0)
        with patch.object(m,'find_product_by_id',return_value={'product_id':'p','product_name':'فستان'}):
            response=self.client.post('/api/conversations/link/link_product',json={'product_id':'p','silent':True},headers=self.headers)
        self.assertEqual(response.status_code,200)
        self.close('link')
        self.assertEqual(self.client.get('/api/rewards').json['balance'],50)

    def test_five_problems_rate_missing_time_and_rollback(self):
        self.employee()
        for i in range(5):
            self.review(str(i),60)
            m.get_db().execute('UPDATE human_reviews SET created_at=NULL WHERE sender_id=?',(str(i),))
            m.get_db().commit()
            self.close(str(i))
        self.assertEqual(self.client.get('/api/rewards').json['balance'],250)
        self.review('rollback')
        db=m.get_db()
        with self.assertRaises(RuntimeError):
            with db:
                db.execute('BEGIN IMMEDIATE')
                rewards.award_pending(db,'rollback',{'id':self.id,'owner':False})
                raise RuntimeError('rollback')
        self.assertEqual(self.client.get('/api/rewards').json['balance'],250)

    def test_async_link_attributes_credit_to_authenticated_actor(self):
        self.employee()
        self.review('async-link')
        with patch.dict('os.environ', {'ENABLE_BACKGROUND_JOBS':'0'}):
            response=self.client.post('/api/conversations/async-link/link_product',json={'product_id':'p','async':True,'_reward_actor':{'id':999}},headers=self.headers)
        self.assertEqual(response.status_code,202)
        import json
        job=dict(m.get_db().execute('SELECT * FROM ai_jobs WHERE id=?',(response.json['job_id'],)).fetchone())
        payload=json.loads(job['payload'])
        self.assertEqual(payload['_reward_actor']['id'],self.id)
        with patch.object(m,'find_product_by_id',return_value={'product_id':'p','product_name':'فستان'}), patch.object(m,'auto_reply_after_product_link',return_value={'sent':True}):
            m.run_ai_job(job,payload)
        self.assertEqual(self.client.get('/api/rewards').json['balance'],50)
