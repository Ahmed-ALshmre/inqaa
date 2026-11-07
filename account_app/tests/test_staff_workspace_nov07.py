"""Store-wide manual followup and employee reward controls, no customer sends."""
import json,unittest
from unittest.mock import patch
from account_app import app as m,rewards,staff_access
from account_app.tests import test_staff_access as staff_tests,test_rewards as reward_tests


class StaffWorkspaceTests(unittest.TestCase):
    setUp=staff_tests.StaffAccessTests.setUp
    tearDown=staff_tests.StaffAccessTests.tearDown
    employee=staff_tests.StaffAccessTests.employee
    review=reward_tests.RewardTests.review
    close=reward_tests.RewardTests.close

    def owner(self):
        with self.client.session_transaction() as s:
            s.pop('staff_id',None);s['csrf_token']='owner-token'
        self.headers={'X-CSRF-Token':'owner-token'}

    def test_rewards_toggle_preserves_earned_balance_and_prevents_new_credit(self):
        self.employee();self.review('earned');self.close('earned')
        self.assertEqual(self.client.get('/api/rewards').json['balance'],50)
        self.owner()
        self.assertEqual(self.client.put('/api/staff/'+str(self.id),headers=self.headers,json=dict(self.data,rewards_enabled=False)).status_code,200)
        self.assertFalse(self.client.get('/api/staff').json['accounts'][0]['rewards_enabled'])
        self.employee();self.review('no-credit');self.assertEqual(self.close('no-credit').status_code,200)
        self.assertEqual(self.client.get('/api/rewards').json['balance'],50)
        self.assertEqual(self.client.put('/api/staff/'+str(self.id),headers=self.headers,json=dict(self.data,rewards_enabled=True)).status_code,403)
        self.owner();self.client.put('/api/staff/'+str(self.id),headers=self.headers,json=dict(self.data,rewards_enabled=True))
        self.employee();self.review('new-credit');self.close('new-credit');self.close('no-credit')
        result=self.client.get('/api/rewards').json
        self.assertEqual(result['solved'],2)
        self.assertIn(result['balance'],(100,110))  # Existing speed bonus remains applicable.
        self.assertEqual(result['balance'],sum(row['amount'] for row in result['history']))

    def test_reward_control_is_strict_and_omitted_updates_keep_previous_value(self):
        for value in ('false',0,None):
            self.assertEqual(self.client.put('/api/staff/'+str(self.id),headers=self.headers,json=dict(self.data,rewards_enabled=value)).status_code,400)
        self.client.put('/api/staff/'+str(self.id),headers=self.headers,json=dict(self.data,rewards_enabled=False))
        self.client.put('/api/staff/'+str(self.id),headers=self.headers,json=self.data)
        self.assertFalse(self.client.get('/api/staff').json['accounts'][0]['rewards_enabled'])
        self.assertEqual(self.client.put('/api/staff/'+str(self.id),json=dict(self.data,rewards_enabled=True)).status_code,403)

    def test_cached_actor_cannot_award_after_owner_disables_credit(self):
        self.review('cached')
        db=m.get_db();rewards.tables(db);db.execute('UPDATE staff_accounts SET rewards_enabled=0 WHERE id=?',(self.id,))
        rewards.award_pending(db,'cached',dict(id=self.id,owner=False,rewards_enabled=True));db.commit()
        self.assertEqual(db.execute('SELECT COUNT(*) FROM staff_rewards').fetchone()[0],0)

    def test_manual_followup_persists_and_filters_across_stores_without_sending(self):
        db=m.get_db()
        for sender,store in [('default::first','default'),('khuyoot::second','khuyoot')]:
            m.get_or_create_customer(db,sender,'page','facebook')
            db.execute('UPDATE customers SET store_id=? WHERE sender_id=?',(store,sender))
        db.commit();self.employee()
        path='/api/conversations/default::first/manual_followup'
        self.assertEqual(self.client.post(path,headers=self.headers,json={'enabled':True}).status_code,200)
        rows=self.client.get('/api/conversations?status=followup&store_id=all').json['conversations']
        self.assertEqual([r['sender_id'] for r in rows],['default::first'])
        self.assertEqual(self.client.get('/api/conversations?status=followup&store_id=khuyoot').json['conversations'],[])
        self.assertEqual(self.client.post(path,headers=self.headers,json={'enabled':False}).status_code,200)
        self.assertEqual(self.client.get('/api/conversations?status=followup').json['conversations'],[])
        self.assertEqual(db.execute('SELECT COUNT(*) FROM messages').fetchone()[0],0)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM orders').fetchone()[0],0)

    def test_followup_requires_reply_permission_csrf_and_existing_customer(self):
        m.get_or_create_customer(m.get_db(),'test','page','facebook');self.employee()
        path='/api/conversations/test/manual_followup'
        self.assertEqual(self.client.post(path,json={'enabled':True}).status_code,403)
        self.assertEqual(self.client.post(path,headers=self.headers,json={'enabled':'true'}).status_code,400)
        self.assertEqual(self.client.post('/api/conversations/missing/manual_followup',headers=self.headers,json={'enabled':True}).status_code,404)
        m.get_db().execute('UPDATE staff_accounts SET permissions=? WHERE id=?',('[]',self.id));m.get_db().commit()
        self.assertEqual(self.client.post(path,headers=self.headers,json={'enabled':True}).status_code,403)

    def test_sender_name_is_from_authenticated_employee_and_stays_out_of_customer_text(self):
        self.employee();db=m.get_db();m.get_or_create_customer(db,'named','page','facebook')
        with self.client.application.test_request_context('/api/conversations/named/send',method='POST'):
            m.session.update(dashboard_authenticated=True,staff_id=self.id,staff_version=1)
            m.save_message(db,'named','outgoing','text','رد تجريبي',None,None,None,
                           {'manual_reply':True,'staff_name':'اسم مزيف','staff_id':999})
        response=self.client.get('/api/conversations/named/messages').json['messages'][0]
        self.assertEqual(response['staff_name'],self.data['name'])
        self.assertEqual(response['text'],'رد تجريبي')
        self.assertEqual(response['sender_type'],'staff')
