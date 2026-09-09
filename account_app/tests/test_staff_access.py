import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import account_app.app as m
from account_app.staff_access import tables, required_permission


class StaffAccessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old = m.DB_PATH; m.DB_PATH = str(Path(self.temp.name) / 'staff.db'); m.init_db()
        self.ctx = m.app.app_context(); self.ctx.push()
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True; session['csrf_token'] = 'test-token'
        self.headers = {'X-CSRF-Token': 'test-token'}
        self.data = dict(name='موظف تجريبي', username='agent', password='test-password-123', permissions=['reply'], active=True)
        response = self.client.post('/api/staff', json=self.data, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.id = self.client.get('/api/staff').get_json()['accounts'][0]['id']

    def tearDown(self):
        self.ctx.pop(); m.DB_PATH = self.old; self.temp.cleanup()

    def employee(self):
        self.client.get('/logout')
        response = self.client.post('/login', data={'username':'agent', 'password':'test-password-123'})
        self.assertEqual(response.status_code, 302)
        self.client.get('/dashboard')
        with self.client.session_transaction() as session: self.headers = {'X-CSRF-Token':session['csrf_token']}

    def test_password_hash_and_owner_only_management(self):
        row = m.get_db().execute('SELECT password_hash FROM staff_accounts').fetchone()
        self.assertNotEqual(row['password_hash'], self.data['password'])
        self.assertNotIn('password_hash', self.client.get('/api/staff').get_data(as_text=True))
        self.employee()
        for path in ['/api/staff','/settings/staff','/api/settings/ai','/api/export/full-backup','/settings/maintenance','/advisor']:
            self.assertEqual(self.client.get(path).status_code, 403, path)

    def test_revocation_invalidates_existing_session(self):
        self.employee()
        m.get_db().execute('UPDATE staff_accounts SET active=0 WHERE id=?',(self.id,)); m.get_db().commit()
        self.assertEqual(self.client.get('/api/conversations').status_code,401)

    def test_orders_permission_separate_from_reply(self):
        self.employee()
        self.assertEqual(self.client.post('/api/conversations/test/create_order',json={},headers=self.headers).status_code,403)
        self.assertEqual(self.client.delete('/api/conversations/test',headers=self.headers).status_code,403)
        self.assertEqual(self.client.get('/api/products').status_code,200)
        for store in self.client.get('/api/stores').get_json()['stores']:
            self.assertNotIn('webhook_key',store)

    def test_staff_csrf_and_server_permission_enforcement(self):
        self.employee()
        self.assertEqual(self.client.post('/api/conversations/test/send',json={'text':'hello'}).status_code,403)
        with patch.object(m, 'apply_dashboard_ai_checkout', return_value=('hello',{},None)), patch.object(m, 'send_reply_via_manychat',return_value=True):
            # A reply permission cannot be promoted via a supplied JSON field.
            self.assertEqual(self.client.put('/api/staff/'+str(self.id),json=dict(self.data,permissions=['settings']),headers=self.headers).status_code,403)

    def test_reply_permission_cannot_create_order_through_ai_draft(self):
        self.employee()
        def checkout(db, sender, text):
            return m.create_order_if_valid(db, sender, {'order': {}}, None)
        with patch.object(m, 'apply_dashboard_ai_checkout', side_effect=checkout), patch.object(m, 'send_text_via_manychat_detailed') as send:
            response=self.client.post('/api/conversations/test/send',json={'text':'رد مقترح'},headers=self.headers)
        self.assertEqual(response.status_code,403)
        send.assert_not_called()
        self.assertEqual(m.get_db().execute('SELECT count(*) FROM orders').fetchone()[0],0)

    def test_permitted_customer_edit_is_audited(self):
        m.get_or_create_customer(m.get_db(),'test-staff-customer','page','facebook')
        self.employee()
        response=self.client.post('/api/conversations/test-staff-customer/customer',json={'name':'اسم مصحح'},headers=self.headers)
        self.assertEqual(response.status_code,200)
        row=m.get_db().execute('SELECT staff_id,method,path FROM staff_audit ORDER BY id DESC').fetchone()
        self.assertEqual(row['staff_id'],self.id)
        self.assertEqual(row['method'],'POST')

    def test_owner_csrf_duplicate_and_invalid_permissions(self):
        self.assertEqual(self.client.post('/api/staff',json=self.data).status_code,403)
        self.assertEqual(self.client.post('/api/staff',json=self.data,headers=self.headers).status_code,409)
        self.assertEqual(self.client.post('/api/staff',json=dict(self.data,username='other',permissions=['owner']),headers=self.headers).status_code,400)

    def test_permission_update_revokes_session_and_keeps_password(self):
        employee = m.app.test_client()
        employee.post('/login',data={'username':'agent','password':'test-password-123'})
        response=self.client.put('/api/staff/'+str(self.id),json=dict(self.data,password='',permissions=['orders']),headers=self.headers)
        self.assertEqual(response.status_code,200)
        self.assertEqual(employee.get('/api/products').status_code,401)
        self.assertEqual(employee.post('/login',data={'username':'agent','password':'test-password-123'}).status_code,302)
        self.assertEqual(employee.get('/orders').status_code,200)

    def test_unknown_endpoints_deny_and_login_lockout(self):
        self.assertEqual(required_permission('/api/future-sensitive','POST'),'owner')
        self.client.get('/logout')
        for _ in range(8): self.client.post('/login',data={'username':'agent','password':'wrong'})
        response=self.client.post('/login',data={'username':'agent','password':'test-password-123'})
        self.assertEqual(response.status_code,200)
        with self.client.session_transaction() as session: self.assertNotIn('staff_id',session)

    def test_settings_and_advisor_templates_render(self):
        for path in ['/dashboard','/settings','/settings/staff','/advisor','/products','/orders']:
            self.assertEqual(self.client.get(path).status_code,200,path)
