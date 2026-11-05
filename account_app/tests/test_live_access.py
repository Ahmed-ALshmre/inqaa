"""Read connection authorization, isolation, pagination and media boundaries."""
import hashlib
import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import account_app.app as m
from account_app import live_access
from tools import live_system


class LiveAccessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.old=m.DB_PATH
        m.DB_PATH=str(Path(self.temp.name)/'live.db'); m.init_db()
        self.ctx=m.app.app_context(); self.ctx.push()
        self.db=m.get_db()
        self.owner=m.app.test_client()
        with self.owner.session_transaction() as s:
            s['dashboard_authenticated']=True; s['csrf_token']='test-csrf'
        self.csrf={'X-CSRF-Token':'test-csrf'}
        created=self.owner.post('/api/settings/live-access',json={'label':'Private test connection','days':30},headers=self.csrf)
        self.assertEqual(created.status_code,201,created.json)
        self.key=created.json
        self.reader=m.app.test_client()
        self.headers={'Authorization':'Bearer '+self.key['token']}
        self.add_customer('test-one','خيوط','khuyoot')
        self.add_customer('test-two','أطفال','baraah-kids')

    def tearDown(self):
        self.ctx.pop(); m.DB_PATH=self.old; self.temp.cleanup()

    def add_customer(self,sender,name,store):
        self.db.execute('INSERT INTO customers(sender_id,name,store_id) VALUES(?,?,?)',(sender,name,store)); self.db.commit()

    def message(self,sender,text,store='khuyoot',media='[]'):
        result=self.db.execute('INSERT INTO messages(sender_id,text,store_id,direction,message_type,media_json,raw_payload,created_at) VALUES(?,?,?,?,?,?,?,?)',
            (sender,text,store,'incoming','text',media,'{"api_key":"not exposed"}',m.now_baghdad_iso()))
        self.db.commit(); return result.lastrowid

    def get(self,path,params=None):
        return self.reader.get('/api/live/v1/'+path,query_string=params,headers=self.headers)

    def test_only_owner_can_issue_revoke_or_view_access_logs_and_csrf_is_required(self):
        self.assertEqual(self.reader.get('/api/settings/live-access').status_code,403)
        self.assertEqual(self.owner.post('/api/settings/live-access',json={'label':'bad'}).status_code,403)
        self.assertEqual(self.owner.post('/api/settings/live-access/'+self.key['id']+'/revoke').status_code,403)
        self.owner.post('/api/staff',json=dict(name='Staff',username='agent',password='test-password-123',permissions=['settings','reply']),headers=self.csrf)
        self.owner.get('/logout')
        self.owner.post('/login',data=dict(username='agent',password='test-password-123'))
        for path in ('/settings/live-access','/api/settings/live-access','/api/settings/live-access/audit'):
            self.assertEqual(self.owner.get(path).status_code,403,path)

    def test_token_is_one_time_hash_only_and_never_an_admin_write_credential(self):
        row=self.db.execute('SELECT * FROM live_access_tokens').fetchone()
        self.assertEqual(row['token_hash'],hashlib.sha256(self.key['token'].encode()).hexdigest())
        self.assertNotIn(self.key['token'],self.owner.get('/api/settings/live-access').get_data(as_text=True))
        for path in ('/api/conversations/test-one/send','/api/stores/khuyoot/ai','/api/settings/live-access'):
            self.assertEqual(self.reader.post(path,json=dict(text='Do not send',enabled=False),headers=self.headers).status_code,403,path)
        self.assertEqual(self.reader.post('/api/live/v1/records/orders',json={},headers=self.headers).status_code,405)
        with self.reader.session_transaction() as s:
            self.assertNotIn('dashboard_authenticated',s)

    def test_cookie_or_query_key_cannot_replace_bearer_and_expiry_and_revocation_are_immediate(self):
        path='/api/live/v1/manifest'
        self.assertEqual(self.owner.get(path).status_code,401)
        self.assertEqual(self.reader.get(path,query_string={'key':self.key['token']}).status_code,401)
        self.assertEqual(self.get('manifest').status_code,200)
        self.db.execute('UPDATE live_access_tokens SET expires_at=?',(time.time()-1,)); self.db.commit()
        self.assertEqual(self.get('manifest').status_code,401)
        self.db.execute('UPDATE live_access_tokens SET expires_at=?',(time.time()+999,)); self.db.commit()
        self.owner.post('/api/settings/live-access/'+self.key['id']+'/revoke',json={},headers=self.csrf)
        self.assertEqual(self.get('manifest').status_code,401)

    def test_pagination_keeps_upper_bound_does_not_drop_rows_or_include_new_inserts(self):
        ids=[self.message('test-one',str(i)) for i in range(5)]
        for order in ('asc','desc'):
            params=dict(sender_id='test-one',order=order,limit=2)
            page=self.get('records/messages',params).json
            expected=ids if order=='asc' else ids[::-1]
            self.assertEqual([r['id'] for r in page['items']],expected[:2])
            added=self.message('test-one','new insertion')
            found=[r['id'] for r in page['items']]
            upper=page['upper_rowid']
            while page['has_more']:
                params.update(cursor=page['next_cursor'],upper_rowid=upper)
                page=self.get('records/messages',params).json
                found.extend(r['id'] for r in page['items'])
            self.assertEqual(found,expected)
            self.db.execute('DELETE FROM messages WHERE id=?',(added,)); self.db.commit()

    def test_store_filter_works_for_direct_columns_and_customer_join_with_encoded_sender(self):
        self.message('test-one','one'); self.message('test-two','two','baraah-kids')
        self.db.execute("INSERT INTO customer_ai_settings(sender_id,enabled) VALUES('test-one',0)")
        self.db.execute("INSERT INTO customer_ai_settings(sender_id,enabled) VALUES('test-two',1)"); self.db.commit()
        for resource in ('conversations','messages','conversation_ai'):
            result=self.get('records/'+resource,dict(store_id='khuyoot')).json
            self.assertEqual({r['sender_id'] for r in result['items']},{'test-one'})
        self.assertEqual(len(self.get('records/conversations',dict(q='خيوط')).json['items']),1)
        self.assertEqual(self.get('records/conversations',dict(q="%' OR 1=1--")).json['items'],[])

    def test_only_allowlisted_fields_and_resources_are_read_and_nested_secrets_are_redacted(self):
        self.db.execute("ALTER TABLE messages ADD COLUMN private_secret TEXT DEFAULT 'never-export'")
        self.message('test-one','hello',media=json.dumps([dict(type='image',url='https://images.test/a.jpg',access_token='sensitive',prompt_tokens=20)]))
        result=self.get('records/messages').json
        row=result['items'][0]
        self.assertNotIn('raw_payload',row); self.assertNotIn('private_secret',row)
        self.assertEqual(json.loads(row['media_json'])[0]['access_token'],'[redacted]')
        self.assertEqual(json.loads(row['media_json'])[0]['prompt_tokens'],20)
        for resource in ('app_settings','live_access_tokens','staff_accounts','sqlite_master','messages; DROP TABLE orders'):
            self.assertEqual(self.get('records/'+resource).status_code,404)
        for params in (dict(limit=0),dict(limit=201),dict(cursor='1 OR 1'),dict(order='desc;delete'),dict(limit=-1)):
            self.assertEqual(self.get('records/messages',params).status_code,400)

    def test_current_catalog_all_stores_and_settings_do_not_expose_provider_credentials(self):
        products=[dict(store_id='khuyoot',product_id='P008',category='طقم',ad_ids=['AD1','AD2'],image_urls=['https://images.test/a.jpg'],api_key='never')]
        with patch.object(m,'load_products_from_file',return_value=products):
            result=self.get('products').json['items'][0]
        self.assertEqual(result['ad_ids'],['AD1','AD2']); self.assertNotIn('api_key',result)
        m.set_store_setting(self.db,'OPENROUTER_API_KEY','never','khuyoot')
        m.set_store_setting(self.db,'ai_main_max_tokens','900','khuyoot')
        m.set_store_setting(self.db,'ai_enabled','0','khuyoot')
        settings=self.get('settings',dict(store_id='khuyoot')).json['items'][0]
        self.assertEqual(settings['values']['ai_main_max_tokens'],'900')
        self.assertEqual(settings['values']['ai_enabled'],'0')
        self.assertNotIn('OPENROUTER_API_KEY',settings['values'])
        self.assertNotIn('webhook_key',self.get('stores').get_data(as_text=True))

    def test_inbox_reads_latest_activity_without_modifying_ai_reviews_or_orders(self):
        self.message('test-two','earlier','baraah-kids')
        self.message('test-one','latest')
        before=self.db.total_changes
        result=self.get('inbox',dict(limit=1)).json
        self.assertEqual(result['conversations'][0]['sender_id'],'test-one')
        self.assertTrue(result['has_more'])
        # Only access metadata is written: no customer state changes.
        self.assertEqual(self.db.total_changes-before,2)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0],0)

    def test_media_reads_only_archived_evidence_and_does_not_fetch_urls_or_allow_path_escape(self):
        root=Path(self.temp.name)/'media';root.mkdir()
        filename='a'*32+'.jpg'; (root/filename).write_bytes(b'private image bytes')
        media=json.dumps([dict(type='image',url='https://images.test/a.jpg',local_file=filename)])
        mid=self.message('test-one','photo',media=media)
        with patch.object(m,'INCOMING_MEDIA_DIR',str(root)),patch.object(m.inbound_media,'fetch') as fetch:
            good=self.get(f'media/{mid}/0')
            self.assertEqual(good.data,b'private image bytes'); good.close()
            self.assertEqual(self.get(f'media/{mid}/99').status_code,404)
            self.db.execute('UPDATE messages SET media_json=? WHERE id=?',(media.replace(filename,'../live.db'),mid)); self.db.commit()
            self.assertEqual(self.get(f'media/{mid}/0').status_code,404)
            fetch.assert_not_called()
        self.assertEqual(self.reader.get(f'/api/live/v1/media/{mid}/0').status_code,401)

    def test_access_audit_and_no_cache_headers_do_not_include_keys_or_customer_contents(self):
        self.message('test-one','private customer question')
        response=self.get('records/messages')
        self.assertEqual(response.headers['Cache-Control'],'no-store')
        audit=self.owner.get('/api/settings/live-access/audit').get_data(as_text=True)
        self.assertNotIn(self.key['token'],audit); self.assertNotIn('private customer question',audit)
        self.assertIn('/api/live/v1/records/messages',audit)
        self.db.executemany('INSERT INTO live_access_audit(token_id,path,status,created_at) VALUES(?,?,200,?)',
            [(self.key['id'],'test',time.time())]*120); self.db.commit()
        self.assertEqual(self.get('manifest').status_code,429)

    def test_exported_database_cannot_restore_old_access_keys(self):
        data=m._backup_sqlite_file_to_bytes(m.DB_PATH)
        target=Path(self.temp.name)/'copy.db';target.write_bytes(data.read())
        db=sqlite3.connect(target)
        try:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM live_access_tokens').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM customers').fetchone()[0],2)
        finally:
            db.close()
        self.assertEqual(self.get('manifest').status_code,200)

    def test_locally_saved_connection_is_protected_and_client_refuses_redirect_and_foreign_paths(self):
        path=Path(self.temp.name)/'connection.json'
        live_system.save_profile(self.key['token'],self.key['id'],self.key['expires_at'],path=path)
        if os.name=='nt':
            self.assertNotIn(self.key['token'],path.read_text(encoding='utf8'))
        client=live_system.Client(path)
        self.assertEqual(client.session.headers['Authorization'],self.headers['Authorization'])
        with patch.object(client.session,'get') as get:
            for url in ('https://evil.test/','//evil.test/api/live/v1/x','/api/settings/ai'):
                with self.assertRaises(ValueError): client.request(url)
            get.assert_not_called()
            get.return_value.status_code=302
            with self.assertRaises(RuntimeError): client.get('manifest')
            self.assertFalse(get.call_args.kwargs['allow_redirects'])
