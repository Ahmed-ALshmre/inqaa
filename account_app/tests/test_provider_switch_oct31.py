"""Provider swap: Fatena returns to Chatwoot; kids use isolated Zernio accounts."""
import copy
import hashlib
import hmac
import json
import os
import time
from unittest.mock import patch

from account_app import zernio
from account_app.tests import test_zernio as fixtures
import account_app.app as m
import unittest


class ProviderSwitchTests(unittest.TestCase):
    setUp = fixtures.ZernioTests.setUp
    post = fixtures.ZernioTests.post
    response = fixtures.ZernioTests.response

    def tearDown(self):
        if hasattr(self, 'env_switch'):
            self.env_switch.stop()
        fixtures.ZernioTests.tearDown(self)

    def switch(self):
        self.env_switch = patch.dict(os.environ, {
            'ZERNIO_AL_FATENA_ENABLED': '0', 'ZERNIO_BARAAH_KIDS_ENABLED': '1',
            'CHATWOOT_INBOX_STORES': json.dumps({'1:2': 'al-fatena', '1:4': 'baraah-kids'}),
            'ZERNIO_ACCOUNTS': json.dumps({
                self.facebook: {'store_id': 'baraah-kids', 'platform': 'facebook'},
                self.instagram: {'store_id': 'baraah-kids', 'platform': 'instagram'},
                'c' * 24: {'store_id': 'al-fatena', 'platform': 'facebook'},
            }),
        })
        self.env_switch.start()
        with m.app.app_context():
            m.ensure_store(m.get_db(), 'baraah-kids', 'عالم البراءة')

    def chatwoot(self, inbox, conversation):
        raw = json.dumps({'event': 'message_created', 'id': conversation, 'account': {'id': 1},
                          'inbox': {'id': inbox}, 'conversation': {'id': conversation},
                          'message_type': 'incoming', 'content': 'مرحبا'}).encode()
        timestamp = str(int(time.time()))
        signature = 'sha256=' + hmac.new(b'test-cw-secret', timestamp.encode() + b'.' + raw, hashlib.sha256).hexdigest()
        return self.client.post('/chatwoot/webhook', data=raw, content_type='application/json',
                                headers={'X-Chatwoot-Timestamp': timestamp, 'X-Chatwoot-Signature': signature})

    def test_kids_inbox_is_deduplicated_and_outbound_uses_its_account(self):
        self.switch()
        for account, platform in ((self.facebook, 'facebook'), (self.instagram, 'instagram')):
            payload = copy.deepcopy(self.payload)
            payload['account'] = {'id': account, 'platform': platform}
            payload['message']['platform'] = platform
            payload['message']['platformMessageId'] += platform
            self.assertEqual(self.post(payload, path='/zernio/webhook/baraah-kids').status_code, 200)
            self.assertEqual(self.post(payload).status_code, 200)
        with m.app.app_context():
            db = m.get_db()
            rows = db.execute('SELECT * FROM zernio_conversations').fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual({row['store_id'] for row in rows}, {'baraah-kids'})
            self.assertEqual(db.execute('SELECT count(*) FROM messages').fetchone()[0], 2)
            self.assertEqual({row['store_id'] for row in db.execute('SELECT store_id FROM ai_jobs')}, {'baraah-kids'})
            for row in rows:
                with patch.object(zernio.requests, 'post', return_value=self.response()) as send, patch.object(m.chatwoot, 'send') as cw:
                    self.assertTrue(m._post_manychat_send(row['sender_id'], [{'type': 'text', 'text': 'متوفر'}])['ok'])
                    self.assertEqual(send.call_args.kwargs['json']['accountId'], row['account_id'])
                    cw.assert_not_called()

    def test_fatena_chatwoot_resumes_and_kids_chatwoot_is_ignored(self):
        self.switch()
        self.assertEqual(self.chatwoot(4, 44).get_json()['provider'], 'zernio')
        self.assertEqual(self.chatwoot(2, 22).status_code, 200)
        with m.app.app_context():
            self.assertEqual([row['store_id'] for row in m.get_db().execute('SELECT store_id FROM ai_jobs')], ['al-fatena'])
            with patch.object(m.chatwoot, 'send', return_value={'ok': True}) as cw, patch.object(zernio.requests, 'post') as zn:
                self.assertTrue(m._post_manychat_send('al-fatena::cw-1-22', [{'type': 'text', 'text': 'متوفر'}])['ok'])
                cw.assert_called_once()
                zn.assert_not_called()

    def test_wrong_store_path_and_disabled_fatena_do_not_create_records(self):
        self.switch()
        self.assertEqual(self.post(path='/zernio/webhook/al-fatena').status_code, 409)
        with patch.dict(os.environ, ZERNIO_AL_FATENA_ENABLED='1'):
            self.assertEqual(self.post(path='/zernio/webhook/al-fatena').status_code, 422)
        payload = copy.deepcopy(self.payload)
        payload['account'] = {'id': 'c' * 24, 'platform': 'facebook'}
        self.assertEqual(self.post(payload).status_code, 422)
        with m.app.app_context():
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM customers').fetchone()[0], 0)

    def test_store_readiness_and_catalog_are_isolated(self):
        self.switch()
        self.assertFalse(zernio.configuration('al-fatena')['ready'])
        self.assertTrue(zernio.configuration('baraah-kids')['ready'])
        self.post()
        with m.app.app_context():
            db = m.get_db()
            sender = db.execute('SELECT sender_id FROM zernio_conversations').fetchone()[0]
            products = [dict(store_id=store, product_id=pid, product_name='موديل', ad_id='123', price='10000')
                        for store, pid in [('al-fatena', 'F1'), ('baraah-kids', 'K1')]]
            result = zernio.product_event(m, db, {'sender_id': sender, 'text': 'شكد سعره؟'}, products)
            self.assertEqual(result['_ad_source']['product_id'], 'K1')
        with patch.dict(os.environ, ZERNIO_ACCOUNTS='{}'):
            self.assertFalse(zernio.configuration('baraah-kids')['ready'])
            self.assertEqual(self.chatwoot(4, 45).get_json()['provider'], 'zernio')

    def test_account_binding_cannot_send_across_stores(self):
        self.switch()
        self.post()
        with m.app.app_context():
            db = m.get_db()
            sender = db.execute('SELECT sender_id FROM zernio_conversations').fetchone()[0]
            db.execute("UPDATE zernio_conversations SET store_id='al-fatena'")
            db.commit()
            with patch.object(zernio.requests, 'post') as send:
                self.assertEqual(zernio.send(m, sender, [{'type': 'text', 'text': 'test'}])['status'], 'wrong_account')
                send.assert_not_called()
