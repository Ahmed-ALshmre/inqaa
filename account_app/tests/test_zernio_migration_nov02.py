"""Exact Meta identity reuse keeps children's existing checkout history intact."""
import copy
import json
import os
import unittest
from unittest.mock import patch

import account_app.app as m
from account_app import zernio
from account_app.tests import test_provider_switch_oct31 as fixtures


class KidsMigrationTests(unittest.TestCase):
    post = fixtures.ProviderSwitchTests.post
    response = fixtures.ProviderSwitchTests.response

    def setUp(self):
        fixtures.ProviderSwitchTests.setUp(self)
        fixtures.ProviderSwitchTests.switch(self)
        self.store_token = m._current_store_id.set('baraah-kids')
        self.sender = 'baraah-kids::cw-1-44'
        self.entry = dict(sender_id=self.sender, account_id=self.facebook,
                          participant_id='1234567890', conversation_id='verified-thread')
        with m.app.app_context():
            m.get_or_create_customer(m.get_db(), self.sender, 'chatwoot-1-4', 'facebook')

    def tearDown(self):
        m._current_store_id.reset(self.store_token)
        fixtures.ProviderSwitchTests.tearDown(self)

    def event(self):
        data = copy.deepcopy(self.payload)
        data['conversation'].update(id='new-thread', participantId=self.entry['participant_id'])
        data['message']['sender']['id'] = self.entry['participant_id']
        return data

    def test_preseed_reuses_history_order_and_ad_context_without_new_customer(self):
        with m.app.app_context():
            db = m.get_db()
            m.save_message(db, self.sender, 'incoming', 'text', 'أريد الأخضر عمر 3 سنين', None, None, None, {})
            db.execute("INSERT INTO orders(sender_id,store_id,product_name,status) VALUES(?,?,?,'new')",
                       (self.sender, 'baraah-kids', 'منتج محفوظ'))
            db.commit()
            before = dict(m.get_latest_customer_order(db, self.sender))
            with patch.dict(os.environ, ZERNIO_BARAAH_KIDS_ENABLED='0'):
                entry = dict(self.entry, ad_context={'meta_ad_id': '123', 'meta_ad_title': 'مصدر الإعلان',
                             'meta_ad_video_url': 'https://cdn.example/ad.mp4', 'ignored_secret': 'unused'})
                self.assertEqual(zernio.seed_kids_bindings(db, [entry]), 1)
            ad = m.ad_attribution.load(db, self.sender)
            self.assertEqual(ad['headline'], 'مصدر الإعلان')
            self.assertEqual(ad['video_url'], 'https://cdn.example/ad.mp4')
            self.assertNotIn('ignored_secret', ad)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM ai_jobs').fetchone()[0], 0)
            sender, account, platform = zernio._binding(m, self.event(), 'baraah-kids')
            self.assertEqual((sender, account, platform), (self.sender, self.facebook, 'facebook'))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM customers').fetchone()[0], 1)
            self.assertEqual(dict(m.get_latest_customer_order(db, sender)), before)
            self.assertIn('الأخضر', m.load_history(db, sender)[0]['text'])
            self.assertEqual(db.execute('SELECT conversation_id FROM zernio_conversations').fetchone()[0], 'new-thread')
            zernio._save_ad(m, sender, self.event(), '2026-11-02T12:00:00+00:00')
            self.assertEqual(m.ad_attribution.load(db, self.sender)['ad_id'], '123')
            with patch.object(zernio.requests, 'post', return_value=self.response()) as send, patch.object(m.chatwoot, 'send') as old:
                self.assertTrue(m._post_manychat_send(sender, [{'type': 'text', 'text': 'متوفر'}])['ok'])
                self.assertIn('/new-thread/messages', send.call_args.args[0])
                old.assert_not_called()

    def test_binding_batch_is_atomic_and_rejects_cross_store_platform_or_identity(self):
        changes = [dict(sender_id='al-fatena::cw-1-44'), dict(account_id='c'*24),
                   dict(account_id=self.instagram), dict(participant_id='guess-by-name'),
                   dict(ad_context=[]), dict(ad_context=None), dict(ad_context={'image_url': 'https://cdn.example/customer.jpg'})]
        with m.app.app_context():
            db = m.get_db()
            for fields in changes:
                with self.subTest(fields=fields), self.assertRaises(ValueError):
                    zernio.seed_kids_bindings(db, [self.entry, dict(self.entry, **fields)])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM zernio_conversations').fetchone()[0], 0)

    def test_retry_is_idempotent_and_cannot_reassign_an_existing_participant(self):
        with m.app.app_context():
            db = m.get_db()
            zernio.seed_kids_bindings(db, [self.entry])
            zernio.seed_kids_bindings(db, [dict(self.entry, conversation_id='updated-thread')])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM zernio_conversations').fetchone()[0], 1)
            other = 'baraah-kids::cw-1-45'
            m.get_or_create_customer(db, other, 'chatwoot-1-4', 'facebook')
            with self.assertRaises(ValueError):
                zernio.seed_kids_bindings(db, [dict(self.entry, sender_id=other)])
            self.assertEqual(db.execute('SELECT sender_id FROM zernio_conversations').fetchone()[0], self.sender)

    def test_same_name_without_exact_participant_mapping_creates_a_separate_customer(self):
        with m.app.app_context():
            db = m.get_db()
            zernio.seed_kids_bindings(db, [self.entry])
            event = self.event()
            event['conversation']['participantId'] = '9876543210'
            event['message']['sender']['id'] = '9876543210'
            sender, _, _ = zernio._binding(m, event, 'baraah-kids')
            self.assertTrue(sender.startswith('baraah-kids::zn-'))
            self.assertNotEqual(sender, self.sender)

    def test_seed_endpoint_is_owner_only_and_uses_verified_batch(self):
        path = '/api/zernio/kids-bindings'
        self.assertEqual(self.client.post(path, json={'bindings': [self.entry]}).status_code, 403)
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        response = self.client.post(path, json={'bindings': [self.entry]})
        self.assertEqual(response.status_code, 200, response.json)
        self.assertEqual(response.json['bound'], 1)
        with m.app.app_context():
            db = m.get_db()
            m.staff_tables(db)
            cursor = db.execute('INSERT INTO staff_accounts(username,name,password_hash,permissions) VALUES(?,?,?,?)',
                                ('migration-staff', 'موظف', 'unused-test-hash', json.dumps(['reply', 'settings', 'orders'])))
            db.commit()
            staff_id = cursor.lastrowid
        with self.client.session_transaction() as session:
            session.update(staff_id=staff_id, staff_version=1, csrf_token='test-token')
        self.assertEqual(self.client.post(path, json={'bindings': [self.entry]},
                                         headers={'X-CSRF-Token': 'test-token'}).status_code, 403)

