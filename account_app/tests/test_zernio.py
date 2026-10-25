import copy
import hashlib
import hmac
import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

import account_app.app as m
from account_app import zernio


class ZernioTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old_db = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'zernio.db')
        m.init_db()
        self.facebook, self.instagram = 'a' * 24, 'b' * 24
        self.env = patch.dict(os.environ, {
            'MESSAGING_PROVIDER': 'chatwoot', 'ZERNIO_AL_FATENA_ENABLED': '1',
            'ZERNIO_API_KEY': 'test-zernio-key', 'ZERNIO_WEBHOOK_SECRET': 'test-zernio-secret',
            'ENABLE_BACKGROUND_JOBS': '0', 'CHATWOOT_ACCOUNT_ID': '1',
            'CHATWOOT_WEBHOOK_SECRET': 'test-cw-secret',
            'CHATWOOT_INBOX_STORES': json.dumps({'1:2': 'al-fatena', '1:3': m.DEFAULT_STORE_ID}),
            'ZERNIO_ACCOUNTS': json.dumps({
                self.facebook: {'store_id': 'al-fatena', 'platform': 'facebook'},
                self.instagram: {'store_id': 'al-fatena', 'platform': 'instagram'},
            }),
        })
        self.env.start()
        self.client = m.app.test_client()
        with m.app.app_context():
            m.ensure_store(m.get_db(), 'al-fatena', 'الفاتنة')
        self.payload = {
            'id': 'event-1', 'event': 'message.received', 'timestamp': '2026-10-16T14:00:00Z',
            'account': {'id': self.facebook, 'accountId': self.facebook, 'platform': 'facebook'},
            'conversation': {'id': 'internal-conversation', 'platformConversationId': 'native-1', 'participantId': 'person-1'},
            'message': {'id': 'msg-1', 'platformMessageId': 'native-msg-1', 'platform': 'facebook',
                        'direction': 'incoming', 'text': 'مرحبا', 'sentAt': '2026-10-16T14:00:00Z',
                        'sender': {'id': 'person-1', 'name': 'Test Customer'}, 'attachments': []},
            'metadata': {'referral': {'ad_id': '123', 'source': 'ADS',
                         'ads_context_data': {'ad_title': 'Test ad', 'photo_url': 'https://cdn.example/ad.jpg'}}},
        }

    def tearDown(self):
        self.env.stop()
        m.DB_PATH = self.old_db
        self.temp.cleanup()

    def post(self, data=None, secret='test-zernio-secret', path='/zernio/webhook'):
        raw = json.dumps(self.payload if data is None else data).encode()
        signature = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
        return self.client.post(path, data=raw, content_type='application/json',
                                headers={'X-Zernio-Signature': signature})

    def sender(self):
        with m.app.app_context():
            return m.get_db().execute('SELECT sender_id FROM zernio_conversations LIMIT 1').fetchone()[0]

    def response(self, status=200, body=None):
        result = Mock(status_code=status)
        result.json.return_value = body if body is not None else {'success': True, 'data': {'messageId': 'delivered-1'}}
        return result

    def catalog(self):
        return [dict(store_id='al-fatena', product_id=pid, product_name=name, ad_id=ad,
                     price=price, status='active', stock='متوفر', colors='اسود', sizes='38 إلى 52',
                     fabric='لينن', image_url='') for pid, name, ad, price in [
                         ('F1', 'تنورة سادة', '111', '12000'),
                         ('F2', 'فستان دانتيل', '123', '16000')]]

    def incoming(self, text='شكد سعره؟', *, referral=False):
        payload = copy.deepcopy(self.payload)
        payload['id'] = 'event-' + text
        payload['message']['platformMessageId'] = 'message-' + text
        payload['message']['text'] = text
        if not referral:
            payload.pop('metadata')
        self.assertEqual(self.post(payload).status_code, 200)

    def referral(self):
        payload = copy.deepcopy(self.payload)
        payload.update(event='referral.received', sender=payload['message']['sender'])
        payload.pop('message')
        self.assertEqual(self.post(payload).status_code, 200)

    def process_latest(self, catalog=None):
        catalog = self.catalog() if catalog is None else catalog
        def model(ev, kind, customer, history, products, matched, *args, **kwargs):
            return {'reply': (f"{matched['product_name']} سعره {int(matched['price']):,}."
                              if matched else 'دزيلي صورة الموديل حتى أتأكد إلج.'),
                    'create_order': False, 'order': {}}
        with m.app.app_context(), ExitStack() as stack:
            for target, value in [('load_products_from_file', catalog), ('load_active_products', catalog),
                                  ('is_ai_enabled', True), ('is_customer_ai_enabled', True),
                                  ('is_store_feature_enabled', False), ('send_telegram_message', False),
                                  ('get_auto_product_settings', {'enabled': True, 'product_id': 'F1',
                                                                 'product': catalog[0], 'send_image': False})]:
                stack.enter_context(patch.object(m, target, return_value=value))
            stack.enter_context(patch.object(m, '_call_main_ai_once', side_effect=model))
            db = m.get_db()
            row = db.execute("SELECT payload FROM ai_jobs ORDER BY created_at DESC,rowid DESC LIMIT 1").fetchone()
            payload = json.loads(row['payload'])
            return m.process_webhook(db, payload['fake_body'], use_debounce=False,
                                     incoming_message_id=payload['saved_id'], send_direct_facebook_images=False)

    def test_referral_before_message_selects_ad_product_and_real_catalog_price(self):
        self.referral()
        self.incoming()
        result = self.process_latest()
        self.assertIn('16,000', result['reply'])
        self.assertNotIn('12,000', result['reply'])
        with m.app.app_context():
            binding = m.get_db().execute("SELECT * FROM customer_product_interests WHERE status='active'").fetchone()
            self.assertEqual(binding['product_id'], 'F2')
            self.assertEqual(binding['source'], 'ad_ref')

    def test_referral_after_intake_is_loaded_when_worker_processes_message(self):
        self.incoming()
        self.referral()
        result = self.process_latest()
        self.assertIn('16,000', result['reply'])
        with m.app.app_context():
            self.assertEqual(m.get_db().execute("SELECT count(*) FROM messages WHERE direction='incoming'").fetchone()[0], 1)
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM ai_jobs').fetchone()[0], 1)

    def test_debounce_latest_text_without_referral_keeps_first_message_ad(self):
        self.incoming('مرحبا', referral=True)
        self.incoming()
        self.assertIn('16,000', self.process_latest()['reply'])

    def test_instagram_referral_uses_same_product_resolution(self):
        self.payload['account'].update(id=self.instagram, accountId=self.instagram, platform='instagram')
        self.payload['message']['platform'] = 'instagram'
        self.referral()
        self.incoming()
        self.assertIn('16,000', self.process_latest()['reply'])

    def test_ref_mapping_without_ad_id_resolves_product(self):
        self.payload['metadata'] = {'referral': {'ref': 'dress-campaign'}}
        self.referral()
        self.incoming()
        catalog = self.catalog()
        catalog[1]['ref'] = 'dress-campaign'
        self.assertIn('16,000', self.process_latest(catalog)['reply'])

    def test_fresh_referral_can_replace_older_selection(self):
        self.referral()
        self.incoming()
        catalog = self.catalog()
        with m.app.app_context(), patch.object(m, 'load_products_from_file', return_value=catalog):
            m._current_store_id.set('al-fatena')
            m.complete_customer_product_link(m.get_db(), self.sender(), catalog[0], 'manual', source='manual_admin')
            m.get_db().execute("UPDATE customer_product_interests SET last_seen_at='2026-10-15T12:00:00+03:00'")
            m.get_db().commit()
        self.assertIn('16,000', self.process_latest(catalog)['reply'])

    def test_same_second_customer_selection_precedes_ad_milliseconds(self):
        self.payload['timestamp'] = '2026-10-16T14:00:00.900Z'
        self.referral()
        self.incoming()
        catalog = self.catalog()
        with m.app.app_context(), patch.object(m, 'load_products_from_file', return_value=catalog):
            m._current_store_id.set('al-fatena')
            m.complete_customer_product_link(m.get_db(), self.sender(), catalog[0], 'manual', source='manual_admin')
            m.get_db().execute("UPDATE customer_product_interests SET last_seen_at='2026-10-16T17:00:00+03:00'")
            m.get_db().commit()
        self.assertIn('12,000', self.process_latest(catalog)['reply'])

    def test_explicit_change_and_followup_do_not_return_to_old_ad_product(self):
        self.referral()
        self.incoming()
        self.process_latest()
        self.incoming('اريد تنورة سادة')
        changed = self.process_latest()
        self.assertIn('12,000', changed['reply'])
        self.incoming('شكد سعرها؟')
        followup = self.process_latest()
        self.assertIn('12,000', followup['reply'])
        self.assertNotIn('16,000', followup['reply'])

    def test_unknown_duplicate_and_other_store_ad_never_choose_default_product(self):
        self.referral()
        self.incoming()
        for catalog in [self.catalog()[:1], [self.catalog()[1], dict(self.catalog()[1], product_id='F3')],
                        [self.catalog()[0], dict(self.catalog()[1], store_id='khuyoot')]]:
            with self.subTest(catalog=catalog):
                result = self.process_latest(catalog)
                self.assertTrue(result['meta']['ad_product_unresolved'])
                self.assertIn('الإعلان', result['reply'])
                self.assertNotRegex(result['reply'], r'دزيلي|أرسلي|ارسلي')
                self.assertTrue(result['meta']['human_review_id'])
                self.assertNotIn('غير متوفر', result['reply'])
                self.assertNotIn('12,000', result['reply'])
        with m.app.app_context():
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM customer_product_interests').fetchone()[0], 0)

    def test_rejected_ad_product_is_not_restored_on_next_question(self):
        self.referral()
        self.incoming()
        self.process_latest()
        self.incoming('مو هذا اريد غيره')
        self.process_latest()
        self.incoming('شكد سعره هسه؟')
        self.process_latest()
        with m.app.app_context():
            self.assertEqual(m.get_db().execute("SELECT count(*) FROM customer_product_interests WHERE status='active'").fetchone()[0], 0)

    def test_unavailable_ad_product_does_not_become_available_default(self):
        self.referral()
        self.incoming()
        catalog = self.catalog()
        catalog[1]['stock'] = 'نفذ'
        result = self.process_latest(catalog)
        self.assertNotIn('12,000', result['reply'])
        with m.app.app_context():
            self.assertEqual(m.get_db().execute("SELECT product_id FROM customer_product_interests WHERE status='active'").fetchone()[0], 'F2')

    def test_ad_context_ignores_customer_images_and_unrelated_store(self):
        self.referral()
        self.incoming()
        with m.app.app_context():
            db = m.get_db()
            ev = dict(sender_id=self.sender(), text='شكد سعره؟', image_url='https://images.test/customer.jpg', attachments=[])
            result = zernio.product_event(m, db, ev, self.catalog())
            self.assertFalse(result.get('ad_id'))
            self.assertEqual(result['image_url'], ev['image_url'])
            other = dict(ev, sender_id='khuyoot::other', ad_id='old')
            self.assertIs(zernio.product_event(m, db, other, self.catalog()), other)

    def test_request_for_different_category_does_not_select_advertised_dress(self):
        self.referral()
        self.incoming('اريد بنطلون')
        with m.app.app_context():
            event = dict(sender_id=self.sender(), text='اريد بنطلون')
            selected = zernio.product_event(m, m.get_db(), event, self.catalog())
            self.assertFalse(selected.get('ad_id'))
            self.assertEqual(selected['_ad_source']['resolution'], 'customer_choice')

    def test_unmapped_ad_still_answers_independent_delivery_question(self):
        self.referral()
        self.incoming('شلون الدفع والتوصيل؟')
        result = self.process_latest(self.catalog()[:1])
        self.assertIn('عند الاستلام', result['reply'])
        self.assertNotIn('صورة الموديل', result['reply'])

    def test_direct_ad_creative_can_identify_catalog_product_without_customer_photo(self):
        self.payload['metadata']['referral']['ad_id'] = 'synthetic-unmapped'
        self.payload['metadata']['referral']['ads_context_data']['photo_url'] = (
            'https://scontent.xx.fbcdn.net/synthetic-creative.jpg')
        self.referral()
        self.incoming('شكد سعره؟')
        catalog = self.catalog()
        with patch.object(m, 'ad_creative_product', return_value=(
                catalog[1], {'product_found': True, 'product_id': 'F2'},
                'https://scontent.xx.fbcdn.net/synthetic-creative.jpg')) as recognition:
            result = self.process_latest(catalog)
        self.assertIn('16,000', result['reply'])
        self.assertNotIn('دزيلي صورة', result['reply'])
        recognition.assert_called_once()
        with m.app.app_context():
            binding = m.get_db().execute("SELECT product_id FROM customer_product_interests WHERE status='active'").fetchone()
            self.assertEqual(binding['product_id'], 'F2')

    def test_consistent_recent_image_evidence_suggests_ad_model_then_yes_selects_only_model(self):
        self.referral()
        self.incoming('شكد سعره؟')
        catalog = self.catalog()
        catalog[1]['image_url'] = 'https://images.test/f2.jpg'
        with m.app.app_context(), patch.object(m, 'current_store_id', return_value='al-fatena'):
            db = m.get_db()
            for index in range(6):
                sender = f'al-fatena::historical-{index}'
                db.execute('INSERT INTO conversation_ad_context(sender_id,message_id,data) VALUES(?,?,?)',
                           (sender, 1, json.dumps({'ad_id': 'synthetic-unmapped',
                              'captured_at': '2026-10-16T14:00:00+00:00'})))
                db.execute('''INSERT INTO customer_product_interests
                    (sender_id,product_id,product_name,match_method,confidence,last_seen_at,source,status)
                    VALUES(?,?,?,?,?,?,?,?)''',
                    (sender, 'F2', 'فستان دانتيل', 'image_recognition', 100,
                     '2026-10-16T14:05:00+00:00', 'image_recognition', 'active'))
            db.commit()
            event = {'sender_id': self.sender(), '_ad_source': {'ad_id': 'synthetic-unmapped'},
                     'page_id': 'test', 'platform': 'facebook'}
            candidate = m.historical_ad_candidate(db, event, catalog)
            self.assertEqual(candidate['product_id'], 'F2')
            suggestion = m.saved_ad_candidate_reply(db, event, candidate)
            self.assertIn('إذا هو نفسه', suggestion['reply'])
            self.assertNotRegex(suggestion['reply'], r'دزيلي صورة|أرسلي صورة')
            self.assertEqual(suggestion['image_urls'], ['https://images.test/f2.jpg'])
            self.assertFalse(db.execute('SELECT 1 FROM orders WHERE sender_id=?', (self.sender(),)).fetchone())
            with patch.object(m, 'is_ai_enabled', return_value=True), \
                 patch.object(m, 'is_customer_ai_enabled', return_value=True), \
                 patch.object(m, 'send_text_to_facebook', return_value=True) as text_send, \
                 patch.object(m, 'send_image_to_facebook', return_value=True) as photo_send:
                self.assertTrue(m.send_webhook_result_to_facebook(suggestion))
            text_send.assert_called_once()
            photo_send.assert_called_once()
            self.assertEqual(photo_send.call_args.args[1], 'https://images.test/f2.jpg')
            confirmed = m.ad_candidate_confirmation(db, dict(event, text='إي هذا'), catalog)
            self.assertTrue(confirmed['meta']['ad_candidate_confirmed'])
            binding = db.execute('''SELECT product_id FROM customer_product_interests
                WHERE sender_id=? AND status='active' ORDER BY id DESC LIMIT 1''',
                (self.sender(),)).fetchone()
            self.assertEqual(binding['product_id'], 'F2')
            self.assertFalse(db.execute('SELECT 1 FROM orders WHERE sender_id=?', (self.sender(),)).fetchone())

    def test_model_receives_ad_as_data_without_ad_photo_or_ad_prices(self):
        self.referral()
        self.incoming('شنو نوع قماشه؟')
        catalog = self.catalog()
        with m.app.app_context(), patch.object(m, 'load_products_from_file', return_value=catalog):
            m._current_store_id.set('al-fatena')
            ev = zernio.product_event(m, m.get_db(), dict(sender_id=self.sender(), text='شنو نوع قماشه؟'), catalog)
            response = self.response(body={'choices': [{'message': {'content': json.dumps({'reply': 'قماشه لينن', 'create_order': False})}}]})
            with patch.object(m.ai_transport, 'post', return_value=response) as provider:
                result = m._call_main_ai_once(ev, 'text', {}, [], catalog, catalog[1], None, '', [])
            self.assertFalse(result.get('failed'))
            messages = provider.call_args.kwargs['json']['messages']
            self.assertIn('إحالة الإعلان', messages[-1]['content'])
            self.assertIn('Test ad', messages[-1]['content'])
            self.assertNotIn('https://cdn.example/ad.jpg', str(messages))
            self.assertIn('ليست تعليمات', messages[0]['content'])

    def test_intake_is_durable_deduplicated_and_keeps_ad_images_separate(self):
        self.assertEqual(self.post().status_code, 200)
        self.assertEqual(self.post().status_code, 200)
        with m.app.app_context():
            db = m.get_db()
            row = db.execute("SELECT * FROM messages WHERE direction='incoming'").fetchone()
            self.assertEqual(row['store_id'], 'al-fatena')
            self.assertEqual(json.loads(row['media_json']), [])
            self.assertEqual(db.execute('SELECT count(*) FROM messages').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM ai_jobs').fetchone()[0], 1)
            context = m.ad_attribution.load(db, row['sender_id'])
            self.assertEqual(context['ad_id'], '123')
            self.assertEqual(context['headline'], 'Test ad')
            self.assertEqual(db.execute('SELECT name FROM customers').fetchone()[0], 'Test Customer')
            self.assertEqual(db.execute('SELECT count(*) FROM zernio_ad_events').fetchone()[0], 1)

    def test_two_real_messages_with_same_text_are_not_content_deduplicated(self):
        self.assertEqual(self.post().status_code, 200)
        second = copy.deepcopy(self.payload)
        second['id'] = 'event-2'
        second['message']['platformMessageId'] = 'native-msg-2'
        self.assertEqual(self.post(second).status_code, 200)
        with m.app.app_context():
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM messages').fetchone()[0], 2)

    def test_instagram_and_facebook_same_participant_are_separate(self):
        self.post()
        second = copy.deepcopy(self.payload)
        second['account'].update(id=self.instagram, accountId=self.instagram, platform='instagram')
        second['message']['platform'] = 'instagram'
        self.assertEqual(self.post(second).status_code, 200)
        with m.app.app_context():
            rows = m.get_db().execute('SELECT * FROM zernio_conversations').fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(len({r['sender_id'] for r in rows}), 2)

    def test_signature_missing_secret_and_disabled_migration(self):
        self.assertEqual(self.post(secret='wrong').status_code, 401)
        with patch.dict(os.environ, ZERNIO_WEBHOOK_SECRET=''):
            self.assertEqual(self.post().status_code, 503)
        with patch.dict(os.environ, ZERNIO_AL_FATENA_ENABLED='0'):
            self.assertEqual(self.post().status_code, 409)

    def test_unknown_account_wrong_store_platform_and_sender_are_rejected(self):
        changes = [
            {'account': {'id': 'c' * 24, 'platform': 'facebook'}},
            {'account': {'id': self.facebook, 'platform': 'instagram'}},
            {'account': {'id': self.facebook, 'accountId': self.instagram, 'platform': 'facebook'}},
            {'conversation': {'id': 'conv', 'participantId': 'other-person'}},
            {'conversation': None}, {'message': []}, {'timestamp': 'invalid'}, {'id': ''},
            {'message': dict(self.payload['message'], conversationId='another-conversation')},
            {'message': dict(self.payload['message'], attachments='invalid')},
        ]
        for change in changes:
            with self.subTest(change=change):
                self.assertEqual(self.post(dict(self.payload, **change)).status_code, 422)
        self.assertEqual(self.post(path='/zernio/webhook/khuyoot').status_code, 403)
        self.assertEqual(self.post([]).status_code, 400)

    def test_mapping_cannot_enable_another_store(self):
        with patch.dict(os.environ, ZERNIO_ACCOUNTS=json.dumps({self.facebook: {'store_id': 'khuyoot', 'platform': 'facebook'}})):
            self.assertFalse(zernio.enabled_for_store('khuyoot'))
            self.assertEqual(self.post().status_code, 422)

    def test_enabled_store_with_broken_mapping_never_falls_back_to_chatwoot(self):
        with patch.dict(os.environ, ZERNIO_ACCOUNTS='{}'), m.app.app_context(), patch.object(m.chatwoot, 'send') as cw:
            self.assertTrue(zernio.enabled_for_store('al-fatena'))
            self.assertFalse(zernio.configuration()['ready'])
            self.assertEqual(self.post().status_code, 422)
            self.assertFalse(m._post_manychat_send('al-fatena::cw-1-10', [{'type': 'text', 'text': 'test'}])['ok'])
            cw.assert_not_called()

    def test_message_without_ad_metadata_does_not_invent_attribution(self):
        payload = copy.deepcopy(self.payload)
        payload.pop('metadata')
        self.assertEqual(self.post(payload).status_code, 200)
        with m.app.app_context():
            db = m.get_db()
            self.assertEqual(db.execute('SELECT count(*) FROM messages').fetchone()[0], 1)
            self.assertIsNone(m.ad_attribution.load(db, self.sender()))
            self.assertEqual(db.execute('SELECT count(*) FROM zernio_ad_events').fetchone()[0], 0)

    def test_test_and_outgoing_events_never_trigger_ai(self):
        for payload in [dict(self.payload, test=True), dict(self.payload, event='webhook.test'),
                        dict(self.payload, event='message.sent')]:
            self.assertEqual(self.post(payload).status_code, 200)
        outgoing = copy.deepcopy(self.payload)
        outgoing['message']['direction'] = 'outgoing'
        self.assertEqual(self.post(outgoing).status_code, 200)
        with m.app.app_context():
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM ai_jobs').fetchone()[0], 0)
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM customers').fetchone()[0], 0)

    def test_referral_only_and_standby_preserve_ads_without_ai(self):
        payload = copy.deepcopy(self.payload)
        payload.update(event='referral.received', sender=payload['message']['sender'], referral={'ad_id': '456'})
        payload.pop('message')
        self.assertTrue(self.post(payload).get_json()['referral'])
        self.assertTrue(self.post(payload).get_json()['referral'])
        standby = copy.deepcopy(self.payload)
        standby['id'] = 'standby-2'
        standby['metadata']['standby'] = True
        self.assertTrue(self.post(standby).get_json()['standby'])
        with m.app.app_context():
            db = m.get_db()
            self.assertEqual(db.execute('SELECT count(*) FROM messages').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM ai_jobs').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM zernio_ad_events').fetchone()[0], 2)

    def test_older_referral_is_kept_in_history_but_does_not_replace_latest(self):
        self.post()
        older = copy.deepcopy(self.payload)
        older.update(id='older', event='referral.received', timestamp='2026-10-15T14:00:00Z', referral={'ad_id': 'older-ad'})
        self.post(older)
        with m.app.app_context():
            db = m.get_db()
            self.assertEqual(m.ad_attribution.load(db, self.sender())['ad_id'], '123')
            self.assertEqual(db.execute('SELECT count(*) FROM zernio_ad_events').fetchone()[0], 2)

    def test_old_conversation_snapshot_keeps_its_original_capture_time(self):
        self.post()
        snapshot = copy.deepcopy(self.payload)
        snapshot.update(id='snapshot', timestamp='2026-10-16T16:00:00Z', metadata={
            'meta_ad_id': 'old-ad', 'meta_ad_captured_at': '2026-10-15T14:00:00Z'})
        snapshot['message']['platformMessageId'] = 'snapshot-message'
        self.assertEqual(self.post(snapshot).status_code, 200)
        with m.app.app_context():
            self.assertEqual(m.ad_attribution.load(m.get_db(), self.sender())['ad_id'], '123')

    def test_chatwoot_al_fatena_is_ignored_but_other_stores_still_receive(self):
        import time
        payload = {'event': 'message_created', 'id': 100, 'account': {'id': 1}, 'inbox': {'id': 2},
                   'conversation': {'id': 22}, 'message_type': 'incoming', 'content': 'hello'}
        def post_cw(data):
            raw = json.dumps(data).encode()
            ts = str(int(time.time()))
            signature = 'sha256=' + hmac.new(b'test-cw-secret', ts.encode() + b'.' + raw, hashlib.sha256).hexdigest()
            return self.client.post('/chatwoot/webhook', data=raw, content_type='application/json',
                headers={'X-Chatwoot-Timestamp': ts, 'X-Chatwoot-Signature': signature})
        self.assertEqual(post_cw(payload).get_json()['provider'], 'zernio')
        payload['inbox']['id'] = 3
        self.assertEqual(post_cw(payload).status_code, 200)
        with m.app.app_context():
            jobs = m.get_db().execute('SELECT store_id FROM ai_jobs').fetchall()
            self.assertEqual([row['store_id'] for row in jobs], [m.DEFAULT_STORE_ID])

    def test_outbound_routes_al_fatena_only_and_never_falls_back(self):
        self.post()
        sender = self.sender()
        with m.app.app_context(), patch.object(zernio.requests, 'post', return_value=self.response()) as send, patch.object(m.chatwoot, 'send') as cw:
            self.assertTrue(m._post_manychat_send(sender, [{'type': 'text', 'text': 'متوفر'}])['ok'])
            self.assertIn('/internal-conversation/messages', send.call_args.args[0])
            self.assertEqual(send.call_args.kwargs['json'], {'accountId': self.facebook, 'message': 'متوفر'})
            self.assertFalse(send.call_args.kwargs['allow_redirects'])
            cw.assert_not_called()
            result = m._post_manychat_send('al-fatena::cw-1-10', [{'type': 'text', 'text': 'test'}])
            self.assertEqual(result['status'], 'unmapped_conversation')
            cw.assert_not_called()
            m._post_manychat_send(f'{m.DEFAULT_STORE_ID}::cw-1-11', [{'type': 'text', 'text': 'test'}])
            cw.assert_called_once()

    def test_job_retries_share_idempotency_key_and_partial_success_is_failure(self):
        self.post()
        sender = self.sender()
        token = m.ai_jobs.active_job.set('job-1')
        try:
            with m.app.app_context(), patch.object(zernio.requests, 'post', return_value=self.response()) as send:
                for _ in range(2):
                    self.assertTrue(zernio.send(m, sender, [{'type': 'text', 'text': 'reply'}])['ok'])
                self.assertEqual(send.call_args_list[0].kwargs['headers']['Idempotency-Key'], send.call_args_list[1].kwargs['headers']['Idempotency-Key'])
        finally:
            m.ai_jobs.active_job.reset(token)
        for status, body in [(500, {}), (200, {}), (200, {'success': True, 'data': {'messageId': '1', 'partialFailure': {'part': 'text'}}})]:
            with m.app.app_context(), patch.object(zernio.requests, 'post', return_value=self.response(status, body)) as send:
                self.assertFalse(zernio.send(m, sender, [{'type': 'text', 'text': 'one'}, {'type': 'text', 'text': 'two'}])['ok'])
                self.assertEqual(send.call_count, 1)

    def test_missing_key_never_sends(self):
        self.post()
        with m.app.app_context(), patch.dict(os.environ, ZERNIO_API_KEY=''), patch.object(zernio.requests, 'post') as send:
            self.assertEqual(zernio.send(m, self.sender(), [{'type': 'text', 'text': 'reply'}])['status'], 'missing_key')
            send.assert_not_called()

    def test_catalog_image_delivery_and_external_image_rejection(self):
        self.post()
        folder = Path(self.temp.name) / 'product_image'
        folder.mkdir()
        (folder / 'test.png').write_bytes(b'image-fixture')
        sender = self.sender()
        with m.app.app_context(), patch.object(m, 'PUBLIC_URL', 'https://store.example'), patch.object(m, 'PRODUCT_IMAGE_DIR', str(folder)), patch.object(zernio.requests, 'post', return_value=self.response()) as send:
            good = zernio.send(m, sender, [{'type': 'image', 'url': 'https://store.example/product_image/test.png'}])
            self.assertTrue(good['ok'])
            self.assertEqual(send.call_args.kwargs['json']['attachmentType'], 'image')
            send.reset_mock()
            self.assertFalse(zernio.send(m, sender, [{'type': 'image', 'url': 'https://other.example/customer.jpg'}])['ok'])
            self.assertEqual(zernio.send(m, sender, [{'type': 'image', 'url': 'https://store.example/product_image/../../outside.png'}])['status'], 'invalid_image')
            send.assert_not_called()

    def test_recorded_audio_delivery_uses_audio_attachment(self):
        self.post()
        sender = self.sender()
        folder = Path(self.temp.name) / 'voices'
        folder.mkdir()
        name = 'a' * 64 + '.ogg'
        path = folder / name
        path.write_bytes(b'OggS123')
        with m.app.app_context(), patch.object(m, 'PUBLIC_URL', 'https://store.example'), \
             patch.object(m, 'OUTGOING_AUDIO_DIR', str(folder)), \
             patch.object(zernio.requests, 'post', return_value=self.response()) as send:
            result = zernio.send(m, sender, [{'type': 'audio',
                'url': f'https://store.example/outgoing_audio/{name}',
                'local_path': str(path), 'mime_type': 'audio/ogg'}])
            self.assertTrue(result['ok'])
            self.assertEqual(send.call_args.kwargs['json']['attachmentType'], 'audio')
            self.assertEqual(send.call_args.kwargs['json']['attachmentUrl'], f'https://store.example/outgoing_audio/{name}')

    def test_dashboard_shows_ad_context_and_diagnostics_do_not_leak_secrets(self):
        self.post()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        response = self.client.get('/api/zernio/diag')
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('test-zernio-key', response.text)
        self.assertNotIn('test-zernio-secret', response.text)
        data = self.client.get('/api/conversations/' + self.sender() + '/messages').get_json()
        self.assertEqual(data['ad_context']['ad_id'], '123')
        self.assertEqual(data['messages'][0]['ad_id'], '123')
        self.assertEqual(data['messages'][0]['media'], [])


class ZernioAdShapesTests(unittest.TestCase):
    def test_zernio_metadata_referral_and_conversation_snapshot(self):
        ad = m.ad_attribution
        self.assertEqual(ad.extract({'metadata': {'referral': {'ad_id': '1', 'ads_context_data': {'ad_title': 'Title'}}}})['headline'], 'Title')
        self.assertEqual(ad.extract({'metadata': {'meta_ad_id': '2', 'meta_ad_title': 'Snapshot'}})['ad_id'], '2')
        self.assertEqual(ad.extract({'conversation': {'metadata': {'meta_ad_id': '3'}}})['ad_id'], '3')
        self.assertEqual(ad.extract({'metadata': {'meta_ad_id': 'old', 'referral': {'ad_id': 'new'}}})['ad_id'], 'new')
        self.assertEqual(ad.extract({'metadata': {'image_url': 'https://customer.example/photo.jpg'}}), {})
