import io
import json
import os
import secrets
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from account_app import app as m


class ConversationControlsVoiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ctx = m.app.app_context(); self.ctx.push()
        self.addCleanup(self.ctx.pop)
        self.store_token = m._current_store_id.set('default')
        self.addCleanup(m._current_store_id.reset, self.store_token)
        self.db = m.get_db()
        self.sender = f'default::cw-1-{secrets.randbelow(1_000_000_000) + 1}'
        m.get_or_create_customer(self.db, self.sender, 'page')
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        self.audio_dir = patch.object(m, 'OUTGOING_AUDIO_DIR', str(Path(self.temp.name) / 'voices'))
        self.audio_dir.start(); self.addCleanup(self.audio_dir.stop)

    def test_switch_stays_off_after_automatic_resume_and_delayed_send(self):
        off = self.client.post(f'/api/conversations/{self.sender}/ai', json={'enabled': False})
        self.assertTrue(off.json['ok']); self.assertFalse(off.json['ai_enabled'])
        self.assertFalse(m.set_customer_ai_enabled(self.db, self.sender, True))
        self.assertFalse(m.is_customer_ai_enabled(self.db, self.sender))
        with patch.object(m, 'call_main_ai') as model, patch.object(m, 'send_text_to_facebook') as send:
            linked = m.auto_reply_after_product_link(self.db, self.sender,
                {'product_id': 'A', 'product_name': 'قطعة'}, staff_action=True)
            self.assertFalse(linked['sent'])
            self.assertFalse(m.send_webhook_result_to_facebook({'sender_id': self.sender, 'reply': 'رد قديم'}))
            model.assert_not_called(); send.assert_not_called()
        with patch.object(m, 'load_customer_products', return_value=[]):
            on = self.client.post(f'/api/conversations/{self.sender}/ai', json={'enabled': True})
        self.assertTrue(on.json['ai_enabled'])
        self.assertTrue(m.is_customer_ai_enabled(self.db, self.sender))

    def test_switch_during_first_reply_generation_saves_and_sends_nothing(self):
        ev = dict(sender_id=self.sender, store_id='default', page_id='page',
                  platform='facebook', text='مرحبا', image_url=None, attachments=[],
                  ad_id=None, ref=None, timestamp=0, postback=None, quick_reply=None,
                  referral_source=None, referral_type=None)
        def stop_during_generation(*_args, **_kwargs):
            response = self.client.post(f'/api/conversations/{self.sender}/ai', json={'enabled': False})
            self.assertEqual(response.status_code, 200)
            return 'رد لا يجب إرساله', 'female'
        with patch.object(m, 'extract_facebook_event', return_value=ev), \
             patch.object(m, 'is_ai_enabled', return_value=True), \
             patch.object(m, 'load_active_products', return_value=[]), \
             patch.object(m, 'should_use_auto_product', return_value=False), \
             patch.object(m, 'generate_first_message_reply', side_effect=stop_during_generation), \
             patch.object(m, 'send_text_to_facebook') as send:
            result = m.process_webhook(self.db, {}, use_debounce=False)
            self.assertEqual(result['reply'], '')
            self.assertEqual(result['meta']['reason'], 'ai_disabled_during_generation')
            self.assertFalse(m.send_webhook_result_to_facebook(result, self.sender))
            send.assert_not_called()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM messages WHERE sender_id=? AND direction='outgoing'", (self.sender,)).fetchone()[0], 0)

    def test_automatic_product_image_does_not_send_while_switch_is_off(self):
        self.client.post(f'/api/conversations/{self.sender}/ai', json={'enabled': False})
        product = {'product_id': 'A', 'product_name': 'قطعة'}
        with patch.object(m, 'is_ai_enabled', return_value=True), \
             patch.object(m, 'get_auto_product_settings', return_value={'send_image': True}), \
             patch.object(m, 'product_image_urls', return_value=['https://shop.test/a.jpg']), \
             patch.object(m, 'send_image_to_facebook') as send:
            self.assertFalse(m.send_product_image_if_available(self.db, self.sender, 'page', 'facebook', product))
            send.assert_not_called()

    def test_outgoing_messages_report_staff_ai_and_legacy_unknown(self):
        m.save_message(self.db, self.sender, 'outgoing', 'text', 'رد موظف', None, None, None,
                       {'manual_reply': True, 'ai_generated': True})
        m.save_message(self.db, self.sender, 'outgoing', 'text', 'رد تلقائي', None, None, None,
                       {'automatic_followup': True})
        self.db.execute("INSERT INTO messages(sender_id,store_id,direction,message_type,text,raw_payload,created_at) VALUES(?,?,'outgoing','text','قديم','{}',?)",
                        (self.sender, 'default', m.now_baghdad_iso())); self.db.commit()
        rows = self.client.get(f'/api/conversations/{self.sender}/messages').json['messages']
        self.assertEqual([(r['sender_type'], r['ai_assisted']) for r in rows],
                         [('staff', True), ('ai', False), ('unknown', False)])

    def test_staff_catalog_does_not_appear_as_ai_reply(self):
        with patch.object(m, 'load_active_products', return_value=[]), \
             patch.object(m, 'build_catalog_image_messages', return_value=[
                 {'type': 'image', 'url': 'https://shop.test/catalog.jpg'}]), \
             patch.object(m, 'send_manychat_messages', return_value=True):
            sent, _, _ = m.send_catalog_to_customer(self.db, self.sender)
        self.assertTrue(sent)
        rows = self.client.get(f'/api/conversations/{self.sender}/messages').json['messages']
        self.assertEqual(rows[-1]['sender_type'], 'staff')

    def test_voice_upload_chatwoot_confirms_then_appears_as_staff_audio(self):
        response = Mock(status_code=200)
        response.json.return_value = {'id': 42}
        env = patch.dict(os.environ, {'MESSAGING_PROVIDER': 'chatwoot',
                                      'CHATWOOT_BASE_URL': 'https://chat.example',
                                      'CHATWOOT_API_TOKEN': 'test-token', 'CHATWOOT_ACCOUNT_ID': '1'})
        with env, patch.object(m.chatwoot.requests, 'post', return_value=response) as post:
            sent = self.client.post(f'/api/conversations/{self.sender}/send_audio',
                data={'audio': (io.BytesIO(b'OggS'+b'\0'*100), 'voice.ogg', 'audio/ogg')})
        self.assertEqual(sent.status_code, 200, sent.json)
        self.assertTrue(sent.json['ok'])
        self.assertEqual(post.call_args.kwargs['files']['attachments[]'][2], 'audio/ogg')
        rows = self.client.get(f'/api/conversations/{self.sender}/messages').json['messages']
        self.assertEqual(rows[-1]['sender_type'], 'staff')
        self.assertEqual(rows[-1]['message_type'], 'audio')
        self.assertEqual(rows[-1]['media'][0]['type'], 'audio')
        url = rows[-1]['media'][0]['url']
        fetched = self.client.get(url)
        self.assertEqual(fetched.status_code, 200)
        self.assertTrue(fetched.data.startswith(b'OggS'))
        self.assertEqual(fetched.content_type, 'audio/ogg')
        fetched.close()

    def test_invalid_audio_and_wrong_store_never_contact_provider(self):
        with patch.object(m, 'send_manychat_messages_detailed') as send:
            invalid = self.client.post(f'/api/conversations/{self.sender}/send_audio',
                data={'audio': (io.BytesIO(b'not audio'), 'voice.ogg', 'audio/ogg')})
            self.assertEqual(invalid.status_code, 400)
            foreign = self.client.post('/api/conversations/another-store::cw-1-3/send_audio',
                data={'audio': (io.BytesIO(b'OggS123'), 'voice.ogg', 'audio/ogg')})
            self.assertEqual(foreign.status_code, 404)
            send.assert_not_called()

    def test_manychat_rejects_unsupported_channel_without_saving_audio(self):
        self.db.execute("UPDATE customers SET platform='instagram' WHERE sender_id=?", (self.sender,)); self.db.commit()
        with patch.dict(os.environ, {'MESSAGING_PROVIDER': 'manychat'}), \
             patch.object(m, 'PUBLIC_URL', 'https://shop.test'), \
             patch.object(m, 'send_manychat_messages_detailed') as send:
            result = self.client.post(f'/api/conversations/{self.sender}/send_audio',
                data={'audio': (io.BytesIO(b'OggS123'), 'voice.ogg', 'audio/ogg')})
        self.assertEqual(result.status_code, 409)
        self.assertFalse(list(Path(m.OUTGOING_AUDIO_DIR).glob('*')) if Path(m.OUTGOING_AUDIO_DIR).exists() else [])
        send.assert_not_called()

    def test_manychat_messenger_receives_public_audio_url_without_local_path(self):
        with patch.dict(os.environ, {'MESSAGING_PROVIDER': 'manychat'}), \
             patch.object(m, 'PUBLIC_URL', 'https://shop.test'), \
             patch.object(m, 'send_manychat_messages_detailed', return_value={'ok': True}) as send:
            result = self.client.post(f'/api/conversations/{self.sender}/send_audio',
                data={'audio': (io.BytesIO(b'OggS123'), 'voice.ogg', 'audio/ogg')})
        self.assertEqual(result.status_code, 200, result.json)
        payload = send.call_args.args[1][0]
        self.assertEqual(payload['type'], 'audio')
        self.assertRegex(payload['url'], r'^https://shop\.test/outgoing_audio/[a-f0-9]{64}\.ogg$')
        self.assertNotIn('local_path', payload)
        self.assertEqual(self.client.get(f'/api/conversations/{self.sender}/messages').json['messages'][-1]['sender_type'], 'staff')

    def test_public_audio_path_cannot_traverse_or_guess_short_name(self):
        self.assertEqual(self.client.get('/outgoing_audio/a.ogg').status_code, 404)
        self.assertEqual(self.client.get('/outgoing_audio/../sales.db').status_code, 404)


if __name__ == '__main__':
    unittest.main()
