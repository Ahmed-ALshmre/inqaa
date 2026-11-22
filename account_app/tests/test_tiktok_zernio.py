"""Signed TikTok DMs and transport receipts, using synthetic identities only."""
import copy
import base64
import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from PIL import Image
import requests

from account_app import app as m, inbound_media, zernio, zernio_tiktok
from account_app.tests.test_zernio import ZernioTests as Fixtures
from account_app.tests import test_sales_channel_preview as preview_fixture
from account_app.sales_runtime.preview import install


class TikTokTests(unittest.TestCase):
    post = Fixtures.post
    response = Fixtures.response
    sender = Fixtures.sender

    def setUp(self):
        Fixtures.setUp(self)
        self.addCleanup(Fixtures.tearDown, self)
        self.account = 'c' * 24
        env = patch.dict('os.environ', ZERNIO_TIKTOK_ACCOUNT_ID=self.account, ZERNIO_BARAAH_KIDS_ENABLED='1')
        env.start()
        self.addCleanup(env.stop)
        with m.app.app_context():
            m.ensure_store(m.get_db(), 'baraah-kids', 'متجر اختبار')
        self.payload['account'].update(id=self.account, accountId=self.account, platform='tiktok')
        self.payload['message']['platform'] = 'tiktok'
        self.payload.pop('metadata')
        self.fresh()

    def fresh(self, *, age=0, suffix='1'):
        stamp = (datetime.now(timezone.utc) - timedelta(hours=age)).isoformat()
        self.payload.update(id='event-' + suffix, timestamp=stamp)
        self.payload['message'].update(id='message-' + suffix, platformMessageId='native-' + suffix, sentAt=stamp)

    def receive(self):
        response = self.post(path='/zernio/webhook/baraah-kids')
        self.assertEqual(200, response.status_code, response.get_json())
        return self.sender()

    def send(self, sender, messages=None, *, key='send-1', response=None):
        messages = messages or [dict(type='text', text='متوفر عيوني')]
        with patch.object(zernio.requests, 'post', return_value=response or self.response()) as wire:
            result = zernio.send(m, sender, messages, operation_key=key)
        return result, wire

    def test_signed_dm_preserves_platform_and_existing_account_maps(self):
        with patch.object(m, '_process_manychat_webhook_async') as enqueue:
            sender = self.receive()
        event = m.extract_facebook_event(enqueue.call_args.args[0])
        self.assertEqual(('tiktok', 'baraah-kids', sender), (event['platform'], event['store_id'], event['sender_id']))
        self.assertEqual(('zernio', self.account), enqueue.call_args.kwargs['signed_channel'])
        self.assertEqual({'facebook','instagram','tiktok'}, {x['platform'] for x in zernio.account_bindings().values()})
        with m.app.app_context():
            customer = m.get_db().execute('SELECT platform,store_id FROM customers WHERE sender_id=?',(sender,)).fetchone()
            self.assertEqual(('tiktok','baraah-kids'), tuple(customer))

    def test_real_intake_persists_a_durable_job_without_sending(self):
        sender = self.receive()
        with m.app.app_context():
            row = m.get_db().execute('SELECT kind,payload FROM ai_jobs WHERE sender_id=?',(sender,)).fetchone()
            self.assertEqual('webhook', row['kind'])
            self.assertEqual('tiktok',json.loads(row['payload'])['platform'])

    def test_diagnostic_reports_tiktok_activity_without_secrets(self):
        self.receive()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        response = self.client.get('/api/zernio/diag')
        self.assertEqual(200,response.status_code)
        self.assertEqual(1,response.get_json()['tiktok_activity']['received_messages'])
        self.assertNotIn('test-zernio-key',response.text)

    def test_delete_conversation_removes_tiktok_windows_and_receipts(self):
        sender = self.receive()
        with m.app.app_context():
            self.assertTrue(self.send(sender)[0]['ok'])
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        response = self.client.delete('/api/conversations/' + sender)
        self.assertEqual(200,response.status_code,response.get_json())
        with m.app.app_context():
            for table in ('zernio_tiktok_inbound','zernio_tiktok_sends','zernio_conversations'):
                self.assertEqual(0,m.get_db().execute(f'SELECT COUNT(*) FROM {table} WHERE sender_id=?',(sender,)).fetchone()[0],table)

    def test_text_reply_uses_only_verified_tiktok_account(self):
        sender = self.receive()
        with m.app.app_context():
            result, wire = self.send(sender)
        self.assertTrue(result['ok'], result)
        self.assertEqual(dict(accountId=self.account, message='متوفر عيوني'), wire.call_args.kwargs['json'])
        self.assertFalse(wire.call_args.kwargs['allow_redirects'])

    def test_retry_of_accepted_packet_does_not_send_again(self):
        sender = self.receive()
        with m.app.app_context():
            self.assertTrue(self.send(sender)[0]['ok'])
            result, wire = self.send(sender)
            self.assertTrue(result['ok'])
            wire.assert_not_called()
            self.assertEqual(1, m.get_db().execute('SELECT COUNT(*) FROM zernio_tiktok_sends').fetchone()[0])

    def test_ten_replies_then_new_customer_message_reopens_budget(self):
        sender = self.receive()
        with m.app.app_context():
            for index in range(10):
                self.assertTrue(self.send(sender, key=str(index))[0]['ok'])
            result, wire = self.send(sender, key='eleventh')
            self.assertEqual('tiktok_reply_limit', result['status'])
            wire.assert_not_called()
        old = copy.deepcopy(self.payload)
        self.fresh(suffix='2')
        self.receive()
        self.post(old)  # An old redelivery cannot restore the old window.
        with m.app.app_context():
            self.assertTrue(self.send(sender, key='new-turn')[0]['ok'])
            self.assertEqual('native-2', m.get_db().execute('SELECT incoming_id FROM zernio_tiktok_sends WHERE delivery_key=(SELECT delivery_key FROM zernio_tiktok_sends ORDER BY rowid DESC LIMIT 1)').fetchone()[0])

    def test_expired_and_missing_incoming_window_never_send(self):
        self.fresh(age=49)
        sender = self.receive()
        with m.app.app_context():
            result, wire = self.send(sender)
            self.assertEqual('tiktok_reply_window_closed', result['status'])
            wire.assert_not_called()
            m.get_db().execute('DELETE FROM zernio_tiktok_inbound')
            result, wire = self.send(sender)
            self.assertEqual('tiktok_no_customer_message', result['status'])
            wire.assert_not_called()

    def test_image_and_caption_are_separate_and_webp_is_converted(self):
        sender = self.receive()
        folder = Path(self.temp.name) / 'product_image'
        folder.mkdir()
        for ext in ('png','webp'):
            Image.new('RGB', (10,10), 'blue').save(folder / ('fixture.'+ext))
        with m.app.app_context(), patch.object(m, 'PUBLIC_URL', 'https://store.example'), patch.object(m, 'PRODUCT_IMAGE_DIR', str(folder)):
            result, wire = self.send(sender, [dict(type='image', url='https://store.example/product_image/fixture.png', caption='يا لون عجبج؟')])
            self.assertTrue(result['ok'], result)
            self.assertEqual(2, wire.call_count)
            self.assertNotIn('message', wire.call_args_list[0].kwargs['json'])
            self.assertEqual('يا لون عجبج؟', wire.call_args_list[1].kwargs['json']['message'])
            result, wire = self.send(sender, [dict(type='image',url='https://store.example/product_image/fixture.webp')])
            self.assertTrue(result['ok'],result)
            converted = wire.call_args.kwargs['json']['attachmentUrl'].split('/')[-1]
            self.assertTrue(converted.endswith('.jpg'))
            with Image.open(folder / converted) as image:
                self.assertEqual('JPEG',image.format)
            result, wire = self.send(sender,[dict(type='audio',url='https://store.example/audio.mp3')])
            self.assertFalse(result['ok'])
            wire.assert_not_called()

    def test_oversized_photo_is_compressed_and_oversized_text_is_rejected(self):
        sender = self.receive()
        folder = Path(self.temp.name) / 'product_image'
        folder.mkdir()
        path = folder / 'large.png'
        Image.new('RGB', (10,10), 'blue').save(path)
        with path.open('ab') as out:
            out.write(b'\0' * (3 * 1024 * 1024))
        with m.app.app_context(), patch.object(m,'PUBLIC_URL','https://store.example'), patch.object(m,'PRODUCT_IMAGE_DIR',str(folder)):
            result, wire = self.send(sender,[dict(type='image',url='https://store.example/product_image/large.png')])
            self.assertTrue(result['ok'],result)
            converted = folder / wire.call_args.kwargs['json']['attachmentUrl'].split('/')[-1]
            self.assertLessEqual(converted.stat().st_size,3 * 1024 * 1024)
            result, wire = self.send(sender,[dict(type='text',text='a'*6001)])
            self.assertFalse(result['ok'])
            wire.assert_not_called()

    def test_comments_echoes_and_wrong_signature_do_not_trigger_ai(self):
        for kind in ('comment.received','message.sent'):
            self.payload['event'] = kind
            with patch.object(m,'_process_manychat_webhook_async') as enqueue:
                self.assertTrue(self.post().get_json()['ignored'])
                enqueue.assert_not_called()
        self.payload['event'] = 'message.received'
        self.assertEqual(401,self.post(secret='wrong').status_code)
        self.assertEqual(422,self.post(path='/zernio/webhook/al-fatena').status_code)

    def test_sticker_marker_never_becomes_product_photo(self):
        self.payload['message']['attachments'] = [dict(type='image', tiktokMessageType='emoji', url='https://cdn.example/sticker.png')]
        self.payload['message']['text'] = ''
        with patch.object(m,'_process_manychat_webhook_async') as enqueue:
            self.receive()
        event = m.extract_facebook_event(enqueue.call_args.args[0])
        self.assertTrue(event['_has_sticker'])
        self.assertIsNone(event['image_url'])

    def test_uncertain_delivery_is_not_replayed_automatically(self):
        sender = self.receive()
        with m.app.app_context(), patch.object(zernio.requests,'post',side_effect=requests.Timeout):
            self.assertEqual('transport_error',zernio.send(m,sender,[dict(type='text',text='متوفر')],operation_key='uncertain')['status'])
        with m.app.app_context():
            result, wire = self.send(sender,[dict(type='text',text='متوفر')],key='uncertain')
            self.assertEqual('delivery_uncertain',result['status'])
            wire.assert_not_called()

    def test_definite_rejection_can_be_retried_without_losing_budget(self):
        sender = self.receive()
        with m.app.app_context():
            result, wire = self.send(sender,response=self.response(status=400,body={}))
            self.assertEqual('rejected',result['status'])
            self.assertTrue(self.send(sender)[0]['ok'])
            self.assertEqual(1,m.get_db().execute('SELECT COUNT(*) FROM zernio_tiktok_sends').fetchone()[0])

    def test_unconfirmed_second_packet_does_not_reuse_first_receipt(self):
        sender = self.receive()
        with m.app.app_context(), patch.object(zernio.requests,'post',side_effect=[self.response(),self.response(body={'success':True,'data':{'messageIds':['a','b']}})]):
            result = zernio.send(m,sender,[dict(type='text',text='أول'),dict(type='text',text='ثاني')])
            self.assertEqual('unconfirmed',result['status'])

    def test_disabled_or_conflicting_mapping_fails_closed(self):
        with patch.dict('os.environ', ZERNIO_BARAAH_KIDS_ENABLED='0'):
            self.assertEqual(409,self.post(path='/zernio/webhook/baraah-kids').status_code)
        with patch.dict('os.environ', ZERNIO_ACCOUNTS=json.dumps({self.account:dict(store_id='al-fatena',platform='facebook')})):
            self.assertEqual({},zernio.account_bindings())

    def preview(self, enabled=True):
        image = io.BytesIO()
        Image.new('RGB',(12,12),'blue').save(image,format='PNG')
        data = image.getvalue()
        product = dict(store_id='baraah-kids',product_id='P1',product_name='طقم',price='17000',
                       colors='عنابي، أسود',sizes='40 إلى 48',stock='متوفر',status='active',
                       image_url='data:image/png;base64,'+base64.b64encode(data).decode())
        old_testing = m.app.testing
        m.app.testing = True
        self.addCleanup(setattr,m.app,'testing',old_testing)
        self.addCleanup(m.app.extensions.pop,'sales_runtime_preview',None)
        wire = Mock(return_value=self.response())
        decide = Mock(side_effect=preview_fixture.ChannelPreviewTests.decide_fixture.__get__(self))
        install(m,products=lambda _: [product],delivery_fee=lambda *_:5000,decide=decide,
                writer=preview_fixture.ChannelPreviewTests.write,
                fetch_image=lambda *_: (data,'image','.png','image/png'),
                match_image=lambda *_: dict(product_found=True,product_id='P1'),post=wire,
                enabled=lambda _:enabled)
        return wire,decide

    def test_signed_image_to_selection_to_contacts_books_once_and_confirms_after_save(self):
        wire,decide = self.preview()
        for index,(text,photo) in enumerate((('',True),('أريد عنابي قياس 44',False),
                                            ('07701234567',False),('بغداد حي التجربة قرب المدرسة',False))):
            self.fresh(suffix=str(index))
            self.payload['message'].update(text=text,attachments=[dict(type='image',url='https://p16-sign-sg.tiktokcdn.com/photo.png')] if photo else [])
            result = self.post(path='/zernio/webhook/baraah-kids')
            self.assertEqual(200,result.status_code,result.get_json())
        with m.app.app_context():
            state = [dict(row) for row in m.get_db().execute('SELECT * FROM sales_v2_scopes')]
        self.assertEqual('booked',result.get_json()['action'], state)
        sent_count = wire.call_count
        self.post(path='/zernio/webhook/baraah-kids')
        self.assertEqual(sent_count,wire.call_count)
        with m.app.app_context():
            orders = m.get_db().execute('SELECT store_id,color,size,total_amount FROM orders').fetchall()
            self.assertEqual([('baraah-kids','عنابي','44',22000)],[tuple(x) for x in orders])
            self.assertEqual(1,m.get_db().execute("SELECT COUNT(*) FROM sales_v2_order_tasks WHERE kind='confirmation' AND status='accepted'").fetchone()[0])
        self.assertTrue(all(call.kwargs['json']['accountId']==self.account for call in wire.call_args_list))

    def test_paused_ai_does_not_call_model_or_send(self):
        wire,decide = self.preview(enabled=False)
        self.receive()
        decide.assert_not_called()
        wire.assert_not_called()

    def test_tiktok_cdn_is_trusted_without_allowing_arbitrary_hosts(self):
        self.assertTrue(inbound_media.trusted_source('https://p16-sign-sg.tiktokcdn.com/photo.png'))
        for url in ('https://tiktokcdn.com.evil.test/photo.png','http://p16-sign-sg.tiktokcdn.com/photo.png',
                    'https://127.0.0.1/photo.png','https://user:secret@p16-sign-sg.tiktokcdn.com/photo.png'):
            self.assertFalse(inbound_media.trusted_source(url),url)


if __name__ == '__main__':
    unittest.main()
