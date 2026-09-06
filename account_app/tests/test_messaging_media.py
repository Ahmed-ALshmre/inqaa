import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

os.environ['ENABLE_BACKGROUND_JOBS']='0'
os.environ['DISABLE_CLIP']='1'
from account_app.media import extract_media, media_type


class MessagingMediaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import account_app.app as module
        cls.m=module
        cls.temp=tempfile.TemporaryDirectory()
        cls.old_path=module.DB_PATH
        module.DB_PATH=str(Path(cls.temp.name)/'media.db')
        module.init_db()

    @classmethod
    def tearDownClass(cls):
        cls.m.DB_PATH=cls.old_path;cls.temp.cleanup()

    def setUp(self):
        self.ctx=self.m.app.app_context();self.ctx.push();self.db=self.m.get_db()
        self.sender='media-test-'+uuid.uuid4().hex
        self.token=self.m._current_store_id.set('default')
        self.m.get_or_create_customer(self.db,self.sender,'page','facebook')
        self.client=self.m.app.test_client()
        with self.client.session_transaction() as session:session['dashboard_authenticated']=True

    def tearDown(self):
        self.m._current_store_id.reset(self.token);self.ctx.pop()

    def body(self,attachments,text=''):
        return {'entry':[{'id':'page','_store_id':'default','messaging':[{'sender':{'id':self.sender},'message':{'mid':uuid.uuid4().hex,'text':text,'attachments':attachments}}]}]}

    def test_all_images_survive_extraction_storage_and_api(self):
        body=self.body([{'type':'image','payload':{'url':f'https://images.test/{i}.jpg'}} for i in range(3)])
        ev=self.m.extract_facebook_event(body)
        self.assertEqual(len(ev['attachments']),3)
        self.m.save_message(self.db,self.sender,'incoming','image','',ev['image_url'],None,None,body)
        message=self.client.get(f'/api/conversations/{self.sender}/messages').get_json()['messages'][0]
        self.assertEqual(len(message['media']),3)
        self.assertNotIn('raw_payload',message)

    def test_voice_mp4_and_ogg_are_not_images_even_in_old_records(self):
        for ext in ('mp4','ogg'):
            url=f'https://cdn.fbsbx.com/audioclip-test.{ext}'
            self.assertEqual(media_type(url,'image'),'audio')
            self.db.execute("INSERT INTO messages(sender_id,direction,message_type,text,image_url) VALUES(?,'incoming','image',?,?)",(self.sender,url,url))
        self.db.commit()
        messages=self.client.get(f'/api/conversations/{self.sender}/messages').get_json()['messages']
        self.assertTrue(all(m['media'][0]['type']=='audio' and m['image_url'] is None for m in messages))

    def test_unrelated_profile_photo_is_not_an_attachment(self):
        media=extract_media({'profile_pic':'https://cdn.test/avatar.jpg','last_input_text':'السلام عليكم','attachments':[{'type':'audio','payload':{'url':'https://cdn.test/opaque'}}]})
        self.assertEqual(media,[{'type':'audio','url':'https://cdn.test/opaque'}])
        self.assertEqual(extract_media({'image_url':'javascript:alert(1)'}),[])

    @patch('account_app.app.requests.head')
    def test_opaque_meta_media_uses_content_type(self,head):
        head.return_value.status_code=200;head.return_value.headers={'Content-Type':'audio/ogg'}
        media=self.m.resolve_incoming_media({'last_input_text':'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=test'})
        self.assertEqual(media[0]['type'],'audio')
        self.assertFalse(head.call_args.kwargs['allow_redirects'])

    def test_different_albums_with_same_caption_are_not_deduplicated(self):
        one=self.body([{'type':'image','payload':{'url':'https://img.test/one.jpg'}}], 'هذه الصور')
        self.m.save_message(self.db,self.sender,'incoming','image','هذه الصور','https://img.test/one.jpg',None,None,one)
        album=extract_media({'images':['https://img.test/one.jpg','https://img.test/two.jpg']})
        self.assertFalse(self.m.is_recent_duplicate_incoming(self.db,self.sender,'هذه الصور','https://img.test/one.jpg',media=album))
        self.assertTrue(self.m.is_recent_duplicate_incoming(self.db,self.sender,'هذه الصور','https://img.test/one.jpg',media=album[:1]))

    def test_incremental_and_older_messages(self):
        for i in range(105):self.m.save_message(self.db,self.sender,'incoming','text',str(i))
        base=f'/api/conversations/{self.sender}/messages'
        page=self.client.get(base).get_json();self.assertEqual(len(page['messages']),100);self.assertTrue(page['has_more'])
        old=self.client.get(base+'?before_id='+str(page['messages'][0]['id'])).get_json()
        self.assertEqual(len(old['messages']),5)
        last=page['messages'][-1]['id']
        self.assertEqual(self.client.get(base+f'?after_id={last}').get_json()['messages'],[])
        self.m.save_message(self.db,self.sender,'incoming','text','new')
        self.assertEqual(self.client.get(base+f'?after_id={last}').get_json()['messages'][0]['text'],'new')

    @patch('account_app.app._process_manychat_webhook_async_locked')
    @patch('account_app.app.DEBOUNCE_DELAY',0)
    def test_incoming_is_saved_before_ai_worker_and_only_once(self,worker):
        body=self.body([], 'أريد قياس 42')
        def checked(*args,**kwargs):
            count=self.db.execute('SELECT count(*) FROM messages WHERE sender_id=?',(self.sender,)).fetchone()[0]
            self.assertEqual(count,1);self.assertTrue(kwargs['incoming_message_id'])
        worker.side_effect=checked
        self.m._process_manychat_webhook_async(body,self.sender,'facebook')
        worker.assert_called_once()
        self.m._process_manychat_webhook_async(body,self.sender,'facebook')
        worker.assert_called_once()

    @patch('account_app.app.threading.Thread')
    def test_manychat_keeps_media_types_and_all_images(self,thread):
        self.client.post('/manychat/webhook/lamsa-store',json={'subscriber_id':self.sender,'attachments':[{'type':'image','url':'https://img.test/a.jpg'},{'type':'image','url':'https://img.test/b.jpg'},{'type':'audio','url':'https://cdn.fbsbx.com/audioclip-test.mp4'}]})
        body=thread.call_args.kwargs['args'][0]
        media=self.m.extract_facebook_event(body)['attachments']
        self.assertEqual([m['type'] for m in media],['image','image','audio'])

    @patch('account_app.app.send_text_to_facebook',return_value=True)
    def test_checked_reply_parts_sent_in_order_without_duplicate_combined_text(self,send):
        result=self.m.normalize_ai_reply_parts({'reply_parts':['القياس متوفر','شنو اللون المطلوب؟']})
        result['sender_id']=self.sender
        self.m.send_webhook_result_to_facebook(result)
        self.assertEqual([c.args[1] for c in send.call_args_list],['القياس متوفر','شنو اللون المطلوب؟'])
        self.assertEqual(self.m.approved_reply_parts(result,'جواب مصحح'),['جواب مصحح'])

    def test_fatena_missing_key_does_not_use_another_account(self):
        with patch.dict(os.environ, {'MANYCHAT_API_KEY_AL_FATENA': '', 'MANYCHAT_KEYS_BY_PAGE': ''}), patch.object(self.m, 'current_manychat_api_key', return_value='other-account'):
            self.assertEqual(self.m.manychat_api_key_for_page('', 'al-fatena'), '')
        with patch.dict(os.environ, {'MANYCHAT_API_KEY_AL_FATENA': 'fatena-test-key'}):
            self.assertEqual(self.m.manychat_api_key_for_page('', 'al-fatena'), 'fatena-test-key')

    def test_async_delivery_keeps_store_and_reports_failure(self):
        body=self.body([], 'hello')
        body['entry'][0]['_store_id']='al-fatena'
        body['entry'][0]['messaging'][0]['sender']['id']='al-fatena::123'
        result={'reply':'first\n\nsecond','reply_parts':['first','second'], 'send_image':True,'image_urls':['https://example.test/a.jpg']}
        with patch.object(self.m,'process_webhook',return_value=result), patch.object(self.m,'_post_manychat_send',return_value={'ok':False,'status':'error','status_code':400,'message':'Subscriber not found'}) as send, patch.object(self.m,'create_human_review') as review:
            self.m._process_manychat_webhook_async_locked(body,'al-fatena::123','facebook','123')
            send.assert_called_once()
            self.assertEqual(send.call_args.args[0],'al-fatena::123')
            self.assertEqual(send.call_args.kwargs['page_id'],'page')
            self.assertEqual(send.call_args.kwargs['store_id'],'al-fatena')
            review.assert_called_once()
            self.assertIn('Subscriber not found',review.call_args.args[2])

if __name__=='__main__':unittest.main()
