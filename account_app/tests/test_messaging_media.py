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

    def test_specific_image_hint_overrides_opaque_file(self):
        url = 'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=photo'
        self.assertEqual(extract_media({'attachments': [{'type':'file','url':url}], 'image_url':url}), [{'type':'image','url':url}])

    def test_owner_memory_edits_survive_reads_and_are_versioned(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(self.m, '_ADVISOR_MEMORY_FILE', str(Path(directory)/'advisor_memory.md')):
            path = Path(directory)/'advisor_memory.md'
            path.write_text('ذاكرة مكتوبة يدوياً', encoding='utf-8')
            initial = self.client.get('/api/advisor/memory?format=json').get_json()
            self.assertEqual(initial['content'], 'ذاكرة مكتوبة يدوياً')
            response = self.client.put('/api/advisor/memory', json={'content':'تعليمات المالك الجديدة','version':initial['version']})
            self.assertEqual(response.status_code,200)
            self.assertEqual(self.m._advisor_sync_memory_file(self.db),'تعليمات المالك الجديدة')
            self.assertEqual(Path(str(path)+'.previous').read_text(encoding='utf-8'),'ذاكرة مكتوبة يدوياً')
            self.assertEqual(self.client.put('/api/advisor/memory',json={'content':'قديم','version':initial['version']}).status_code,409)
            self.assertIn('تعليمات المالك الجديدة',self.client.get('/api/advisor/memory?download=1').data.decode('utf-8'))

    def test_advisor_receives_owner_file_without_truncation(self):
        content = 'معلومة مهمة في البداية\n' + 'س'*13000
        with tempfile.TemporaryDirectory() as directory, patch.object(self.m, '_ADVISOR_MEMORY_FILE', str(Path(directory)/'advisor_memory.md')), patch.object(self.m,'OPENROUTER_KEY','test'), patch.object(self.m.requests,'post') as post:
            Path(directory,'advisor_memory.md').write_text(content,encoding='utf-8')
            post.return_value.json.return_value = {'choices':[{'message':{'content':'{"reply":"تم"}'}}]}
            self.m._advisor_call_model(self.db,'ماذا تتذكر؟')
            sent = post.call_args.kwargs['json']['messages'][-1]['content']
            self.assertIn(content,sent)

    def test_album_links_all_products_and_ignores_old_ad(self):
        products = [{'product_id':'album-A','product_name':'A','stock':'متوفر'}, {'product_id':'album-B','product_name':'B','stock':'متوفر'}]
        catalog_patch = patch.object(self.m, 'load_products_from_file', return_value=products); catalog_patch.start(); self.addCleanup(catalog_patch.stop)
        ev = {'sender_id':self.sender, 'text':'', 'ad_id':'old-ad', 'attachments':[{'type':'image','url':'https://img.test/a.jpg'}, {'type':'image','url':'https://img.test/b.jpg'}]}
        matches = [{'product_found':True,'product_id':p['product_id']} for p in products]
        with patch.object(self.m,'match_customer_image_with_catalog',side_effect=matches):
            product, method, result = self.m.match_product(self.db,ev,products)
        active = self.m.load_customer_products(self.db,self.sender)
        self.assertEqual({p['product_id'] for p in active}, {'album-A','album-B'})
        self.assertEqual(result['product_ids'], ['album-A','album-B'])

    def test_image_correction_replaces_previous_product(self):
        old = {'product_id':'old','product_name':'Old','stock':'متوفر'}
        new = {'product_id':'new','product_name':'New','stock':'متوفر'}
        catalog_patch = patch.object(self.m, 'load_products_from_file', return_value=[old,new]); catalog_patch.start(); self.addCleanup(catalog_patch.stop)
        self.m.complete_customer_product_link(self.db,self.sender,old,'manual')
        ev = {'sender_id':self.sender,'text':'مو هذا','image_url':'https://img.test/new.jpg'}
        with patch.object(self.m,'match_customer_image_with_catalog',return_value={'product_found':True,'product_id':'new'}):
            self.m.match_product(self.db,ev,[old,new])
        self.assertEqual([p['product_id'] for p in self.m.load_customer_products(self.db,self.sender)], ['new'])

    def test_new_photo_waits_for_choice_unless_intent_is_explicit(self):
        old = {'product_id':'old','product_name':'Old','stock':'متوفر'}
        new = {'product_id':'new','product_name':'New','stock':'متوفر'}
        catalog_patch = patch.object(self.m, 'load_products_from_file', return_value=[old,new]); catalog_patch.start(); self.addCleanup(catalog_patch.stop)
        for caption, expected in [('', {'new'}), ('بس هذا', {'new'}), ('ضيفي هذا ويا الطلب', {'old','new'})]:
            self.m.complete_customer_product_link(self.db,self.sender,old,'manual')
            ev = {'sender_id':self.sender,'text':caption,'image_url':'https://img.test/new.jpg'}
            with patch.object(self.m,'match_customer_image_with_catalog',return_value={'product_found':True,'product_id':'new'}):
                self.m.match_product(self.db,ev,[old,new])
            self.assertEqual({p['product_id'] for p in self.m.load_customer_products(self.db,self.sender)}, expected)

    def test_text_target_requires_unique_available_model(self):
        products = [dict(product_id=str(i), product_name='سوت موديل'+str(i), stock='متوفر') for i in range(25)]
        self.assertEqual(self.m.customer_product_target('اريد سوت موديل24',products)['product_id'], '24')
        self.assertIsNone(self.m.customer_product_target('اريد سوت',products))
        self.assertIsNone(self.m.customer_product_target('ما اريد سوت موديل24',products))
        products[-1]['stock']='غير متوفر'
        self.assertIsNone(self.m.customer_product_target('اريد سوت موديل24',products))

    def test_auto_reply_does_not_remove_other_album_products(self):
        products = [{'product_id':'keep-A','product_name':'A'}, {'product_id':'keep-B','product_name':'B'}]
        catalog_patch = patch.object(self.m, 'load_products_from_file', return_value=products); catalog_patch.start(); self.addCleanup(catalog_patch.stop)
        for product in products:
            self.m.complete_customer_product_link(self.db,self.sender,product,'image_recognition',preserve_existing=True)
        with patch.object(self.m,'is_ai_enabled',return_value=False):
            self.m.auto_reply_after_product_link(self.db,self.sender,products[-1])
        self.assertEqual({p['product_id'] for p in self.m.load_customer_products(self.db,self.sender)}, {'keep-A','keep-B'})

    @patch('account_app.app.resolve_incoming_media')
    def test_old_opaque_attachment_can_be_reclassified(self,resolve):
        url = 'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=old'
        self.db.execute("INSERT INTO messages(sender_id,direction,message_type,media_json) VALUES(?,'incoming','file',?)", (self.sender, '[{"type":"file","url":"'+url+'"}]'))
        self.db.commit()
        mid = self.db.execute('SELECT max(id) FROM messages WHERE sender_id=?',(self.sender,)).fetchone()[0]
        resolve.return_value = [{'type':'audio','url':url}]
        response = self.client.post(f'/api/conversations/{self.sender}/messages/{mid}/media')
        self.assertEqual(response.get_json()['media'][0]['type'],'audio')
        self.assertEqual(self.client.get(f'/api/conversations/{self.sender}/messages').get_json()['messages'][0]['message_type'],'audio')
        self.assertEqual(self.client.post(f'/api/conversations/other/messages/{mid}/media').status_code,404)

    @patch('account_app.app.requests.get')
    @patch('account_app.app.requests.head')
    def test_head_unsupported_falls_back_to_stream_headers(self,head,get):
        head.return_value.status_code = 405
        response = get.return_value.__enter__.return_value
        response.status_code = 206
        response.headers = {'Content-Type':'image/jpeg'}
        media = self.m.resolve_incoming_media({'attachment_url':'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=fallback'})
        self.assertEqual(media[0]['type'],'image')
        self.assertTrue(get.call_args.kwargs['stream'])
        self.assertFalse(get.call_args.kwargs['allow_redirects'])

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
        self.assertEqual([c.args[1] for c in send.call_args_list],[result['reply']])
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

    @patch('account_app.app.threading.Thread')
    def test_fatena_defaults_to_facebook_and_preserves_explicit_channel(self, thread):
        self.client.post('/manychat/webhook/al-fatena', json={'subscriber_id':self.sender,'text':'hello'})
        body=thread.call_args.kwargs['args'][0]
        self.assertEqual(self.m.extract_facebook_event(body)['platform'],'facebook')
        self.assertEqual(self.m.manychat_content_type('facebook'),'messenger')
        self.client.post('/manychat/webhook/al-fatena', json={'subscriber_id':self.sender,'text':'hello','platform':'whatsapp'})
        body=thread.call_args.kwargs['args'][0]
        self.assertEqual(self.m.extract_facebook_event(body)['platform'],'whatsapp')
        self.assertEqual(self.m.manychat_content_type('whatsapp'),'whatsapp')
        self.assertEqual(self.m.detect_manychat_platform({'platform':'facebook','whatsapp_phone':'123'}),'facebook')
        self.assertEqual(self.m.detect_manychat_platform({'whatsapp_phone':'123'}),'whatsapp')

    def test_whatsapp_send_uses_correct_channel_without_messenger_tag(self):
        from unittest.mock import Mock
        response=Mock(ok=True,status_code=200)
        response.json.return_value={'status':'success'}
        with patch.object(self.m,'manychat_api_key_for_page',return_value='test'), patch.object(self.m.requests,'post',return_value=response) as send:
            result=self.m._post_manychat_send('al-fatena::123',[{'type':'text','text':'hello'}],platform='whatsapp',message_tag='ACCOUNT_UPDATE')
            self.assertTrue(result['ok'])
            payload=send.call_args.kwargs['json']
            self.assertEqual(payload['data']['content']['type'],'whatsapp')
            self.assertNotIn('message_tag',payload)

    def test_photo_requests_resend_even_after_auto_image(self):
        product={'image_url':['https://example.test/a.jpg','https://example.test/b.jpg']}
        with patch.object(self.m,'get_active_product_binding',return_value={'source':'auto_default_product','image_sent':1}):
            for text in ['وين صورتها؟','دزيلياه بصوره','تصوريلياه حتى اشوفه','أرسل صور','ممكن صورته']:
                with self.subTest(text=text):
                    self.assertTrue(self.m._should_send_image(self.db,self.sender,product,{'text':text}))
            self.assertFalse(self.m._should_send_image(self.db,self.sender,product,{'text':'ممكن سعره؟'}))

    def test_unlabelled_single_color_album_is_not_dropped(self):
        product={'image_url':['https://example.test/a.jpg','https://example.test/b.jpg'],'colors':'اسود'}
        self.assertEqual(len(self.m.select_product_image_urls(product,'أسود')),2)
        self.assertEqual(self.m.select_product_image_urls(product,'احمر'),[])
        product['colors']='اسود احمر'
        self.assertEqual(self.m.select_product_image_urls(product,'اسود'),[])

    def test_photo_promise_is_detected_when_last_customer_message_is_size(self):
        self.assertTrue(self.m._promises_product_photo('من عيوني هذي صورة الفستان الأسود'))
        self.assertTrue(self.m._promises_product_photo('ثواني وأدزلج الصور'))
        self.assertFalse(self.m._promises_product_photo('الفحص عند الاستلام إذا مو نفس الصورة'))

    def test_unreadable_media_is_saved_and_reviewed_without_customer_reply(self):
        for kind, url in [('audio','https://example.test/a.ogg'),('video','https://example.test/a.mp4'),('file','https://example.test/a.pdf')]:
            with self.subTest(kind=kind):
                body=self.body([{'type':kind,'payload':{'url':url}}])
                with patch.object(self.m,'is_ai_enabled',return_value=True), patch.object(self.m,'is_customer_ai_enabled',return_value=True), patch.object(self.m,'create_human_review',return_value=901) as review, patch.object(self.m,'has_pending_human_review',return_value=None), patch.object(self.m,'call_main_ai') as ai:
                    response=self.m.process_webhook(self.db,body,use_debounce=False)
                self.assertEqual(response['reply'],'')
                self.assertFalse(response['send_image'])
                self.assertTrue(response['meta']['waiting_for_human_action'])
                review.assert_called_once();ai.assert_not_called()
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='outgoing'",(self.sender,)).fetchone()[0],0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='incoming'",(self.sender,)).fetchone()[0],3)

    def test_short_sales_reply_sends_one_complete_message(self):
        parts=['أهلاً بك.','السعر 17000 دينار.','القماش باربي.','أي قياس تحتاج؟']
        result=self.m.normalize_ai_reply_parts({'reply_parts':parts})
        self.assertEqual(self.m.approved_reply_parts(result,result['reply']),[result['reply']])
        with patch.object(self.m,'send_text_to_facebook',return_value=True) as send:
            self.m.send_webhook_result_to_facebook(dict(result,sender_id=self.sender))
            self.assertEqual([call.args[1] for call in send.call_args_list],[result['reply']])

    def test_corrected_sales_text_stays_one_message_without_old_claims(self):
        result={'reply_parts':['باقي قطعتين','أحجز الآن']}
        reply='السعر 17000 دينار. القماش باربي. أي قياس تحتاج؟'
        self.assertEqual(self.m.approved_reply_parts(result,reply),[reply])
        self.assertEqual(self.m.approved_reply_parts({},'تدللين 🌷'),['تدللين 🌷'])
        self.assertEqual(self.m.approved_reply_parts({},''),[])

    def test_extra_sales_parts_keep_all_information_under_limit(self):
        parts=['تحية','سعر','قماش','قياس','توصيل','سؤال']
        result=self.m.normalize_ai_reply_parts({'reply_parts':parts})
        self.assertTrue(1 <= len(result['reply_parts']) <= 5)
        self.assertEqual(result['reply'],'\n\n'.join(parts))

    def test_meaningful_parts_sent_once_in_order_for_every_store(self):
        parts=['الفستان متوفر باللون الأسود والأحمر حسب الخيارات الموجودة للقطعة.',
               'القماش باربي وتفاصيل الخامة الموجودة موضحة بهذا المنتج حتى تختارين براحتج.',
               'القياسات متوفرة من 38 إلى 52 حسب جدول هذا الموديل الموجود عندنا.',
               'الفحص عند الاستلام متاح حسب سياسة المتجر، وتكدرين تتأكدين من القطعة.',
               'شنو القياس واللون اللي تحبين نكمل عليه حتى نراجع توفر الاختيار؟']
        for store in ['default', 'khuyoot', 'golden-threads', 'al-fatena']:
            token=self.m._current_store_id.set(store)
            try:
                result={'sender_id':self.sender,'reply':'\n\n'.join(parts),'reply_parts':parts}
                self.assertEqual(self.m.approved_reply_parts(result,result['reply']),parts)
                with patch.object(self.m,'send_text_to_facebook',return_value=True) as send:
                    self.m.send_webhook_result_to_facebook(result)
                self.assertEqual([call.args[1] for call in send.call_args_list],parts)
                result['_single_message']=True
                with patch.object(self.m,'send_text_to_facebook',return_value=True) as send:
                    self.m.send_webhook_result_to_facebook(result)
                self.assertEqual([call.args[1] for call in send.call_args_list],[result['reply']])
            finally:self.m._current_store_id.reset(token)

    def test_manychat_splits_one_long_model_part_into_ordered_messages(self):
        parts = ['الفستان متوفر باللون الأسود والأحمر حسب الخيارات المسجلة بالموديل.',
                 'القماش لينن حسب تفاصيل الخامة الموجودة عندنا بالمتجر.',
                 'القياسات متوفرة من 38 إلى 52 حسب الجدول المسجل لهذا الموديل.',
                 'الفحص متاح عند الاستلام بوجود المندوب حسب سياسة المتجر.',
                 'شنو اللون اللي تفضلينه حتى نراجع توفره بالقياس اللي اخترتيه؟']
        reply = ' '.join(parts)
        result = {'reply': reply, 'reply_parts': [reply], 'send_image': False}
        with patch.object(self.m, 'process_webhook', return_value=result), patch.object(
                self.m, '_post_manychat_send', return_value={'ok': True}) as send:
            self.m._process_manychat_webhook_async_locked(self.body([], 'مواصفات'), self.sender, 'facebook')
        self.assertEqual([c.args[1] for c in send.call_args_list],
                         [[{'type': 'text', 'text': part}] for part in parts])

    def test_delivery_fees_are_separate_by_store_and_destination(self):
        for sid,baghdad,other in [('al-fatena',3000,7000),('khuyoot',0,4500)]:
            token=self.m._current_store_id.set(sid)
            try:
                self.m.save_delivery_settings(self.db,{'baghdad_fee':baghdad,'other_fee':other,'delivery_time':'مدة اختبار'})
            finally:self.m._current_store_id.reset(token)
        for sid,baghdad,other in [('al-fatena',3000,7000),('khuyoot',0,4500)]:
            token=self.m._current_store_id.set(sid)
            try:
                self.assertEqual(self.m.delivery_fee_for_province('بغداد',self.db),baghdad)
                self.assertEqual(self.m.delivery_fee_for_province('بابل',self.db),other)
                self.assertEqual(self.m.get_delivery_settings(self.db)['delivery_time'],'مدة اختبار')
                self.m.save_delivery_settings(self.db,{'fast_delivery':False})
                self.assertEqual(self.m.delivery_fee_for_province('بغداد',self.db),baghdad)
            finally:self.m._current_store_id.reset(token)

    def test_photo_never_rebinds_automatic_product_before_matching(self):
        with patch.object(self.m,'get_auto_product_settings',return_value={'enabled':True, 'product':{'product_id':'P1','product_name':'فستان'}}), patch.object(self.m,'get_active_product_binding',return_value=None):
            self.assertFalse(self.m.should_use_auto_product(self.db,self.sender,{'image_url':'https://example.test/new.jpg'},'image',[]))
            self.assertTrue(self.m.should_use_auto_product(self.db,self.sender,{'text':'مرحبا'},'text',[]))

if __name__=='__main__':unittest.main()
