import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

os.environ['ENABLE_BACKGROUND_JOBS'] = '0'
os.environ['DISABLE_CLIP'] = '1'


class PostOrderServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import account_app.app as module
        cls.m = module
        cls.temp = tempfile.TemporaryDirectory()
        cls.old_path = module.DB_PATH
        module.DB_PATH = str(Path(cls.temp.name) / 'service.db')
        module.init_db()

    @classmethod
    def tearDownClass(cls):
        cls.m.DB_PATH = cls.old_path
        cls.temp.cleanup()

    def setUp(self):
        self.ctx = self.m.app.app_context(); self.ctx.push()
        self.db = self.m.get_db()
        self.sender = 'khuyoot::service-' + uuid.uuid4().hex
        self.token = self.m._current_store_id.set('khuyoot')
        self.db.execute("INSERT INTO customers(sender_id,store_id,lead_stage) VALUES(?,?,'booked')", (self.sender,'khuyoot'))
        self.db.execute("INSERT INTO orders(sender_id,store_id,status,created_at,order_items) VALUES(?,?,'new',?,'[]')", (self.sender,'khuyoot',self.m.now_baghdad_iso()))
        self.db.commit()
        self.m.set_customer_ai_enabled(self.db,self.sender,True)
        self.order = self.m.get_latest_customer_order(self.db,self.sender)
        self.ev = {'sender_id':self.sender,'page_id':'page-test','platform':'facebook','text':'شوكت يوصل؟','image_url':None,'store_id':'khuyoot','ref':None,'ad_id':None,'timestamp':0,'postback':None,'quick_reply':None,'referral_source':None,'referral_type':None,'attachments':[]}
        self.customer = dict(self.db.execute('SELECT * FROM customers WHERE sender_id=?',(self.sender,)).fetchone())
        self.client = self.m.app.test_client()
        with self.client.session_transaction() as session: session['dashboard_authenticated'] = True

    def tearDown(self):
        self.m._current_store_id.reset(self.token)
        self.ctx.pop()

    def handle(self, result, text=None, history=None):
        if text is not None: self.ev['text'] = text
        history = history if history is not None else [{'direction':'incoming','text':self.ev['text']}]
        with patch.object(self.m,'call_main_ai',return_value=result) as ai, patch.object(self.m,'is_store_feature_enabled',return_value=False), patch.object(self.m,'send_human_review_to_telegram',return_value=False,create=True):
            # create_human_review uses this notification function in this project.
            with patch.object(self.m,'send_telegram_message',return_value=False,create=True), patch.object(self.m,'send_telegram_photo',return_value=False):
                response = self.m.handle_post_order_message(self.db,self.ev,self.customer,history,[],[],self.order)
            return response,ai.call_count

    def test_normal_question_continues_without_pause_or_duplicate(self):
        response,_ = self.handle({'reply':'الطلب مسجل، موعد الوصول الدقيق مو ظاهر عندي حالياً.','create_order':False})
        self.assertTrue(response['reply'])
        self.assertFalse(response['meta']['needs_human'])
        self.assertTrue(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],1)
        self.assertIsNone(self.m.has_pending_human_review(self.db,self.sender))

    @patch('account_app.app.requests.post')
    def test_review_notices_are_internal_and_never_sent_or_saved_as_reply(self, post):
        post.return_value.ok = True
        notices = [
            'هذا الطلب يحتاج تأكد من فريق المتجر، وسجلت رسالتج للمراجعة.',
            'أگدر أجاوبج هنا عن تفاصيل القطعة وسياسة المتجر المتوفرة 🌸',
            'طلبج مسجل مسبقاً. رسالتج موجودة بالمراجعة؛ ما تم تغيير الطلب بعد.',
        ]
        for notice in notices:
            with self.subTest(notice=notice):
                response, _ = self.handle({'reply': notice})
                self.assertEqual(response['reply'], '')
                self.assertEqual(response['reply_parts'], [])
                self.assertTrue(response['meta']['human_review_id'])
                with patch.object(self.m, 'send_text_to_facebook') as send:
                    self.m.send_webhook_result_to_facebook(response)
                send.assert_not_called()
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='outgoing'", (self.sender,)).fetchone()[0], 0)
        self.assertTrue(self.m.is_customer_ai_enabled(self.db, self.sender))

    @patch('account_app.app.requests.post')
    def test_action_review_does_not_silence_following_questions(self, post):
        post.return_value.ok = True
        response,_ = self.handle({'reply':'سأراجع تغيير رقم الهاتف','requires_human':True,'handoff_reason':'تغيير هاتف الطلب'})
        self.assertEqual(response['reply'], ''); self.assertTrue(response['meta']['needs_human'])
        review = response['meta']['human_review_id']
        again,_ = self.handle({'reply':'طلب تعديل','requires_human':True})
        self.assertEqual(again['meta']['human_review_id'],review)
        follow,_ = self.handle({'reply':'الفحص حسب سياسة المتجر عند الاستلام.'},text='اكدر افحص القطعة؟')
        self.assertFalse(follow['meta']['needs_human'])
        self.assertTrue(self.m.is_customer_ai_enabled(self.db,self.sender))

    def test_thanks_does_not_drop_unanswered_question(self):
        result,calls = self.handle({'reply':'أجاوبج عن التوصيل أولاً.'},text='تمام',history=[{'direction':'incoming','text':'اشكد الحساب النهائي؟'},{'direction':'incoming','text':'تمام'}])
        self.assertEqual(calls,1);self.assertIn('التوصيل',result['reply'])
        _,calls=self.handle({'reply':'unused'},text='تمام')
        self.assertEqual(calls,0)

    @patch('account_app.app.requests.post')
    def test_no_false_order_creation_or_operational_claims(self,post):
        post.return_value.ok = True
        for result in ({'reply':'تم تثبيت الطلب','create_order':True},{'reply':'ثبتنالج ملاحظة استعجال حتى يوصل باجر'}, {'reply':'تم إلغاء الطلب'}):
            response,_=self.handle(result)
            self.assertTrue(response['meta']['needs_human'])
            self.assertNotIn('تم تثبيت',response['reply'])
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],1)

    def test_conditional_return_is_not_cancellation_or_fault(self):
        text='بس اذا مو نفس الصوره او نوعيه رديئه ارجعه بيد المندوب'
        self.assertFalse(self.m.is_product_objection(text))
        self.assertIsNone(self.m.classify_customer_problem(text))
        self.assertTrue(self.m.is_product_objection('لا مو هذا اريد غيره'))
        self.assertTrue(self.m.classify_customer_problem('استلمت الطلب وطلع قياس غلط'))

    @patch('account_app.app.send_text_via_manychat_detailed',return_value={'ok':True})
    def test_manual_outgoing_keeps_owner_store(self,send):
        res=self.client.post(f'/api/conversations/{self.sender}/send',json={'text':'رد تجريبي'})
        self.assertEqual(res.status_code,200)
        row=self.db.execute('SELECT store_id FROM messages WHERE sender_id=? ORDER BY id DESC LIMIT 1',(self.sender,)).fetchone()
        self.assertEqual(row['store_id'],'khuyoot')

    def test_webhook_preserves_store_display_name(self):
        self.m.ensure_store(self.db,'khuyoot','خيوط',webhook_key='khuyoot')
        self.m.resolve_webhook_store(self.db,{},'khuyoot')
        self.assertEqual(self.m.get_store(self.db,'khuyoot')['name'],'خيوط')

    @patch('account_app.app.requests.post')
    def test_model_failure_gets_visible_review_without_permanent_pause(self,post):
        post.return_value.ok=True
        response,_=self.handle({'failed':True,'failure_reason':'timeout','reply':''})
        self.assertEqual(response['reply'], '')
        self.assertEqual(response['reply_parts'], [])
        self.assertTrue(response['meta']['human_review_id'])
        self.assertTrue(self.m.is_customer_ai_enabled(self.db,self.sender))

    def test_post_order_prompt_includes_saved_order_and_no_mutation_rules(self):
        event=dict(self.ev, _post_order=self.order)
        with patch.object(self.m,'OPENROUTER_KEY','test-key'), patch.object(self.m.requests,'post') as post:
            post.return_value.json.return_value={'choices':[{'message':{'content':'{"reply":"الطلب مسجل","create_order":false}'}}]}
            self.m.call_main_ai(event,'text',self.customer,[],[],None,None,'',[])
        prompt=post.call_args.kwargs['json']['messages']
        self.assertIn('requires_human',prompt[0]['content'])
        self.assertIn('لا تنشئ طلباً جديداً',prompt[0]['content'])
        self.assertIn(str(self.order['id']),prompt[-1]['content'])

    def test_request_does_not_leak_store_into_next_request(self):
        before=self.m._current_store_id.get()
        self.client.get('/api/stores?store_id=golden-threads')
        self.assertEqual(self.m._current_store_id.get(),before)
        res=self.client.get('/api/stores')
        self.assertEqual(res.get_json()['current_store_id'],'default')

    @patch('account_app.app.send_telegram_message',return_value=False)
    def test_existing_manual_pause_still_prevents_automatic_reply(self,notify):
        self.m.set_customer_ai_enabled(self.db,self.sender,False)
        with patch.object(self.m,'extract_facebook_event',return_value=self.ev), patch.object(self.m,'log'), patch.object(self.m,'log_sep'), patch.object(self.m,'is_ai_enabled',return_value=True), patch.object(self.m,'load_active_products',return_value=[]), patch.object(self.m,'handle_post_order_message') as handler:
            response=self.m.process_webhook(self.db,{},use_debounce=False)
        handler.assert_not_called()
        self.assertEqual(response['meta']['reason'],'ai_disabled_for_conversation')

    def test_webhook_reaches_after_sales_before_old_image_block(self):
        self.m.save_message(self.db,self.sender,'incoming','image',None,'https://example.test/old.png',None,None,{})
        with patch.object(self.m,'extract_facebook_event',return_value=self.ev), patch.object(self.m,'log'), patch.object(self.m,'log_sep'), patch.object(self.m,'is_ai_enabled',return_value=True), patch.object(self.m,'load_active_products',return_value=[]), patch.object(self.m,'handle_post_order_message',return_value={'reply':'متابعة'}) as handler:
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertEqual(result['reply'],'متابعة');handler.assert_called_once()

if __name__ == '__main__': unittest.main()
