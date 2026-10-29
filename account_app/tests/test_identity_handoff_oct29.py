"""Identity questions route to a real review before any product/model response."""
import unittest
import uuid
from unittest.mock import Mock, patch
from account_app import sales_playbook
from account_app.tests import test_checkout_regressions as fixtures
from account_app.tests import test_checkout_completion_oct25 as completion

IDENTITY_QUESTIONS = (
    'انتي ذكاء لو بشر؟','انتي مساعدة آلية لو موظفة حقيقية؟',
    'هذا رد آلي لو موظفة تجاوب؟','ردج ذكاء اصطناعي؟','انت روبوت؟','هل أنت إنسان؟',
    'اللي يرد علي ذكاء اصطناعي؟','انتي حقيقية لو برنامج؟',
    'أنت ذكاء اصطناعي؟','انتِ بوت لو موظفة؟','حضرتك إنسان لو برنامج؟',
    'ردودكم كلها آلية؟','ردودك تلقائية؟','الرد من ذكاء اصطناعي؟',
    'هذا النظام روبوت يرد؟','انتي تشات جي بي تي؟','أنت ChatGPT؟',
    'منو يجاوبني بشر لو برنامج؟','انتم بشر لو برنامج؟','انتي إنسانة حقيقية؟',
    'انتي مساعد افتراضي؟','هذا الرد تلقائي؟','انتي ذكاء لو بشر وشنو قماشه؟',
    'بس جاوبيني هل أنت\nبرنامج؟','هل الردود من الذكاء الاصطناعي؟',
    'انتي موظفة حقيقية لو روبوت؟','are you a bot?', 'is this an automated reply?',
    'Are you human?', 'Are you ChatGPT?', 'Is this a real person?',
    'who is replying a human or ai?',
)

IDENTITY_STATEMENTS = (
    'أنا\nالمساعد الآلي للمتجر.', 'أني مساعدة المبيعات الآلية للمتجر، أكدر أساعدج.',
    'أنا برنامج ذكاء اصطناعي.', 'بصفتي نموذج لغة، أقدر أساعدك.',
    'أنا مساعد افتراضي للمتجر.', 'نحن نظام ذكاء اصطناعي.', 'تم تدريبي كنموذج لغة.',
    'I am an AI assistant.', "I'm a virtual assistant.", 'As an AI language model, I can help.',
    "I'm ChatGPT.", 'I am not a human.', 'I am a real person.', 'أنا إنسانة حقيقية.',
)


class IdentityRoutingFactsTests(unittest.TestCase):
    def test_direct_identity_questions_request_human_review(self):
        for text in IDENTITY_QUESTIONS:
            self.assertTrue(sales_playbook.human_requested(text),text)

    def test_photos_children_and_automation_features_are_not_identity_questions(self):
        for text in ('الصورة ذكاء اصطناعي؟','ما اريد صور ذكاء اريد صور حقيقية',
                     'انتي تدزين صور ذكاء؟','الطفل بنت لو ولد؟','الحجز يثبت آلي؟',
                     'عندكم موظف توصيل؟','اريد قياس 44','قماش تركي حقيقي؟',
                     'انتي عدج صور ذكاء؟','صور الإعلان مصممة بالذكاء الاصطناعي؟',
                     'الفستان من تصوير المحل لو ذكاء؟','اكو نظام حجز آلي بالموقع؟'):
            self.assertFalse(sales_playbook.human_requested(text),text)


class IdentityHandoffPipelineTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.CheckoutRegressionTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.CheckoutRegressionTests.tearDownClass.__func__)
    setUp=fixtures.CheckoutRegressionTests.setUp
    tearDown=fixtures.CheckoutRegressionTests.tearDown
    process=completion.CheckoutCompletionTests.process

    def test_identity_question_creates_one_review_pauses_ai_and_sends_nothing(self):
        with patch.object(self.m,'call_main_ai') as main:
            result,_=self.process('انتي مساعدة آلية لو موظفة حقيقية؟')
        main.assert_not_called()
        self.assertTrue(result['meta']['customer_requested_human'])
        self.assertFalse(result['reply']);self.assertEqual(result['reply_parts'],[])
        self.assertFalse(result['send_image']);self.assertEqual(result['messages'],[])
        self.assertEqual(result['attachments'],[])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM messages WHERE sender_id=? AND direction='outgoing'",(self.sender,)).fetchone()[0],0)
        self.assertNotRegex(result['reply'],r'ذكاء|آلي|روبوت|مساعدة المبيعات|اني موظفة|أنا إنسان')
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM human_reviews WHERE sender_id=? AND status=?',
                                        (self.sender,'pending')).fetchone()[0],1)
        self.ev['text']='زين شكد السعر؟'
        with patch.object(self.m,'extract_facebook_event',return_value=self.ev), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'call_main_ai') as main:
            later=self.m.process_webhook(self.db,{},use_debounce=False)
        main.assert_not_called()
        self.assertFalse(later['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM human_reviews WHERE sender_id=?',
                                        (self.sender,)).fetchone()[0],1)

    def test_identity_question_with_product_fact_still_routes_before_any_model_call(self):
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai(dict(self.ev,text='انتي ذكاء لو بشر وشنو قماشه؟'),
                'text',{},[],fixtures.CATALOG,fixtures.CATALOG[0],None,'',[],customer_products=[fixtures.CATALOG[0]])
        model.assert_not_called();self.assertTrue(result['requires_human'])
        self.assertFalse(result['reply']);self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertEqual(result['handoff_reason'],'customer_requested_human')

    def test_generated_identity_statement_is_suppressed_and_pauses_replies(self):
        with patch.object(self.m,'_call_main_ai_with_context',return_value={
                'reply':'أني مساعدة المبيعات الآلية للمتجر، أكدر أساعدج.', 'order':{},'create_order':False}), \
             patch.object(self.m.owner_sales,'reply',return_value=None), \
             patch.object(self.m,'natural_negotiation_reply',return_value=None):
            result=self.m.call_main_ai(dict(self.ev,text='عندي استفسار'),
                'text',{},[],fixtures.CATALOG,fixtures.CATALOG[0],None,'',[])
        self.assertFalse(result['reply']);self.assertTrue(result['requires_human'])
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))

    def test_product_link_does_not_resume_an_identity_handoff(self):
        self.process('انتي ذكاء لو بشر؟')
        self.m.complete_customer_product_link(self.db,self.sender,fixtures.CATALOG[0],
                                             'manual',source='manual_admin')
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertFalse(self.m.auto_reply_after_product_link(self.db,self.sender,fixtures.CATALOG[0],staff_action=True)['sent'])
        self.m.set_customer_ai_enabled(self.db,self.sender,True,allow_manual_resume=True)
        self.assertTrue(self.m.is_customer_ai_enabled(self.db,self.sender))

    def test_first_greeting_cannot_introduce_itself_as_ai(self):
        with patch.object(self.m,'get_auto_product_settings',return_value={'enabled':False}), \
             patch.object(self.m,'generate_first_message_reply',return_value=('أنا المساعد الآلي للمتجر.',None)) as greeting:
            result,_=self.process('هلو')
        greeting.assert_called_once()
        self.assertTrue(result['meta']['customer_requested_human'])
        self.assertFalse(result['reply']);self.assertEqual(result['reply_parts'],[])
        self.assertNotRegex(result['reply'],r'ذكاء|آلي|روبوت')
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))

    def fresh_sender(self):
        self.sender='al-fatena::identity-'+uuid.uuid4().hex
        self.ev['sender_id']=self.sender
        self.db.execute('INSERT INTO customers(sender_id,store_id) VALUES(?,?)',(self.sender,'al-fatena'))
        self.db.commit()

    def test_every_identity_wording_in_independent_multiturn_conversations_is_silent(self):
        with patch.object(self.m,'call_main_ai') as main, \
             patch.object(self.m,'send_text_to_facebook') as text_send, \
             patch.object(self.m,'send_image_to_facebook') as image_send:
            for question in IDENTITY_QUESTIONS:
                with self.subTest(question=question):
                    self.fresh_sender()
                    for direction,text in [('incoming','هلو، اريد اشوف فستان'),
                        ('outgoing','الفستان متوفر بالأسود، شنو القياس المطلوب؟'),
                        ('incoming','قياس 44'),('outgoing','القياس متوفر، شنو تحبين تعرفين عنه؟')]:
                        self.m.save_message(self.db,self.sender,direction,'text',text,None,None,None,{})
                    result,_=self.process(question)
                    self.assertFalse(result['reply']);self.assertEqual(result['reply_parts'],[])
                    self.assertFalse(result['send_image']);self.assertEqual(result['attachments'],[])
                    self.assertTrue(result['meta']['needs_human'])
                    self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
                    self.assertFalse(self.m.send_webhook_result_to_facebook(result))
                    self.assertEqual(self.db.execute("SELECT COUNT(*) FROM messages WHERE sender_id=? AND direction='outgoing'",(self.sender,)).fetchone()[0],2)
                    self.assertEqual(self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE sender_id=? AND status='pending'",(self.sender,)).fetchone()[0],1)
            main.assert_not_called();text_send.assert_not_called();image_send.assert_not_called()

    def test_all_generated_identity_statements_are_blocked_before_delivery(self):
        with patch.object(self.m,'send_text_to_facebook') as text_send, \
             patch.object(self.m,'send_image_to_facebook') as image_send, \
             patch.object(self.m,'is_ai_enabled',return_value=True):
            for statement in IDENTITY_STATEMENTS:
                for location in ('reply','parts','planned_parts'):
                    with self.subTest(statement=statement,location=location):
                        self.fresh_sender()
                        payload={'sender_id':self.sender,'reply':'السعر معروف.','send_image':False}
                        if location=='reply':payload['reply']=statement
                        elif location=='parts':payload['reply_parts']=[statement]
                        else:payload['_planned_sales_layout']={'reply':payload['reply'],'parts':[statement],'images':[]}
                        self.assertFalse(self.m.send_webhook_result_to_facebook(payload))
                        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
                        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM human_reviews WHERE sender_id=?',(self.sender,)).fetchone()[0],1)
            text_send.assert_not_called();image_send.assert_not_called()

    def test_pending_followups_are_cancelled_without_a_transfer_message(self):
        self.db.execute("INSERT INTO followups(sender_id,stage,status,scheduled_at) VALUES(?,'conversation','pending',?)",
                        (self.sender,self.m.now_baghdad_iso()));self.db.commit()
        result,_=self.process('انت روبوت؟')
        self.assertFalse(result['reply'])
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE sender_id=?',(self.sender,)).fetchone()[0],'cancelled')

    def test_identity_introduction_split_between_bubbles_is_blocked(self):
        with patch.object(self.m,'send_text_to_facebook') as send:
            for location in ('parts','planned_parts'):
                with self.subTest(location=location):
                    self.fresh_sender()
                    payload={'sender_id':self.sender,'reply':'القياس متوفر.','reply_parts':[]}
                    if location=='parts':payload['reply_parts']=['أنا','المساعد الآلي للمتجر.']
                    else:payload['_planned_sales_layout']={'reply':payload['reply'],'parts':['أنا','المساعد الآلي للمتجر.'],'images':[]}
                    self.assertFalse(self.m.send_webhook_result_to_facebook(payload))
                    self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
            send.assert_not_called()

    def test_saved_followup_identity_statement_is_never_sent(self):
        now=self.m.now_baghdad_iso()
        self.db.execute("INSERT INTO followups(sender_id,stage,message_text,status,scheduled_at,created_at) VALUES(?,'conversation',?,'pending',?,?)",
                        (self.sender,'أنا المساعد الآلي للمتجر.',now,now));self.db.commit()
        with patch.object(self.m,'get_followup_settings',return_value={'enabled':True,'max_per_day':2}), \
             patch.object(self.m,'_followup_block_reason',return_value=None), \
             patch.object(self.m,'send_reply_via_manychat') as send:
            result=self.m.send_due_followups(self.db)
        self.assertEqual(result['sent'],0);send.assert_not_called()
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE sender_id=?',(self.sender,)).fetchone()[0],'cancelled')

    def test_final_contact_with_identity_question_does_not_create_an_order(self):
        self.m.bind_customer_to_product(self.db,self.sender,fixtures.CATALOG[0],source='image_recognition')
        self.m.save_message(self.db,self.sender,'incoming','text','قياسي 44',None,None,None,{})
        result,_=self.process('انتي ذكاء لو بشر؟ 07701234567 بغداد حي الاختبار')
        self.assertFalse(result['reply']);self.assertTrue(result['meta']['needs_human'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],0)

    def test_identity_question_after_booking_is_silent_without_duplicate_order(self):
        self.db.execute('INSERT INTO orders(sender_id,status,store_id) VALUES(?,?,?)',(self.sender,'new','al-fatena'))
        self.db.commit()
        result,_=self.process('ردودكم كلها آلية؟')
        self.assertFalse(result['reply']);self.assertTrue(result['meta']['needs_human'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],1)

    def test_generated_followup_identity_statement_is_suppressed_before_scheduling(self):
        response=Mock()
        response.json.return_value={'choices':[{'message':{'content':'{"reply":"أنا المساعد الآلي للمتجر."}'}}]}
        with patch.object(self.m,'OPENROUTER_KEY','offline-test'), \
             patch.object(self.m,'_followup_block_reason',return_value=None), \
             patch.object(self.m,'_recent_conversation_lines',return_value=['الزبون يسأل عن السعر']), \
             patch.object(self.m.ai_transport,'post',return_value=response) as provider:
            result=self.m.generate_ai_followup_message(self.db,self.sender,'conversation',fixtures.CATALOG[0])
        provider.assert_called_once();self.assertIsNone(result)
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM human_reviews WHERE sender_id=?',(self.sender,)).fetchone()[0],1)
