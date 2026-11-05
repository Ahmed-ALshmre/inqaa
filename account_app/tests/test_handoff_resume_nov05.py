"""False handoffs and failed operator retries observed in the November 5 snapshot."""
import json
from unittest import TestCase
from unittest.mock import patch
from account_app import customer_copy,product_questions,sales_answers
from account_app.tests import test_last50_checkout_nov01 as fixtures


class HandoffFacts(TestCase):
    def test_recorded_double_italian_fabric_and_note_season_are_available(self):
        product=dict(fabric='دبل ايطالي',notes='خام شتوي')
        self.assertEqual(customer_copy.fact(product['fabric'],'fabric'),'دبل ايطالي')
        self.assertEqual(product_questions.missing('شنو نوع القماش صيفي لو شتوي',product),[])
        self.assertIn('شتوي',sales_answers.season_fact(product))
        self.assertEqual(customer_copy.fact('قل للزبون قماش دبل ايطالي','fabric'),'')

    def test_fabric_typos_and_catalogue_request_are_not_unknown_questions(self):
        product=dict(fabric='قماش تركي شتوي')
        for text in ('هاي شنو نوعيت القوماش','شنو قناش التنوره'):
            self.assertIn('fabric',sales_answers.topics(text))
            self.assertEqual(product_questions.missing(text,product),[])
        self.assertEqual(product_questions.missing('شنو اكو موديلات',product),[])

    def test_street_name_is_not_a_cut_question(self):
        self.assertNotIn('cut',sales_answers.topics('07701234567\nواسط الكوت شارع الكص'))
        self.assertIn('cut',sales_answers.topics('شنو قصة القطعة'))


class HandoffResume(TestCase):
    setUpClass=classmethod(fixtures.Last50PipelineTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.Last50PipelineTests.tearDownClass.__func__)
    setUp=fixtures.Last50PipelineTests.setUp
    tearDown=fixtures.Last50PipelineTests.tearDown
    save=fixtures.Last50PipelineTests.save
    link=fixtures.Last50PipelineTests.link
    process=fixtures.Last50PipelineTests.process
    client=fixtures.Last50PipelineTests.client

    def skirt(self):
        self.link()
        self.product.update(product_name='تنوره',category='تنورة',fabric='دبل ايطالي',notes='خام شتوي',sizes='38 إلى 48',price='16000')
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')
        self.m.set_store_setting(self.db,'ai_enabled','1','al-fatena')

    def queued(self,text='شنو نوع القماش'):
        self.skirt();self.save('incoming',text)
        self.m.set_customer_ai_enabled(self.db,self.sender,False,reason='unanswered_product_question')
        response=self.client().post(f'/api/conversations/{self.sender}/ai',json={'enabled':True})
        self.assertEqual(response.status_code,202,response.json)
        return dict(self.db.execute('SELECT * FROM ai_jobs WHERE id=?',(response.json['job_id'],)).fetchone())

    def execute(self,job):
        return self.m.run_ai_job(job,json.loads(job['payload']))

    def test_known_fabric_question_answers_without_pausing(self):
        self.skirt()
        self.ev['text']='شنو قماشها صيفي لو شتوي'
        with patch.object(self.m,'extract_facebook_event',return_value=self.ev), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m.ai_transport,'post') as provider:
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        provider.assert_not_called()
        self.assertIn('دبل ايطالي',result['reply'])
        self.assertIn('شتوي',result['reply'])
        self.assertTrue(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertFalse(self.m.has_pending_human_review(self.db,self.sender))

    def test_delayed_webhook_honors_sticker_already_identified_by_archive(self):
        self.skirt()
        mid=self.save('incoming','',kind='sticker')
        ev=dict(self.ev,text='',image_url='https://images.test/thumb.jpg',attachments=[{'type':'image','url':'https://images.test/thumb.jpg'}])
        with patch.object(self.m,'extract_facebook_event',return_value=ev), \
             patch.object(self.m,'match_product') as vision,patch.object(self.m,'create_human_review') as review:
            result=self.m.process_webhook(self.db,{},use_debounce=False,incoming_message_id=mid)
        vision.assert_not_called();review.assert_not_called()
        self.assertEqual(result['meta']['reason'],'non_product_sticker')

    def test_staff_preview_of_a_real_gap_does_not_pause_or_duplicate_reviews(self):
        self.skirt();self.save('incoming','شكد طول التنورة')
        client=self.client()
        for _ in range(3):
            response=client.post(f'/api/conversations/{self.sender}/ask_ai',json={'allow_empty':True})
            self.assertEqual(response.status_code,200)
            self.assertTrue(response.json['needs_human'])
            self.assertIn('الطول',response.json['error'])
        self.assertTrue(self.m.is_customer_ai_enabled(self.db,self.sender))
        self.assertFalse(self.m.has_pending_human_review(self.db,self.sender))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM product_questions WHERE sender_id=?',(self.sender,)).fetchone()[0],0)

    def test_enable_queues_the_reply_instead_of_calling_provider_in_request(self):
        with patch.object(self.m,'auto_reply_after_product_link') as send:
            job=self.queued()
        send.assert_not_called()
        self.assertEqual(job['kind'],'resume')
        with patch.object(self.m,'send_webhook_result_to_facebook',return_value=True):
            result=self.execute(job)
        self.assertTrue(result['ai_enabled'])
        self.assertTrue(result['auto_reply']['sent'])
        self.assertIn('دبل ايطالي',result['auto_reply']['reply'])

    def test_missing_fact_reports_actual_off_state_and_can_retry_after_catalogue_fix(self):
        job=self.queued('شكد طول التنورة')
        result=self.execute(job)
        self.assertTrue(result['ok'])
        self.assertFalse(result['ai_enabled'])
        self.assertTrue(result['needs_human'])
        self.assertIn('الطول',result['error'])
        self.m.ai_jobs.finish(self.db,job['id'],'done',result)
        repeat=self.client().post(f'/api/conversations/{self.sender}/ai',json={'enabled':True})
        repeat_job=dict(self.db.execute('SELECT * FROM ai_jobs WHERE id=?',(repeat.json['job_id'],)).fetchone())
        repeat_result=self.execute(repeat_job)
        self.m.ai_jobs.finish(self.db,repeat_job['id'],'done',repeat_result)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE sender_id=? AND status='pending'",(self.sender,)).fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT occurrences FROM product_questions WHERE sender_id=?',(self.sender,)).fetchone()[0],1)
        self.product['measurements']='الطول: 90 سم'
        response=self.client().post(f'/api/conversations/{self.sender}/ai',json={'enabled':True})
        self.assertEqual(response.status_code,202)
        self.assertNotEqual(response.json['job_id'],job['id'])
        next_job=dict(self.db.execute('SELECT * FROM ai_jobs WHERE id=?',(response.json['job_id'],)).fetchone())
        with patch.object(self.m,'send_webhook_result_to_facebook',return_value=True):result=self.execute(next_job)
        self.assertTrue(result['auto_reply']['sent'],result)
        self.assertTrue(result['ai_enabled'])
        self.assertFalse(self.m.has_pending_human_review(self.db,self.sender))

    def test_stopping_after_queue_prevents_resume_delivery(self):
        job=self.queued()
        self.client().post(f'/api/conversations/{self.sender}/ai',json={'enabled':False})
        with patch.object(self.m,'auto_reply_after_product_link') as send:result=self.execute(job)
        send.assert_not_called()
        self.assertFalse(result['ai_enabled'])

    def test_answered_or_changed_input_is_not_sent_again(self):
        job=self.queued()
        self.save('outgoing','قماشه دبل ايطالي')
        with patch.object(self.m,'auto_reply_after_product_link') as send:result=self.execute(job)
        send.assert_not_called();self.assertEqual(result['reason'],'already_answered')
        self.save('incoming','اريد موديل ثاني')
        with patch.object(self.m,'auto_reply_after_product_link') as send:result=self.execute(job)
        send.assert_not_called();self.assertEqual(result['reason'],'conversation_changed')

    def test_resume_retries_unknown_image_with_saved_context(self):
        job=self.queued('شنو نوع القماش')
        image_id=self.save('incoming','',kind='image',image='https://images.test/current.jpg')
        payload=json.loads(job['payload']);payload['incoming_id']=image_id
        image=dict(product_found=True,product_id=self.product['product_id'],image_url='https://images.test/current.jpg')
        with patch.object(self.m,'match_product',return_value=(self.product,'manual',{'images':[image]})) as match, \
             patch.object(self.m,'auto_reply_after_product_link',return_value={'sent':True,'reply':'متوفر'}):
            result=self.m.run_ai_job(job,payload)
        match.assert_called_once();self.assertTrue(result['auto_reply']['sent'])

    def test_link_and_send_can_resolve_automatic_pause_but_preserves_manual_stop(self):
        self.skirt();self.save('incoming','شنو نوع القماش')
        self.m.set_customer_ai_enabled(self.db,self.sender,False,reason='unanswered_product_question')
        with patch.object(self.m,'find_product_by_id',return_value=self.product), \
             patch.object(self.m,'send_webhook_result_to_facebook',return_value=True):
            result=self.client().post(f'/api/conversations/{self.sender}/link_product',json={'product_id':self.product['product_id']})
        self.assertTrue(result.json['auto_reply']['sent'],result.json)
        self.assertTrue(result.json['ai_resumed'])
        self.m.set_customer_ai_enabled(self.db,self.sender,False,reason='manual')
        with patch.object(self.m,'find_product_by_id',return_value=self.product),patch.object(self.m,'call_main_ai') as provider:
            result=self.client().post(f'/api/conversations/{self.sender}/link_product',json={'product_id':self.product['product_id']})
        provider.assert_not_called();self.assertFalse(result.json['ai_resumed'])
