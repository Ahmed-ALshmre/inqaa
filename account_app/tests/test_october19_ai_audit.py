import unittest
from unittest.mock import Mock, patch

from account_app import sales_context, sales_recovery
from account_app.tests import test_audit_orders as fixtures


CATALOG = [
    dict(product_id='A',product_name='فستان شتوي',price='10000',stock='متوفر',status='active',
         colors='وردي، جوزي، أحمر',sizes='1 إلى 7 سنوات',image_url='https://images.test/a.jpg'),
    dict(product_id='B',product_name='طقم بناتي تركي',price='15000',stock='متوفر',status='active',
         colors='وردي، أسود، كريمي',sizes='1 إلى 7 سنوات',image_url='https://images.test/b.jpg'),
]


class October19AuditTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def test_shipping_questions_from_backup_answered_locally_instead_of_asking_for_photo(self):
        self.customer('policy')
        self.m.save_delivery_settings(self.db, {'baghdad_fee':4000,'other_fee':7000,'delivery_time':'من يومين إلى ثلاثة أيام'})
        for text, amount in [('توصيل شكد', '7,000'),('وتوصيل ميسان شكد؟','7,000'),
                             ('شكد التوصيل لبغداد؟','4,000'),('كم تبلغ تكلفة التوصيل؟','7,000'),
                             ('اكو توصيل','7,000'),('ما هي طريقة التوصيل؟','7,000')]:
            with self.subTest(text=text), patch.object(self.m.ai_transport,'post') as provider:
                reply, _ = self.m.generate_first_message_reply(self.db,{'text':text,'sender_id':'policy'},[], '', [])
                provider.assert_not_called()
                self.assertIn(amount,reply)
                self.assertNotIn('صورة',reply)
        result=self.m.store_policy_question_reply('متى يتم التوصيل؟')
        self.assertIn('يومين إلى ثلاثة',result['reply'])
        self.assertFalse(result['create_order'])

    def test_shipping_policy_does_not_drop_other_parts_of_customer_request(self):
        for text in ['اريد قطعتين نفس اللون والتوصيل مجاني؟','السعر مع التوصيل بشكد للموصل',
                     'دزي صور الموديلات وشكد التوصيل','اذا بيهم خلل ما استلمهم وانطي توصيل']:
            self.assertEqual(sales_recovery.policy_question(text),'',text)

    def test_store_shipping_fee_and_free_product_offer_stay_separate(self):
        token=self.m._current_store_id.set('khuyoot')
        try:
            self.m.save_delivery_settings(self.db,{'other_fee':8000,'baghdad_fee':6000})
            self.assertIn('8,000',self.m.store_policy_question_reply('شكد التوصيل للبصرة؟')['reply'])
        finally:
            self.m._current_store_id.reset(token)
        self.assertNotIn('8,000',self.m.store_policy_question_reply('شكد التوصيل؟')['reply'])
        product=dict(CATALOG[0],delivery='توصيل مجاني')
        self.assertIn('مجاني',self.m.store_policy_question_reply('شكد التوصيل؟',product)['reply'])

    def test_short_photo_reply_sends_previously_offered_models_without_model_or_purchase(self):
        history=[{'direction':'outgoing','text':'عمر 4 سنوات متوفر بيه:\nفستان شتوي، وطقم بناتي تركي'},
                 {'direction':'outgoing','text':'تحبين تشوفين صورهم؟'}]
        for text in ['دزو','دزهن','شوفينياهن','اي','اشوف صورته']:
            with self.subTest(text=text), patch.object(self.m,'_call_main_ai_once') as provider:
                result=self.m.call_main_ai({'text':text},'text',{},history,CATALOG,None,None,'',[])
                provider.assert_not_called()
                self.assertEqual(set(result['image_product_ids']),{'A','B'})
                self.assertEqual(set(self.m.requested_reply_images(result,CATALOG,None,text,result['reply'])),
                                 {'https://images.test/a.jpg','https://images.test/b.jpg'})
                self.assertFalse(result['create_order'])
                self.assertFalse(result['requires_human'])

    def test_photo_of_recent_alternative_wins_over_old_matched_product(self):
        history=[{'direction':'outgoing','text':'متوفر طقم بناتي تركي، تحبين تشوفين صورته؟'}]
        with patch.object(self.m,'_call_main_ai_once') as provider:
            result=self.m.call_main_ai({'text':'اشوف صورته'},'text',{},history,CATALOG,CATALOG[0],None,'',[])
        provider.assert_not_called()
        self.assertEqual(result['image_product_ids'],['B'])

    def test_same_named_models_use_the_offered_price_and_fabric_not_all_name_matches(self):
        models=[dict(CATALOG[0],product_name='سوت',price='15000',fabric='لينن'),
                dict(CATALOG[1],product_name='سوت',price='17000',fabric='قماش فرسان درجة أولى')]
        history=[{'direction':'outgoing','text':'متوفر سوت قماش فرسان درجة أولى، سعره 17 ألف.'},
                 {'direction':'outgoing','text':'تحبين نثبته الج؟'}]
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai({'text':'اشوف صورته'},'text',{},history,models,models[0],None,'',[])
        model.assert_not_called()
        self.assertEqual(result['image_product_ids'],['B'])
        self.assertEqual(self.m.requested_reply_images(result,models,models[0],'اشوف صورته',result['reply']),
                         ['https://images.test/b.jpg'])
        self.assertEqual(self.m.requested_reply_images({'image_product_ids':['A']},models,models[0],
                         'اشوف صورته','هاي صور سوت قماش فرسان سعره 17 ألف'),['https://images.test/b.jpg'])

    def test_rejected_or_unavailable_model_is_not_restored_by_short_photo_reply(self):
        context=sales_context.advance({},[{'direction':'outgoing','text':'فستان شتوي، تحبين صورته؟'},
                                         {'direction':'incoming','text':'لا ما اريد هذا'}])
        ev={'text':'دزو','_sales_context':context}
        self.assertIsNone(self.m.offered_product_photo_reply(ev,CATALOG,CATALOG[0]))
        unavailable=[dict(CATALOG[0],stock='نفذ')]
        ev={'text':'اشوف صورته','_sales_context':{'last_staff_turn':[{'text':'فستان شتوي'}]}}
        self.assertIsNone(self.m.offered_product_photo_reply(ev,unavailable,unavailable[0]))

    def test_photo_shortcut_does_not_drop_fabric_or_additional_photo_questions(self):
        for text in ['شنو الالوان وقماشها\nاكو صوره الها','قماشها شنو\nاكو تصوير حقيقي\nللقطعة',
                     'اي التنوره\nدريت صورتها']:
            self.assertFalse(sales_recovery.simple_photo_request(text),text)
            self.assertIsNone(self.m.offered_product_photo_reply({'text':text},CATALOG,CATALOG[0]))

    def test_policy_queue_has_no_collection_delay_while_orders_keep_it(self):
        self.customer('fast-faq')
        with patch.object(self.m,'DEBOUNCE_DELAY',35), patch.object(self.m.zernio,'enabled_for_store',return_value=False):
            for text in ['شكد التوصيل؟','اريد قطعتين اسود قياس 44']:
                body={'entry':[{'id':'page','_store_id':'default','messaging':[{
                    'sender':{'id':'fast-faq'},'message':{'mid':text,'text':text}}]}]}
                self.m._process_manychat_webhook_async(body,'fast-faq','facebook',background_after_intake=True)
        jobs=self.db.execute('SELECT created_at,available_at FROM ai_jobs ORDER BY rowid').fetchall()
        self.assertEqual(len(jobs),2)
        self.assertAlmostEqual(jobs[0]['available_at']-jobs[0]['created_at'],0)
        self.assertAlmostEqual(jobs[1]['available_at']-jobs[1]['created_at'],35)

    def test_claim_to_have_sent_models_is_detected_even_without_word_photo(self):
        text='تدللين عيني، شوفي الخاص دزيتلج الموديلات الشرعية السادة المتوفرة عدنا، كليلي يا قياس يناسبج؟'
        self.assertTrue(self.m._promises_product_photo(text))
        self.assertFalse(sales_recovery.claims_sent_models('ما دزيتلج الموديلات بعد'))
        self.assertFalse(self.m._promises_product_photo('دزيلي صورة الموديل'))

    def test_first_message_usage_is_recorded_once_with_provider_cost(self):
        self.customer('usage')
        payload={'choices':[{'message':{'content':'{"reply":"هلا بيج، شنو حابة تشوفين؟"}'},'finish_reason':'stop'}],
                 'usage':{'prompt_tokens':100,'completion_tokens':20,'cost':0.001}}
        response=Mock();response.json.return_value=payload
        with patch.object(self.m,'OPENROUTER_KEY','test-key'),patch.object(self.m.ai_transport,'post',return_value=response):
            reply,_=self.m.generate_first_message_reply(self.db,{'text':'اريد اشوف الجديد','sender_id':'usage'},[], '', [])
        row=self.db.execute("SELECT * FROM ai_usage_events WHERE purpose='first_message'").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual((row['prompt_tokens'],row['completion_tokens'],row['cost']),(100,20,.001))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM ai_usage_events WHERE purpose='first_message'").fetchone()[0],1)

    def test_image_match_usage_records_cost_without_changing_match(self):
        payload={'choices':[{'message':{'content':'A'},'finish_reason':'stop'}],
                 'usage':{'prompt_tokens':200,'completion_tokens':1,'cost':0.002}}
        response=Mock();response.json.return_value=payload
        with patch.object(self.m,'OPENROUTER_KEY','test-key'), \
             patch.object(self.m,'is_store_feature_enabled',return_value=True), \
             patch.object(self.m,'_image_ref_for_openrouter',side_effect=lambda value:value), \
             patch.object(self.m.ai_transport,'post',return_value=response):
            result=self.m.confirm_with_vision('https://images.test/customer.jpg',CATALOG)
        self.assertEqual(result['product_id'],'A')
        row=self.db.execute("SELECT * FROM ai_usage_events WHERE purpose='vision_match'").fetchone()
        self.assertEqual((row['prompt_tokens'],row['completion_tokens'],row['cost']),(200,1,.002))


if __name__ == '__main__':
    unittest.main()
