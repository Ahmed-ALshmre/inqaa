"""Regressions observed after the latest reply release, with all sends intercepted."""
from unittest import TestCase
from unittest.mock import patch
from account_app import checkout,sales_recovery,sales_playbook,sales_context
from account_app.tests import test_last50_checkout_nov01 as fixtures
from account_app.tests import test_handoff_resume_nov05 as resume

RECEIPT=('تم تثبيت الطلب. يرجى فحص الطلب بحضور المندوب والتأكد من الموديل والقياس. '
         'إذا لم يطابق الطلب يرجع مع المندوب بدون دفع. أهم شي فحص الطلب قبل دفع المبلغ.')


class LatestReviewFacts(TestCase):
    def test_how_to_order_and_store_location_are_not_old_order_tracking(self):
        self.assertFalse(checkout.is_existing_order_followup('كيفية الطلب والتوصيل إلى محافظتي؟\nمكانكم وين'))
        self.assertFalse(checkout.is_existing_order_followup('شلون اطلب ومتى يوصل الطلب؟'))
        for text in ('وصيت منكم صارلي اسبوع','طلبي وين ما وصل','كيفية الطلب؟ طلبي السابق ما وصل'):
            self.assertTrue(checkout.is_existing_order_followup(text),text)

    def test_common_shipping_wording_and_cart_totals_keep_their_intent(self):
        for text in ('خدمة توصيل كم','شكد توصيل لمحافظات','شكد توصيل لجميع المحافظات'):
            self.assertEqual(sales_recovery.policy_question(text),'delivery',text)
        for text in ('هسه حسابي جكد\nصار','يعني جكد صار سعرنع','شكد حسابنا'):
            self.assertTrue(sales_playbook.cart_total_question(text),text)
        for text in ('حسابي خلي 20 الف','كم قياس وشنو المجموع','اذا حسابي اقل احجز'):
            self.assertFalse(sales_playbook.cart_total_question(text),text)

    def test_quote_keeps_latest_quantity_and_does_not_copy_another_product(self):
        context=sales_context.advance({},[
            dict(direction='incoming',text='اريد ثنين'),
            dict(direction='incoming',text='اريد واحد 38 وثاني 52')])
        self.assertEqual(sales_playbook.quote_quantity('حسابي شكد',context),2)
        single=sales_context.advance(context,[dict(direction='incoming',text='اريد واحد فقط قياس 44')])
        self.assertEqual(sales_playbook.quote_quantity('حسابي شكد',single),1)
        context.update(selection_product_id='NEW',evidence=[dict(text='اريد ثنين',product_id='OLD')])
        self.assertEqual(sales_playbook.quote_quantity('حسابي شكد',context),1)


class LatestReviewPipeline(TestCase):
    setUpClass=classmethod(fixtures.Last50PipelineTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.Last50PipelineTests.tearDownClass.__func__)
    setUp=fixtures.Last50PipelineTests.setUp
    tearDown=fixtures.Last50PipelineTests.tearDown
    link=fixtures.Last50PipelineTests.link
    save=fixtures.Last50PipelineTests.save
    process=fixtures.Last50PipelineTests.process
    client=fixtures.Last50PipelineTests.client
    order=fixtures.Last50PipelineTests.order

    def test_exact_full_receipt_only_after_saved_order_and_no_duplicate_on_retry(self):
        self.link('اسود')
        self.save('incoming','اريد اسود قياس 44 ثبتي')
        self.save('outgoing','دزيلي رقم الموبايل والعنوان')
        result,_=self.process('07701234567')
        self.assertNotIn('تم تثبيت الطلب',result['reply'])
        self.assertIsNone(self.order())
        result,_=self.process('بغداد حي الاختبار قرب مدرسة الاختبار')
        self.assertEqual(result['reply'],RECEIPT)
        self.assertEqual(result['reply_parts'],[RECEIPT])
        self.assertIsNotNone(self.order())
        result,_=self.process('تم')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0],1)

    def test_default_and_old_short_receipts_upgrade_but_owner_custom_receipt_is_preserved(self):
        for saved in ('','تم تثبيت الطلب عيوني 🌷',RECEIPT):
            with patch.object(self.m,'get_store_settings',return_value={'inspection_message':saved}):
                self.assertEqual(self.m.get_order_confirmation_text(self.db),RECEIPT)
        with patch.object(self.m,'get_store_settings',return_value={'inspection_message':'تأكيد خاص بالمتجر'}):
            self.assertEqual(self.m.get_order_confirmation_text(self.db),'تأكيد خاص بالمتجر')

    def test_shipping_question_without_product_does_not_ask_for_photo(self):
        result,model=self.process('خدمة توصيل كم')
        model.assert_not_called()
        self.assertIn('5',result['reply'])
        self.assertNotIn('صورة',result['reply'])
        self.assertFalse(self.m.has_pending_human_review(self.db,self.sender))

    def test_payment_question_is_answered_even_for_an_unavailable_ad_product(self):
        self.link();self.product.update(stock='نفذ',status='active')
        result,model=self.process('هل يمكنني الدفع عند الاستلام؟')
        model.assert_not_called()
        self.assertIn('الدفع يصير عند الاستلام',result['reply'])
        self.assertNotIn('ما متوفر',result['reply'])
        self.assertIsNone(self.order())

    def test_policy_opening_keeps_a_unique_known_ad_bound_for_the_next_turn(self):
        self.link();self.product['ad_id']='TEST-AD'
        self.db.execute('DELETE FROM customer_product_interests WHERE sender_id=?',(self.sender,));self.db.commit()
        self.ev['ad_id']='TEST-AD'
        result,model=self.process('شكد توصيل لمحافظات')
        model.assert_not_called()
        self.assertIn('5',result['reply'])
        self.assertEqual(self.m.get_active_product_binding(self.db,self.sender)['product_id'],self.product['product_id'])
        self.assertFalse(self.m.has_pending_human_review(self.db,self.sender))
        self.assertIsNone(self.order())

    def test_cart_total_remembers_two_pieces_and_different_sizes_without_booking(self):
        self.link('اسود');self.product.update(price='16000',offer='',sizes='38 إلى 52')
        for direction,text in [('incoming','اريد ثنين'),('outgoing','قطعتين بـ32 ألف والتوصيل 5 آلاف'),
                ('incoming','اريد واحد 38 وثاني 52'),('outgoing','تحبين اثبتلج الحجز؟')]:
            self.save(direction,text)
        result,model=self.process('هسه حسابي جكد\nصار')
        model.assert_not_called()
        self.assertIn('32 ألف',result['reply']);self.assertIn('37 ألف',result['reply'])
        self.assertNotIn('16 ألف',result['reply']);self.assertIsNone(self.order())

    def test_negotiating_on_a_known_photo_does_not_switch_to_a_generic_dress_sku(self):
        self.link('احمر بنفسجي')
        self.product.update(product_name='دشداشة قديفة',category='دشداشة',price='12000',offer='قطعتين ب20 الف')
        self.save('incoming','متوفرات')
        self.save('outgoing','متوفر، اللون أحمر موجود وسعره 12 ألف')
        other=dict(self.product,product_id='OTHER',product_name='فستان',colors='وردي سماوي')
        catalog=[self.product,other]
        with patch.object(self.m,'load_active_products',return_value=catalog),patch.object(self.m,'load_products_from_file',return_value=catalog):
            self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')
            result,_=self.process('واذا اخذ 2\nيصير الفستان ب10')
            self.assertEqual(self.m.get_active_product_binding(self.db,self.sender)['product_id'],self.product['product_id'])
        self.assertNotIn('وردي وسماوي',result['reply'])
        self.assertIsNone(self.order())

    def test_manual_confirmation_uses_the_same_full_text(self):
        self.link('اسود')
        with patch.object(self.m,'send_text_to_facebook',return_value=True) as send:
            response=self.client().post(f'/api/conversations/{self.sender}/create_order',json={
                'product_name':'قطعة اختبار','quantity':1,'price':12000})
        self.assertEqual(response.status_code,200,response.json)
        self.assertIn(RECEIPT,send.call_args.args)

    def test_paused_image_callback_is_not_a_failed_webhook_but_transport_failure_stays_failed(self):
        self.link();self.m.set_customer_ai_enabled(self.db,self.sender,False,reason='manual')
        for meta,expected in [({'auto_reply':{'sent':False,'reason':'ai_disabled_for_conversation'}},True),
                ({'skipped':True,'auto_reply':{'sent':False,'reason':'ai_globally_disabled'}},True),
                ({'auto_reply':{'sent':False,'needs_human':True}},True),
                ({'auto_reply':{'sent':False,'reason':'delivery_failed'}},False),
                ({'service_error':True},False)]:
            with patch.object(self.m,'process_webhook',return_value={'reply':'','meta':meta}), \
                 patch.object(self.m,'extract_facebook_event',return_value=self.ev), \
                 patch.object(self.m,'send_webhook_result_to_facebook') as send:
                result=self.m._process_manychat_webhook_async_locked({},self.sender,'facebook')
            self.assertEqual(result['ok'],expected,meta)
            send.assert_not_called()
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))


class LatestReviewResume(TestCase):
    setUpClass=resume.HandoffResume.__dict__['setUpClass']
    tearDownClass=resume.HandoffResume.__dict__['tearDownClass']
    setUp=resume.HandoffResume.setUp
    tearDown=resume.HandoffResume.tearDown
    save=resume.HandoffResume.save
    link=resume.HandoffResume.link
    client=resume.HandoffResume.client
    skirt=resume.HandoffResume.skirt
    queued=resume.HandoffResume.queued
    execute=resume.HandoffResume.execute

    def test_known_fabric_does_not_depend_on_a_model_deciding_to_handoff(self):
        self.skirt()
        with patch.object(self.m,'_call_main_ai_once',return_value={'reply':'','requires_human':True}) as model:
            result=self.m.call_main_ai(dict(self.ev,text='شنو نوعية القماش'),'text',{},[],[self.product],self.product,None,'',[],customer_products=[self.product])
        model.assert_not_called()
        self.assertIn('دبل ايطالي',result['reply'])
        self.assertFalse(result.get('requires_human'))

    def test_worker_records_an_intentional_review_as_completed_not_a_system_error(self):
        job=self.queued('شكد طول التنورة')
        result=self.execute(job)
        self.assertTrue(result['needs_human'])
        self.assertIn('الطول',result['error'])
        self.m.ai_jobs.finish_result(self.db,job['id'],result)
        self.assertEqual(self.db.execute('SELECT status FROM ai_jobs WHERE id=?',(job['id'],)).fetchone()[0],'done')
        self.m.ai_jobs.finish_result(self.db,job['id'],dict(ok=False,error='provider failure',needs_human=True))
        self.assertEqual(self.db.execute('SELECT status FROM ai_jobs WHERE id=?',(job['id'],)).fetchone()[0],'failed')
        self.m.ai_jobs.finish_result(self.db,job['id'],dict(ok=True,error='image service failure',needs_human=True,service_error=True))
        self.assertEqual(self.db.execute('SELECT status FROM ai_jobs WHERE id=?',(job['id'],)).fetchone()[0],'failed')
