"""Anonymized shapes from the Nov 6 live audit; no customer delivery is allowed."""
from unittest import TestCase
import json
from unittest.mock import patch
from account_app import sales_context,sales_playbook,sales_voice,sales_answers,checkout,conversation_quality
from account_app.tests import test_last50_checkout_nov01 as fixtures
from account_app.tests import test_handoff_resume_nov05 as resume


class LiveStopMemory(TestCase):
    def test_available_size_in_opening_turn_is_remembered_without_purchase_consent(self):
        for text in ('متوفر 40','٤٠ متوفر','متوفر ٤٠؟'):
            context=sales_context.advance({},[dict(direction='incoming',text=text)])
            self.assertEqual(sales_playbook.customer_selection(context)['size'],'40',text)
            self.assertFalse(sales_playbook.checkout_consent(context))

    def test_iraqi_plural_quantity_survives_later_contacts_and_price_questions(self):
        for text in ('رايدين قطعتين دشداشة','رايده قطعتين','نريد قطعتين'):
            context=sales_context.advance({},[dict(direction='incoming',text=text),
                dict(direction='incoming',text='لون أحمر و أخضر'),dict(direction='incoming',text='قياس 38')])
            self.assertEqual(sales_playbook.quote_quantity('بيش سعره مع التوصيل',context),2,text)
            self.assertEqual(sales_playbook.quote_quantity('اريد واحد فقط',context),1)

    def test_short_total_question_is_price_but_length_and_shipping_only_stay_separate(self):
        for text in ('اشكد يطلع','شكد يطلع الحساب','هسه شكد صار'):
            self.assertTrue(sales_playbook.cart_total_question(text),text)
            self.assertIn('price',sales_answers.topics(text),text)
        for text in ('شكد يطلع طولها','شكد التوصيل','شكد قياس يلبس'):
            self.assertFalse(sales_playbook.cart_total_question(text),text)

    def test_if_i_order_when_does_it_arrive_is_not_a_guaranteed_deadline(self):
        for text in ('حبيبتي اذا اطلب شگد وتوصل يوم يومين ها','اذا احجز شوكت يوصل','اذا اطلب يوصل يوم يومين؟'):
            self.assertNotEqual(checkout.unresolved_purchase_terms(text),'conditional_purchase_terms',text)
        for text in ('اذا يوصل باجر اريد','بشرط يوصل باجر','اذا مو يومين ما اريد'):
            self.assertEqual(checkout.unresolved_purchase_terms(text),'conditional_purchase_terms',text)

    def test_browsing_size_is_not_a_request_to_change_an_existing_order(self):
        self.assertIsNone(conversation_quality.operational_request('لا فساتين أكثر قياس 40'))
        self.assertEqual(conversation_quality.operational_request('بدلي القياس الى 40')['kind'],'size')
        self.assertEqual(conversation_quality.operational_request('لا اني طالبه قياس 40')['kind'],'size')

    def test_fit_doubts_are_not_replaced_with_stock_confirmation(self):
        product=dict(product_id='P',sizes='38 إلى 48',colors='أسود',price='16000',stock='متوفر')
        for text in ('أخاف قياس 40 يطلع جبير علي','هو قياس L نفسه 44 لو يحتاج أكبر','يعني شنو قياس 38'):
            self.assertIsNone(sales_voice.answer(text,product,{}, {},[],{}),text)


class LiveStopPipeline(TestCase):
    setUpClass=classmethod(fixtures.Last50PipelineTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.Last50PipelineTests.tearDownClass.__func__)
    setUp=fixtures.Last50PipelineTests.setUp
    tearDown=fixtures.Last50PipelineTests.tearDown
    link=fixtures.Last50PipelineTests.link
    save=fixtures.Last50PipelineTests.save
    process=fixtures.Last50PipelineTests.process
    order=fixtures.Last50PipelineTests.order

    def test_known_product_price_followup_keeps_the_photo_binding(self):
        self.link();self.product.update(product_name='تنورة تركي',price='16000')
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')
        self.save('incoming','شنو قماشها')
        self.save('outgoing','القماش دبل إيطالي، شكد قياس تلبسين؟')
        result,model=self.process('اشكد سعر')
        self.assertNotIn('دزيلي صورة',result['reply'])
        self.assertIn('16',result['reply'],(result,model.call_args))

    def test_saved_two_piece_quantity_is_used_for_shipping_total(self):
        self.link('أحمر أخضر');self.product.update(price='12000',offer='قطعتين ب20 الف')
        self.save('incoming','رايدين قطعتين')
        self.save('incoming','قياس 38 لون أحمر و أخضر')
        result,model=self.process('بيش سعره مع توصيل')
        model.assert_not_called()
        self.assertIn('25 ألف',result['reply'])
        self.assertNotIn('17 ألف',result['reply'])
        self.assertIsNone(self.order())

    def test_known_bundle_sizes_do_not_depend_on_provider_credit(self):
        self.link();self.product.update(product_name='بكج ولادي',sizes='من 1 الى 4 سنة',
            bundle_contents='بكج مكون من قطعتين طقم و تراك',sale_unit='بكج فقط',price='28000',delivery='توصيل مجاني')
        with patch.object(self.m,'_call_main_ai_once',return_value=dict(reply='',failed=True,failure_reason='provider_http_402')) as model:
            result=self.m.call_main_ai(dict(self.ev,text='عيني هذا الطقم شكد عمر يلبس'),'text',{},[],[self.product],self.product,None,'',[])
        model.assert_not_called()
        self.assertIn('1',result['reply']);self.assertIn('4',result['reply'])
        self.assertFalse(result.get('requires_human'))

    def test_known_fabric_and_length_answer_together_without_provider(self):
        self.link();self.product.update(product_name='تنورة',fabric='دبل ايطالي',measurements='طول التنورة 105 سم')
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai(dict(self.ev,text='نوع قوماش\nطول تنوره'),'text',{},[],[self.product],self.product,None,'',[])
        model.assert_not_called()
        self.assertIn('دبل ايطالي',result['reply']);self.assertIn('105',result['reply'])

    def test_true_unknown_closure_still_hands_off_silently(self):
        self.link();self.product.update(closure='',description='',notes='',fabric='قطن')
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai(dict(self.ev,text='الكمر لاستيك لو سحاب؟'),'text',{},[],[self.product],self.product,None,'',[])
        model.assert_not_called();self.assertEqual(result['reply'],'');self.assertTrue(result['requires_human'])

    def test_unstocked_shirts_and_scarves_do_not_confirm_or_book_the_old_skirt(self):
        self.link();self.product.update(product_name='تنورة',category='تنورة')
        for text in ('اريد قمصان هم موجود؟','اريد وشاحات متوفر','لا صور القمصان'):
            with patch.object(self.m,'_call_main_ai_once') as model:
                result=self.m.call_main_ai(dict(self.ev,text=text),'text',{},[],[self.product],self.product,None,'',[])
            model.assert_not_called()
            self.assertIn('ما عدنا',result['reply']);self.assertFalse(result['create_order'])
            self.assertFalse(result.get('requires_human'));self.assertTrue(result['_suppress_product_images'])

    def test_existing_shirt_and_component_question_keep_their_routes(self):
        self.link();self.product.update(product_name='قميص',category='قميص')
        self.assertIsNone(sales_voice.other_category_reply('قميص متوفر؟',self.product,[self.product]))
        self.assertIsNone(sales_voice.other_category_reply('القميص يجي ويا الطقم؟',{},[self.product]))

    def test_fit_explanation_is_preserved_after_generation(self):
        self.link()
        text='أخاف قياس 40 يطلع جبير علي'
        expected='المقاس المتوفر 40، ونحتاج قياسات الجسم حتى نتأكد من ملاءمته.'
        with patch.object(self.m,'_call_main_ai_with_context',return_value=dict(reply=expected,create_order=False,order={})):
            result=self.m.call_main_ai(dict(self.ev,text=text),'text',{},[],[self.product],self.product,None,'',[])
        self.assertIn('ملاءمته',result['reply']);self.assertNotIn('إي، قياسج موجود',result['reply'])

    def test_new_category_question_never_restores_an_old_ready_cart(self):
        self.link();self.product.update(product_name='تنورة',category='تنورة')
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')
        self.save('incoming','اريد تنورة اسود قياس 38')
        self.save('outgoing','دزيلي رقم الموبايل والعنوان')
        self.save('incoming','07701234567 بغداد حي الاختبار')
        result,model=self.process('اريد وشاحات متوفر')
        model.assert_not_called()
        self.assertIn('ما عدنا',result['reply']);self.assertIsNone(self.order())

    def test_two_colors_and_two_pieces_book_once_on_contact_without_a_second_yes(self):
        self.link('أحمر أخضر');self.product.update(price='12000',offer='قطعتين ب20 الف')
        self.save('incoming','رايدين قطعتين')
        self.save('incoming','قياس 38 للقطعتين لون أحمر و أخضر')
        self.save('outgoing','دزيلي رقم الموبايل والعنوان')
        result,model=self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called()
        self.assertIn('تم تثبيت الطلب',result['reply'])
        order=self.order();items=json.loads(order['order_items'])
        self.assertEqual({(i['color'],i['size'],i['quantity']) for i in items},{('أحمر','38',1),('أخضر','38',1)})
        self.assertEqual(order['total_amount'],25000)
        self.process('تم');self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],1)

    def test_unallocated_four_pieces_and_two_colors_do_not_guess_a_cart(self):
        self.link('أحمر أخضر')
        context=sales_context.advance({},[dict(direction='incoming',text='اريد 4 قطع'),
            dict(direction='incoming',text='قياس 38 لون أحمر و أخضر')])
        self.assertIsNone(sales_playbook.same_size_color_cart(self.product,context))

    def test_different_color_sizes_are_not_flattened_to_the_last_size(self):
        self.link('أحمر أخضر')
        context=sales_context.advance({},[dict(direction='incoming',text=text) for text in
            ('اريد قطعتين','الاحمر قياس 44','الاخضر قياس 46','اريد الاحمر والاخضر')])
        self.assertIsNone(sales_playbook.same_size_color_cart(self.product,context))

    def test_later_single_piece_correction_cannot_resurrect_two_pieces(self):
        self.link('أحمر أخضر')
        context=sales_context.advance({},[dict(direction='incoming',text=text) for text in
            ('اريد قطعتين','قياس 38 لون أحمر و أخضر','اريد واحد فقط')])
        self.assertIsNone(sales_playbook.same_size_color_cart(self.product,context))


class LiveStopRetry(TestCase):
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

    def test_actual_billing_failure_explains_reason_to_employee_without_a_customer_reply(self):
        job=self.queued('هذا مناسب للسفر؟')
        failure=dict(reply='',failed=True,failure_reason='provider_http_402',create_order=False)
        with patch.object(self.m,'call_main_ai',return_value=failure),patch.object(self.m,'send_webhook_result_to_facebook') as send:
            result=self.execute(job)
        send.assert_not_called()
        self.assertIn('رصيد',result['error'])
        self.assertEqual(result['reason'],'provider_http_402')
        self.assertTrue(result['service_error'])

    def test_transport_failure_is_distinct_from_a_completed_human_handoff(self):
        token=self.m._last_transport_result.set(dict(status='rejected',status_code=403))
        try:
            failure=self.m.linked_reply_delivery_result({'reply':'رد اختبار'},False)
            self.assertTrue(failure['service_error']);self.assertIn('HTTP 403',failure['error'])
            handoff=self.m.linked_reply_delivery_result({'reply':'','meta':{'needs_human':True}},False)
            self.assertTrue(handoff['needs_human']);self.assertFalse(handoff.get('service_error'))
            changed=self.m.linked_reply_delivery_result({'reply':'','meta':{'superseded':True}},False)
            self.assertEqual(changed['reason'],'conversation_changed');self.assertFalse(changed.get('service_error'))
        finally:self.m._last_transport_result.reset(token)
