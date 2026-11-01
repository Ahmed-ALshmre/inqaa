"""Regressions from the owner's reading of the diverse conversation report."""
import json
import unittest
from unittest.mock import patch
from account_app import checkout, sales_answers, product_questions, sales_voice, reply_layout, conversation_quality
from account_app.tests import test_booking_failures_nov01 as fixtures


class ReviewFactsTests(unittest.TestCase):
    def test_delivered_pair_price_paraphrases_are_answerable_catalog_questions(self):
        product=dict(product_id='K',price='12000',offer='قطعتين ب20 الف',colors='أخضر برتقالي')
        for text in ('شكد القطعتين واصلات لبغداد؟','بكم القطعتين مع التوصيل لبغداد؟',
                     'إذا قطعتين شكد؟ مو أريد أحجز هسه بس أقارن',
                     'شكد الأول وشكد الثاني وشنو قماش كل واحد؟'):
            with self.subTest(text=text):
                self.assertIn('price',sales_answers.topics(text))
                # Fabric is separately supplied when the question requests it.
                self.assertFalse(product_questions.missing(text,dict(product,fabric='قطن')))
        self.assertIn('delivery',sales_answers.topics('الفستان بكم واصل لبغداد؟'))

    def test_child_cart_validation_uses_corrected_ages(self):
        product=dict(product_id='K',sizes='من 2 الى 7 سنوات')
        history=['أريد قطعتين أسود، وحدة لعمر ثلاث سنوات ووحدة لعمر ست سنوات',
                 'صححي الأولى أربع سنوات مو ثلاث، الثانية خليها ست سنوات','اي حجزيهم بهذي الأعمار']
        def items(ages):return [dict(product_id='K',size=f'{age} سنوات',quantity=1) for age in ages]
        self.assertFalse(checkout.child_cart_error(items([4,6]),history,[product]))
        self.assertTrue(checkout.child_cart_error(items([3,6]),history,[product]))
        self.assertTrue(checkout.child_cart_error(items([4]),history,[product]))

    def test_available_selection_can_invite_contacts_but_questions_cannot(self):
        p=dict(product_id='K',colors='أسود',sizes='من 38 الى 52',stock='متوفر')
        self.assertTrue(sales_voice.buying_choice('أريد الأسود قياس 44',p))
        for text in ('قياس 44 موجود؟','إذا أريد الأسود قياس 44 بكم؟','ما أريد الأسود قياس 44','أريد الأخضر قياس 44'):
            self.assertIsNone(sales_voice.buying_choice(text,p))

    def test_booking_status_variants_keep_actions_and_specifications_separate(self):
        for text in ('صار بعيونج ثبت الطلب؟','يعني صار الحجز؟','صار الحجز لو بعد؟'):
            self.assertTrue(conversation_quality.booking_status_question(text),text)
        for text in ('ألغي الطلب','يعني اعتمدتي 46 مو 44؟','ثبت الطلب الأسود 44'):
            self.assertFalse(conversation_quality.booking_status_question(text),text)

    def test_compound_questions_are_answered_even_when_customer_defers_booking(self):
        product=dict(product_id='K',product_name='قديفة',price='12000',offer='قطعتين ب20 الف',
                     stock='متوفر',fabric='قديفة ثقيل',colors='ماروني أزرق',sizes='38 إلى 52')
        result=sales_voice.answer('شنو القماش والسعر وسعر القطعتين والتوصيل لبغداد؟ بعدني بس أسأل لا تثبتين شي',
                                  product,{}, {},[],{'baghdad_fee':5000,'other_fee':5000})
        self.assertIsNotNone(result)
        for amount in ('12 ألف','20 ألف','5 آلاف'):self.assertIn(amount,result['reply'])
        self.assertNotIn('أحجزلج',result['reply']);self.assertNotIn('دزيلي',result['reply'])

    def test_neutral_contact_wording_has_only_one_booking_noun(self):
        from account_app import owner_sales
        result=owner_sales.surface('دزيلي الرقم حتى أثبتلج الطلب.','',warm=False)
        self.assertNotIn('الحجز الطلب',result)
        self.assertIn('أثبت الطلب',result)

    def test_two_child_allocation_prices_two_pieces_without_quantity_keyword(self):
        product=dict(product_id='K',price='12000',offer='قطعتين ب20 الف',colors='أخضر برتقالي',
                     sizes='2 إلى 7 سنوات',stock='متوفر')
        question='عندي طفلين، واحد عمره 5 سنوات وواحد 6 سنوات. أريد للأول أخضر وللثاني برتقالي، متوفرات؟ وبيش السعر واصل لبغداد؟'
        result=sales_voice.answer(question,product,{}, {},[],{'baghdad_fee':5000,'other_fee':5000})
        self.assertIn('المجموع 25 ألف',result['reply'])
        self.assertNotIn('المجموع 17',result['reply'])
        self.assertFalse(product_questions.missing('شلون المجموع 17 ألف؟ القطعتين بـ20 والتوصيل5 يعني25 مو17',product))

    def test_feminine_transfer_request_is_a_real_human_request(self):
        from account_app import sales_playbook
        for text in ('حبيبتي حوليني لموظفة تجاوبني','حولوني لموظف','أريد موظفة'):
            self.assertTrue(sales_playbook.human_requested(text))

    def test_related_short_answer_stays_one_message_and_long_answers_keep_facts(self):
        text='إي، قياسج موجود والأسود موجود. دزيلي الرقم والعنوان حتى أثبتلج الطلب.'
        plan=reply_layout.plan_sales_bubbles({'_human_sales_voice':True},text,[])
        self.assertEqual(plan['reply_parts'],[text])
        long=' '.join(f'تفصيل إضافي {i} عن القطعة.' for i in range(35))
        plan=reply_layout.plan_sales_bubbles({'_human_sales_voice':True},long,[])
        self.assertGreater(len(plan['reply_parts']),1)
        self.assertEqual(reply_layout.compact(' '.join(plan['reply_parts'])),long)


class ReviewPipelineTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.BookingPipelineNov01Tests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.BookingPipelineNov01Tests.tearDownClass.__func__)
    setUp=fixtures.BookingPipelineNov01Tests.setUp
    tearDown=fixtures.BookingPipelineNov01Tests.tearDown
    process=fixtures.BookingPipelineNov01Tests.process
    save=fixtures.BookingPipelineNov01Tests.save
    count=fixtures.BookingPipelineNov01Tests.count

    def test_same_color_two_children_book_after_age_correction_and_split_contacts(self):
        for text in ('أريد قطعتين أسود، وحدة لعمر ثلاث سنوات ووحدة لعمر ست سنوات',
                     'صححي الأولى أربع سنوات مو ثلاث، الثانية خليها ست سنوات','اي حجزيهم بهذي الأعمار',
                     '07701234567','بغداد حي الاختبار قرب مدرسة التجربة'):
            self.save('incoming',text)
            self.m.capture_customer_contact(self.db,self.sender,text)
        # A correctly resolved semantic cart must not be rejected against the
        # superseded ages. The real-provider dialogue separately covers parsing.
        cart=dict(create_order=True,order=dict(items=[dict(product_id='K1',color='أسود',size=f'{age} سنوات',quantity=1) for age in (4,6)]))
        created,reply=self.m.create_order_if_valid(self.db,self.sender,cart,self.product)
        self.assertTrue(created,reply);self.assertEqual(self.count(),1)
        row=self.db.execute('SELECT order_items FROM orders WHERE sender_id=?',(self.sender,)).fetchone()
        items=json.loads(row[0]);self.assertEqual(sorted(i['size'] for i in items),['4 سنوات','6 سنوات'])
        self.assertEqual(sum(i['quantity'] for i in items),2)
        self.process('صار الحجز لو بعد؟');self.assertEqual(self.count(),1)

    def test_later_fabric_answer_does_not_greet_or_reask_known_size(self):
        self.product.update(sizes='من 38 الى 52',fabric='باربي درجة أولى')
        history=[dict(direction='incoming',text='قياسي 44'),dict(direction='outgoing',text='قياس 44 موجود.')]
        result=self.m.owner_result_surface(dict(reply='أهلا وسهلا، القماش باربي درجة أولى. شكد قياس تلبسين؟'),
            dict(self.ev,text='القماش باربي لو نوع ثاني؟',_reply_product=self.product),{},history)
        self.assertNotIn('أهلا',result['reply']);self.assertNotIn('شكد قياس',result['reply'])
        self.assertIn('باربي',result['reply'])

    def test_selection_with_known_address_requests_only_phone(self):
        self.product.update(sizes='من 38 الى 52')
        self.m.capture_customer_contact(self.db,self.sender,'بغداد حي الاختبار قرب مدرسة التجربة')
        result=self.m.owner_result_surface(dict(reply='إي الأسود موجود.',create_order=False),
            dict(self.ev,text='أريد الأسود قياس 44',_reply_product=self.product),{},[])
        self.assertIn('رقم',result['reply']);self.assertNotIn('العنوان',result['reply'])
        self.assertFalse(result.get('create_order'));self.assertEqual(self.count(),0)

    def test_single_color_photo_request_has_short_caption_without_new_fit_question(self):
        result,reply=self.m.prepare_color_photo_reply(dict(reply='صار من، سعره15 ألف شكد قياس تلبسين؟',image_product_ids=['K1']),
            'صار من، سعره15 ألف شكد قياس تلبسين؟',dict(self.ev,text='دزيلي صورته'),self.product,['https://images.test/a.jpg'])
        self.assertIn('صار بعيوني',reply);self.assertNotIn('قياس',reply)
        self.assertFalse(result.get('_post_image_question'))

    def test_color_word_in_address_does_not_replace_the_garment_choice(self):
        self.save('incoming','ثبتي أسود لعمر أربع سنوات قطعة وحدة')
        self.save('incoming','07701234567')
        self.m.capture_customer_contact(self.db,self.sender,'07701234567')
        address='بغداد الحي الأخضر قرب مدرسة الاختبار'
        self.save('incoming',address);self.m.capture_customer_contact(self.db,self.sender,address)
        cart=dict(order=dict(items=[dict(product_id='K1',color='أسود',size='4 سنوات',quantity=1)]))
        created,reply=self.m.create_order_if_valid(self.db,self.sender,cart,self.product)
        self.assertTrue(created,reply);self.assertEqual(self.count(),1)

    def test_remembered_product_receives_staff_knowledge_without_explicit_match(self):
        self.product['fabric']='قطن'
        for topic,answer in [('fabric_feel','قماش ثخين شتوي'),('stretch','بيه ليكرا')]:
            ids=product_questions.capture(self.db,'baraah-kids',self.sender,self.product,'تفصيل '+topic,[topic],None,self.m.now_baghdad_iso())
            product_questions.save_answer(self.db,'baraah-kids',ids[0],answer,'اختبار',self.m.now_baghdad_iso())
        with patch.object(self.m,'_call_main_ai_once',return_value=dict(reply='متوفر',create_order=False,order={})):
            result=self.m.call_main_ai(dict(self.ev,text='القماش ثخين وبيه ليكرا؟'),'text',{},[],[self.product],None,None,'',[],customer_products=[self.product])
        self.assertFalse(result.get('requires_human'),result)
        self.assertIn('ثخين',result['reply']);self.assertIn('ليكرا',result['reply'])

    def test_hypothetical_pair_question_does_not_ask_for_contacts(self):
        result=dict(reply='القطعتين ب20 ألف، دزيلي رقم الموبايل والعنوان حتى أثبت الحجز.',order={})
        event=dict(self.ev,text='والقطعة بكم؟ إذا آخذ اثنين يصير عرض؟',_reply_product=self.product)
        self.assertFalse(self.m.checkout_requested(self.db,event,result))
        result=self.m.owner_result_surface(result,event,{},[])
        self.assertNotIn('دزيلي',result['reply']);self.assertEqual(self.count(),0)

    def test_photo_followup_is_delivered_after_both_images_and_stops_on_failure(self):
        intro='إي صار بعيوني، هاي الصور.';question='يا لون عجبج حبيبتي؟'
        text=intro+'\n\n'+question
        plan=reply_layout.plan_sales_bubbles({'_post_image_question':question},text,['https://images.test/a.jpg','https://images.test/b.jpg'])
        result=dict(sender_id=self.sender,reply=text,reply_parts=plan['reply_parts'],send_image=True,
            _planned_sales_layout=dict(reply=text,parts=plan['reply_parts'],images=plan['image_urls'],post_image_parts=plan['post_image_parts']))
        calls=[]
        with patch.object(self.m,'send_text_to_facebook',side_effect=lambda s,t,*a:calls.append(('text',t)) or True),\
             patch.object(self.m,'send_image_to_facebook',side_effect=lambda s,u,*a:calls.append(('image',u)) or True):
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual([c[0] for c in calls],['text','image','image','text'])
        self.assertEqual(calls[-1][1],question)
        calls=[]
        with patch.object(self.m,'send_text_to_facebook',side_effect=lambda s,t,*a:calls.append(t) or True),\
             patch.object(self.m,'send_image_to_facebook',return_value=False):
            self.assertFalse(self.m.send_webhook_result_to_facebook(result))
        self.assertNotIn(question,calls)

    def test_model_cannot_replace_sold_out_colors_without_customer_acceptance(self):
        self.product.update(colors='أسود',sizes='من 2 الى 7 سنوات')
        for direction,text in (
            ('incoming','أريد الوردي لعمر ثلاث سنوات والكريمي لعمر ست سنوات'),
            ('outgoing','باقي بس أسود، الوردي والكريمي مخلصين.'),
            ('incoming','صححي الوردي أربع سنوات مو ثلاث والكريمي ست سنوات'),
            ('outgoing','عمر أربع وست سنوات متوفر.'),('incoming','اي حجزيهم بهذي الأعمار'),
            ('incoming','07701234567'),('incoming','بغداد حي الاختبار قرب مدرسة التجربة')):
            self.save(direction,text)
            if direction=='incoming':self.m.capture_customer_contact(self.db,self.sender,text)
        cart=dict(create_order=True,order=dict(items=[dict(product_id='K1',color='أسود',size=f'{age} سنوات',quantity=1) for age in (4,6)]))
        created,reply=self.m.create_order_if_valid(self.db,self.sender,cart,self.product)
        self.assertFalse(created);self.assertEqual(self.count(),0)
        self.assertIn('اللون',reply)
        self.save('incoming','خليهم أسود للقطعتين، موافقة ثبتي')
        created,reply=self.m.create_order_if_valid(self.db,self.sender,cart,self.product)
        self.assertTrue(created,reply);self.assertEqual(self.count(),1)

    def test_two_known_album_products_both_get_price_and_fabric_answers(self):
        first=dict(self.product,fabric='لينن',price='16000')
        second=dict(self.product,product_id='K2',product_name='فستان ثان',fabric='باربي',price='15000')
        products=[first,second]
        for product in products:
            self.m.complete_customer_product_link(self.db,self.sender,product,'manual',source='manual_admin',preserve_existing=True)
        self.m.customer_image_positions(self.db,self.sender,[dict(image_index=i+1,product_found=True,product_id=p['product_id'],product_name=p['product_name']) for i,p in enumerate(products)])
        with patch.object(self.m,'_call_main_ai_once',return_value=dict(reply='متوفر',create_order=False,order={})),\
             patch.object(self.m,'load_active_products',return_value=products):
            result=self.m.call_main_ai(dict(self.ev,text='شكد الأول وشكد الثاني وشنو قماش كل واحد؟'),'text',{},[],products,second,None,'',[],customer_products=products)
        for fact in ('16','15','لينن','باربي'):self.assertIn(fact,result['reply'])
        self.assertFalse(result.get('requires_human'))


if __name__=='__main__':unittest.main()
