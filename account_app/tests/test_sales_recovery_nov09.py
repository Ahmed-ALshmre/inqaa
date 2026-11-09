"""Anonymized regressions from the Nov 9 Khuyoot sales audit."""
import json
import unittest
from unittest.mock import patch
from account_app import checkout, delivery_copy, product_questions, product_variants, sales_answers, sales_context, sales_playbook, sales_progress, sales_strategy, owner_sales
from account_app.tests import test_product_questions_nov01 as fixtures


def skirt():
    return dict(product_id='K1', product_name='تنورة اختبار', reply_category='تنورة',
                product_family='skirts', price='16000', stock='متوفر', status='active',
                sizes='من 38 إلى 48', colors='أسود نيلي جوزي', fabric='دبل إيطالي', measurements='الطول 105 سم')


class SalesFactsNov09Tests(unittest.TestCase):
    def test_courier_whatsapp_does_not_ask_for_zipper_specification(self):
        for q in ('هو المندوب من يتصل انطيه الموقع ع الوات ساب', 'ادزلكم لوكيشن على واتساب', 'العنوان بالوات ساب'):
            with self.subTest(q=q):
                self.assertNotIn('closure', sales_answers.topics(q))
                self.assertFalse(product_questions.missing(q, skirt()))
        self.assertIn('closure', sales_answers.topics('ادزلكم بالوات ساب، بس بيه سحاب لو دكم؟'))
        self.assertTrue(product_questions.missing('بيه سحاب؟', skirt()))

    def test_explicit_size_with_copula_is_remembered(self):
        for q in ('قياسي هو ٣٨', 'مقاسي هو ٤٤', 'قياسها هو 46'):
            expected=product_variants.requested(q)['sizes']
            self.assertEqual(len(expected),1)
            context=sales_context.advance({},[dict(direction='incoming',text=q)])
            self.assertEqual(sales_playbook.customer_selection(context)['size'],expected[0])
        self.assertFalse(product_variants.requested('وزني هو 60')['sizes'])

    def test_address_without_explicit_baghdad_and_kurdish_letters(self):
        contact=checkout.contact_fields('عنوان المنصور ١٤ رمضان م/07700000001')
        self.assertEqual(contact['province'],'بغداد')
        self.assertIn('14 رمضان',contact['address'])
        self.assertEqual(checkout.contact_fields('سلیماني')['province'],'السليمانية')
        self.assertNotIn('address',checkout.contact_fields('التوصيل للمنصور شكد؟'))

    def test_approximate_weight_is_optional_with_valid_chosen_size(self):
        item=dict(product_id='K1',color='نيلي',size='38',size_type='size',weight='58-60',quantity=1)
        self.assertFalse(checkout.order_line_error([item],[skirt()]))
        self.assertEqual(checkout.optional_order_weight(item,skirt()),'')
        self.assertTrue(checkout.order_line_error([dict(item,size='54')],[skirt()]))
        weight_product=dict(skirt(),sizes='55 إلى 100 كيلو')
        self.assertTrue(checkout.order_line_error([dict(item,size='58-60',size_type='weight')],[weight_product]))

    def test_ready_selection_asks_only_missing_contact_without_order_authority(self):
        context=sales_context.advance({},[dict(direction='incoming',text='اريد النيلي قياس 44')])
        self.assertIn('رقم',sales_progress.contact_invitation(skirt(),context))
        customer=dict(phone='07700000001')
        step=sales_progress.contact_invitation(skirt(),context,customer)
        self.assertIn('العنوان',step);self.assertNotIn('رقم',step)
        customer=dict(province='بغداد',address='بغداد حي التجربة')
        step=sales_progress.contact_invitation(skirt(),context,customer)
        self.assertIn('رقم',step);self.assertNotIn('العنوان',step)
        for text in ('بس اقارن','اخاف يطلع مو مناسب','لا ماريد','خلي افكر','اذا ينزل السعر احجز','شكرا'):
            self.assertFalse(sales_progress.contact_invitation(skirt(),context,question=text),text)

    def test_unique_category_is_scoped_and_ambiguous_catalog_is_not_guessed(self):
        p=dict(skirt(),store_id='khuyoot'); other=dict(p,product_id='K2')
        self.assertEqual(sales_progress.unique_category_product('شنو قماش التنورة؟',[p],'khuyoot'),p)
        self.assertEqual(sales_progress.unique_category_product('السلام عليكم ما هي المواد المستخدمة في التنورة؟',[p],'khuyoot'),p)
        self.assertIsNone(sales_progress.unique_category_product('شنو قماش التنورة؟',[p,other],'khuyoot'))
        self.assertIsNone(sales_progress.unique_category_product('شنو قماش التنورة؟',[p],'default'))
        self.assertIsNone(sales_progress.unique_category_product('عندكم تنورات غيرها؟',[p],'khuyoot'))

    def test_cart_total_after_two_colors_is_not_assigned_to_last_color(self):
        context=sales_context.advance({},[dict(direction='incoming',text='اريد نيلي وجوزي قطعتين'),
                                         dict(direction='incoming',text='قياسي هو 38')])
        plan=sales_playbook.same_size_color_cart(skirt(),context)
        self.assertEqual([(i['color'],i['quantity']) for i in plan['items']],[('نيلي',1),('جوزي',1)])
        self.assertIn('رقم',sales_progress.contact_invitation(skirt(),context))
        context=sales_context.advance({},[dict(direction='incoming',text='اريد نيلي واحد وجوزي قطعتين قياس 38')])
        plan=sales_playbook.same_size_color_cart(skirt(),context)
        self.assertFalse(plan and sum(i['quantity'] for i in plan['items'])==2)

    def test_policy_repair_never_leaves_half_a_condition(self):
        store='إلكم حق الفحص بوجود المندوب قبل الدفع'
        policy=delivery_copy.inspection_policy(dict(inspection_policy='طول التنورة 105'),store)
        self.assertEqual(policy,store)
        override='الفحص فقط، لا يسمح بالإرجاع'
        self.assertEqual(delivery_copy.inspection_policy(dict(inspection_policy=override),store),override)
        reply='إي حبيبتي، إذا بيها مشكلة أو ما طابقت طلبج، ترجعينها بدون دفع. شنو القياس؟'
        repaired=sales_strategy.repair_inspection_reply(reply,store)
        self.assertNotIn('إذا',repaired);self.assertIn('الفحص',repaired);self.assertIn('شنو القياس',repaired)

    def test_bounded_debounce_keeps_a_burst_window(self):
        for q,kind,expected in [('07700000001','text',4),('اريد النيلي قياس 44','text',4),
                                ('شنو قماشها','text',6),('','image',8)]:
            self.assertEqual(sales_progress.debounce_seconds(q,kind,35),expected)
            self.assertEqual(sales_progress.debounce_seconds(q,kind,0),0)
            self.assertEqual(sales_progress.debounce_seconds(q,kind,1),1)

    def test_natural_warmth_is_not_removed_by_turn_counter(self):
        h=[dict(direction='outgoing',text='حبيبتي قياسج متوفر')]
        self.assertTrue(owner_sales.warmth_allowed('صار عيوني',h))
        self.assertFalse(owner_sales.warmth_allowed('بدون القاب لو سمحتي',h))
        text='صار بعيوني، النيلي متوفر. دزيلي عنوانج حبيبتي حتى أثبتلج.'
        self.assertEqual(owner_sales.surface(text,'female',warm=False),text)


class SalesPipelineNov09Tests(unittest.TestCase):
    setUpClass=classmethod(fixtures.ProductQuestionsTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.ProductQuestionsTests.tearDownClass.__func__)
    tearDown=fixtures.ProductQuestionsTests.tearDown
    save=fixtures.ProductQuestionsTests.save
    process=fixtures.ProductQuestionsTests.process
    count=fixtures.ProductQuestionsTests.count

    def setUp(self):
        fixtures.ProductQuestionsTests.setUp(self)
        self.product.clear();self.product.update(skirt())
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')

    def test_two_color_order_does_not_require_exact_optional_weight(self):
        self.save('incoming','اريد نيلي وجوزي قطعتين قياس 38')
        draft={'items':[dict(product_id='K1',color=color,size='38',size_type='size',weight='58-60',quantity=1)
                        for color in ('نيلي','جوزي')]}
        self.m.save_message(self.db,self.sender,'outgoing','text','دزيلي الرقم والعنوان حتى أثبت الطلب.',None,None,None,{'checkout_draft':draft})
        result,_=self.process('07700000001 بغداد حي الاختبار قرب مدرسة التجربة')
        self.assertEqual(self.count(),1,result)
        order=self.db.execute('SELECT * FROM orders WHERE sender_id=?',(self.sender,)).fetchone()
        lines=json.loads(order['order_items'])
        self.assertEqual({r['color'] for r in lines},{'نيلي','جوزي'})
        self.assertTrue(all(r['size']=='38' and not r.get('weight') for r in lines))
        self.assertEqual(order['total_amount'],37000)
        self.process('تمام');self.assertEqual(self.count(),1)

    def test_generic_question_cards_do_not_hide_different_unanswered_details(self):
        now=self.m.now_baghdad_iso()
        a=product_questions.capture(self.db,'baraah-kids',self.sender,self.product,'سؤال تفاصيل اول',['details'],None,now)
        b=product_questions.capture(self.db,'baraah-kids',self.sender,self.product,'سؤال تفاصيل مختلف',['details'],None,now)
        c=product_questions.capture(self.db,'baraah-kids',self.sender,self.product,'سؤال تفاصيل اول',['details'],None,now)
        self.assertNotEqual(a,b);self.assertEqual(a,c)

    def test_model_answer_with_context_is_not_replaced_by_faq_surface(self):
        text='يروحي خامته دبل إيطالي، والنيلي اللي اخترتيه موجود. بس دزيلي رقمج حتى أثبتلج الطلب.'
        context=sales_context.advance({},[dict(direction='incoming',text='اريد النيلي قياس 44')])
        ev=dict(self.ev,text='شنو قماشه؟',_reply_product=self.product,_sales_context=context)
        with patch.object(self.m,'checkout_requested',return_value=True):
            result=self.m.owner_result_surface(dict(reply=text,create_order=False,order={}),ev,dict(gender='female'),[])
        self.assertIn('النيلي',result['reply']);self.assertIn('رقمج',result['reply'])

    def test_complete_address_books_once_without_landmark_question(self):
        self.save('incoming','ثبتي النيلي قياس 44')
        draft={'items':[dict(product_id='K1',color='نيلي',size='44',quantity=1)]}
        self.m.save_message(self.db,self.sender,'outgoing','text','دزيلي الرقم والعنوان.',None,None,None,{'checkout_draft':draft})
        result,_=self.process('عنوان المنصور ١٤ رمضان م/07700000001')
        self.assertEqual(self.count(),1,result)
        self.assertIn('تم تثبيت',result['reply'])

    def test_delivery_followup_keeps_next_step_even_after_previous_invitation(self):
        self.save('incoming','اريد نيلي قياس 44')
        self.save('outgoing','دزيلي رقم الموبايل والعنوان حتى أثبت الطلب.')
        self.save('incoming','لا خلي الجوزي بدل النيلي')
        self.save('outgoing','اعتمدتلج الجوزي 44. دزيلي رقم الموبايل والعنوان.')
        result,_=self.process('شكد ويا التوصيل للبصرة؟')
        self.assertIn('21',result['reply']);self.assertIn('رقم',result['reply']);self.assertIn('عنوان',result['reply'])
        self.assertEqual(self.count(),0)

    def test_second_transport_surface_preserves_verified_contact_invitation(self):
        history=[dict(direction='incoming',text='قياسي هو 44'),dict(direction='incoming',text='اريد النيلي')]
        ev=dict(self.ev,text='اريد النيلي',_reply_product=self.product,_sales_context=sales_context.advance({},history))
        reply='النيلي متوفر عيوني. دزيلي رقم الموبايل والعنوان حتى أثبتلج الطلب.'
        first=self.m.owner_result_surface(dict(reply=reply,create_order=False),ev,{},history)
        second=self.m.owner_result_surface(first,dict(self.ev,text='اريد النيلي'),{},[])
        self.assertIn('رقم الموبايل',second['reply']);self.assertIn('العنوان',second['reply'])
        self.assertFalse(second['create_order']);self.assertEqual(self.count(),0)


if __name__=='__main__':unittest.main()
