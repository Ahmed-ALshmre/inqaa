"""Synthetic regressions from the production audit after release 45."""
import unittest
import json
from unittest.mock import patch
from account_app import checkout, pricing, sales_answers, sales_context, sales_playbook, sales_progress
from account_app import app as app_module
from account_app.tests import test_product_questions_nov01 as fixtures


FUR = dict(product_id='P013',product_name='سوت فرو',store_id='default',
    price='15000 او بكج قطعتين ب 25000',offer='عرض خاص سعر القطعة ب 15 قطعتين ب 25 الف',
    colors='ماروني ازرق رصاصي',sizes='قياسات من وزن 60 كيلو إلى 130 كيلو.',
    status='active',stock='متوفر')


class PostdeployCheckoutTests(unittest.TestCase):
    def test_retail_price_with_named_pair_package_can_book(self):
        self.assertEqual(pricing.terms(FUR)[:2],(1,15000))
        self.assertEqual(pricing.offer_tiers(FUR),[(2,25000)])
        quote=pricing.quote([dict(product_id='P013',quantity=2)],[FUR],5000)
        self.assertEqual((quote['product_total'],quote['product_total']+quote['delivery_fee']),(25000,30000))
        self.assertEqual(pricing.terms(dict(FUR,price='بكج 3 قطع سعره 25 الف'))[:2],(3,25000))
        self.assertEqual(pricing.terms(dict(FUR,price='بكج قطعتين ب 25 الف'))[:2],(2,25000))
        with self.assertRaises(ValueError):pricing.terms(dict(FUR,price='15000 او بكج قطعتين ب 25000 و 30000'))

    def test_unanswered_short_price_condition_blocks_checkout(self):
        condition='إذا 20 ثبتي'
        self.assertEqual(checkout.unresolved_condition([],condition)['kind'],'price')
        self.assertIsNotNone(checkout.unresolved_condition([dict(direction='incoming',text=condition)],'07700000001 بغداد حي الاختبار'))
        self.assertIsNone(checkout.unresolved_condition([dict(direction='incoming',text=condition),
            dict(direction='outgoing',text='ما نكدر ننزل أكثر، السعر 22 ألف.')],'ثبتي'))

    def test_each_color_keeps_its_own_weight_and_quantity(self):
        turns=['اريد قطعتين','الازرق اكبر قياس لوزن 130 والماروني لوزن 95',
               'قطعة واحدة الازرق اكبر قياس و قطعة واحدة من ماروني ل وزن 95']
        context=sales_context.advance({},[dict(direction='incoming',text=t) for t in turns])
        plan=sales_playbook.weight_cart_choices(FUR,context)
        self.assertEqual([(i['color'],i['size'],i['quantity']) for i in plan['items']],
                         [('أزرق','130',1),('ماروني','95',1)])
        self.assertFalse(checkout.order_line_error(plan['items'],[FUR]))
        quote=pricing.quote(plan['items'],[FUR],5000)
        self.assertEqual(quote['product_total']+quote['delivery_fee'],30000)
        ambiguity=sales_context.advance({},[dict(direction='incoming',text='أزرق وماروني وزن 95')])
        self.assertNotIn('items',sales_playbook.weight_cart_choices(FUR,ambiguity) or {})
        self.assertIn('رقم',sales_progress.contact_invitation(FUR,context))

    def test_colloquial_shared_weight_survives_followup_color_message(self):
        context=sales_context.advance({},[dict(direction='incoming',text='اريد الوان والاحجام130'),
            dict(direction='incoming',text='هي نفس الالوان الماروني والازرق والرصاصي')])
        self.assertTrue(any('احجام130' in e['text'] for e in context['evidence']))
        plan=sales_playbook.weight_cart_choices(FUR,context)
        self.assertEqual(plan['question'],'قطعة من كل لون؟')
        context=sales_context.advance(context,[dict(direction='outgoing',text=plan['question']),
            dict(direction='incoming',text='اي قطعة من كل لون')])
        plan=sales_playbook.weight_cart_choices(FUR,context)
        self.assertEqual(plan['weight'],'130')
        self.assertEqual({item['color'] for item in plan['items']},{'ماروني','أزرق','رصاصي'})
        stated=sales_context.advance({},[dict(direction='incoming',text='اريد 3 قطع لون ماروني وازرق ورصاصي الاحجام 130')])
        plan=sales_playbook.weight_cart_choices(FUR,stated)
        self.assertEqual(sum(item['quantity'] for item in plan['items']),3)

    def test_wholesale_unit_from_matching_catalog_price(self):
        product=dict(product_id='P002',store_id='golden-threads',price='48000',
            description='الوان تجي كاملة 4 الوان جملة فقط اقل كمية لطلب 1 درزن سعر الدرزن ب 48 الف')
        self.assertEqual(sales_answers.wholesale_unit(product),'الدرزن')
        self.assertEqual(sales_answers.wholesale_unit(dict(product,price='50000')),'')
        self.assertIn('درزن',' '.join(sales_answers.wholesale_lines(product)))
        corrected=sales_answers.wholesale_unit_reply(product,'هذا سعر القطعة الحالي 48 ألف','غالي 48')
        self.assertIn('للدرزن',corrected)
        self.assertNotIn('القطعة',corrected)
        self.assertNotIn('شكد الكمية',corrected)

    def test_wholesale_surface_corrects_a_generated_piece_price(self):
        product=dict(product_id='P002',store_id='golden-threads',price='48000',
            description='جملة فقط اقل كمية للطلب 1 درزن سعر الدرزن ب 48 الف')
        result=dict(reply='سعر القطعة 48 ألف، دزيلي العنوان حتى أثبت الطلب.',
            create_order=True,order={'items':[{'product_id':'P002','quantity':1}]})
        with patch.object(app_module,'current_store_id',return_value='golden-threads'):
            corrected=app_module.owner_result_surface(result,{'text':'غالي 48','_reply_product':product},None,[])
        self.assertIn('للدرزن',corrected['reply'])
        self.assertNotIn('القطعة',corrected['reply'])
        self.assertFalse(corrected['create_order'])


class PostdeployCheckoutPipelineTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.ProductQuestionsTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.ProductQuestionsTests.tearDownClass.__func__)
    tearDown=fixtures.ProductQuestionsTests.tearDown
    save=fixtures.ProductQuestionsTests.save
    process=fixtures.ProductQuestionsTests.process
    count=fixtures.ProductQuestionsTests.count

    def setUp(self):
        fixtures.ProductQuestionsTests.setUp(self)
        self.product.update(price=FUR['price'],offer=FUR['offer'],colors='ماروني ازرق رصاصي',
            sizes=FUR['sizes'])
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')

    def test_distinct_weights_and_offer_save_one_order_after_contacts(self):
        self.save('incoming','اريد قطعتين قطعة واحدة الازرق لوزن 130 وقطعة واحدة الماروني لوزن 95 ثبتي')
        context=sales_context.advance({},[dict(direction='incoming',text='اريد قطعتين قطعة واحدة الازرق لوزن 130 وقطعة واحدة الماروني لوزن 95 ثبتي')])
        plan=sales_playbook.weight_cart_choices(self.product,context)
        self.assertEqual(len(plan['items']),2,plan)
        draft={'items':plan['items']}
        self.m.save_message(self.db,self.sender,'outgoing','text','دزيلي الرقم والعنوان حتى أثبت الطلب.',
            None,None,None,{'checkout_draft':draft})
        result,_=self.process('07700000001 بغداد حي الاختبار قرب مدرسة التجربة')
        self.assertEqual(self.count(),1,result)
        order=self.db.execute('SELECT * FROM orders WHERE sender_id=?',(self.sender,)).fetchone()
        self.assertEqual((order['product_total'],order['total_amount']),(25000,30000))
        lines=json.loads(order['order_items'])
        self.assertEqual({(line['color'],line['size']) for line in lines},{('أزرق','130'),('ماروني','95')})
        self.process('تمام')
        self.assertEqual(self.count(),1)

    def test_short_conditional_price_does_not_request_contacts_as_acceptance(self):
        self.product.update(price='17000',offer='')
        self.save('incoming','اريد الازرق لوزن 95 قطعة وحدة')
        self.m.save_message(self.db,self.sender,'outgoing','text','المجموع ويا التوصيل 22 ألف. دزيلي الرقم والعنوان.',
            None,None,None,{'checkout_draft':{'items':[dict(product_id='K1',color='أزرق',size='95',size_type='weight',weight='95',quantity=1)]}})
        result,_=self.process('إذا 20 ثبتي')
        self.assertEqual(self.count(),0,result)
        self.assertIn('22',result['reply'])
        self.assertNotIn('دزيلي',result['reply'])


if __name__=='__main__':unittest.main()
