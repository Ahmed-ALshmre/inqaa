"""Delivery details and final variant answers continue a child's known cart."""
from datetime import timedelta
from unittest.mock import patch
import unittest

from account_app.tests import test_checkout_regressions as fixtures
from account_app.tests import test_checkout_completion_oct25 as completion


class BaraahCheckoutContinuityTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.CheckoutRegressionTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.CheckoutRegressionTests.tearDownClass.__func__)
    tearDown = fixtures.CheckoutRegressionTests.tearDown
    process = completion.CheckoutCompletionTests.process
    assert_confirmation = completion.CheckoutCompletionTests.assert_confirmation

    def setUp(self):
        fixtures.CheckoutRegressionTests.setUp(self)
        self.m._current_store_id.reset(self.token)
        self.token = self.m._current_store_id.set('baraah-kids')
        self.db.execute('UPDATE customers SET store_id=? WHERE sender_id=?', ('baraah-kids',self.sender))
        self.db.commit(); self.ev['store_id']='baraah-kids'
        self.product=dict(product_id='K1', product_name='طقم اختبار أطفال', sizes='من 2 الى 6 سنوات',
                          colors='أسود', price='15000',stock='متوفر',status='active')
        for name in ('load_products_from_file','load_active_products'):
            p=patch.object(self.m,name,return_value=[self.product]);p.start();self.patches.append(p)
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')

    def save(self,direction,text,**kwargs):
        self.m.save_message(self.db,self.sender,direction,kwargs.get('kind','text'),text,
                            kwargs.get('image'),None,None,{})

    def contacts(self):
        for text in ('بغداد حي الاختبار','07701234567'):
            self.save('incoming',text)
            self.m.capture_customer_contact(self.db,self.sender,text)

    def count(self):
        return self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0]

    def test_affirmation_after_availability_uses_delivery_details_already_sent(self):
        self.save('incoming','عمرها 4 سنوات');self.contacts()
        self.save('outgoing','طقم اختبار أطفال متوفر لعمر 4 سنوات.')
        result,model=self.process('اي عيني')
        model.assert_not_called();self.assert_confirmation(result)
        self.process('اي عيني');self.assertEqual(self.count(),1)

    def test_last_age_and_colour_answer_completes_contacts_without_another_confirmation(self):
        self.product['colors']='أسود وردي'
        self.contacts();self.save('outgoing','باقي اللون والعمر حتى أثبت الطلب.')
        result,model=self.process('الأسود لعمر 4 سنوات')
        model.assert_not_called();self.assert_confirmation(result)

    def test_explicit_purchase_after_contacts_can_include_final_variant(self):
        self.contacts();self.save('outgoing','شكد العمر المطلوب؟')
        result,model=self.process('ثبتي الأسود لعمر 4 سنوات')
        model.assert_not_called();self.assert_confirmation(result)

    def test_recognized_single_photo_and_delivery_caption_book_in_same_link_flow(self):
        ev=dict(self.ev,text='عمرها 4 سنوات 07701234567 بغداد حي الاختبار',
                image_url='https://images.test/chosen.jpg',_image_already_linked=True,
                _image_matches=[dict(image_index=1,product_found=True,product_id='K1',image_url='https://images.test/chosen.jpg')])
        self.save('incoming',ev['text'])
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai(ev,'image',{},[],[self.product],self.product,None,'',[],customer_products=[self.product])
        model.assert_not_called()
        self.assertTrue(result.get('order',{}).get('items'))
        self.assertTrue(self.m.checkout_requested(self.db,ev,result))
        created,_=self.m.create_order_if_valid(self.db,self.sender,result,self.product)
        self.assertTrue(created);self.assertEqual(self.count(),1)

    def test_yes_to_photo_offer_does_not_book_from_old_contacts(self):
        self.save('incoming','عمرها 4 سنوات');self.contacts()
        self.save('outgoing','تحبين أرسلج صور هالموديل؟')
        result,_=self.process('اي عيني')
        self.assertEqual(self.count(),0);self.assertNotEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)

    def test_saved_contacts_without_recent_delivery_action_do_not_make_yes_a_purchase(self):
        self.save('incoming','عمرها 4 سنوات');self.contacts()
        old=(self.m.datetime.now(self.m.BAGHDAD_TZ)-timedelta(days=2)).isoformat()
        self.db.execute('UPDATE messages SET created_at=? WHERE sender_id=?',(old,self.sender));self.db.commit()
        self.save('outgoing','طقم اختبار أطفال متوفر لعمر 4 سنوات.')
        self.process('اي عيني');self.assertEqual(self.count(),0)

    def test_new_question_with_final_age_does_not_book(self):
        self.contacts();self.save('outgoing','شكد العمر المطلوب؟')
        self.process('عمرها 4 سنوات وشكد طوله؟');self.assertEqual(self.count(),0)

    def test_out_of_stock_does_not_book_on_recent_contacts_and_affirmation(self):
        self.save('incoming','عمرها 4 سنوات');self.contacts();self.product['stock']='نفذ'
        self.save('outgoing','طقم اختبار أطفال متوفر لعمر 4 سنوات.')
        self.process('اي عيني');self.assertEqual(self.count(),0)

    def test_new_unresolved_photo_prevents_using_earlier_contacts_as_purchase_action(self):
        self.save('incoming','عمرها 4 سنوات');self.contacts()
        self.save('incoming','',kind='image',image='https://images.test/another.jpg')
        self.save('outgoing','طقم اختبار أطفال متوفر لعمر 4 سنوات.')
        self.process('اي عيني');self.assertEqual(self.count(),0)

    def test_two_children_are_not_silently_reduced_to_one_order_line(self):
        self.contacts();self.save('outgoing','يا عمر مطلوب؟')
        self.process('اريد لعمر 3 سنوات و5 سنوات');self.assertEqual(self.count(),0)

    def test_delivery_condition_still_blocks_reservation(self):
        self.save('incoming','عمرها 4 سنوات');self.contacts()
        self.save('outgoing','أثبت الطلب؟')
        self.process('اذا التوصيل مجاني ثبتي');self.assertEqual(self.count(),0)

    def test_explicit_refusal_is_not_a_final_colour_answer(self):
        self.save('incoming','عمرها 4 سنوات');self.contacts()
        self.save('outgoing','أثبت الطلب؟')
        self.process('لا ما اريد الاسود');self.assertEqual(self.count(),0)

    def test_colour_alternatives_remain_unresolved(self):
        self.product['colors']='أسود وردي'
        self.save('incoming','عمرها 4 سنوات');self.contacts()
        self.save('outgoing','يا لون تختارين؟')
        self.process('الاسود لو الوردي');self.assertEqual(self.count(),0)

    def test_unverified_image_cannot_use_delivery_caption_for_local_checkout(self):
        ev=dict(self.ev,text='عمرها 4 سنوات 07701234567 بغداد حي الاختبار',
                image_url='https://images.test/unknown.jpg',_image_already_linked=True,
                _image_matches=[dict(product_found=False,image_url='https://images.test/unknown.jpg')])
        self.assertIsNone(self.m.resume_known_checkout(self.db,ev,self.product,{},[self.product],
                         {'evidence':[{'text':'عمرها 4 سنوات'}]}))

    def test_size_evidence_from_a_different_product_cannot_be_reused(self):
        self.save('incoming','عمرها 4 سنوات');self.contacts()
        other=dict(self.product,product_id='K2',product_name='طقم آخر')
        self.m.bind_customer_to_product(self.db,self.sender,other,source='image_recognition')
        context={'evidence':[{'text':'عمرها 4 سنوات'}]}
        self.assertIsNone(self.m.resume_known_checkout(self.db,dict(self.ev,text='ثبتي'),
                                                      other,{},[other],context))

    def multi_item_context(self):
        other=dict(self.product,product_id='K2',product_name='فستان اختبار',colors='وردي')
        self.m.bind_customer_to_product(self.db,self.sender,other,source='image_recognition')
        self.save('incoming','اريد الطقم الاسود لعمر 4 سنوات والفستان الوردي لعمر 3 سنوات')
        self.save('outgoing','دزي رقم الموبايل والمحافظة والعنوان حتى نكمل الحجز.')
        self.contacts()
        ev=dict(self.ev,text='07701234567 بغداد حي الاختبار')
        items=[dict(product_id=p['product_id'],product_name=p['product_name'],
                    color=color,size=age,size_type='size',quantity=1)
               for p,color,age in [(self.product,'اسود','4 سنوات'),(other,'وردي','3 سنوات')]]
        return other,ev,items

    def test_plain_multi_item_ack_is_repaired_into_both_selected_lines(self):
        other,ev,items=self.multi_item_context()
        with patch.object(self.m,'_call_main_ai_once',side_effect=[
                dict(reply='تمام، نعتمد القطعتين.',order={},create_order=False),
                dict(reply='وصلت تفاصيل الطلب.',order={'items':items},create_order=True)]) as model:
            result=self.m.call_main_ai(ev,'text',dict(self.db.execute('SELECT * FROM customers WHERE sender_id=?',(self.sender,)).fetchone()),
                                      self.m.load_history(self.db,self.sender),[self.product,other],other,None,'',[],customer_products=[self.product,other])
        self.assertEqual(model.call_count,2,result)
        self.assertIn('order.items',model.call_args.kwargs['fix_instruction'])
        self.assertEqual(len(result['order']['items']),2)
        with patch.object(self.m,'load_products_from_file',return_value=[self.product,other]):
            self.assertTrue(self.m.checkout_requested(self.db,ev,result))
            created,_=self.m.create_order_if_valid(self.db,self.sender,result,other)
        self.assertTrue(created);self.assertEqual(self.count(),1)

    def test_repeated_empty_cart_ack_creates_review_instead_of_false_progress(self):
        other,ev,_=self.multi_item_context()
        with patch.object(self.m,'_call_main_ai_once',return_value=dict(reply='تمام، نعتمد القطعتين.',order={},create_order=False)):
            result=self.m.call_main_ai(ev,'text',{},self.m.load_history(self.db,self.sender),
                                      [self.product,other],other,None,'',[],customer_products=[self.product,other])
        self.assertTrue(result.get('failed'),result);self.assertTrue(result['requires_human'])
        self.assertEqual(result['failure_reason'],'checkout_cart_not_built');self.assertFalse(result['reply'])
        self.assertEqual(self.count(),0)

    def test_focused_missing_option_question_is_not_an_empty_checkout_ack(self):
        _,ev,_=self.multi_item_context()
        result=dict(reply='باقي لون الفستان، يا لون تختارين؟',order={},create_order=False)
        self.assertFalse(self.m.unfulfilled_checkout_ack(self.db,ev,result))

    def test_legacy_flat_cart_is_not_mistaken_for_an_empty_ack(self):
        _,ev,items=self.multi_item_context()
        self.assertFalse(self.m.unfulfilled_checkout_ack(self.db,ev,
                         dict(reply='وصلت تفاصيل الطلب.',order=items[0],create_order=True)))
