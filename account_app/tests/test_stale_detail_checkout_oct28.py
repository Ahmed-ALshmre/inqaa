"""A stale nonconditional question cannot swallow later checkout contacts."""
import json
from datetime import timedelta
from unittest.mock import patch
import unittest

from account_app.tests import test_checkout_completion_oct25 as completion
from account_app.tests import test_checkout_regressions as fixtures


class StaleDetailCheckoutTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.CheckoutRegressionTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.CheckoutRegressionTests.tearDownClass.__func__)
    tearDown = fixtures.CheckoutRegressionTests.tearDown
    process = completion.CheckoutCompletionTests.process
    assert_confirmation = completion.CheckoutCompletionTests.assert_confirmation

    def setUp(self):
        fixtures.CheckoutRegressionTests.setUp(self)
        self.m._current_store_id.reset(self.token)
        self.token=self.m._current_store_id.set('default')
        self.db.execute('UPDATE customers SET store_id=? WHERE sender_id=?',('default',self.sender))
        self.db.commit();self.ev['store_id']='default'
        self.product=dict(product_id='W1',product_name='سوت تجريبي',category='سوت نسائي',
                          colors='اسود',sizes='من 70 الى 130 كيلو',price='18000',stock='متوفر',status='active')
        for name in ('load_products_from_file','load_active_products'):
            p=patch.object(self.m,name,return_value=[self.product]);p.start();self.patches.append(p)
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')

    def save(self,direction,text,payload=None):
        self.m.save_message(self.db,self.sender,direction,'text',text,None,None,None,payload or {})

    def order_count(self):
        return self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0]

    def seed(self,*,detail='شكد طول القطعة؟',saved_cart=False,selection='80'):
        self.save('outgoing','الأسود متوفر، شكد قياسج؟')
        self.save('incoming',selection)
        self.save('outgoing','أثبتلج حجز على هالاختيار؟')
        self.save('incoming','ثبتي')
        self.save('incoming',detail)
        draft={'items':[dict(product_id='W1',product_name=self.product['product_name'],color='اسود',
                             size=selection,size_type='weight',weight=selection,quantity=1)]}
        self.save('outgoing','دزيلي رقم الموبايل والمحافظة والعنوان حتى أثبت الحجز.',
                  {'checkout_draft':draft} if saved_cart else {})
        old=(self.m.datetime.now(self.m.BAGHDAD_TZ)-timedelta(days=2)).isoformat()
        self.db.execute('UPDATE messages SET created_at=? WHERE sender_id=?',(old,self.sender));self.db.commit()

    def test_weight_catalogue_and_old_length_question_book_full_contact_without_model(self):
        self.seed()
        result,model=self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assert_confirmation(result)
        order=self.db.execute('SELECT * FROM orders WHERE sender_id=?',(self.sender,)).fetchone()
        item=json.loads(order['order_items'])[0]
        self.assertEqual((item['size'],item['size_type'],item['weight']),('80','weight','80'))

    def test_split_contacts_collect_missing_only_then_book_once_after_two_days(self):
        self.seed(saved_cart=True)
        address,model=self.process('بغداد حي الاختبار')
        model.assert_not_called()
        self.assertIn('رقم الموبايل',address['reply']);self.assertNotIn('طول',address['reply'])
        self.assertEqual(self.order_count(),0)
        final,model=self.process('07701234567')
        model.assert_not_called();self.assert_confirmation(final)
        self.process('07701234567')
        self.assertEqual(self.order_count(),1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM messages WHERE sender_id=? AND direction=\'outgoing\' AND text=?',
            (self.sender,self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)).fetchone()[0],1)

    def test_partial_contact_can_advance_past_old_question_for_numbered_sizes_too(self):
        self.product['sizes']='38 إلى 52'
        self.seed(selection='44')
        result,model=self.process('بغداد حي الاختبار')
        model.assert_not_called();self.assertIn('رقم الموبايل',result['reply'])
        self.assertNotIn('طول',result['reply']);self.assertEqual(self.order_count(),0)

    def test_explicit_length_condition_remains_blocking(self):
        self.seed(detail='اذا طول القطعة 140 ثبتي، غير هذا لا تحجزين')
        result,_=self.process('07701234567 بغداد حي الاختبار')
        self.assertNotEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.order_count(),0)

    def test_new_unknown_detail_with_final_contact_is_not_silently_ignored(self):
        self.seed()
        result,_=self.process('07701234567 بغداد حي الاختبار وشكد طول القطعة؟')
        self.assertNotEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.order_count(),0)

    def test_out_of_range_weight_is_not_a_supported_garment_size(self):
        self.seed(selection='150')
        result,_=self.process('07701234567 بغداد حي الاختبار')
        self.assertNotEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.order_count(),0)

    def test_weight_guidance_does_not_replace_actual_numbered_size_inventory(self):
        self.product['sizes']='قياسات من 38 إلى 52 وتناسب أوزان من 70 إلى 130 كيلو'
        self.seed()
        result,_=self.process('07701234567 بغداد حي الاختبار')
        self.assertNotEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.order_count(),0)

    def test_sold_out_product_is_not_booked_from_old_selection(self):
        self.seed();self.product['stock']='نفذ'
        result,_=self.process('07701234567 بغداد حي الاختبار')
        self.assertNotEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.order_count(),0)
