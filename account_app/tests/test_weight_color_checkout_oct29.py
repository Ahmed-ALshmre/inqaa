"""Multi-turn shapes from the all-store review; all customer data is synthetic."""
import json
import unittest
from unittest.mock import patch
from account_app import sales_context, sales_playbook, sales_strategy, owner_sales, product_variants
from account_app.tests import test_stale_detail_checkout_oct28 as fixtures


class WeightMemoryTests(unittest.TestCase):
    def test_written_year_range_endpoints_keep_age_units(self):
        for text,expected in [('من سنة الى 8 سنوات',(1,8)),('سنه لحد سنتين',(1,2)),('من سنتين الى 7',(2,7))]:
            self.assertEqual(product_variants.size_data(text)['years'],[expected])
        for mixed in ('من سنة الى 18 شهر','من سنتين الى 7 كيلو'):
            self.assertEqual(product_variants.size_data(mixed)['years'],[])

    def test_weight_is_separate_from_a_garment_size_and_survives_other_answers(self):
        context=sales_context.advance({},[{'direction':'outgoing','text':'شكد قياسج؟'},
            {'direction':'incoming','text':'وزني ٨٠ كيلو'},
            {'direction':'outgoing','text':'يا لون؟'},{'direction':'incoming','text':'الماروني والرمادي'}])
        selected=sales_playbook.customer_selection(context)
        self.assertEqual(selected.get('weight'),'80');self.assertIsNone(selected['size'])
        self.assertEqual(len(selected['colors']),2)

    def test_bare_weight_answer_uses_its_question_and_not_an_unrelated_number(self):
        context=sales_context.advance({},[{'direction':'outgoing','text':'شكد الوزن بالكيلو؟'},
            {'direction':'incoming','text':'80'},{'direction':'outgoing','text':'شكد قطعة؟'},
            {'direction':'incoming','text':'2'}])
        self.assertEqual(sales_playbook.customer_selection(context).get('weight'),'80')

    def test_written_weight_is_kept_as_kilograms(self):
        for text,expected in [('وزني سبعين كيلو تقريباً','70'),('اني خمسة وسبعين كيلو','75'),
                              ('وزنها تسعين كغم','90'),('وزني مية كيلو','100')]:
            self.assertEqual(product_variants.weight_answer(text)['value'],expected,text)
        self.assertTrue(product_variants.weight_answer('وزني سبعين لو ثمانين كيلو')['ambiguous'])

    def test_ambiguous_weight_does_not_reuse_the_previous_single_value(self):
        context=sales_context.advance({},[{'direction':'incoming','text':'وزني 80'},
            {'direction':'incoming','text':'وزني 75 لو 80 مو متأكدة'}])
        self.assertIsNone(sales_playbook.customer_selection(context).get('weight'))

    def test_no_garment_size_guarantee_can_be_invented_from_weight(self):
        product=dict(sizes='من 55 كيلو الى 90',stock='متوفر')
        for reply in ('قياس 44 يجي تمام عليج','وزنج 80 يعني قياس 44 مضبوط',
                      'قياس 44 يناسبج أكيد'):
            self.assertTrue(sales_strategy.reply_error(reply,'وزني 80',product),reply)


class WeightColorCheckoutTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.StaleDetailCheckoutTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.StaleDetailCheckoutTests.tearDownClass.__func__)
    tearDown=fixtures.StaleDetailCheckoutTests.tearDown
    save=fixtures.StaleDetailCheckoutTests.save
    order_count=fixtures.StaleDetailCheckoutTests.order_count
    assert_confirmation=fixtures.StaleDetailCheckoutTests.assert_confirmation

    def setUp(self):
        fixtures.StaleDetailCheckoutTests.setUp(self)
        self.product.update(product_name='دشداشة اختبار',colors='ماروني رمادي',sizes='من 55 كيلو الى 90')
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')

    def process(self,text):
        result,model=fixtures.StaleDetailCheckoutTests.process(self,text)
        # The next customer answer presumes delivery of the previous question.
        # Run real receipt bookkeeping, replacing only outbound adapters.
        with patch.object(self.m,'send_text_to_facebook',return_value=True), \
             patch.object(self.m,'send_image_to_facebook',return_value=True):
            self.m.send_webhook_result_to_facebook(result,self.sender)
        return result,model

    def test_weight_then_two_colors_contacts_and_yes_books_both_once(self):
        first,model=self.process('وزني 80')
        model.assert_not_called();self.assertNotIn('قياس',first['reply']);self.assertIn('لون',first['reply'])
        choice,model=self.process('الماروني والرمادي')
        model.assert_not_called();self.assertIn('قطعة من كل لون',choice['reply'])
        contact,model=self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assertIn('قطعة من كل لون',contact['reply'])
        self.assertNotIn('رقم الموبايل',contact['reply']);self.assertEqual(self.order_count(),0)
        final,model=self.process('اي')
        model.assert_not_called();self.assert_confirmation(final)
        row=self.db.execute('SELECT * FROM orders WHERE sender_id=?',(self.sender,)).fetchone()
        items=json.loads(row['order_items'])
        self.assertEqual({(i['color'],i['quantity'],i['size_type'],i['weight']) for i in items},
                         {('ماروني',1,'weight','80'),('رصاصي',1,'weight','80')})
        self.assertEqual(row['product_total'],36000)
        self.process('07701234567');self.assertEqual(self.order_count(),1)

    def test_known_product_weight_answer_advances_when_match_is_loaded_from_memory(self):
        """The live inbox can resolve the product from memory after call_main_ai starts."""
        self.save('outgoing','شكد الوزن بالكيلو برقم واحد؟')
        self.save('outgoing','الدشداشة تلبس من 55 لحد 90 كيلو، يناسب وزنج؟')
        history=self.m.load_history(self.db,self.sender)
        ev=dict(self.ev,text='اني سبعين كيلو تقريباً')
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai(
                ev,'text',{},history,[self.product],None,None,'',[],
                customer_products=[self.product],conversation_history=history)
        model.assert_not_called()
        self.assertNotIn('شكد قياس',result['reply'])
        self.assertIn('لون',result['reply'])

    def test_explicit_different_quantities_are_preserved(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        result,model=self.process('ثبتي ماروني قطعتين ورمادي قطعة 07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assert_confirmation(result)
        row=self.db.execute('SELECT * FROM orders WHERE sender_id=?',(self.sender,)).fetchone()
        self.assertEqual({i['color']:i['quantity'] for i in json.loads(row['order_items'])},{'ماروني':2,'رصاصي':1})
        self.assertEqual(row['product_total'],54000)

    def test_quantity_confirmation_keeps_cart_while_collecting_split_contacts(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        self.save('outgoing','قطعة من كل لون؟')
        first,model=self.process('اي')
        model.assert_not_called();self.assertIn('رقم الموبايل',first['reply'])
        second,model=self.process('07701234567')
        model.assert_not_called();self.assertNotIn('رقم الموبايل',second['reply']);self.assertNotIn('قياس',second['reply'])
        final,model=self.process('بغداد حي الاختبار')
        model.assert_not_called();self.assert_confirmation(final)
        items=json.loads(self.db.execute('SELECT order_items FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0])
        self.assertEqual(len(items),2)

    def test_missing_only_one_color_quantity_does_not_reset_other_choices(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        self.save('incoming','ماروني قطعتين')
        # The customer retains both colors, then supplies the missing count.
        self.save('incoming','الماروني والرمادي')
        first,model=self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assertIn('رصاصي',first['reply']);self.assertNotIn('قياس',first['reply'])
        final,model=self.process('واحدة')
        model.assert_not_called();self.assert_confirmation(final)

    def test_out_of_range_weight_does_not_book_or_guarantee_fit(self):
        self.save('incoming','وزني 120');self.save('incoming','الماروني والرمادي')
        result,_=self.process('قطعة من كل لون 07701234567 بغداد حي الاختبار')
        self.assertEqual(self.order_count(),0);self.assertNotIn('يجي تمام',result['reply'])

    def test_mixed_real_size_inventory_still_requires_a_real_selected_size(self):
        self.product['sizes']='قياسات من 38 الى 52 وتناسب أوزان من 55 الى 90 كيلو'
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        result,_=self.process('قطعة من كل لون 07701234567 بغداد حي الاختبار')
        self.assertEqual(self.order_count(),0)

    def test_color_alternatives_are_not_two_purchased_lines(self):
        self.save('incoming','وزني 80');self.save('incoming','ماروني لو رمادي')
        result,_=self.process('07701234567 بغداد حي الاختبار')
        self.assertEqual(self.order_count(),0);self.assertNotEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)

    def test_sold_out_color_cannot_be_silently_dropped(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        self.product['colors']='ماروني'
        self.process('قطعة من كل لون 07701234567 بغداد حي الاختبار');self.assertEqual(self.order_count(),0)

    def test_delivery_condition_still_prevents_order(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        self.process('اذا التوصيل مجاني ثبتي قطعة من كل لون 07701234567 بغداد حي الاختبار')
        self.assertEqual(self.order_count(),0)

    def test_recent_unknown_product_detail_still_prevents_order(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        self.process('قطعة من كل لون 07701234567 بغداد حي الاختبار وشكد طولها؟')
        self.assertEqual(self.order_count(),0)

    def test_single_weight_color_contact_books_without_asking_garment_size(self):
        self.product['colors']='ماروني';self.save('incoming','وزني 80')
        result,model=self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assert_confirmation(result)

    def test_attached_color_counts_are_read_without_becoming_weights(self):
        self.save('incoming','وزني 80')
        result,model=self.process('ثبتي ماروني2 ورمادي1 07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assert_confirmation(result)
        items=json.loads(self.db.execute('SELECT order_items FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0])
        self.assertEqual({i['color']:i['quantity'] for i in items},{'ماروني':2,'رصاصي':1})
        self.assertTrue(all(i['weight']=='80' for i in items))

    def test_ambiguous_quantity_does_not_reuse_old_count_or_remove_other_color(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        self.save('incoming','قطعة من كل لون')
        self.save('incoming','الماروني قطعتين لو 3 مو متأكدة')
        result,model=self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assertEqual(self.order_count(),0)
        self.assertIn('ماروني',result['reply']);self.assertNotIn('قياس',result['reply'])

    def test_replacing_one_confirmed_color_with_the_other_keeps_total_piece_count(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        self.save('incoming','قطعة من كل لون')
        self.save('incoming','خلي الماروني بدل الرمادي')
        result,model=self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assert_confirmation(result)
        items=json.loads(self.db.execute('SELECT order_items FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0])
        self.assertEqual([(i['color'],i['quantity']) for i in items],[('ماروني',2)])

    def test_zero_quantity_is_unresolved_and_never_defaults_to_one(self):
        self.product['colors']='ماروني';self.save('incoming','وزني 80');self.save('incoming','ماروني 0')
        result,_=self.process('07701234567 بغداد حي الاختبار')
        self.assertEqual(self.order_count(),0);self.assertIn('قطعة',result['reply'])

    def test_unexecuted_claim_requests_missing_quantity_not_known_contacts(self):
        self.save('incoming','وزني 80');self.save('incoming','الماروني والرمادي')
        self.m.capture_customer_contact(self.db,self.sender,'07701234567 بغداد حي الاختبار')
        reply=self.m.unexecuted_checkout_reply(dict(self.ev,text='اي'),'تم تثبيت الطلب',self.product)
        self.assertIn('قطعة من كل لون',reply)
        self.assertNotIn('رقم الموبايل',reply);self.assertNotIn('قياس',reply)

    def test_recent_endearment_is_removed_from_the_next_generated_reply(self):
        history=[{'direction':'outgoing','text':'حبيبتي، الماروني متوفر.'}]
        result=self.m.owner_result_surface({'reply':'روحي السعر 18 ألف، التوصيل 5 آلاف.',
                                           'reply_parts':['روحي السعر 18 ألف،','التوصيل 5 آلاف.']},
                                          dict(self.ev,text='شكد سعره؟'),{'gender':'female'},history)
        self.assertFalse(owner_sales.AFFECTION.search(result['reply']))
        self.assertIn('18 ألف',result['reply']);self.assertIn('5 آلاف',result['reply'])

    def test_empty_local_human_review_is_not_replaced_with_product_reask(self):
        with patch.object(self.m,'extract_facebook_event',return_value=dict(self.ev,text='عمرها 4 سنوات')), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'is_customer_ai_enabled',return_value=True), \
             patch.object(self.m,'is_store_feature_enabled',return_value=False), \
             patch.object(self.m,'call_main_ai',return_value={'reply':'','requires_human':True,
                 'create_order':False,'order':{},'handoff_reason':'جدول أعمار غير موثق'}):
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertEqual(result['reply'],'')
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE sender_id=? AND status='pending'",(self.sender,)).fetchone()[0],1)
        self.assertEqual(self.order_count(),0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM messages WHERE sender_id=? AND direction='outgoing'",(self.sender,)).fetchone()[0],0)

    def test_known_model_reask_variant_is_repaired_after_age_answer(self):
        self.m._current_store_id.reset(self.token)
        self.token=self.m._current_store_id.set('baraah-kids')
        self.db.execute('UPDATE customers SET store_id=? WHERE sender_id=?',('baraah-kids',self.sender));self.db.commit()
        self.ev['store_id']='baraah-kids'
        self.product.update(category='ملابس أطفال',product_name='فستان أطفال اختبار',sizes='من سنة الى 8 سنوات',colors='وردي')
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')
        self.save('outgoing','شكد عمر البنوتة؟')
        with patch.object(self.m,'extract_facebook_event',return_value=dict(self.ev,text='عمرها 4 سنوات')), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'is_customer_ai_enabled',return_value=True), \
             patch.object(self.m,'is_store_feature_enabled',return_value=False), \
             patch.object(self.m,'_call_main_ai_once',return_value={
                'reply':'ممكن توضحين أكثر شنو الموديل المطلوب؟ حتى أتأكدلج من التوفر والسعر',
                'create_order':False,'order':{}}):
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertFalse(self.m.reply_reasks_known_product(result['reply']),result)
        self.assertNotIn('الموديل المطلوب',result['reply'])


class NaturalWarmthTests(unittest.TestCase):
    def test_neutral_checkout_wording_does_not_duplicate_booking_noun(self):
        for text in ('دزيلي الرقم حتى أثبتلج الحجز','دزيلي الرقم حتى أثبتلج حجز'):
            reply=owner_sales.surface(text,'',warm=False)
            self.assertEqual(reply.count('الحجز'),1,reply)

    def test_warmth_policy_avoids_repeated_endearments_and_respects_formal_request(self):
        self.assertFalse(owner_sales.warmth_allowed('اي',[
            {'direction':'outgoing','text':'حبيبتي، الأسود متوفر.'}]))
        self.assertFalse(owner_sales.warmth_allowed('بلا ألقاب خلينا رسمي',[]))
        self.assertFalse(owner_sales.warmth_allowed('ليش ما ثبت طلبي، انزعجت',[]))

    def test_surface_can_remove_endearments_without_changing_facts(self):
        text=owner_sales.surface('حبيبتي السعر 18 ألف، عيني التوصيل 5 آلاف، قياس 44.',
                                'female',warm=False,allow_endearments=False)
        self.assertFalse(owner_sales.AFFECTION.search(text))
        for fact in ('18 ألف','5 آلاف','44'):self.assertIn(fact,text)
