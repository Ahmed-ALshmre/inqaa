"""Regressions from the November 7 audit, with synthetic people and no delivery."""
import unittest
from unittest.mock import patch
from account_app import sales_answers,sales_recovery,product_albums
from account_app.tests import test_last50_checkout_nov01 as fixtures


class AuditedQuestionShapes(unittest.TestCase):
    def test_delivery_destination_words_and_arabic_kurdish_letters(self):
        for question in ('كم تبلغ تكلفة التوصيل إلى منزلي؟','كم تبلغ تكلفة التوصيل إلى محافظتي؟',
                         'كم تبلغ تكلفة التوصيل للبيت؟','كم تبلغ تكلفة التوصيل لمنزلي؟'):
            self.assertEqual(sales_recovery.policy_question(question),'delivery',question)
        for question in ('متی توسیل؟','متى التوصيل؟','متي توسيل؟'):
            self.assertEqual(sales_recovery.policy_question(question),'delivery_time',question)
            self.assertIn('delivery_time',sales_answers.topics(question),question)

    def test_delivery_aliases_never_consume_a_complaint_condition_or_product_question(self):
        for question in ('متی توسیل طلبي المتأخر؟','اذا يوصل باجر ثبتيه',
                         'كم تبلغ تكلفة التوصيل إلى منزلي وشنو القماش؟','ليش التوصيل تاخر؟'):
            self.assertEqual(sales_recovery.policy_question(question),'',question)

    def test_material_questions_are_fabric_but_washing_materials_are_not(self):
        for question in ('ما هي المواد المستخدمة في التنورة؟','شنو المادة المصنوع منها الفستان؟',
                         'من أي مادة مصنوعة الحقيبة؟'):
            self.assertIn('fabric',sales_answers.topics(question),question)
        self.assertNotIn('fabric',sales_answers.topics('ما هي المواد المستخدمة بالغسيل؟'))


class AuditedReplyPipeline(unittest.TestCase):
    setUpClass=classmethod(fixtures.Last50PipelineTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.Last50PipelineTests.tearDownClass.__func__)
    setUp=fixtures.Last50PipelineTests.setUp
    tearDown=fixtures.Last50PipelineTests.tearDown
    link=fixtures.Last50PipelineTests.link
    save=fixtures.Last50PipelineTests.save
    process=fixtures.Last50PipelineTests.process
    order=fixtures.Last50PipelineTests.order

    def test_home_delivery_answers_even_when_an_ad_has_no_product_mapping(self):
        def unknown(module,db,ev,products):
            return dict(ev,_ad_product_unresolved=True,_ad_source={'resolution':'unmapped'})
        with patch.object(self.m.zernio,'product_event',side_effect=unknown),\
             patch.object(self.m,'ad_creative_product') as vision:
            result,model=self.process('كم تبلغ تكلفة التوصيل إلى منزلي؟')
        model.assert_not_called();vision.assert_not_called()
        self.assertIn('توصيل',result['reply']);self.assertIn('5',result['reply'])
        self.assertTrue(result['meta']['store_policy_reply']);self.assertIsNone(self.order())
        self.assertFalse(self.m.has_pending_human_review(self.db,self.sender))

    def test_kurdish_delivery_time_does_not_repeat_saved_age_and_price(self):
        self.link();self.product.update(product_name='تراك',sizes='1 إلى 4 سنوات')
        self.m.set_store_setting(self.db,'delivery_time','من 24 ساعة إلى يومين')
        self.save('incoming','عمر سنة')
        result,model=self.process('متی توسیل؟')
        model.assert_not_called();self.assertIn('توصيل',result['reply'])
        self.assertNotIn('عمر سنة متوفر',result['reply']);self.assertNotIn('سعره',result['reply'])
        self.assertIsNone(self.order())

    def test_latest_policy_answer_does_not_mark_an_older_price_question_answered(self):
        self.link();self.m.set_store_setting(self.db,'delivery_time','من 24 ساعة إلى يومين')
        self.save('incoming','شكد سعر قطعتين؟')
        result,model=self.process('متی توسیل؟')
        model.assert_not_called();self.assertIn('يومين',result['reply'])
        self.assertTrue(result['meta']['partial_policy_reply'])
        last=self.db.execute("SELECT raw_payload FROM messages WHERE sender_id=? AND direction='outgoing' ORDER BY id DESC",(self.sender,)).fetchone()
        self.assertIn('"_partial_policy_reply": true',last['raw_payload'])
        self.assertIsNone(self.order())

    def test_pending_complete_checkout_is_not_replaced_by_its_last_shipping_question(self):
        self.link();self.m.set_store_setting(self.db,'delivery_time','من 24 ساعة إلى يومين')
        self.save('incoming','أريد الأسود قياس 44 قطعة وحدة')
        self.save('outgoing','دزيلي رقم الموبايل والعنوان حتى أثبتلج الحجز')
        self.save('incoming','07701234567 بغداد حي الاختبار')
        self.ev['text']='متی توسیل؟'
        basket=dict(reply='صار بعيوني',create_order=True,requires_human=False,intent='order',order=dict(items=[
            dict(product_id=self.product['product_id'],color='اسود',size='44',size_type='size',quantity=1)]))
        with patch.object(self.m,'extract_facebook_event',return_value=self.ev),\
             patch.object(self.m,'is_ai_enabled',return_value=True),\
             patch.object(self.m,'is_customer_ai_enabled',return_value=True),\
             patch.object(self.m,'is_store_feature_enabled',return_value=False),\
             patch.object(self.m,'_call_main_ai_once',return_value=basket):
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertFalse(result.get('meta',{}).get('store_policy_reply'))
        self.assertIsNotNone(self.order())
        self.assertIn('تم تثبيت الطلب',result['reply'])
        self.assertIn(self.m.DEFAULT_ORDER_CONFIRMATION_TEXT,result['reply'])

    def test_recorded_material_answers_without_model_or_a_new_greeting(self):
        self.link();self.product.update(product_name='تنورة',fabric='دبل إيطالي',notes='قماش شتوي')
        self.save('outgoing','هلا عيني، شنو حابة تعرفين؟')
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai(dict(self.ev,text='ما هي المواد المستخدمة في التنورة؟'),
                'text',{},[dict(direction='outgoing',text='هلا عيني')],[self.product],self.product,None,'',[])
        model.assert_not_called();self.assertIn('دبل إيطالي',result['reply'])
        self.assertNotIn('أهلا وسهلا',result['reply']);self.assertFalse(result.get('requires_human'))

    def test_missing_material_still_hands_off_silently(self):
        self.link();self.product.update(product_name='تنورة',fabric='',notes='',description='',fabric_feel='')
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai(dict(self.ev,text='ما هي المواد المستخدمة في التنورة؟'),
                'text',{},[],[self.product],self.product,None,'',[])
        model.assert_not_called();self.assertEqual(result['reply'],'');self.assertTrue(result['requires_human'])

    def album(self):
        self.link();self.product.update(product_name='تنورة تركي',reply_category='تنورة',fabric='دبل إيطالي',
            measurements='طول التنورة 105 سم',sizes='38 إلى 48')
        return [dict(image_index=i,product_found=True,product_id=self.product['product_id'],
                     image_url='https://images.test/'+str(i)+'.jpg',observed_color='اسود') for i in (1,2)]

    def test_two_photos_of_one_product_answer_length_and_size_once(self):
        photos=self.album()
        with patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.call_main_ai(dict(self.ev,text='ممكن الطول التنورة وبيها قياس 46',_image_matches=photos),
                'image',{},[],[self.product],self.product,{'images':photos},'',[])
        model.assert_not_called()
        self.assertEqual(result['reply'].count('105'),1,result)
        self.assertLessEqual(result['reply'].count('46'),1,result)
        self.assertEqual(result['reply'].count('قياسج موجود'),1,result)
        self.assertLessEqual(result['reply'].count('أهلا وسهلا'),1,result)
        self.assertNotIn('سعر',result['reply']);self.assertFalse(result.get('create_order'))

    def test_album_with_unknown_different_or_unavailable_color_is_not_collapsed(self):
        photos=self.album()
        self.assertTrue(product_albums.single_product_album(photos,self.product))
        for change in (dict(product_found=False),dict(product_id='other'),dict(observed_color='أخضر')):
            different=[photos[0],dict(photos[1],**change)]
            self.assertFalse(product_albums.single_product_album(different,self.product),change)
