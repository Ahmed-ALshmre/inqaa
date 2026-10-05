"""Adversarial response, buying-intent and catalog-context regressions."""
import copy
import json
import unittest
from unittest.mock import Mock, patch

from account_app import sales_strategy as strategy
from account_app.tests import test_audit_orders as fixtures
from account_app.tests.sales_scenarios import CASES,DRESS,CHEAPER,BUNDLE


class SalesMethodsTests(unittest.TestCase):
    def test_each_evaluation_scenario_gets_specific_guidance(self):
        for case in CASES:
            with self.subTest(case=case['id']):
                selected=strategy.signals(case['text'])
                self.assertTrue(selected)
                guide=strategy.guidance({'text':case['text']},case.get('customer',{}),[])
                for key in selected:self.assertIn(strategy.METHODS[key],guide)
                self.assertIn('لا تعتبر السؤال موافقة',guide)

    def test_combined_objections_are_not_reduced_to_one_keyword(self):
        selected=strategy.signals('غالي واخاف القياس ما يناسبني والتوصيل يتاخر')
        for key in ['price','trust','fit','delivery']:self.assertIn(key,selected)

    def test_rejecting_one_color_is_not_an_optout(self):
        text='الكريمي ما اريده، اريد الاسود قياس 44'
        self.assertNotEqual(strategy.signals(text),['refusal'])
        self.assertFalse(strategy.purchase_block_reason(text))

    def test_explicit_no_purchase_and_deferral_override_ai(self):
        for text in ['لا تثبتين بس اتأكد','بدون لا تثبتين الطلب','خلي استلم الراتب وبعدين اطلب','بعدني محتارة','ما اريد اطلب لا تراسلوني']:
            with self.subTest(text=text):
                self.assertTrue(strategy.purchase_block_reason(text))
                self.assertTrue(strategy.decision_error({'create_order':True},text))

    def test_affirmative_buying_is_not_blocked(self):
        for text in ['ثبتي الاسود قياس 42','استلمت الراتب ثبتي الطلب','الوردي ما اريده اريد الاسود','ثنينهم حلوات ثبتيهن']:
            self.assertFalse(strategy.purchase_block_reason(text),text)

    def test_false_scarcity_fit_and_photo_guarantees_are_rejected(self):
        for text in ['احجزي قبل ما يخلص','آخر قطعة','الكمية محدودة','يوصلج نفس الصورة بالضبط','القياس مضمون يلبس']:
            with self.subTest(text=text):self.assertTrue(strategy.reply_error(text,'اريد معلومات',DRESS))
        self.assertFalse(strategy.reply_error('السعر 15 ألف والتوصيل 5 آلاف؛ متوفر أسود وكريمي','شكد',DRESS))

    def test_post_order_does_not_get_new_sales_pressure(self):
        self.assertEqual(strategy.guidance({'_post_order':{'id':1},'text':'شوكت يوصل'}, {}, []),'')

    def test_weight_without_mapping_and_unverified_photo_are_rejected(self):
        self.assertTrue(strategy.reply_error('وزن 69 يناسبج قياس 42','وزني 69',DRESS))
        self.assertTrue(strategy.reply_error('وزن 69 يلبسج قياس 42','وزني 69',DRESS))
        self.assertIn('الوزن وحده', strategy.missing_fact_reply('وزني 69 شنو قياسي؟', DRESS))
        self.assertTrue(strategy.reply_error('تصوير الموديل حقيقي','اريد فيديو',DRESS))
        self.assertFalse(strategy.reply_error('ما عندي فيديو حقيقي موثق','اريد فيديو',DRESS))

    def test_price_objection_must_not_end_with_booking(self):
        self.assertTrue(strategy.premature_close('عندج فحص، أحجزه الج؟','غالي علي'))
        self.assertFalse(strategy.premature_close('تحبين تشوفين بديل بـ13 ألف؟','غالي علي'))

    def test_legacy_rule_upgrade_retains_custom_facts(self):
        text="التوصيل لبغداد 5000. طول الرد إلزامي: من جملة إلى جملتين قصيرتين فقط (≤ 25 كلمة). ممنوع الإطالة."
        upgraded=strategy.upgrade_legacy_rules(text)
        self.assertIn('5000',upgraded)
        self.assertNotIn('≤ 25',upgraded)


class SalesReplyPipelineTests(unittest.TestCase):
    setUp=fixtures.AuditFixTests.setUp
    tearDown=fixtures.AuditFixTests.tearDown
    customer=fixtures.AuditFixTests.customer

    def test_main_prompt_locks_natural_iraqi_style(self):
        self.assertIn('رسالة موظفة عراقية حقيقية', self.m.IRAQI_HUMAN_STYLE_LOCK)
        self.assertIn('ممنوع الفصحى الرسمية', self.m.IRAQI_HUMAN_STYLE_LOCK)

    def ask(self,text,responses):
        self.customer('test')
        ev={'sender_id':'test','text':text,'image_url':None}
        with patch.object(self.m,'_call_main_ai_once',side_effect=responses) as model:
            result=self.m.call_main_ai(ev,'text',{},[],[DRESS,CHEAPER],DRESS,None,'',[],customer_products=[DRESS])
        return result,model

    def test_fabric_value_is_preserved_after_scarcity_repair(self):
        bad={'reply':'آخر قطعة، احجزي قبل ما يخلص','create_order':False}
        good={'reply':'قماشه باربي وسعره 15 ألف؛ الأسود والكريمي متوفرات.','create_order':False}
        result,model=self.ask('غالي بس شنو قماشه؟',[bad,good])
        self.assertEqual(result['reply'],good['reply'])
        self.assertEqual(model.call_count,2)
        self.assertIn('الندرة',model.call_args.kwargs['fix_instruction'])

    def test_repeated_false_guarantee_is_not_delivered(self):
        bad={'reply':'نعم يوصلك نفس الصورة بالضبط','create_order':False}
        result,_=self.ask('تضمنون الصورة؟',[bad,bad])
        self.assertNotIn('بالضبط',result['reply'])
        self.assertTrue(result['_needs_fact_review'])

    def test_bad_model_cannot_book_against_explicit_refusal(self):
        bad={'reply':'تم تثبيت الطلب','create_order':True,'order':{'items':[{'product_id':'D1'}]}}
        result,_=self.ask('لا تثبتين بس اتأكد من القياس',[bad,bad])
        self.assertFalse(result['create_order'])
        self.assertFalse(self.m.checkout_requested(self.db,{'text':'لا تثبتين'},bad))
        self.assertNotIn('تم تثبيت',result['reply'])

    def test_complete_contact_is_not_reasked_in_model_instruction(self):
        customer={'phone':'07700000000','province':'بغداد','address':'حي تجريبي'}
        guide=strategy.guidance({'text':'ثبتي الاسود'},customer,[])
        self.assertIn('[]',guide)

    def test_unknown_video_has_truthful_reply_and_internal_review(self):
        bad={'reply':'','create_order':False,'requires_human':True,'handoff_reason':'video unavailable'}
        result,_=self.ask('اريد فيديو حقيقي قبل اطلب',[bad,bad])
        self.assertIn('ما عندي فيديو',result['reply'])
        self.assertFalse(result['_needs_fact_review'])
        self.assertFalse(result.get('failed'))

    def test_linked_product_missing_fact_still_opens_internal_review(self):
        self.customer('test')
        m=self.m
        m.save_message(self.db,'test','incoming','text','اريد فيديو حقيقي',None,None,None,{})
        reply={'reply':'حالياً ما عندي فيديو حقيقي موثق.','create_order':False,'_needs_fact_review':True}
        with patch.object(m,'load_active_products',return_value=[DRESS]), patch.object(m,'call_main_ai',return_value=reply), patch.object(m,'create_human_review') as review, patch.object(m,'send_webhook_result_to_facebook',return_value=True), patch.object(m,'schedule_followup_if_needed'):
            result=m.auto_reply_after_product_link(self.db,'test',DRESS,staff_action=True)
        self.assertTrue(result['sent'])
        review.assert_called_once()
        self.assertIn('غير موثقة',review.call_args.args[2])

    def test_premature_close_retries_then_asks_budget_without_handoff(self):
        bad={'reply':'قماشه باربي، أحجزه الج؟','create_order':False}
        result,model=self.ask('غالي',[bad,bad])
        self.assertEqual(model.call_count,2)
        self.assertIn('الميزانية',result['reply'])
        self.assertFalse(result.get('_needs_fact_review'))

    def test_real_prompt_includes_notes_and_targeted_sales_guidance(self):
        self.customer('test')
        response=Mock()
        response.json.return_value={'choices':[{'message':{'content':json.dumps({'reply':'طوله 135 سم وإغلاقه سحاب خلفي.','create_order':False})},'finish_reason':'stop'}]}
        with patch.object(self.m,'OPENROUTER_KEY','test-key'),patch.object(self.m.ai_transport,'post',return_value=response) as provider:
            self.m._call_main_ai_once({'sender_id':'test','text':'شكد طوله ومنين ينفتح','image_url':None},'text',{},[],[DRESS],DRESS,None,'',[],customer_products=[DRESS])
        messages=provider.call_args.kwargs['json']['messages']
        self.assertIn('135',messages[1]['content'])
        self.assertIn('سحاب خلفي',messages[1]['content'])
        self.assertIn('خطة الرد البيعي',messages[0]['content'])

    def test_multi_variant_checkout_charges_shipping_once(self):
        self.customer('buyer')
        data={'phone':'07700000000','province':'بغداد','address':'بغداد حي تجريبي','items':[
            dict(product_id='D1',product_name='فستان رباط',color='اسود',size='42',quantity=1),
            dict(product_id='D1',product_name='فستان رباط',color='كريمي',size='46',quantity=1)]}
        with patch.object(self.m,'load_products_from_file',return_value=[DRESS]),patch.object(self.m,'send_order_to_telegram',return_value=True),patch.object(self.m,'save_booking_to_file'):
            created,_=self.m.create_order_if_valid(self.db,'buyer',{'order':data},DRESS)
        self.assertTrue(created)
        row=self.db.execute('SELECT * FROM orders WHERE sender_id=?',('buyer',)).fetchone()
        self.assertEqual(row['total_amount'],35000)
        self.assertEqual([(i['color'],i['size']) for i in json.loads(row['order_items'])],[('اسود','42'),('كريمي','46')])

    def test_bundle_quantity_is_three_items_with_free_delivery(self):
        self.customer('bundle')
        data={'phone':'07700000000','province':'بغداد','address':'بغداد حي تجريبي','items':[
            dict(product_id='B1',product_name='سوت بهاري',color=color,size='سنة',quantity=1) for color in ('وردي','ماروني','تركوازي')]}
        with patch.object(self.m,'load_products_from_file',return_value=[BUNDLE]),patch.object(self.m,'send_order_to_telegram',return_value=True),patch.object(self.m,'save_booking_to_file'):
            created,_=self.m.create_order_if_valid(self.db,'bundle',{'order':data},BUNDLE)
        self.assertTrue(created)
        row=self.db.execute('SELECT * FROM orders WHERE sender_id=?',('bundle',)).fetchone()
        self.assertEqual((row['product_total'],row['delivery_fee'],row['total_amount']),(25000,0,25000))
