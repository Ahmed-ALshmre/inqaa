import copy
import json
import sqlite3
import unittest
from unittest.mock import Mock, patch

from account_app import sales_context, sales_decision, sales_recovery, conversion_growth
from account_app.checkout import order_line_error
from account_app.tests import test_audit_orders as fixtures

PRODUCT = dict(product_id='A', product_name='كيلوت تجريبي', category='بنطلون', price='17000',
               colors='اسود، كريمي', sizes='38 إلى 52', stock='متوفر', status='active', fabric='باربي',
               image_url='https://images.test/a.jpg')


class DecisionMemoryTests(unittest.TestCase):
    def test_unanswered_length_survives_size_and_color_answers(self):
        state = sales_context.advance({}, [
            dict(direction='incoming', text='طول البنطرون لطفا'),
            dict(direction='outgoing', text='شنو قياسج؟'),
            dict(direction='incoming', text='48'),
            dict(direction='outgoing', text='قياس 48 متوفر بالأسود'),
            dict(direction='incoming', text='الاسود')])
        self.assertEqual(state['open_details'], ['length'])
        self.assertIn('طول القطعة', sales_decision.brief({'text':'ثبتيه', '_sales_context':state}, PRODUCT))
        self.assertTrue(sales_decision.unresolved_purchase_questions(state, PRODUCT))

    def test_factual_answer_resolves_length_but_delay_does_not(self):
        self.assertEqual(sales_decision.advance_details(['length'], 'outgoing', 'الطول يحتاج تأكيد من المتجر'), ['length'])
        self.assertEqual(sales_decision.advance_details(['length'], 'outgoing', 'طوله مناسب'), ['length'])
        self.assertEqual(sales_decision.advance_details(['length'], 'outgoing', 'طول القطعة 104 سم. تحبين الأسود؟'), [])
        self.assertEqual(sales_decision.advance_details(['length'], 'outgoing', 'طوله 104 سم؟'), ['length'])
        self.assertEqual(sales_decision.advance_details(['length'], 'outgoing', 'طول القطعة 104 سم.'), [])
        self.assertEqual(sales_decision.advance_details(['length'], 'outgoing', '104 سم'), [])
        self.assertFalse(sales_decision.unresolved_purchase_questions({'open_details':['length']}, dict(PRODUCT,measurements='طول القطعة 104 سم')))

    def test_waiver_or_explicit_model_change_clears_old_requirement(self):
        self.assertEqual(sales_decision.advance_details(['length'], 'incoming', 'ما يهمني الطول'), [])
        self.assertEqual(sales_decision.advance_details(['length'], 'incoming', 'ما اريد هذا الموديل'), [])
        self.assertEqual(sales_decision.advance_details([], 'incoming', 'طولي 160'), [])

    def test_price_discrepancy_is_addressed_before_fit_and_not_copied_as_instruction(self):
        text='ليش مو كاتبين 10؟ سعر 10'
        self.assertTrue(sales_decision.price_mismatch(text))
        self.assertFalse(sales_decision.price_mismatch('ليش سعره 25 الف؟'))
        self.assertFalse(sales_decision.price_mismatch('شكد السعر؟'))
        reply=sales_decision.brief({'text':text})
        self.assertIn('اختلاف السعر',reply)
        self.assertIn('لا تكرر سؤال الوزن',reply)
        self.assertNotIn(text,reply)
        self.assertEqual(sales_decision.brief({'text':text,'_post_order':{'id':1}}),'')

    def test_budget_alternatives_preserve_size_color_and_total_budget(self):
        candidates=[dict(PRODUCT,product_id='B',price='12000'),
                    dict(PRODUCT,product_id='C',price='10000',sizes='38 إلى 42'),
                    dict(PRODUCT,product_id='D',price='11000',colors='وردي'),
                    dict(PRODUCT,product_id='E',price='9000',stock='نفذ')]
        state={'evidence':[{'text':'قياس 48 اسود'},{'text':'ميزانيتي 18 الف ويا التوصيل'}]}
        result=sales_decision.budget_alternatives('اريد ارخص',state,candidates,PRODUCT,5000)
        self.assertEqual([p['product_id'] for p in result],['B'])
        self.assertEqual(result[0]['total'],17000)
        self.assertEqual(sales_decision.budget_alternatives('ميزانيتي 15 الف ويا التوصيل',state,candidates,PRODUCT,5000),[])

    def test_unknown_age_weight_or_wrong_category_is_not_a_verified_alternative(self):
        candidate=dict(PRODUCT,product_id='B',price='10000')
        for text in ['وزني 100', 'عمر سنة ونص', 'طفلين واحد 2 سنوات والثاني 5 سنوات']:
            self.assertEqual(sales_decision.budget_alternatives('اريد ارخص',{'evidence':[{'text':text}]},[candidate],PRODUCT,5000),[])
        self.assertEqual(sales_decision.budget_alternatives('اريد ارخص',{},[dict(candidate,category='فستان')],PRODUCT,5000),[])

    def test_child_age_requires_a_matching_recorded_age_range(self):
        original=dict(PRODUCT,category='اطفال',sizes='1 إلى 7 سنوات')
        young=dict(original,product_id='Y',price='10000',sizes='1 إلى 3 سنوات')
        older=dict(original,product_id='O',price='12000',sizes='4 إلى 7 سنوات')
        result=sales_decision.budget_alternatives('اريد ارخص',{'evidence':[{'text':'عمر 6 سنوات'}]},[young,older],original,5000)
        self.assertEqual([p['product_id'] for p in result],['O'])

    def test_split_photo_acceptance_is_browsing_only(self):
        self.assertTrue(sales_recovery.simple_photo_request('دزي\nبله\nخل اشوف'))
        self.assertFalse(sales_recovery.simple_photo_request('دزي بله خل اشوف وشنو قماشه'))

    def test_weight_range_never_becomes_a_guessed_weight(self):
        line=dict(product_id='A',quantity=1,color='اسود',size='44',size_type='weight',weight='75-80')
        self.assertIn('برقم واحد',order_line_error([line],[PRODUCT]))
        self.assertEqual(line['weight'],'75-80')
        self.assertEqual(order_line_error([dict(line,weight='75')],[PRODUCT]),'')
        self.assertEqual(order_line_error([dict(line,weight='',size='75',size_type='weight')],
                                         [dict(PRODUCT,sizes='70 إلى 90 كيلو')]),'')
        self.assertIn('برقم واحد',order_line_error([dict(line,weight='',size='75-80')],[PRODUCT]))


class DecisionPipelineTests(unittest.TestCase):
    setUp=fixtures.AuditFixTests.setUp
    tearDown=fixtures.AuditFixTests.tearDown
    customer=fixtures.AuditFixTests.customer

    def test_order_is_held_until_prior_decision_question_is_answered(self):
        self.customer('pending-length')
        for direction,text in [('incoming','طول البنطرون لطفا'),('outgoing','شنو قياسج؟'),('incoming','قياس 44')]:
            self.m.save_message(self.db,'pending-length',direction,'text',text,None,None,None,{})
        order=dict(phone='07700000000',province='بغداد',address='بغداد حي تجريبي',
                   items=[dict(product_id='A',quantity=1,color='اسود',size='44')])
        with patch.object(self.m,'_call_main_ai_once',return_value=dict(reply='نكمل الطلب',create_order=True,order=copy.deepcopy(order))):
            result=self.m.call_main_ai(dict(sender_id='pending-length',text='ثبتيه'),'text',{},[],[PRODUCT],PRODUCT,None,'',[],customer_products=[PRODUCT])
        self.assertFalse(result['create_order'])
        self.assertTrue(result['_needs_fact_review'])
        self.assertIn('طول القطعة',result['reply'])
        self.assertEqual(result['order']['items'],order['items'])
        self.assertEqual(self.db.execute('select count(*) from orders').fetchone()[0],0)

    def test_current_decision_guidance_reaches_provider(self):
        self.customer('decision-prompt')
        response=Mock();response.raise_for_status.return_value=None
        response.json.return_value={'choices':[{'message':{'content':json.dumps({'reply':'السعر يحتاج مراجعة','create_order':False})}}]}
        with patch.object(self.m,'OPENROUTER_KEY','test-key'),patch.object(self.m.ai_transport,'post',return_value=response) as provider:
            self.m._call_main_ai_once(dict(sender_id='decision-prompt',text='ليش مو كاتبين سعر 10؟'), 'text',{},[],[PRODUCT],PRODUCT,None,'',[])
        system=provider.call_args.kwargs['json']['messages'][0]['content']
        self.assertIn('الزبون يعترض على اختلاف السعر',system)

    def test_advertised_price_dispute_gets_facts_and_real_review_before_checkout(self):
        self.customer('ad-price-dispute')
        with patch.object(self.m,'_call_main_ai_once') as provider:
            result=self.m.call_main_ai(dict(sender_id='ad-price-dispute',text='ليش مو كاتبين 10؟ سعر 10'),
                'text',{},[],[PRODUCT],PRODUCT,None,'',[])
        provider.assert_not_called()
        self.assertFalse(result['create_order'])
        self.assertTrue(result['_needs_fact_review'])
        self.assertIn('17,000',result['reply'])
        self.assertIn('فرق السعر',result['reply'])
        self.assertNotIn('وزنج',result['reply'])

    def test_split_photo_request_returns_offered_images_without_checkout(self):
        self.customer('photo-decision')
        self.m.save_message(self.db,'photo-decision','outgoing','text','كيلوت تجريبي سعره 17 ألف، تحبين تشوفين صوره؟',None,None,None,{})
        with patch.object(self.m,'_call_main_ai_once') as provider:
            result=self.m.call_main_ai(dict(sender_id='photo-decision',text='دزي\nبله\nخل اشوف'),'text',{},[],[PRODUCT],PRODUCT,None,'',[])
        provider.assert_not_called()
        self.assertEqual(result['image_product_ids'],['A'])
        self.assertFalse(result['create_order'])


class MaturedConversionTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript('CREATE TABLE messages(store_id TEXT,sender_id TEXT,direction TEXT,created_at TEXT); CREATE TABLE orders(store_id TEXT,sender_id TEXT,status TEXT,created_at TEXT);')

    def contact(self,sender,time,store='default'):
        self.db.execute("INSERT INTO messages VALUES(?,?,'incoming',?)",(store,sender,time))

    def order(self,sender,time,status='new',store='default'):
        self.db.execute('INSERT INTO orders VALUES(?,?,?,?)',(store,sender,status,time))

    def report(self,**kwargs):
        return conversion_growth.matured_metrics(self.db,'2026-10-19','2026-10-21','2026-10-21T12:00:00+03:00',booking_source='local',**kwargs)

    def test_equal_windows_exclude_immature_contacts_and_late_orders(self):
        self.contact('within','2026-10-19T10:00:00+03:00');self.order('within','2026-10-20T09:59:00+03:00')
        self.order('within','2026-10-20T09:58:00+03:00')
        self.contact('late','2026-10-19T10:00:00+03:00');self.order('late','2026-10-20T10:00:00+03:00')
        self.contact('immature','2026-10-20T20:00:00+03:00');self.order('immature','2026-10-20T21:00:00+03:00')
        result=self.report()
        self.assertEqual((result['people'],result['buyers'],result['immature_contacts']),(2,1,1))
        self.assertEqual(result['conversion'],50)

    def test_store_timezone_returning_and_cancellation_boundaries(self):
        self.contact('same','2026-10-19T07:00:00Z');self.order('same','2026-10-20T06:59:00Z')
        self.contact('same','2026-10-19T10:00:00',store='other');self.order('same','2026-10-19T11:00:00',status='cancelled',store='other')
        self.contact('returning','2026-10-18');self.contact('returning','2026-10-19');self.order('returning','2026-10-19T12:00:00')
        self.assertEqual(self.report(store_id='default')['people'],1)
        self.assertEqual(self.report(store_id='default')['buyers'],1)
        self.assertEqual(self.report(store_id='other')['buyers'],0)
        self.assertEqual(self.report()['people'],2)

    def test_target_changes_required_buyers_not_measured_conversion(self):
        for n in range(10): self.contact(str(n),'2026-10-19T10:00:00')
        self.order('0','2026-10-19T11:00:00')
        self.assertEqual(self.report(target=30)['additional_buyers'],2)
        self.assertEqual(self.report(target=30)['conversion'],self.report(target=50)['conversion'])
        with self.assertRaises(ValueError):self.report(hours=0)

    def test_confirmed_source_excludes_unlinked_and_remote_cancelled_orders(self):
        self.db.executescript('ALTER TABLE orders ADD COLUMN id INTEGER; CREATE TABLE menger_deliveries(order_id INTEGER,status TEXT,remote_order_id TEXT,remote_status TEXT);')
        for n, remote_status in enumerate(['new','cancelled','draft']):
            self.contact(str(n),'2026-10-19T10:00:00')
            self.db.execute('INSERT INTO orders VALUES(?,?,?,?,?)',('default',str(n),'new','2026-10-19T11:00:00',n))
            self.db.execute('INSERT INTO menger_deliveries VALUES(?,?,?,?)',(n,'sent',str(n+1),remote_status))
        self.contact('unlinked','2026-10-19T10:00:00')
        self.db.execute('INSERT INTO orders VALUES(?,?,?,?,?)',('default','unlinked','new','2026-10-19T11:00:00',3))
        report=conversion_growth.matured_metrics(self.db,'2026-10-19','2026-10-21',
            '2026-10-21T12:00:00+03:00',booking_source='menger')
        self.assertEqual((report['people'],report['buyers'],report['booking_source']),(4,1,'menger'))
