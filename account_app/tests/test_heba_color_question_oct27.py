"""Reported colour question must not match buttons inside Iraqi availability words."""
import unittest
from unittest.mock import patch

from account_app import sales_answers, sales_decision, sales_strategy
from account_app.tests import test_audit_orders as fixtures


PRODUCT = dict(store_id='al-fatena', product_id='P003', product_name='فستان دانتيل',
               colors='اسود', sizes='38 إلى 52', fabric='لينن', price='16000',
               stock='متوفر', status='active', closure='', stretch='', notes='')


class AvailabilityWordTests(unittest.TestCase):
    def test_availability_words_do_not_invent_a_missing_closure(self):
        for question in ('شنو الوان الي عدكم', 'شنو الألوان اللي عندكم؟',
                         'متوفر عدكم فستان تخرج', 'المتوفر عندكم شنو؟'):
            with self.subTest(question=question):
                self.assertEqual(sales_strategy.missing_fact_reply(question, PRODUCT), '')
                self.assertFalse(sales_strategy.fact_requires_human(question, PRODUCT))
                self.assertNotIn('closure', sales_answers.topics(question))
                self.assertNotIn('closure', sales_decision.advance_details([], 'incoming', question))

    def test_catalog_availability_prose_is_not_a_closure_fact(self):
        product = dict(PRODUCT, description='متوفر عدكم بالأسود', notes='الألوان الموجودة عندكم')
        self.assertEqual(sales_strategy.detail_fact(product, 'closure'), '')

    def test_real_button_questions_and_facts_keep_their_meaning(self):
        for question in ('بيه دكم لو لاستيك؟', 'دگم لو سحاب؟'):
            self.assertTrue(sales_strategy.missing_fact_reply(question, PRODUCT))
            self.assertTrue(sales_strategy.fact_requires_human(question, PRODUCT))
        product = dict(PRODUCT, closure='الخصر دكم')
        self.assertEqual(sales_strategy.missing_fact_reply('بيه دكم؟', product), '')
        self.assertFalse(sales_strategy.fact_requires_human('بيه دكم؟', product))
        self.assertEqual(sales_strategy.detail_fact(product, 'closure'), 'الخصر دكم')


class ColorQuestionPreviewTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token=self.m._current_store_id.set('al-fatena')
        self.sender='al-fatena::synthetic-reported-color-question'
        self.m.get_or_create_customer(self.db,self.sender,'synthetic-page','facebook')
        self.catalog_patch=patch.object(self.m,'load_products_from_file',return_value=[PRODUCT])
        self.catalog_patch.start()
        self.m.complete_customer_product_link(self.db,self.sender,PRODUCT,'manual',source='manual_admin')
        for direction,text in [('incoming','شقد القياسات'),
                               ('outgoing','القياسات من 38 إلى 52. شنو القياس المطلوب؟'),
                               ('incoming','شنو الوان الي عدكم'),('incoming','🤔')]:
            self.m.save_message(self.db,self.sender,direction,'text',text,None,None,None,{})

    def tearDown(self):
        self.catalog_patch.stop()
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def preview(self,payload):
        with patch.object(self.m,'_call_main_ai_once',return_value={
                'reply':'فستان الدانتيل متوفر بالأسود فقط. شنو القياس المطلوب؟',
                'create_order':False,'order':{},'requires_human':False}):
            response=self.client.post('/api/conversations/'+self.sender+'/ask_ai?store_id=al-fatena',json=payload)
        self.assertEqual(response.status_code,200,response.get_json())
        data=response.get_json()
        self.assertIn('الأسود',data['reply'])
        for wrong in ('لاستيك','دگم','تشيّكه'):
            self.assertNotIn(wrong,data['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0],0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM messages WHERE sender_id=? AND direction='outgoing'",(self.sender,)).fetchone()[0],1)

    def test_exact_reported_question_succeeds_in_dashboard_preview(self):
        self.preview({'text':'شنو الوان الي عدكم','product_id':'P003'})

    def test_followup_emoji_preserves_the_unanswered_colour_question(self):
        self.preview({'text':'','allow_empty':True,'product_id':'P003'})
