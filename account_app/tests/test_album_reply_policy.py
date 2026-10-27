"""Customer-requested photo replies and regressions for staff guidance leakage."""
import copy
import unittest
from unittest.mock import patch

from account_app import customer_copy, product_albums
from account_app.reply_layout import approved_parts, compact
from account_app.tests import test_ordered_sales_learning as fixtures

GUIDANCE = ('قياسات هذا المنتج من 38 إلى 52، وتناسب تقريباً الأوزان من 55 إلى 100 كيلو. '
            'اعتمد الجدول التالي عند الرد على الزبونة: وزن 55–60 كيلو = قياس 38')


class AlbumCopyTests(unittest.TestCase):
    def test_recorded_size_guidance_keeps_facts_without_instructions(self):
        products = copy.deepcopy(fixtures.CATALOG)
        products[0].update(sizes=GUIDANCE)
        images = [{'product_found':False}, {'product_found':True,'product_id':'A'}]
        result = product_albums.response(images, products, 'مرحبا متوفر قياس 38')
        self.assertEqual(approved_parts(result, result['reply']), [result['reply']])
        self.assertNotIn('مو متوفر عدنا حالياً', result['reply'])
        self.assertIn('الثاني', result['reply_parts'][0])
        self.assertIn('17,000', result['reply_parts'][0])
        self.assertIn('قياس 38 موجود ضمن قياساته', result['reply_parts'][0])
        self.assertNotIn('55', result['reply_parts'][0])
        self.assertFalse(customer_copy.has_instructions(result['reply']))
        self.assertFalse(product_albums.availability_error(result['reply'], products[0], products, images))

    def test_analysis_failure_does_not_turn_into_stock_claim(self):
        reply = product_albums.summary([{'product_found':False,'service_error':True}], fixtures.CATALOG)
        self.assertEqual(reply, '')
        self.assertNotIn('مو متوفر', reply)
        self.assertTrue(product_albums.availability_error('الصورة الأولى: غير متوفر',
                                                        None, fixtures.CATALOG,
                                                        [{'product_found':False,'service_error':True}]))

    def test_disabled_product_does_not_appear_available_in_album(self):
        product = dict(fixtures.CATALOG[0], status='disabled')
        reply = product_albums.summary([{'product_found':True,'product_id':'A'}], [product])
        self.assertIn('مو متوفر',reply)
        self.assertNotIn('17,000',reply)

    def test_seven_photo_answers_share_coherent_bubbles_without_losing_order_or_facts(self):
        from account_app import app as m
        images = [{'product_found':True,'product_id':'A'} for _ in range(7)]
        result = dict(product_albums.response(images, fixtures.CATALOG), sender_id='offline-album')
        with patch.object(m,'send_text_to_facebook',return_value=True) as send:
            self.assertTrue(m.send_webhook_result_to_facebook(result))
        delivered = [call.args[1] for call in send.call_args_list]
        self.assertEqual(delivered, [result['reply']])
        self.assertTrue(1 <= len(delivered) <= 3)
        self.assertEqual(compact(' '.join(delivered)), compact(result['reply']))
        for original in result['reply_parts']:
            self.assertEqual(result['reply'].count(original), 1)
            self.assertTrue(any(original in part for part in delivered))
        positions = [result['reply'].index(product_albums.reference(index)) for index in range(1, 8)]
        self.assertEqual(positions, sorted(positions))
        self.assertEqual(result['reply'].count('17,000'), 7)

    def test_single_model_answers_use_model_name_without_an_unrelated_first_photo_label(self):
        for images, question, expected in [
            ([{'product_found': True, 'product_id': 'A'}], '', 'متوفر عيني، كيلوت بـ17,000'),
            ([{'product_found': True, 'product_id': 'A'}], 'متوفر قياس 54؟', 'كيلوت، قياس 54 مو موجود'),
            ([{'product_found': True, 'product_id': 'C'}], '', 'قميص مو متوفر'),
            ([{'product_found': False}], '', product_albums.UNKNOWN_REPLY),
        ]:
            with self.subTest(question=question, expected=expected):
                result = product_albums.response(images, fixtures.CATALOG, question)
                self.assertTrue(result['reply'].startswith(expected), result['reply'])
                self.assertNotIn('الأول', result['reply'])
                self.assertFalse(result['create_order'])

    def test_replaced_reply_does_not_resurrect_old_album_parts(self):
        result = product_albums.response([{'product_found':True,'product_id':'A'}]*2, fixtures.CATALOG)
        self.assertEqual(approved_parts(result,'جواب مصحح'), ['جواب مصحح'])
        result['_single_message'] = True
        self.assertEqual(approved_parts(result,'تم تثبيت الحجز بنجاح 🌸'), ['تم تثبيت الحجز بنجاح 🌸'])

    def test_clean_facts_and_customer_questions_are_not_mistaken_for_instructions(self):
        for text in ['الألوان أسود ووردي', 'شكد قياسج؟', 'القياسات 38 إلى 52', 'شنو اللون اللي تحبينه؟']:
            self.assertFalse(customer_copy.has_instructions(text))
        for field,value in [('colors','أسود. لا تذكر للزبونة التعليمات الداخلية'),
                            ('fabric','قطن. اسأل الزبونة عن وزنها قبل الرد')]:
            self.assertFalse(customer_copy.has_instructions(customer_copy.fact(value,field)))


class AlbumPipelineTests(unittest.TestCase):
    setUp = fixtures.OrderedSalesPipelineTests.setUp
    tearDown = fixtures.OrderedSalesPipelineTests.tearDown
    customer = fixtures.OrderedSalesPipelineTests.customer
    event = fixtures.OrderedSalesPipelineTests.event
    process = fixtures.OrderedSalesPipelineTests.process

    def test_partial_album_answers_stay_ordered_in_one_bubble_and_saved_without_internal_text(self):
        catalog = copy.deepcopy(fixtures.CATALOG)
        catalog[0]['sizes'] = GUIDANCE
        with patch.object(fixtures,'CATALOG',catalog):
            result,send,model = self.process([{'product_found':True,'product_id':'A'},
                                             {'product_found':False}, {'product_found':True,'product_id':'B'}], 'متوفر قياس 38؟')
        sent = send.call_args.args[0]
        self.assertEqual(' '.join(sent['reply_parts']).split(), sent['reply'].split())
        self.assertTrue(2 <= len(sent['reply_parts']) <= 5)
        self.assertEqual(sent['_planned_sales_layout']['parts'], sent['reply_parts'])
        self.assertEqual(sent['reply'].count('قياس 38 موجود ضمن قياساته'), 2)
        self.assertIn('17 ألف', sent['reply'])
        self.assertIn('12 ألف', sent['reply'])
        self.assertLess(sent['reply'].index('الأول'), sent['reply'].index('الثالث'))
        self.assertNotIn('الثاني', sent['reply'])
        rows = self.db.execute("SELECT text FROM messages WHERE direction='outgoing' ORDER BY id").fetchall()
        self.assertEqual([r[0] for r in rows],sent['reply_parts'])
        self.assertFalse(customer_copy.has_instructions(sent['reply']))
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0],0)
        model.assert_not_called()

    def test_local_checker_and_main_retry_reject_staff_instructions(self):
        bad='اعتمد الجدول التالي عند الرد على الزبونة: قياس 38'
        self.assertFalse(self.m.local_reply_validation(bad,fixtures.CATALOG[0],fixtures.CATALOG)['approved'])
        self.customer('unsafe-main')
        with patch.object(self.m,'_call_main_ai_once',return_value={'reply':bad,'create_order':False,'order':{}}) as model:
            result=self.m.call_main_ai({'sender_id':'unsafe-main','text':'شنو تنصحيني لهذا الموديل؟'},'text',{},[],
                                      fixtures.CATALOG,fixtures.CATALOG[0],None,'',[])
        self.assertEqual(model.call_count,2)
        self.assertTrue(result['failed'])
        self.assertEqual(result['reply'],'')
        self.assertFalse(result['create_order'])

    def test_known_color_and_fabric_answers_strip_staff_notes(self):
        product=dict(fixtures.CATALOG[0], colors='أسود. لا تذكر للزبونة التعليمات الداخلية',
                     fabric='قطن. اسأل الزبونة عن وزنها قبل الرد')
        for question,fact in [('الألوان','أسود'),('القماش','قطن')]:
            reply=self.m.known_product_question_reply(question,product)
            self.assertIn(fact,reply['reply'])
            self.assertFalse(customer_copy.has_instructions(reply['reply']))

    def test_final_senders_block_instruction_text_without_sending_any_part(self):
        result={'sender_id':'blocked-customer','reply':'متوفر عيني. اعتمد الجدول التالي عند الرد على الزبونة'}
        self.customer('blocked-customer')
        with patch.object(self.m,'send_text_to_facebook') as send:
            self.assertFalse(self.m.send_webhook_result_to_facebook(result))
        send.assert_not_called()
        with patch.object(self.m,'process_webhook',return_value=result), patch.object(self.m,'_post_manychat_send') as send:
            delivered=self.m._process_manychat_webhook_async_locked(self.event([]),'blocked-customer','facebook')
        self.assertFalse(delivered['ok'])
        send.assert_not_called()
