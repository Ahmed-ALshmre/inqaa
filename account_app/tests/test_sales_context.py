import sqlite3
import unittest
from unittest.mock import patch

from account_app import sales_context, checkout


class SalesContextTests(unittest.TestCase):
    def test_short_answers_remain_linked_to_size_age_color_or_quantity_question(self):
        for question, answer in [('شنو القياس؟', 'XL'), ('شنو اللون؟', 'تركواز'),
                                 ('شكد عدد القطع؟', '٢'), ('شنو عمر الطفل؟', 'اربع')]:
            with self.subTest(question=question):
                state = sales_context.advance({}, [
                    {'direction': 'outgoing', 'text': 'الموديل الثاني سوت'},
                    {'direction': 'outgoing', 'text': question},
                    {'direction': 'incoming', 'text': answer}])
                evidence = state['evidence'][-1]
                self.assertEqual(evidence['text'], answer)
                self.assertEqual(evidence['in_reply_to'], question)
                self.assertIn('الموديل الثاني سوت', evidence['staff_context'])
                self.assertNotIn('size', evidence)

    def test_color_correction_is_preserved_in_order_without_erasing_other_item(self):
        state = sales_context.advance({}, [
            {'direction': 'incoming', 'text': 'الاول قياس 44 اسود'},
            {'direction': 'incoming', 'text': 'الثاني XL احمر'},
            {'direction': 'incoming', 'text': 'لا اقصد الثاني ابيض مو احمر'}])
        self.assertEqual([x['text'] for x in state['evidence']],
                         ['الاول قياس 44 اسود', 'الثاني XL احمر', 'لا اقصد الثاني ابيض مو احمر'])
        self.assertEqual(state['corrections'][-1]['text'], 'لا اقصد الثاني ابيض مو احمر')

    def test_loaded_triggering_message_does_not_duplicate_its_evidence(self):
        message = {'id': 8, 'direction': 'incoming', 'text': 'قياس 44'}
        state = sales_context.advance({}, [message])
        self.assertEqual(sales_context.advance(state, [message]), state)

    def test_waiting_for_image_is_not_a_booking_or_model_failure(self):
        from account_app import app as m
        for text in ['هسة ادزلج الموديل', 'هسه ادز الصورة', 'راح ارسل الصورة', 'تمام ادزلج الصورة']:
            with self.subTest(text=text), patch.object(m, '_call_main_ai_once') as model:
                result = m.call_main_ai({'text': text}, 'text', {}, [], [], None, None, '', [])
                model.assert_not_called()
                self.assertFalse(result['create_order'])
                self.assertFalse(result['requires_human'])
                self.assertIn('بانتظار الصورة', result['reply'])
        for text in ['ادزلج الصورة بس شكد التوصيل؟', 'اريد احجز', 'ادز الصورة لو الاسم؟']:
            self.assertFalse(sales_context.awaiting_image_reply(text))

    def test_outgoing_image_before_new_offer_does_not_restore_rejected_product(self):
        state = sales_context.advance({}, [
            {'direction': 'outgoing', 'text': 'فستان أنيقة، تحبين صوره؟'},
            {'direction': 'incoming', 'text': 'لا ما اريد هذا'},
            {'direction': 'outgoing', 'text': '', 'message_type': 'image'},
            {'direction': 'outgoing', 'text': 'عدنا سوت فرسان، تحبين صورته؟'},
            {'direction': 'incoming', 'text': 'اي'}])
        result = sales_context.continuation('اي', state)
        self.assertEqual(result['intent'], 'browse')
        self.assertNotIn('أنيقة', result['offer'])
        self.assertIn('فرسان', result['offer'])

    def test_last_question_inside_one_bubble_wins(self):
        state = sales_context.advance({}, [{'direction': 'outgoing', 'text': 'تحبين تشوفين الصور؟ تحبين أثبتلج الطلب؟'}])
        self.assertIsNone(sales_context.continuation('اي', state))
        state = sales_context.advance({}, [{'direction': 'outgoing', 'text': 'تحبين أثبتلج الطلب؟ لو تحبين تشوفين الصور؟'}])
        self.assertEqual(sales_context.continuation('اي', state)['intent'], 'browse')

    def test_ambiguous_yes_does_not_book_or_choose_browse(self):
        from account_app import app as m
        history = [{'direction': 'outgoing', 'text': 'تحبين تشوفين البدائل لو أثبت هذا؟'}]
        with patch.object(m, '_call_main_ai_once') as model:
            result = m.call_main_ai({'text': 'اي'}, 'text', {}, history, [], None, None, '', [])
        model.assert_not_called()
        self.assertIn('تقصدين', result['reply'])
        self.assertFalse(m.checkout_requested(None, {}, result))

    def test_refusal_invalidates_offer_until_new_staff_offer(self):
        state = sales_context.advance({}, [
            {'direction': 'outgoing', 'text': 'تحبين تشوفين الصور؟'},
            {'direction': 'incoming', 'text': 'لا ما اريد هذا'},
            {'direction': 'incoming', 'text': 'اي'}])
        self.assertIsNone(sales_context.continuation('اي', state))
        state = sales_context.advance(state, [{'direction': 'outgoing', 'text': 'تحبين أشوفلج بديل؟'}])
        self.assertEqual(sales_context.continuation('اي', state)['intent'], 'browse')

    def test_new_attachment_invalidates_old_photo_offer(self):
        state = sales_context.advance({}, [
            {'direction': 'outgoing', 'text': 'تحبين تشوفين الصور؟'},
            {'direction': 'incoming', 'text': '', 'message_type': 'image', 'image_url': 'https://images.test/new.jpg'},
            {'direction': 'incoming', 'text': 'اي'}])
        self.assertIsNone(sales_context.continuation('اي', state))

    def test_two_sizes_are_not_silently_reduced_to_one(self):
        for text in ['قياس 44 لو 46', 'قياس ٤٤ او ٤٦', 'قياس 44 و46']:
            reply = sales_context.opening_reply(text, 'دزيلي الصورة أو القياس')
            self.assertNotIn('44 واضح', reply)
            self.assertIn('القياسات', reply)

    def test_unrelated_confirmation_word_does_not_hide_repeated_phone(self):
        customer = {'phone': '07700000000'}
        for reply in ['القياس صحيح. دزيلي رقم الموبايل', 'نفس القياس متوفر، دزيلي رقم الموبايل']:
            self.assertEqual(sales_context.reasks_contact(reply, customer), 'phone')
        self.assertFalse(sales_context.reasks_contact('دزيلي تأكيد الرقم', customer))

    def test_memory_survives_window_and_isolated_by_store(self):
        db = sqlite3.connect(':memory:')
        db.row_factory = sqlite3.Row
        self.addCleanup(db.close)
        db.execute('CREATE TABLE messages(id INTEGER PRIMARY KEY,store_id,sender_id,direction,text,created_at)')
        sales_context.init_db(db)
        db.execute("INSERT INTO messages VALUES(1,'a','same','incoming','قياس 54','2026-09-29')")
        db.execute("INSERT INTO messages VALUES(2,'b','same','incoming','قياس 38','2026-09-29')")
        db.execute('ALTER TABLE messages ADD COLUMN message_type')
        db.execute('ALTER TABLE messages ADD COLUMN image_url')
        first = sales_context.load(db, 'a', 'same')
        for i in range(3, 140):
            db.execute("INSERT INTO messages(id,store_id,sender_id,direction,text,created_at) VALUES(?,'a','same','incoming','شكرا','2026-09-29')", (i,))
        second = sales_context.load(db, 'a', 'same')
        self.assertEqual(first['evidence'], second['evidence'])
        self.assertNotIn('38', str(second))
        self.assertEqual(second, sales_context.load(db, 'a', 'same'))

    def test_short_yes_resolves_last_question_not_earlier_offer(self):
        state = sales_context.advance({}, [
            {'direction': 'outgoing', 'text': 'متوفر فستان أنيقة وفستان رباط'},
            {'direction': 'outgoing', 'text': 'تحبين أدزلج صورهم؟'},
            {'direction': 'outgoing', 'text': None},
            {'direction': 'incoming', 'text': 'دزيلي حبيبتي'}])
        self.assertEqual(sales_context.continuation('دزيلي حبيبتي', state)['intent'], 'browse')
        self.assertEqual(sales_context.continuation('ي', state)['intent'], 'browse')
        for text in ['لا', 'اي بس احجزي الاسود', 'دزيلي رقم المندوب', 'اي لا تثبتين']:
            self.assertIsNone(sales_context.continuation(text, state))
        state = sales_context.advance(state, [{'direction': 'outgoing', 'text': 'تحبين أثبتلج الطلب؟'}])
        self.assertIsNone(sales_context.continuation('اي', state))

    def test_opening_uses_known_size_and_greeting_is_not_product_pitch(self):
        reply = sales_context.opening_reply('متوفر قياس ٤٤', 'دزيلي الصورة أو القياس')
        self.assertIn('44 واضح', reply)
        self.assertNotIn('أو القياس', reply)
        self.assertNotIn('قياس', sales_context.opening_reply('مرحبا', 'قياس'))

    def test_tracking_variants_do_not_capture_conditional_purchase(self):
        for text in ['بلا زحمه شوكت يوصلني الطلب ؟', 'شوكت يوصل طلبي', 'يمته توصل الطلبيه', 'طلبي متى يوصل']:
            self.assertTrue(checkout.is_existing_order_followup(text), text)
        for text in ['اذا احجز شوكت يوصل الطلب', 'لو اطلب اليوم متى يوصل الطلب', 'اريد احجز شوكت يوصل الطلب']:
            self.assertFalse(checkout.is_existing_order_followup(text), text)

    def test_known_contact_is_not_requested_again_but_confirmation_allowed(self):
        customer = {'phone': '07700000000', 'province': 'بغداد'}
        self.assertEqual(sales_context.reasks_contact('دزيلي رقم الموبايل', customer), 'phone')
        self.assertFalse(sales_context.reasks_contact('نفس رقم الموبايل نعتمد؟', customer))
        self.assertFalse(sales_context.reasks_contact('دزيلي العنوان', customer))
        self.assertNotIn('ناقص بس رقم', sales_context.missing_contact_reply(customer))
        self.assertFalse(sales_context.reasks_contact('دزيلي رقم الموبايل', {'phone': '123'}))
        self.assertFalse(sales_context.reasks_contact('دزيلي العنوان', {'address': 'بغداد'}))

    def test_alternative_question_not_confused_with_lost_product(self):
        from account_app import app as m
        self.assertFalse(m.reply_reasks_known_product('أي موديل يعجبج من هذني؟'))
        self.assertFalse(m.reply_reasks_known_product('يا موديل تحبين تشوفين من البدائل؟'))
        self.assertTrue(m.reply_reasks_known_product('يا موديل تقصدين؟'))

    def test_photo_agreement_recovers_named_images_without_booking(self):
        from account_app import app as m
        products = [dict(product_id='F1', product_name='فستان أنيقة', stock='متوفر', status='active', image_url='https://images.test/1.jpg')]
        history = [{'direction': 'outgoing', 'text': 'عدنا فستان أنيقة. تحبين أدزلج صوره؟'}]
        with patch.object(m, '_call_main_ai_once', return_value={'reply': 'دزيلي صورة الموديل'}), patch.object(m, 'product_image_urls', return_value=['https://images.test/1.jpg']):
            result = m.call_main_ai({'text': 'دزيلي حبيبتي'}, 'text', {}, history, products, products[0], None, '', [])
        self.assertEqual(result['image_product_ids'], ['F1'])
        self.assertFalse(result['create_order'])
        self.assertFalse(m.checkout_requested(None, {}, result))

    def test_photo_agreement_cannot_become_order_even_with_bad_model(self):
        from account_app import app as m
        history = [{'direction': 'outgoing', 'text': 'تحبين تشوفين صورهم؟'}]
        with patch.object(m, '_call_main_ai_once', return_value={'reply': 'تم تثبيت الطلب', 'create_order': True, 'order': {'product_id': 'F1'}}):
            result = m.call_main_ai({'text': 'اي'}, 'text', {}, history, [], None, None, '', [])
        self.assertFalse(result['create_order'])
        self.assertEqual(result['order'], {})
        self.assertNotIn('تم تثبيت', result['reply'])

    def test_repeated_contact_retry_keeps_factual_answer_and_asks_missing(self):
        from account_app import app as m
        with patch.object(m, '_call_main_ai_once', return_value={'reply': 'سعره 15 ألف. دزيلي رقم الموبايل والمحافظة والعنوان', 'order': {}}) as model:
            result = m.call_main_ai({'text': 'اريد احجز'}, 'text', {'phone': '07700000000', 'province': 'بغداد'}, [], [], None, None, '', [])
        self.assertEqual(model.call_count, 2)
        self.assertIn('سعره 15 ألف', result['reply'])
        self.assertIn('ناقص بس العنوان', result['reply'])
        self.assertNotIn('دزيلي رقم', result['reply'])
