import unittest
from unittest.mock import patch

import account_app.app as app


class HandoffRecoveryTests(unittest.TestCase):
    product = dict(product_id='P1', price='20000', colors='وردي', sizes='70', fabric='قطن')

    def ask(self, text, history=None, **kwargs):
        return app.call_main_ai({'text': text}, 'text', {}, history or [], [self.product],
                                self.product, None, '', [], **kwargs)

    def test_literal_newlines_preserve_complete_order_object(self):
        parsed = app._parse_ai_json('prefix {"reply":"line one\nline two", "order":{"items":[]}, "create_order":false} suffix')
        self.assertEqual(parsed['reply'], 'line one\nline two')
        self.assertFalse(parsed['create_order'])

    def test_invalid_response_is_retried_once(self):
        failed = {'reply':'','failed':True,'failure_reason':'invalid_ai_response','create_order':False,'order':{}}
        recovered = {'reply':'التوصيل حسب المحافظة','create_order':False,'order':{}}
        with patch.object(app, '_call_main_ai_once', side_effect=[failed, recovered]) as model:
            self.assertEqual(self.ask('شكد التوصيل؟'), recovered)
        self.assertEqual(model.call_count, 2)
        self.assertIn('JSON', model.call_args.kwargs['fix_instruction'])
        with patch.object(app, '_call_main_ai_once', return_value=failed) as model:
            self.assertFalse(self.ask('شكد التوصيل؟')['create_order'])
        self.assertEqual(model.call_count, 2)


    def test_incomplete_or_ambiguous_questions_are_not_guessed(self):
        for text in ['السعر ويا التوصيل؟', 'قياس 70 يلبس وزن 90؟', 'بدلي اللون وردي', 'شكد السعر وأريد ألغي الحجز']:
            self.assertIsNone(app.known_product_question_reply(text, self.product))
        self.assertIsNone(app.known_product_question_reply('شكد السعر', None))
        self.assertIsNone(app.known_product_question_reply('شنو القماش', {'product_id': 'P1'}))


    def test_compound_does_not_discard_actions_or_unknown_facts(self):
        for question in ['شنو القماش\nأريد ألغي الطلب', 'شكد السعر\nوزن 110 يلبسه',
                         'شنو القماش\nشنو الطول', 'شكد السعر\nالتوصيل للبصرة شكد']:
            self.assertIsNone(app.known_product_questions_reply(question, self.product))

    def test_mentioning_store_policy_is_not_a_handoff(self):
        self.assertFalse(app.is_ai_handoff_reply('حسب سياسة الإدارة الفحص متاح قبل الدفع'))
        self.assertTrue(app.is_ai_handoff_reply('أحول رسالتج للإدارة'))
        self.assertTrue(app.is_ai_handoff_reply('هذا الطلب يحتاج تأكد من فريق المتجر، وسجلت رسالتج للمراجعة.'))

    def test_handoff_reconsidered_once_with_original_context(self):
        recovered = {'reply': 'التوصيل حسب المحافظة المطلوبة.', 'requires_human': False}
        with patch.object(app, '_call_main_ai_once', side_effect=[
                {'reply': 'أحول رسالتج للإدارة', 'requires_human': True}, recovered]) as model:
            result = self.ask('شكد التوصيل؟')
        self.assertEqual(result, recovered)
        self.assertEqual(model.call_count, 2)
        self.assertEqual(model.call_args.args[5], self.product)
        self.assertIn('لا تخترع', model.call_args.kwargs['fix_instruction'])

    def test_real_action_still_requires_human_and_retry_is_bounded(self):
        action = {'reply': '', 'requires_human': True, 'handoff_reason': 'إلغاء حجز مثبت'}
        with patch.object(app, '_call_main_ai_once', return_value=action) as model:
            self.assertTrue(self.ask('ألغي الحجز')['requires_human'])
        self.assertEqual(model.call_count, 2)
        with patch.object(app, '_call_main_ai_once', return_value=action) as model:
            self.ask('ألغي الحجز', fix_instruction='تصحيح')
        self.assertEqual(model.call_count, 1)

    def test_factual_answer_does_not_ignore_other_unanswered_messages(self):
        history = [{'direction': 'incoming', 'text': 'أريد أحجز'},
                   {'direction': 'incoming', 'text': 'شكد السعر؟'}]
        with patch.object(app, '_call_main_ai_once', return_value={'reply': 'جواب كامل'}) as model:
            self.assertEqual(self.ask('شكد السعر؟', history)['reply'], 'جواب كامل')
        model.assert_called_once()




    def test_photo_reply_uses_actual_catalog_availability(self):
        with patch.object(app, 'product_image_urls', return_value=['https://image.test/p']):
            result = app.known_product_question_reply('ارسل صورة', self.product)
            self.assertIn('هذه صورة', result['reply'])
            self.assertFalse(result['requires_human'])
        with patch.object(app, 'product_image_urls', return_value=[]):
            self.assertIn('لا توجد صورة', app.known_product_question_reply('ارسل صورة', self.product)['reply'])
        self.assertIsNone(app.known_product_question_reply('ارسل صورة اللون الأحمر', self.product))

    def test_simple_questions_always_reach_ai_with_product_and_history(self):
        history = [{'direction': 'incoming', 'text': 'شنو قماشة'}]
        for text in ['ممكن السعر', 'شنو القياسات الي بي', 'ارسل صورة', 'السلام عليكم']:
            with patch.object(app, '_call_main_ai_once', return_value={'reply': 'جواب النموذج'}) as model:
                self.assertEqual(self.ask(text, history)['reply'], 'جواب النموذج')
                model.assert_called_once()
                self.assertEqual(model.call_args.args[3], history)
                self.assertEqual(model.call_args.args[5], self.product)

    def test_linked_image_caption_reaches_ai_with_recognition_context(self):
        result = {'reply': 'جواب النموذج'}
        with patch.object(app, '_call_main_ai_once', return_value=result) as model:
            actual = app.call_main_ai({'text': 'ممكن السعر', '_image_already_linked': True},
                                     'image', {}, [], [self.product], self.product,
                                     {'product_found': True}, '', [])
        self.assertEqual(actual, result)
        model.assert_called_once()
        self.assertEqual(model.call_args.args[5], self.product)

    def test_telegram_keeps_each_color_with_its_own_size(self):
        result = app.format_order_for_telegram({'notes': 'اتصلي قبل التوصيل', 'items': [
            {'product_name': 'فستان', 'quantity': 2, 'color': 'أسود', 'size': '40'},
            {'product_name': 'فستان', 'quantity': 1, 'color': 'أحمر', 'size': '44'}]})
        lines = result.splitlines()
        self.assertTrue(any('اللون: أسود' in line and 'القياس: 40' in line and 'العدد: 2' in line for line in lines))
        self.assertTrue(any('اللون: أحمر' in line and 'القياس: 44' in line and 'العدد: 1' in line for line in lines))
        self.assertIn('ملاحظات: اتصلي قبل التوصيل', result)

    def test_telegram_legacy_order_retains_top_level_options(self):
        result = app.format_order_for_telegram({'product_name': 'فستان', 'color': 'أسود',
                                               'size': '44', 'notes': 'الفحص قبل الدفع'})
        for detail in ['اللون: أسود', 'القياس: 44', 'ملاحظات: الفحص قبل الدفع']:
            self.assertIn(detail, result)


if __name__ == '__main__':
    unittest.main()
