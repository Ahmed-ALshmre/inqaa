import unittest
from unittest.mock import patch

import account_app.app as app


class HandoffRecoveryTests(unittest.TestCase):
    product = dict(product_id='P1', price='20000', colors='وردي', sizes='70', fabric='قطن')

    def ask(self, text, history=None, **kwargs):
        return app.call_main_ai({'text': text}, 'text', {}, history or [], [self.product],
                                self.product, None, '', [], **kwargs)

    def test_known_questions_do_not_need_model_or_human_in_any_store(self):
        for store in ['default', 'khuyoot', 'golden-threads', 'al-fatena']:
            token = app._current_store_id.set(store)
            try:
                with patch.object(app, '_call_main_ai_once') as model:
                    for text, value in [('شكد السعر؟', '20,000'), ('شنو الألوان؟', 'وردي'),
                                        ('شنو القياسات؟', '70'), ('شنو القماش؟', 'قطن')]:
                        result = self.ask(text)
                        self.assertIn(value, result['reply'])
                        self.assertFalse(result['requires_human'])
                    model.assert_not_called()
            finally:
                app._current_store_id.reset(token)

    def test_incomplete_or_ambiguous_questions_are_not_guessed(self):
        for text in ['السعر ويا التوصيل؟', 'قياس 70 يلبس وزن 90؟', 'بدلي اللون وردي', 'شكد السعر وأريد ألغي الحجز']:
            self.assertIsNone(app.known_product_question_reply(text, self.product))
        self.assertIsNone(app.known_product_question_reply('شكد السعر', None))
        self.assertIsNone(app.known_product_question_reply('شنو القماش', {'product_id': 'P1'}))

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


if __name__ == '__main__':
    unittest.main()
