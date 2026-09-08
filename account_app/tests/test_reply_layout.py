import unittest

from account_app.reply_layout import approved_parts, compact


class ReplyLayoutTests(unittest.TestCase):
    def layout(self, parts):
        reply = '\n\n'.join(parts)
        result = approved_parts({'reply_parts': parts}, reply)
        self.assertEqual(compact(' '.join(result)), compact(reply))
        self.assertTrue(1 <= len(result) <= 4)
        self.assertEqual(approved_parts({'reply_parts': result}, reply), result)
        return result

    def test_greeting_and_related_contact_fields_stay_together(self):
        result = self.layout(['هلا حبيبتي 🌸',
            'القطعة متوفرة بالخيارات المعروضة وتكدرين تختارين المناسب إلج.',
            'حتى نكمل الطلب أحتاج رقم الموبايل للتواصل وياج.',
            'والمحافظة والعنوان الكامل وأقرب نقطة دالة للتوصيل.'])
        self.assertEqual(len(result), 2)
        self.assertIn('هلا حبيبتي', result[0])
        self.assertIn('العنوان', result[1])

    def test_many_sections_keep_every_fact_and_final_question(self):
        parts = ['المنتج متوفر بالخيارات المعروضة ضمن تفاصيل القطعة الحالية.',
                 'القماش باربي وتفاصيل الخامة مذكورة حسب معلومات هذه القطعة.',
                 'القياس مناسب حسب جدول الأحجام الموجود في تفاصيل المنتج.',
                 'السعر هو 17000 دينار حسب السعر المسجل لهذه القطعة بالمتجر.',
                 'التوصيل متاح للمحافظات وتختلف تكلفته حسب المحافظة المطلوبة.',
                 'الفحص متاح عند الاستلام ضمن سياسة المتجر الخاصة بالطلبات.',
                 'شنو اللون اللي تفضلينه حتى نراجع توفره إلك؟']
        result = self.layout(parts)
        self.assertEqual(len(result), 4)
        self.assertTrue(result[-1].endswith(parts[-1]))

    def test_receipt_and_structured_lines_are_not_cut(self):
        reply = 'تم تثبيت الحجز بنجاح 🌸\n\n' + '\n'.join([
            'القطعة: فستان أسود', 'القياس: لارج', 'السعر: 17000 دينار',
            'التوصيل: 3000 دينار', 'المجموع: 20000 دينار',
            'العنوان: بغداد - العنوان المحدد', 'الهاتف: 07700000000'])
        self.assertEqual(approved_parts({}, reply), [reply])

    def test_replaced_reply_never_uses_stale_parts(self):
        reply = 'تفاصيل القطعة المتوفرة هي المرجع لاختيار اللون والقياس المناسب.\n\nشنو اللون اللي تفضلينه حتى نراجع توفره حسب الخيارات المسجلة؟'
        parts = approved_parts({'reply_parts':['باقي قطعتين', 'الحجز مثبت']}, reply)
        self.assertEqual(compact(' '.join(parts)), compact(reply))

    def test_size_explanation_and_followup_are_separate_messages(self):
        reply="القياسات متوفرة من 38 إلى 52، وتناسب أوزان من 55 للـ 100 كيلو. شكد القياس أو الوزن المطلوب حتى أتأكدلك منه؟"
        parts=approved_parts({},reply)
        self.assertEqual(len(parts),2)
        self.assertTrue(parts[1].startswith("شكد"))
        self.assertEqual(compact(" ".join(parts)),compact(reply))


if __name__ == '__main__':
    unittest.main()
