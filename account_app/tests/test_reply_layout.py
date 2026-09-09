import unittest

from account_app.reply_layout import approved_parts, compact


class ReplyLayoutTests(unittest.TestCase):
    def layout(self, parts):
        reply = '\n\n'.join(parts)
        result = approved_parts({'reply_parts': parts}, reply)
        self.assertEqual(compact(' '.join(result)), compact(reply))
        self.assertTrue(1 <= len(result) <= 5)
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
        self.assertEqual(len(result), 5)
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

    def test_one_model_part_and_single_newlines_do_not_bypass_layout(self):
        facts = [
            'الفستان متوفر بالأسود والأحمر وتكدرين تختارين اللون اللي يعجبج.',
            'القماش لينن حسب تفاصيل الموديل الموجودة عندنا بالمتجر.',
            'الفحص متاح عند الاستلام بوجود المندوب حسب سياسة المتجر.',
            'شنو القياس اللي تلبسينه حتى نراجع الموجود والمناسب إلج؟',
        ]
        for separator in [' ', '\n', '\n\n']:
            reply = separator.join(facts)
            for proposed in [[], [reply], facts]:
                parts = approved_parts({'reply_parts': proposed}, reply)
                self.assertEqual(parts, facts)
                self.assertEqual(compact(' '.join(parts)), compact(reply))
                self.assertEqual(approved_parts({'reply_parts': parts}, reply), parts)

    def test_comma_only_reply_separates_ideas_and_last_question(self):
        reply = ('الفستان متوفر باللون الأسود حسب الخيارات المسجلة لهذا الموديل، '
                 'القماش لينن وخامته مذكورة ضمن تفاصيل المنتج الموجودة عندنا، '
                 'شنو القياس اللي تلبسينه حتى أراجع المناسب إلج؟')
        parts = approved_parts({'reply_parts': [reply]}, reply)
        self.assertEqual(len(parts), 3)
        self.assertTrue(parts[-1].startswith('شنو'))
        self.assertEqual(compact(' '.join(parts)), compact(reply))

    def test_price_delivery_total_and_contact_stay_complete(self):
        parts = self.layout([
            'سعر القطعة حسب الموديل المختار هو 16,000 د.ع.',
            'التوصيل 5,000 د.ع.',
            'المجموع 21,000 د.ع.',
            'حتى نكمل الحجز أحتاج رقم الموبايل الصحيح للتواصل وياج.',
            'والمحافظة والعنوان الكامل وأقرب نقطة دالة حتى يوصل المندوب.',
        ])
        self.assertEqual(len(parts), 2)
        self.assertIn('المجموع', parts[0])
        self.assertIn('المحافظة', parts[1])

    def test_urls_decimals_and_numbered_list_are_preserved(self):
        reply = ('تفاصيل الموديل موجودة بالرابط https://example.test/a?q=1.5 ولازم تختارين اللون المتوفر.\n'
                 'القياسات المتوفرة:\n1. قياس 38\n2. قياس 40\n'
                 'شنو القياس اللي تفضلينه حتى نراجع الموجود لهذا الموديل؟')
        parts = approved_parts({}, reply)
        self.assertEqual(compact(' '.join(parts)), compact(reply))
        self.assertTrue(any('1. قياس 38\n2. قياس 40' in part for part in parts))
        self.assertTrue(any('https://example.test/a?q=1.5' in part for part in parts))

    def test_greeting_is_not_detached_and_five_parts_are_allowed(self):
        parts = self.layout([
            'هلا حبيبتي 🌸',
            'الفستان متوفر باللون الأسود والأحمر حسب الخيارات الحالية للموديل.',
            'القماش لينن حسب التفاصيل المسجلة لهذا المنتج بالمتجر.',
            'القياسات متوفرة من 38 إلى 52 حسب جدول الموديل المسجل.',
            'الفحص عند الاستلام بوجود المندوب وفق سياسة المتجر المعتمدة.',
            'شنو اللون اللي تفضلينه حتى أراجع توفره بالقياس اللي اخترتيه؟',
        ])
        self.assertEqual(len(parts), 5)
        self.assertIn('الفستان', parts[0])

    def test_indivisible_text_is_never_truncated_or_split_midword(self):
        reply = 'تفصيل ' * 100
        self.assertEqual(approved_parts({}, reply), [reply.strip()])


if __name__ == '__main__':
    unittest.main()
