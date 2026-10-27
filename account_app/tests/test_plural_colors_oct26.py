"""Preserve Abdullah's two colors and age without guessing availability or quantity."""
import json
import unittest
from unittest.mock import patch

from account_app import color_choices as choices, sales_context, sales_playbook, product_variants as variants, baraah
from account_app.reply_layout import plan_sales_bubbles
from account_app.tests import test_child_availability_oct26 as pipeline

TEXT = 'اي بيجي وردي اريد عمر سنه ونص'
PRODUCT = dict(product_id='P009', product_name='فستان', category='فستان', store_id=baraah.STORE_ID,
               colors='بيج وردي', sizes='1 إلى 2 سنوات', price='10000', stock='متوفر', status='active')


def incoming(text, state=None):
    return sales_context.advance(state or {}, [dict(direction='incoming', text=text)])


def answer(state, result, text):
    return sales_context.advance(state, [dict(direction='outgoing', text=result['reply']),
                                         dict(direction='incoming', text=text)])


class PluralColorFactsTests(unittest.TestCase):
    def test_actual_text_preserves_two_colors_and_combined_age(self):
        selected = sales_playbook.customer_selection(incoming(TEXT))
        self.assertEqual(selected['colors'], ['بيج', 'وردي'])
        self.assertEqual(selected['age'], 1.5)
        self.assertEqual(selected['ages'], [1.5])
        self.assertIsNone(selected['color'])

    def test_both_available_acknowledges_choices_and_only_asks_quantity(self):
        result = choices.reply(PRODUCT, TEXT, incoming(TEXT))
        self.assertIn('البيج والوردي متوفرات لعمر سنة و6 أشهر', result['reply'])
        self.assertIn('قطعة من كل لون؟', result['reply'])
        self.assertNotIn('اللون اللي يعجبج', result['reply'])
        self.assertEqual(result['order'], {})
        self.assertFalse(result['create_order'])
        self.assertFalse(plan_sales_bubbles(result, result['reply'])['needs_rewrite'])

    def test_catalog_brown_does_not_silently_become_beige(self):
        result = choices.reply(dict(PRODUCT, colors='جوزي وردي'), TEXT, incoming(TEXT))
        self.assertIn('الوردي متوفر لعمر سنة و6 أشهر', result['reply'])
        self.assertIn('البيج مو ضمن الألوان المتوفرة', result['reply'])
        self.assertRegex(result['reply'], r'تحب(?:ين)? الجوزي بدل البيج؟')
        self.assertEqual(result['order'], {})
        self.assertNotEqual(variants.colors('ماروني'), variants.colors('احمر'))

    def test_yes_to_targeted_replacement_preserves_other_color_and_age(self):
        product = dict(PRODUCT, colors='جوزي وردي')
        state = incoming(TEXT)
        first = choices.reply(product, TEXT, state)
        state = answer(state, first, 'اي')
        self.assertEqual(sales_playbook.customer_selection(state)['colors'], ['جوزي', 'وردي'])
        second = choices.reply(product, 'اي', state)
        self.assertIn('الجوزي والوردي متوفرات', second['reply'])
        self.assertIn('قطعة من كل لون؟', second['reply'])
        self.assertNotIn('بدل', second['reply'])
        state = answer(state, second, 'اي')
        final = choices.reply(product, 'اي', state)
        self.assertEqual([i['color'] for i in final['order']['items']], ['جوزي', 'وردي'])

    def test_explicit_replacement_and_single_color_correction(self):
        state = incoming('خلي الجوزي بدل البيج', incoming(TEXT))
        self.assertEqual(sales_playbook.customer_selection(state)['colors'], ['جوزي', 'وردي'])
        state = incoming('بس الوردي اريد', state)
        selected = sales_playbook.customer_selection(state)
        self.assertEqual(selected['colors'], ['وردي'])
        self.assertEqual(selected['age'], 1.5)
        self.assertIsNone(choices.reply(PRODUCT, 'بس الوردي اريد', state))

    def test_or_colors_are_alternatives_not_a_two_piece_cart(self):
        text = 'اريد بيجي لو وردي عمر سنة ونص'
        state = incoming(text)
        selected = sales_playbook.customer_selection(state)
        self.assertEqual(selected['colors'], [])
        result = choices.reply(PRODUCT, text, state)
        self.assertIn('تفضلين البيج لو الوردي؟', result['reply'])
        self.assertEqual(result['order'], {})
        state = answer(state, result, 'اي')
        result = choices.reply(PRODUCT, 'اي', state)
        self.assertIn('تفضلين', result['reply'])
        self.assertEqual(result['order'], {})

    def test_alternatives_survive_age_answer_without_becoming_both(self):
        state = incoming('بيجي لو وردي')
        state = incoming('عمر سنة ونص', state)
        result = choices.reply(PRODUCT, 'عمر سنة ونص', state)
        self.assertIn('تفضلين البيج لو الوردي؟', result['reply'])
        self.assertEqual(result['order'], {})

    def test_age_only_followup_keeps_previously_selected_colors(self):
        state = incoming('عمر سنة ونص', incoming('اريد بيجي وردي'))
        result = choices.reply(PRODUCT, 'عمر سنة ونص', state)
        self.assertIn('البيج والوردي متوفرات لعمر سنة و6 أشهر', result['reply'])
        self.assertNotIn('شكد العمر', result['reply'])

    def test_unknown_age_asks_age_once_without_guessing(self):
        result = choices.reply(PRODUCT, 'اريد بيجي وردي', incoming('اريد بيجي وردي'))
        self.assertIn('شكد العمر المطلوب؟', result['reply'])
        self.assertEqual(result['order'], {})

    def test_availability_question_uses_its_age_without_purchase_consent(self):
        text = 'بيجي وردي عمر سنة ونص متوفرات؟'
        result = choices.reply(PRODUCT, text, incoming(text))
        self.assertIn('متوفرات لعمر سنة و6 أشهر', result['reply'])
        self.assertNotIn('شكد العمر', result['reply'])
        self.assertEqual(result['order'], {})

    def test_two_ages_need_mapping_not_guessed_color_assignment(self):
        text = 'اريد بيجي وردي عمر سنة وعمر سنة ونص'
        result = choices.reply(PRODUCT, text, incoming(text))
        self.assertIn('يا عمر مطلوب لكل لون؟', result['reply'])
        self.assertEqual(result['order'], {})

    def test_unsupported_age_never_claims_it_available(self):
        text = 'اريد بيجي وردي عمر 6 سنوات'
        result = choices.reply(PRODUCT, text, incoming(text))
        self.assertIn('عمر 6 سنوات مو ضمن قياسات', result['reply'])
        self.assertEqual(result['order'], {})

    def test_stock_unknown_requires_review_and_sold_out_is_one_notice(self):
        state = incoming(TEXT)
        for product in (dict(PRODUCT, stock=''), dict(PRODUCT, colors='')):
            result = choices.reply(product, TEXT, state)
            self.assertTrue(result['requires_human'])
            self.assertEqual(result['reply'], '')
        result = choices.reply(dict(PRODUCT, stock='نفذ'), TEXT, state)
        self.assertEqual(result['reply'], 'هالموديل مو متوفر عدنا حالياً.')

    def test_affirmation_only_confirms_quantity_after_exact_question(self):
        for question in ('تحبين أرسل الصور؟', 'أثبتلج؟', 'تفضلين البيج لو الوردي؟'):
            self.assertFalse(choices.one_each('اي', {'last_staff_turn': [dict(text=question)]}))
        self.assertTrue(choices.one_each('اي', {'last_staff_turn': [dict(text='اللونين متوفرات. قطعة من كل لون؟')]}))
        for text in ('لا اريد قطعة من كل لون', 'قطعة من كل لون؟', 'قطعة من كل لون لو قطعتين'):
            self.assertFalse(choices.one_each(text, {}), text)

    def test_explicit_one_each_builds_two_valid_lines_only(self):
        text = 'اريد بيجي وردي عمر سنة ونص قطعة من كل لون'
        result = choices.reply(PRODUCT, text, incoming(text))
        self.assertTrue(result['_confirmed_color_quantities'])
        self.assertFalse(result['create_order'])
        self.assertEqual([(i['color'], i['quantity'], i['size']) for i in result['order']['items']],
                         [('بيج', 1, 'سنة و6 أشهر'), ('وردي', 1, 'سنة و6 أشهر')])

    def test_contacts_before_quantity_do_not_invent_quantity(self):
        text = '07701234567 بغداد حي اور'
        result = choices.reply(PRODUCT, text, incoming(text, incoming(TEXT)))
        self.assertIn('قطعة من كل لون؟', result['reply'])
        self.assertEqual(result['order'], {})

    def test_bundles_new_questions_and_image_only_context_keep_regular_path(self):
        state = incoming(TEXT)
        self.assertIsNone(choices.reply(dict(PRODUCT, price='بكج 3 قطع سعره 25 الف'), TEXT, state))
        for text in ('شكد التوصيل', 'شنو القماش', 'متوفر هذا اللون', ''):
            self.assertIsNone(choices.reply(PRODUCT, text, state))


class PluralColorPipelineTests(unittest.TestCase):
    event = pipeline.ChildAvailabilityPipelineTests.event

    def setUp(self):
        pipeline.ChildAvailabilityPipelineTests.setUp(self)
        self.catalog[:] = [dict(PRODUCT)]
        self.m.bind_customer_to_product(self.db, 'two-ages', PRODUCT, source='image_recognition')
        self.extra_patches = [patch.object(self.m, 'send_order_to_telegram', return_value=True),
                              patch.object(self.m, 'save_booking_to_file')]
        for item in self.extra_patches: item.start()

    def tearDown(self):
        for item in reversed(self.extra_patches): item.stop()
        pipeline.ChildAvailabilityPipelineTests.tearDown(self)

    def process(self, text):
        self.event_number = getattr(self, 'event_number', 0) + 1
        event = dict(self.event(text), message_id='plural-test-' + str(self.event_number),
                     timestamp=self.event_number * 1000)
        with patch.object(self.m, 'extract_facebook_event', return_value=event), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'get_auto_product_settings', return_value=dict(enabled=False, product=None)), \
             patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        model.assert_not_called()
        with patch.object(self.m, 'send_text_to_facebook', return_value=True), \
             patch.object(self.m, 'send_image_to_facebook', return_value=True):
            self.m.send_webhook_result_to_facebook(result)
        return result

    def count(self):
        return self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0]

    def test_quantity_then_complete_contact_books_exact_two_colors_once(self):
        first = self.process(TEXT)
        self.assertIn('قطعة من كل لون؟', first['reply'])
        self.assertEqual(self.count(), 0)
        second = self.process('اي')
        self.assertNotIn('اللون اللي يعجبج', second['reply'])
        self.assertIn('رقم', second['reply'])
        self.assertEqual(self.count(), 0)
        final = self.process('07701234567 بغداد حي اور قرب المدرسة')
        self.assertEqual(final['reply'], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.count(), 1)
        saved = self.db.execute('SELECT * FROM orders').fetchone()
        items = json.loads(saved['order_items'])
        self.assertEqual([(i['color'], i['quantity'], i['size']) for i in items],
                         [('بيج', 1, 'سنة و6 أشهر'), ('وردي', 1, 'سنة و6 أشهر')])
        self.assertEqual(saved['total_amount'], 25000)

    def test_old_single_pink_cart_cannot_swallow_new_plural_choice(self):
        old = dict(items=[dict(product_id='P009', product_name='فستان', color='وردي',
                               size='سنة', size_type='size', quantity=1)])
        self.m.save_message(self.db, 'two-ages', 'outgoing', 'text', 'دزيلي العنوان والرقم',
                            None, None, None, {'checkout_draft': old})
        self.process(TEXT)
        result = self.process('اي')
        self.assertIn('رقم', result['reply'])
        final = self.process('07701234567 بغداد حي اور قرب المدرسة')
        self.assertEqual(final['reply'], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        items = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])
        self.assertEqual([i['color'] for i in items], ['بيج', 'وردي'])

    def test_contact_before_quantity_waits_for_quantity_then_books(self):
        self.process(TEXT)
        result = self.process('07701234567 بغداد حي اور قرب المدرسة')
        self.assertIn('قطعة من كل لون؟', result['reply'])
        self.assertEqual(self.count(), 0)
        result = self.process('اي')
        self.assertEqual(result['reply'], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.count(), 1)

    def test_live_catalog_replacement_is_not_quantity_consent(self):
        self.catalog[0]['colors'] = 'جوزي وردي'
        result = self.process(TEXT)
        self.assertRegex(result['reply'], r'تحب(?:ين)? الجوزي بدل البيج؟')
        result = self.process('اي')
        self.assertIn('الجوزي والوردي متوفرات', result['reply'])
        self.assertIn('قطعة من كل لون؟', result['reply'])
        self.assertEqual(self.count(), 0)
        quantity = self.process('نعم')
        self.assertIn('رقم', quantity['reply'], repr(quantity))
        final = self.process('07701234567 بغداد حي اور قرب المدرسة')
        self.assertEqual(final['reply'], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT, repr(final))
        items = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])
        self.assertEqual([i['color'] for i in items], ['جوزي', 'وردي'])


if __name__ == '__main__':
    unittest.main()
