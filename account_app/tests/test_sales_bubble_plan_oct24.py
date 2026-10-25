"""Visible sales message plans: semantic groups, media budget and safe exceptions."""
import copy
import unittest
import uuid
from unittest.mock import patch

from account_app.reply_layout import approved_parts, compact, plan_sales_bubbles, polish_surface_punctuation


class SalesBubblePlanOct24Tests(unittest.TestCase):
    def test_adjacent_punctuation_is_polished_without_changing_facts(self):
        reply = 'الفحص بحضور المندوب؛، وعليه عرض خاص.، شنو قياسج؟'
        self.assertEqual(polish_surface_punctuation(reply),
                         'الفحص بحضور المندوب؛ وعليه عرض خاص. شنو قياسج؟')

    def plan(self, reply, result=None, images=None):
        result = copy.deepcopy(result or {})
        untouched = copy.deepcopy(result)
        plan = plan_sales_bubbles(result, reply, images)
        self.assertEqual(result, untouched)
        self.assertEqual(compact(' '.join(plan['reply_parts'])), compact(reply))
        self.assertEqual(plan['total_messages'], len(plan['reply_parts']) + len(plan['image_urls']))
        self.assertLessEqual(plan['total_messages'], 5)
        return plan

    def test_direct_answer_complete_cost_and_followup_are_two_thoughtful_bubbles(self):
        reply = 'سعره 17,000 د.ع. التوصيل 5,000 د.ع. المجموع 22,000 د.ع. شنو القياس المطلوب؟'
        plan = self.plan(reply)
        self.assertEqual(plan['total_messages'], 2)
        self.assertFalse(plan['needs_rewrite'])
        self.assertEqual(plan['reply_parts'][-1], 'شنو القياس المطلوب؟')
        self.assertTrue(all(value in plan['reply_parts'][0] for value in ['17,000 د.ع.', '5,000 د.ع.', '22,000 د.ع.']))

    def test_greeting_is_attached_to_answer_and_not_sent_as_padding(self):
        reply = 'هلا عيني 🌸\nالفستان متوفر بالأسود\nالقماش قطن\nشنو قياسج؟'
        plan = self.plan(reply)
        self.assertEqual(len(plan['reply_parts']), 2)
        self.assertIn('الفستان متوفر', plan['reply_parts'][0])
        self.assertEqual(plan['reply_parts'][1], 'شنو قياسج؟')
        self.assertFalse(plan['needs_rewrite'])

    def test_live_style_price_reply_does_not_send_a_lone_vocative_or_soft_wraps(self):
        reply = ('عيني،\nالسعر 16,000 د.ع.\nوقماشه لينن مريح.\n'
                 'التوصيل 5,000 د.ع؛ المجموع 21,000 د.ع.\nشنو قياسج؟')
        plan = self.plan(reply)
        self.assertNotEqual(plan['reply_parts'][0], 'عيني،')
        self.assertTrue(all('\n' not in part for part in plan['reply_parts']))
        self.assertIn('السعر', plan['reply_parts'][0])

    def test_fit_explanation_and_condition_remain_whole_before_next_step(self):
        reply = ('القياسات من 38 إلى 52. قياس 44 موجود حسب الجدول. '
                 'إذا تحبينه واسع لازم نقارن قياس القطعة بملابسج قبل الاختيار. شنو اللون اللي تفضلينه؟')
        plan = self.plan(reply)
        self.assertEqual(len(plan['reply_parts']), 2)
        for value in ['38 إلى 52', '44 موجود', 'إذا تحبينه واسع', 'قبل الاختيار']:
            self.assertIn(value, plan['reply_parts'][0])
        self.assertEqual(plan['reply_parts'][1], 'شنو اللون اللي تفضلينه؟')

    def test_contact_fields_are_one_request_after_selection_acknowledgement(self):
        reply = ('اختيارج الأسود قياس 44 واضح عندي. '
                 'حتى نكمل الحجز أحتاج رقم الموبايل. والمحافظة والعنوان الكامل وأقرب نقطة دالة.')
        plan = self.plan(reply)
        self.assertEqual(len(plan['reply_parts']), 2)
        self.assertIn('اختيارج الأسود', plan['reply_parts'][0])
        self.assertTrue(all(word in plan['reply_parts'][1] for word in ['رقم الموبايل', 'المحافظة', 'العنوان', 'نقطة دالة']))

    def test_fit_reassurance_conditional_question_after_semicolon_splits(self):
        reply = ('حقج تقلقين عيني، الفحص عند الاستلام بوجود المندوب، '
                 'قياساته من 38 إلى 52؛، إذا تحبين كوليلي قياسج المعتاد حتى أساعدج؟')
        plan = self.plan(reply)
        self.assertEqual(len(plan['reply_parts']), 2)
        self.assertFalse(plan['needs_rewrite'])

    def test_fit_reassurance_and_can_you_share_size_question_split(self):
        reply = ('الفحص عند الاستلام بحضور المندوب، قياساته من 38 إلى 52، '
                 'تكدرين تنطيني قياسج المعتاد حتى أشوفلج الأنسب؟')
        plan = self.plan(reply)
        self.assertEqual(len(plan['reply_parts']), 2)
        self.assertFalse(plan['needs_rewrite'])

    def test_soft_hesitation_empathy_and_help_question_are_two_messages(self):
        reply = 'أكيد عيني، براحتج، أهم شي تكونين مقتنعة باختيارِج. بس حتى أساعدج، التردد من القياس لو من السعر؟'
        plan = self.plan(reply)
        self.assertEqual(len(plan['reply_parts']), 2)
        self.assertFalse(plan['needs_rewrite'])
        self.assertIn('التردد من القياس', plan['reply_parts'][1])

    def test_complete_fit_caution_does_not_join_product_answer_into_one_long_bubble(self):
        reply = ('إي عيني هو نفسه الموديل بالصورة، فستان دانتيل وقماشه لينن مريح ومرتب، '
                 'متوفر باللون الأسود بسعر 16 ألف. بس أصغر قياس بي 38، ونحتاج نقارن قياسج المعتاد '
                 'بجدول القطعة قبل ما نختاره.')
        plan = self.plan(reply)
        self.assertEqual(len(plan['reply_parts']), 2)
        self.assertLess(max(map(len, plan['reply_parts'])), 155)

    def test_price_and_shipping_without_total_do_not_swallow_interleaved_fit_fact(self):
        reply = ('فستان دانتيل متوفر وقماشه لينن، سعره 16 ألف. '
                 'قياساته من 38 لحد 52 ونختار المناسب من الجدول. '
                 'والتوصيل 5 آلاف لكل المحافظات مع الفحص عند الاستلام.')
        plan = self.plan(reply)
        self.assertGreaterEqual(len(plan['reply_parts']), 2)
        self.assertLess(max(map(len, plan['reply_parts'])), 155)

    def test_comma_only_reply_can_separate_complete_sales_functions(self):
        reply = 'الموديل متوفر بالأسود، القماش قطن، شنو قياسج؟'
        plan = self.plan(reply)
        self.assertEqual(len(plan['reply_parts']), 2)
        self.assertFalse(plan['needs_rewrite'])
        self.assertEqual(plan['reply_parts'][-1], 'شنو قياسج؟')

    def test_all_price_components_stay_together_even_if_another_fact_was_interleaved(self):
        reply = ('السعر 17,000 د.ع. القماش قطن حسب وصف هذا الموديل. '
                 'التوصيل 5,000 د.ع. المجموع 22,000 د.ع. شنو اللون المطلوب؟')
        plan = self.plan(reply)
        self.assertTrue(any(all(value in part for value in ['17,000', '5,000', '22,000'])
                            for part in plan['reply_parts']))

    def test_images_count_in_five_message_budget_and_duplicate_images_do_not_send_twice(self):
        reply = 'الفستان متوفر بالأسود. القماش قطن. القياسات من 38 إلى 52. شنو قياسج؟'
        images = [f'https://images.test/{index}.png' for index in range(4)]
        plan = self.plan(reply, images=images + [images[0]])
        self.assertEqual(plan['total_messages'], 5)
        self.assertEqual(plan['reply_parts'], [compact(reply)])
        self.assertEqual(plan['image_urls'], images)
        self.assertEqual(plan['deferred_image_urls'], [])
        self.assertFalse(plan['needs_rewrite'])

    def test_one_photo_and_one_indivisible_caption_are_two_real_messages(self):
        reply = 'هاي صورة الموديل باللون الأسود.'
        plan = self.plan(reply, images=['https://images.test/black.png'])
        self.assertEqual(plan['total_messages'], 2)
        self.assertFalse(plan['needs_rewrite'])

    def test_catalog_single_text_marker_does_not_turn_its_four_photos_into_a_service_exception(self):
        reply = 'هاي أول أربعة موديلات متوفرة، ويا كل واحد صورته.'
        images = [f'https://images.test/catalog-{index}.png' for index in range(4)]
        plan = self.plan(reply, {'_single_message': True, 'intent': 'catalog_request'}, images)
        self.assertEqual(plan['reply_parts'], [reply])
        self.assertEqual(plan['total_messages'], 5)
        self.assertIsNone(plan['exception'])
        self.assertFalse(plan['needs_rewrite'])

    def test_extra_photos_are_explicitly_deferred_and_require_pagination_upstream(self):
        images = [f'https://images.test/{index}.png' for index in range(7)]
        plan = self.plan('هاي صور الموديلات المطلوبة.', images=images)
        self.assertEqual(plan['image_urls'], images[:4])
        self.assertEqual(plan['deferred_image_urls'], images[4:])
        self.assertTrue(plan['needs_rewrite'])
        self.assertIn('deferred_images_require_pagination', plan['rewrite_reasons'])
        self.assertEqual(plan['total_messages'], 5)

    def test_image_only_plan_uses_actual_photo_count_without_inventing_text(self):
        images = [f'https://images.test/{index}.png' for index in range(3)]
        plan = self.plan('', images=images)
        self.assertEqual(plan['reply_parts'], [])
        self.assertEqual(plan['total_messages'], 3)

    def test_indivisible_sales_answer_flags_rewrite_instead_of_padding_or_cutting_words(self):
        reply = 'هذا الموديل متوفر باللون الأسود حسب الخيارات المسجلة'
        plan = self.plan(reply)
        self.assertEqual(plan['reply_parts'], [reply])
        self.assertTrue(plan['needs_rewrite'])
        self.assertEqual(plan['rewrite_reasons'], ['insufficient_distinct_sales_messages'])

    def test_ordinary_service_confirmation_thanks_and_optout_stay_one_message(self):
        cases = [({}, 'تم تثبيت الطلب. افحصي القطعة بوجود المندوب.'),
                 ({}, 'تدللين عيني 🌸'),
                 ({}, 'حاضر، ما راح نراسلك بعد.'),
                 ({'_response_kind': 'service'}, 'طلبج بعده مسجل. الشحن يحتاج تأكيد من المتجر.'),
                 ({'meta': {'post_order_followup': True}}, 'القياس المحفوظ بالطلب 44. نراجع التعديل ويا الموظف.')]
        for result, reply in cases:
            with self.subTest(reply=reply):
                plan = self.plan(reply, result)
                self.assertEqual(plan['reply_parts'], [reply])
                self.assertEqual(plan['total_messages'], 1)
                self.assertTrue(plan['exception'])
                self.assertFalse(plan['needs_rewrite'])

    def test_service_text_plus_photo_cannot_silently_pretend_to_be_one_message(self):
        plan = self.plan('هاي صورة القطعة بطلبج.', {'_response_kind': 'service'},
                         ['https://images.test/service.png'])
        self.assertEqual(plan['total_messages'], 2)
        self.assertTrue(plan['needs_rewrite'])
        self.assertIn('exception_requires_one_visible_message', plan['rewrite_reasons'])

    def test_stale_proposed_parts_cannot_restore_wrong_price_or_confirmation(self):
        reply = 'السعر الصحيح 17,000 د.ع. شنو قياسج؟'
        plan = self.plan(reply, {'reply_parts': ['السعر 10,000 د.ع.', 'تم الحجز']})
        self.assertFalse(plan['needs_rewrite'])
        self.assertNotIn('10,000', ' '.join(plan['reply_parts']))
        self.assertNotIn('تم الحجز', ' '.join(plan['reply_parts']))

    def test_repeated_authoritative_sales_sentence_is_a_validation_failure_not_repeated_delivery(self):
        for reply in ['القماش قطن. القماش قطن. شنو قياسج؟',
                      'السعر 17,000 د.ع. السعر 17,000 د.ع. شنو قياسج؟']:
            plan = self.plan(reply)
            self.assertTrue(plan['needs_rewrite'])
            self.assertIn('duplicate_sales_content', plan['rewrite_reasons'])

    def test_a_single_uncaptioned_sales_photo_requires_a_real_second_function(self):
        plan = self.plan('', images=['https://images.test/single.png'])
        self.assertEqual(plan['total_messages'], 1)
        self.assertTrue(plan['needs_rewrite'])
        self.assertIn('insufficient_distinct_sales_messages', plan['rewrite_reasons'])

    def test_lists_links_and_currency_never_split_or_lose_approved_facts(self):
        reply = ('تفاصيل الموديل https://example.test/a?q=1.5.\nالقياسات المتوفرة:\n'
                 '1. قياس 38\n2. قياس 40\nشنو القياس المطلوب؟')
        plan = self.plan(reply)
        self.assertTrue(any('1. قياس 38\n2. قياس 40' in part for part in plan['reply_parts']))
        self.assertTrue(any('https://example.test/a?q=1.5.' in part for part in plan['reply_parts']))

    def test_opt_in_approved_parts_matches_plan_and_is_deterministic(self):
        reply = 'السعر 17,000 د.ع. شنو اللون المطلوب؟'
        result = {'_sales_bubble_plan': True, 'send_image': True,
                  'image_urls': ['https://images.test/product.png']}
        plan = self.plan(reply, result)
        self.assertEqual(approved_parts(result, reply), plan['reply_parts'])
        self.assertEqual(plan_sales_bubbles(dict(result, reply_parts=plan['reply_parts']), reply), plan)


class SalesBubbleDeliveryOct24Tests(unittest.TestCase):
    """Exercise the real sender and delivery ledger; no real customer sends."""
    def setUp(self):
        from account_app.tests.test_audit_orders import AuditFixTests
        AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set('default')
        self.sender = 'default::synthetic-sales-bubbles-' + uuid.uuid4().hex
        self.m.get_or_create_customer(self.db, self.sender, 'synthetic-page', 'facebook')
        self.incoming = self.m.save_message(self.db, self.sender, 'incoming', 'text',
                                            'شكد سعره؟', None, None, None, {})
        self.reply = 'السعر 17,000 د.ع. والتوصيل 5,000 د.ع. المجموع 22,000 د.ع. شنو القياس المطلوب؟'
        self.plan = plan_sales_bubbles({}, self.reply, ['https://images.test/sales-bubble.png'])
        self.assertFalse(self.plan['needs_rewrite'])
        ids = [self.m.save_message(self.db, self.sender, 'outgoing', 'text', part, None, None, None,
                                  {'_delivery_status': 'generated', '_reply_to_message_id': self.incoming})
               for part in self.plan['reply_parts']]
        ids += [self.m.save_message(self.db, self.sender, 'outgoing', 'image', None, url, None, None,
                                   {'_delivery_status': 'generated', '_reply_to_message_id': self.incoming})
                for url in self.plan['image_urls']]
        self.result = dict(sender_id=self.sender, page_id='synthetic-page', platform='facebook',
                           reply=self.reply, reply_parts=self.plan['reply_parts'], send_image=True,
                           image_urls=self.plan['image_urls'], _sales_bubble_plan=True,
                           _reply_to_message_id=self.incoming, _outgoing_message_ids=ids)

    def tearDown(self):
        from account_app.tests.test_audit_orders import AuditFixTests
        self.m._current_store_id.reset(self.token)
        AuditFixTests.tearDown(self)

    def newer_message(self):
        self.m.save_message(self.db, self.sender, 'incoming', 'text', 'غيري القياس إلى 46',
                            None, None, None, {})

    def test_new_customer_message_after_first_part_stops_all_remaining_parts_and_photos(self):
        def accept_then_interrupt(*args, **kwargs):
            self.newer_message()
            return True
        with patch.object(self.m, 'send_text_to_facebook', side_effect=accept_then_interrupt) as send, \
             patch.object(self.m, 'send_image_to_facebook') as image:
            self.assertFalse(self.m.send_webhook_result_to_facebook(self.result))
        self.assertEqual([call.args[1] for call in send.call_args_list], self.plan['reply_parts'][:1])
        self.assertTrue(self.result['meta']['superseded'])
        image.assert_not_called()

    def test_partial_provider_failure_never_replays_accepted_or_uncertain_messages(self):
        with patch.object(self.m, 'send_text_to_facebook', side_effect=[True, False]) as send, \
             patch.object(self.m, 'send_image_to_facebook') as image:
            self.assertFalse(self.m.send_webhook_result_to_facebook(self.result))
            self.assertFalse(self.m.send_webhook_result_to_facebook(self.result))
        self.assertEqual([call.args[1] for call in send.call_args_list], self.plan['reply_parts'])
        image.assert_not_called()
        rows = self.db.execute("SELECT raw_payload FROM messages WHERE sender_id=? AND direction='outgoing' ORDER BY id",
                               (self.sender,)).fetchall()
        metadata = [self.m.delivery_state.metadata(row[0]) for row in rows]
        self.assertEqual([item['_delivery_status'] for item in metadata], ['accepted', 'unconfirmed', 'skipped'])
        self.assertTrue(all(item['_partial_delivery_reply'] for item in metadata))

    def test_internal_product_link_send_uses_matching_active_generation_for_staleness(self):
        self.result.pop('_reply_to_message_id')
        generation = self.m._reply_generation.set(dict(sender_id=self.sender, incoming_id=self.incoming,
                                                       outgoing_ids=[], superseded=False))
        try:
            with patch.object(self.m, 'send_text_to_facebook', side_effect=lambda *a, **k: (self.newer_message() or True)) as send, \
                 patch.object(self.m, 'send_image_to_facebook') as image:
                self.assertFalse(self.m.send_webhook_result_to_facebook(self.result))
            self.assertEqual(send.call_count, 1)
            image.assert_not_called()
        finally:
            self.m._reply_generation.reset(generation)

    def test_another_senders_generation_does_not_cancel_this_customers_plan(self):
        self.result.pop('_reply_to_message_id')
        self.newer_message()
        generation = self.m._reply_generation.set(dict(sender_id='another-sender', incoming_id=self.incoming,
                                                       outgoing_ids=[], superseded=False))
        try:
            with patch.object(self.m, 'send_text_to_facebook', return_value=True) as send, \
                 patch.object(self.m, 'send_image_to_facebook', return_value=True) as image:
                self.assertTrue(self.m.send_webhook_result_to_facebook(self.result))
            self.assertEqual([call.args[1] for call in send.call_args_list], self.plan['reply_parts'])
            image.assert_called_once()
        finally:
            self.m._reply_generation.reset(generation)


if __name__ == '__main__':
    unittest.main()
