import unittest

from account_app import dual_sales,dual_sales_strategy as sales_strategy


class SalesConversationPlanTests(unittest.TestCase):
    def plan(self,count=2,**overrides):
        return dict(message_count=count,message_goals=['هدف'+str(i) for i in range(count)],sales_stage='opening',next_step='choose_variant',next_question='اختيار القياس',image_position='none',**overrides)

    def test_one_to_five_are_executable_counts(self):
        for count in range(1,6):
            with self.subTest(count=count):
                self.assertEqual(sales_strategy.validate_reply_plan(self.plan(count)),[])
                parts=['معلومة مكتملة '+str(i) for i in range(count-1)]+['شكد قياسج كلبي؟']
                self.assertEqual(dual_sales.validate_parts(parts,message_count=count,next_question='القياس',allowed_numbers=range(6)),[])

    def test_six_bubbles_and_goal_count_mismatch_are_rejected(self):
        self.assertIn('message_count',sales_strategy.validate_reply_plan(self.plan(6)))
        plan=self.plan();plan['message_goals']=['هدف واحد']
        self.assertIn('message_goals',sales_strategy.validate_reply_plan(plan))

    def test_opening_cannot_end_with_no_sales_step(self):
        plan=self.plan();plan['next_question']=None
        self.assertIn('missing_followup',sales_strategy.validate_reply_plan(plan))
        plan['next_step']='close'
        self.assertIn('unresolved_sales_step',sales_strategy.validate_reply_plan(plan))

    def test_saved_or_declined_conversation_can_close_without_question(self):
        plan=self.plan(1);plan.update(sales_stage='closed',next_step='close',next_question=None)
        self.assertEqual(sales_strategy.validate_reply_plan(plan),[])
        plan.update(sales_stage='checkout',next_step='confirm_order')
        self.assertIn('booking_action_not_completed',sales_strategy.validate_reply_plan(plan))
        self.assertEqual(sales_strategy.validate_reply_plan(plan,saved_order=True),[])

    def test_warm_iraqi_endearments_are_not_counted_as_errors(self):
        self.assertEqual(dual_sales.validate_parts(['هلا بيج حبيبتي، سعره24 ألف من عيوني.','شكد قياسج كلبي؟'],message_count=2,next_question='القياس',allowed_numbers={'24'}),[])
        self.assertEqual(dual_sales.validate_parts(['حبيبتي كلبي، تدللين يروحي.']),[])

    def test_question_belongs_at_end_of_group(self):
        self.assertIn('question_before_final_part',dual_sales.validate_parts(['يالون تحبين؟','عدنا الأسود.'],message_count=2,next_question='اللون'))
        self.assertEqual(dual_sales.validate_parts(['عدنا الأسود كلبي.','يالون عجبج؟'],message_count=2,next_question='اللون'),[])

    def test_long_part_is_replanned_not_silently_truncated(self):
        text='شرح كامل. '*70
        self.assertIn('long_unsplit_part',dual_sales.validate_parts([text]))
        parts=[text[:len(text)//2],text[len(text)//2:]]
        self.assertEqual(dual_sales.validate_parts(parts,message_count=2),[])

    def test_natural_number_formats_do_not_trigger_false_price_error(self):
        self.assertEqual(dual_sales.validate_parts(['الحساب٥١٬٠٠٠ دينار.'],allowed_numbers={'51000'}),[])
        self.assertIn('unapproved_number',dual_sales.validate_parts(['الحساب٥٠٬٠٠٠ دينار.'],allowed_numbers={'51000'}))

    def test_warmth_does_not_allow_preparation_claim_or_foreign_currency(self):
        self.assertIn('unsupported_fulfilled',dual_sales.validate_parts(['جهزتهن ببالي كلبي.']))
        self.assertIn('unsupported_fulfilled',dual_sales.validate_parts(['جهزت الأسود والجوزي.']))
        self.assertIn('foreign_currency',dual_sales.validate_parts(['تفحصين قبل تدفعين درهم.']))
        self.assertIn('observed_dialect_typo',dual_sales.validate_parts(['واصلة لباب بيتدج.']))
        self.assertEqual(dual_sales.validate_parts(['تفحصين قبل لا تدفعين دينار حبيبتي.']),[])

    def test_collect_contact_is_not_a_new_question_if_both_known(self):
        plan=self.plan();plan.update(sales_stage='checkout',next_step='collect_contact',next_question='الرقم والعنوان')
        self.assertIn('no_contact_missing',sales_strategy.validate_reply_plan(plan,known_contact={'phone','address'}))

    def test_fallback_never_calls_delivery_only_amount_a_product_price(self):
        state=dict(cart=dict(items=[],phone=None,address=None))
        quote=dict(product_total=0,total_amount=5000,delivery_fee=5000)
        plan=dict(intent='سعر السوت',next_step='choose_variant',next_question='القياس')
        parts=sales_strategy.safe_fallback(plan,state,quote,[],[dict(product_id='T',price='24000')])
        self.assertIn('24 ألف',parts[0]);self.assertNotIn('5 ألف',' '.join(parts))
        self.assertEqual(parts[-1],'شكد قياسج كلبي؟')

    def test_fallback_uses_executed_preview_total(self):
        plan=dict(intent='سعر القطعة واصلة',next_step='accept_offer',next_question='قبول السعر')
        action=dict(name='quote_product',result=dict(ok=True,product_total=24000,delivery_fee=5000,total_amount=29000))
        parts=sales_strategy.safe_fallback(plan,dict(cart={}),dict(product_total=0,total_amount=5000),[action],[])
        self.assertIn('29 ألف',parts[0]);self.assertIn('يناسبج',parts[0])


class ReplyBatchTests(unittest.TestCase):
    def test_photo_event_is_between_intro_and_selection_question(self):
        events=sales_strategy.reply_events(['هاي الصور حبيبتي.','يالون عجبج؟'],image_colors=['اسود','جوزي'],image_position='between')
        self.assertEqual([e['kind'] for e in events],['text','images','text'])
        self.assertEqual(events[1]['delivery'],'sandbox_only')

    def test_new_customer_message_cancels_pending_photos_and_question(self):
        events=sales_strategy.reply_events(['هاي الصور.','يالون؟'],image_colors=['اسود'],image_position='between')
        batch=sales_strategy.ReplyBatch(10,events)
        self.assertEqual(batch.next_event(10)['kind'],'text')
        self.assertIsNone(batch.next_event(11))
        self.assertTrue(batch.cancelled)
        self.assertIsNone(batch.next_event(10))

    def test_four_or_five_parts_keep_order_and_finish_once(self):
        for n in (4,5):
            parts=['فكرة'+str(i) for i in range(n)]
            batch=sales_strategy.ReplyBatch(1,sales_strategy.reply_events(parts))
            self.assertEqual([batch.next_event(1)['text'] for _ in parts],parts)
            self.assertIsNone(batch.next_event(1))

    def test_missing_photo_room_is_rejected_in_plan_and_batch(self):
        with self.assertRaises(ValueError):
            sales_strategy.reply_events(['نص واحد'],image_colors=['اسود'],image_position='between')


if __name__=='__main__':unittest.main()
