"""Regressions from the 20 real-customer replay; fixtures contain no private data."""
import json
import unittest
from unittest.mock import Mock
from account_app.checkout import COLOR_REVIEW_STATUS
from account_app.tests import test_sales_runtime_linking as linking
from account_app.sales_runtime.runtime import Runtime
from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.sales_runtime.response import Responses
from account_app.sales_runtime.copy_guard import validate_copy
from account_app.sales_runtime.contracts import DecisionRejected,ServiceRequired
from account_app.sales_runtime.gateway import WriterGateway
from account_app.sales_runtime.policies import APPROVED_DELIVERY_TIME
from account_app.sales_runtime import Scope


class AuditFixTests(unittest.TestCase):
    setUp=linking.LinkingTests.setUp
    tearDown=linking.LinkingTests.tearDown
    event=linking.LinkingTests.event
    version=linking.LinkingTests.version
    run_turn=linking.LinkingTests.run_turn
    select=linking.LinkingTests.select
    prompt=linking.LinkingTests.prompt

    def test_courtesy_fit_and_inspection_questions_do_not_become_conditions(self):
        for text in ('شلون السعر اذا امكن','اذا وزن 60 يا قياس يناسبه','اذا انطيج وزني تعرفين القياس؟','اذا ما رهمت ارجعها؟'):
            _,out=self.run_turn(text)
            self.assertEqual([],out['readiness']['conditions'],text)

    def test_complete_checkout_after_inspection_question_books_once(self):
        self.run_turn('اريد الماروني 44',changes=[dict(values=dict(product_id='P1',color='ماروني',size='44'))])
        self.run_turn('اذا ما رهمت ارجعه؟')
        self.run_turn('07701234567',contact={'phone':'07701234567'})
        _,out=self.run_turn('بغداد حي المنصور قرب المدرسة',contact={'address':'بغداد حي المنصور قرب المدرسة'})
        self.assertEqual('booked',out['action'])
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_all_observed_false_confirmation_variants_are_blocked(self):
        for text in ('ثبتتلج الطلب','طلبج مثبت','تثبت طلبج','تم تثبيت الطلب','حجزتلك الطلب'):
            with self.subTest(text=text),self.assertRaisesRegex(ValueError,'reply.booking'):
                validate_copy([dict(type='text',text=text)],None)
        validate_copy([dict(type='text',text='طلبج مثبت')],None,dict(order_saved=True))

    def test_carrier_promises_require_action_proof(self):
        for text in ('نبلغ المندوب ينسق وياج','احرص انسق ويا المندوب','نحاول ويا المندوب يوصله باجر'):
            with self.subTest(text=text),self.assertRaisesRegex(ValueError,'reply.action'):
                validate_copy([dict(type='text',text=text)],None)

    def test_repeated_invalid_writer_does_not_send_template_or_false_booking(self):
        class BadWriter:
            def __call__(self,context):return [dict(type='text',text='ثبتتلج الطلب')]
            def repair(self,context,previous,error):return self(context)
        self.select()
        event=self.event('هلا');out=self.runtime.turn(self.scope,event)
        with self.assertRaisesRegex(DecisionRejected,'writer_invalid_after_validation'):
            Responses(self.store,self.loader,BadWriter()).prepare(self.scope,event,out)
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM sales_v2_outbox').fetchone()[0])

    def test_replayed_opening_is_removed_from_offer_and_price_followups(self):
        self.select()
        opening = 'وعليكم السلام والرحمة، يا هلا بيچ عيوني نورتينا.'
        writer = Mock(side_effect=[
            [dict(type='text',text=opening)],
            [dict(type='text',text=opening + ' إي، المقصود بدلتين.')],
            [dict(type='text',text=opening), dict(type='text',text='السعر {{subtotal}}.')],
            [dict(type='text',text='وعليكم السلام عيوني.')],
        ])
        turns = ('السلام عليكم', 'بلا زحمة المقصود بالاعلان بدلتين',
                 'شلون السعر اذا امكن', 'السلام عليكم رجعت')
        replies = []
        for text in turns:
            event,out = self.run_turn(text)
            replies.append([part['text'] for part in self.reply(event,out,writer)])
        self.assertEqual([[opening],['إي، المقصود بدلتين.'],['السعر 12 ألف.'],['وعليكم السلام عيوني.']],replies)

    def test_gateway_repairs_a_written_price_and_preserves_the_model_wording(self):
        self.products[0]['price'] = '17000'
        self.run_turn('أريد قطعتين ماروني 44',changes=[
            dict(values=dict(product_id='P1',color='ماروني',size='44',quantity=2))])
        event,out = self.run_turn('سويهن ب36 والتوصيل لبغداد')
        bad = 'المجموع 36 ألف. يناسبج الحساب؟'
        good = ('أدري تردين أوفّر لج. القطعتين {{subtotal}} والتوصيل {{delivery_fee}}، '
                'أگدر أنزل {{discount}} فيصير الحساب {{total}}. يناسبج هالمجموع؟')
        post = Mock(side_effect=[Mock(status_code=200,json=lambda text=text: {
            'choices':[{'message':{'content':json.dumps({'parts':[{'type':'text','text':text}]})}}]
        }) for text in (bad,good)])
        reply = self.reply(event,out,WriterGateway('test-model','test-key',post))[0]
        self.assertEqual(good.replace('{{subtotal}}','34 ألف').replace('{{delivery_fee}}','5 آلاف')
                         .replace('{{discount}}','ألفين').replace('{{total}}','37 ألف'),reply['text'])
        self.assertEqual(37000,reply['quote_total'])
        retry = json.loads(post.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertIn('reply.money',retry['validation_feedback'])

    def test_writer_retries_an_incomplete_response_as_validated_plain_text(self):
        self.products[0]['sizes'] = 'فري سايز من 55 كيلو الى 130 كيلو'
        event,out = self.run_turn('أريد قياس لرج',changes=[dict(values=dict(product_id='P1',size='لرج'))])
        draft = 'هالموديل فري سايز عيوني، وسجلت لج لارج. اختاري اللون اللي يعجبج.'
        post = Mock(side_effect=[
            Mock(status_code=200,json=lambda: {'choices':[{'finish_reason':'length','message':{'content':draft}}]}),
            Mock(status_code=200,json=lambda: {'choices':[{'finish_reason':'stop','message':{'content':draft}}]}),
        ])
        replies = self.reply(event,out,WriterGateway('test-model','test-key',post))
        self.assertEqual(draft,' '.join(part['text'] for part in replies))
        self.assertTrue(replies[-1]['question_field'].endswith('.color'))
        self.assertIn('response_format',post.call_args_list[0].kwargs['json'])
        self.assertNotIn('response_format',post.call_args_list[1].kwargs['json'])

    def test_withdrawal_stops_checkout_questions(self):
        self.run_turn('أريد قياس 44',changes=[dict(values=dict(product_id='P1',size='44'))])
        event,out = self.run_turn('لا ما أريدها')
        writer = Mock(return_value=[dict(type='text',text='من عيوني، براحتج حبيبتي 🌷')])
        reply = self.reply(event,out,writer)[0]
        self.assertEqual('withdrawn',writer.call_args.args[0]['mode'])
        self.assertNotIn('question_field',reply)
        self.assertFalse(self.store.snapshot(self.scope)['data']['purchase_event'])

    def test_negative_color_correction_updates_single_line_even_if_model_withdraws(self):
        self.run_turn('أريد الماروني 44', changes=[
            dict(values=dict(product_id='P1', color='ماروني', size='44'))])
        self.plan = dict(intent='withdraw')
        event = self.event('ما أريد الماروني، أريد الأزرق')
        result = self.runtime.turn(self.scope, event)
        state = self.store.snapshot(self.scope)['data']
        self.assertFalse(result['withdrawn'])
        self.assertEqual(event, state['purchase_event'])
        self.assertEqual([('أزرق', '44')], [(line['color'], line['size']) for line in state['cart']])

    def test_request_for_photos_does_not_activate_checkout(self):
        self.select()
        _, result = self.run_turn('أريد الصور')
        self.assertEqual('continue', result['action'])
        self.assertIsNone(self.store.snapshot(self.scope)['data']['purchase_event'])

    def test_same_cart_does_not_repeat_purchase_question_after_topic_changes(self):
        self.products[0]['fabric'] = 'قديفة'
        self.select()
        writer = Mock(side_effect=[
            [dict(type='text', text='قماشه قديفة، تحبين أثبتلج الحجز؟')],
            [dict(type='text', text='إي عيني، الماروني اختيارچ.')],
        ])
        event, out = self.run_turn('شنو خامته؟', answer=dict(question_field='purchase_intent'))
        first = self.reply(event, out, writer)[-1]
        self.assertEqual('purchase_intent', first['question_field'])
        event, out = self.run_turn('ماروني', answer=dict(question_field='purchase_intent'))
        followup = self.reply(event, out, writer)[-1]
        self.assertNotIn('question_field', followup)
        self.assertIsNone(self.store.snapshot(self.scope)['data']['purchase_event'])

    def test_yes_after_a_purchase_question_with_a_product_answer_starts_checkout(self):
        self.products[0]['fabric'] = 'قديفة'
        self.select()
        event, out = self.run_turn('شنو خامته؟', answer=dict(question_field='purchase_intent'))
        self.reply(event, out, Mock(return_value=[dict(type='text', text='قماشه قديفة، تحبين أثبتلج الحجز؟')]))
        event, out = self.run_turn('يله تمام')
        self.assertEqual(event, self.store.snapshot(self.scope)['data']['purchase_event'])
        self.assertNotIn('purchase_intent', out['readiness']['missing_fields'])
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_default_hint_is_provisional_after_an_unresolved_photo(self):
        hint = Mock(return_value={'source': 'auto_product', 'product_id': 'P1'})
        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, province: 5000,
                           self.decide, text_reference=hint,
                           media=lambda *args: dict(status='unknown', product_ids=[]))
        channel = dict(store_id='khuyoot', channel='chatwoot', account_id='a')
        entry.receive(**channel, sender_id='ad', message_id='opening', text='شكد السعر؟',
                      ad={'ad_id': '120251717777850095'})
        self.assertEqual('ad', self.decide.call_args.args[0]['reference']['source'])
        hint.assert_not_called()
        entry.receive(**channel, sender_id='photo', message_id='image', media_kind='image', media_ref='test')
        result = entry.receive(**channel, sender_id='photo', message_id='followup', text='شكد سعر هذا؟')
        self.assertEqual('P1', result['provisional_product'])
        state = self.store.snapshot(Scope('khuyoot', 'chatwoot', 'a', 'photo'))['data']
        self.assertIsNone(state.get('focus_product_id'))
        self.assertFalse(state['cart'])
        self.decide.assert_called_once()

    def test_photo_promises_need_an_attachment(self):
        from account_app.sales_runtime.copy_guard import validate_copy
        from account_app.sales_runtime.recovery import catalog_photo_request
        for reply in ('هسة أنزلهن الچ هنا بالدردشة', 'حبيبتي ثواني وتوصلچ الصور، دقايق وتكون يمج.',
                      'هسه أرفق صور الألوان'):
            with self.subTest(reply=reply), self.assertRaisesRegex(ValueError, 'reply.media'):
                validate_copy([dict(type='text', text=reply)], None)
        for message in ('ما أريد الصور', 'وين أصور إقامتي؟', 'وين الصور وشكد سعره؟'):
            with self.subTest(message=message):
                self.assertFalse(catalog_photo_request(message))

    def test_known_ad_stays_available_as_photo_context_after_referral_turn(self):
        hint = Mock(return_value={'source': 'auto_product', 'product_id': 'P2'})
        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, province: 5000,
                           self.decide, text_reference=hint,
                           media=lambda *args: dict(status='unknown'))
        target = dict(store_id='khuyoot', channel='zernio', account_id='a', sender_id='ad-photo')
        first = entry.receive(**target, message_id='image', media_kind='image', media_ref='fixture',
                              ad={'ad_id': '120251717777850095'})
        followup = entry.receive(**target, message_id='sizes', text='اكو قياسات')
        self.assertEqual(('P1', 'P1'), (first['provisional_product'], followup['provisional_product']))
        self.decide.assert_not_called()

    def test_writer_splits_valid_long_reply_without_losing_approved_content(self):
        from account_app.reply_layout import compact
        self.products[0].update(fabric='قديفة شتوي ناعم')
        self.select()
        event, outcome = self.run_turn('شنو الألوان المتوفرة وقماشه شنو وشكد التوصيل؟')
        reply = ('القماش قديفة شتوي وملمسه ناعم، يناسب إذا دا تدورين على شي دافي للبيت. '
                 'الألوان المتوفرة ماروني وأزرق هالفترة. '
                 'التوصيل لبغداد {{delivery_fee}} وتدفعين عند الاستلام بعد الفحص.')
        model = Mock(return_value=Mock(status_code=200, json=lambda: {
            'choices': [{'message': {'parsed': {'parts': [dict(type='text', text=reply)]}}}]
        }))
        parts = self.reply(event, outcome, WriterGateway('test-model', 'test-key', model),
                           policy={'delivery_fee': {'بغداد': 5000}})
        self.assertGreater(len(parts), 1)
        self.assertEqual(compact(reply.replace('{{delivery_fee}}', '5 آلاف')),
                         compact(' '.join(part['text'] for part in parts)))

    def test_sales_writer_gets_verified_offer_and_free_delivery_as_distinct_amounts(self):
        self.products[0].update(price='15000', offer='قطعتين بـ25000', delivery='توصيل مجاني')
        self.select()
        event, outcome = self.run_turn('حبيبتي شكد سعره')
        draft = ('عيوني، سعر القطعة {{unit_price}}. العرض قطعتين بـ{{offer_total}}. '
                 'التوصيل مجاني {{delivery_fee}}. تحبين قطعة وحدة لو تستفيدين من عرض القطعتين؟')
        post = Mock(return_value=Mock(status_code=200, json=lambda: {
            'choices': [{'message': {'parsed': {'parts': [dict(type='text', text=draft)]}}}]
        }))
        replies = self.reply(event, outcome, WriterGateway('test-model','test-key',post),
                             policy={'delivery_fee': {'بغداد': 5000}})
        context = json.loads(post.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertEqual({'unit_price': 15000, 'offer_total': 25000, 'delivery_fee': 0}, context['amounts'])
        self.assertEqual(2, context['promotion_quantity'])
        self.assertEqual('offer.quantity', replies[-1]['question_field'])
        self.assertGreaterEqual(len(replies), 2)
        self.assertIn('25 ألف', ' '.join(part['text'] for part in replies[:-1]))
        self.assertEqual(1, sum(part['text'].count('مجان') for part in replies))
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_asking_for_other_colors_does_not_repeat_the_same_palette(self):
        self.select()
        event, outcome = self.run_turn('لا غير الوان ماعدكم')
        writer = Mock(return_value=[dict(type='text', text='هاي المتوفرة بهالموديل. شنو اللون اللي ببالج؟')])
        parts = self.reply(event, outcome, writer)
        context = writer.call_args.args[0]
        self.assertTrue(context['action_results']['omit_color_list'])
        self.assertEqual({'field': 'alternative.color', 'options': []}, context['next_step'])
        self.assertEqual('alternative.color', parts[-1]['question_field'])

    def test_yalla_tamam_resumes_checkout_after_price_discussion(self):
        self.products[0].update(price='16000',colors='أسود، رصاصي')
        event,out=self.run_turn('أريد أسود ورصاصي قياس 44',changes=[
            dict(values=dict(product_id='P1',color='اسود',size='44')),
            dict(new_line=True,values=dict(product_id='P1',color='رصاصي',size='44'))])
        drafts = ('صار عيني، شنو الرقم والعنوان للتوصيل؟',
                  'القطعتين {{subtotal}}، وبعد خصم {{discount}} يصيرن {{discounted_subtotal}}.',
                  'على اختيارج حبيبتي، شنو رقم الموبايل والعنوان؟', 'تدللين عيوني.')
        writer=Mock(side_effect=[[dict(type='text',text=draft)] for draft in drafts])
        self.assertEqual('contact.details',self.reply(event,out,writer)[-1]['question_field'])
        event,out=self.run_turn('سويهن الوحده 15 بالعدله')
        self.reply(event,out,writer)
        self.plan=dict(intent='withdraw')
        event=self.event('يله تمام')
        out=self.runtime.turn(self.scope,event)
        self.assertEqual(event,self.store.snapshot(self.scope)['data']['purchase_event'])
        self.assertEqual('contact.details',self.reply(event,out,writer)[-1]['question_field'])
        self.assertTrue(writer.call_args.args[0]['progress']['checkout_confirmed'])
        event,out=self.run_turn('يلا،تمام')
        self.assertNotIn('question_field',self.reply(event,out,writer)[-1])
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_yalla_tamam_does_not_start_checkout_after_a_price_inquiry(self):
        self.select()
        self.run_turn('شلون السعر اذا امكن')
        _,out=self.run_turn('يلا تمام')
        self.assertIn('purchase_intent',out['readiness']['missing_fields'])
        self.assertFalse(self.store.snapshot(self.scope)['data']['purchase_event'])

    def test_optional_description_does_not_hide_available_fabric_or_length(self):
        self.products[0].update(fabric='قديفة ثخين',measurements='الطول 105 سم',description='')
        self.select()
        self.plan=dict(intent='continue',answer=dict(facts=[dict(product_id='P1',field='description')]))
        event=self.event('شنو قماشه؟');out=self.runtime.turn(self.scope,event)
        writer=Mock(return_value=[dict(type='text',text='قماشه قديفة ثخين حبيبتي')])
        self.assertEqual('queued',Responses(self.store,self.loader,writer).prepare(self.scope,event,out)['action'])
        self.assertTrue(any(f['field']=='fabric' for f in writer.call_args.args[0]['facts']))
        self.plan=dict(intent='continue',answer=dict(facts=[dict(product_id='P1',field='length')]))
        event=self.event('شكد طوله؟');out=self.runtime.turn(self.scope,event)
        Responses(self.store,self.loader,writer).prepare(self.scope,event,out)
        self.assertIn('105',str(writer.call_args.args[0]['facts']))

    def test_human_identity_request_is_silent_before_any_model(self):
        decide=Mock()
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,decide)
        result=entry.receive(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='id',
                             message_id='identity',text='اخي مخليلي ذكاء صطناعي يرد')
        self.assertEqual('human',result['action']);self.assertEqual('',result['reply'])
        decide.assert_not_called()
        self.assertEqual('customer_requested_human',result['reason'])

    def test_photo_ai_question_is_not_identity_handoff(self):
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide)
        result=entry.receive(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='photo',
                             message_id='m',text='الصورة ذكاء اصطناعي؟')
        self.assertNotEqual('human',result['action'])

    def test_delivery_time_comes_from_owner_policy(self):
        for store in ('default','khuyoot','al-fatena','baraah-kids','golden-threads'):
            entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide)
            self.assertEqual(APPROVED_DELIVERY_TIME,entry.policy_loader(store)['delivery_time'])

    def test_unavailable_size_is_business_feedback_not_model_repair(self):
        _,out=self.run_turn('اريد 99',changes=[dict(values=dict(product_id='P1',size='99'))])
        self.assertEqual('continue',out['action'])
        self.assertEqual('size',out['business_feedback']['field'])
        self.assertEqual('',self.store.snapshot(self.scope)['data']['cart'][0]['size'])
        self.assertEqual('active',self.store.snapshot(self.scope)['data']['control_mode'])

    def test_malformed_model_can_fall_back_to_exact_130_weight(self):
        self.products[0]['sizes']='قياسات من وزن 60 كيلو إلى 130 كيلو'
        self.products[0]['ad_ids']=['weight-ad']
        class Model:
            fallback_allowed=True
            def __call__(self,context):raise DecisionRejected('decision_invalid_after_retry')
        runtime=Runtime(self.store,self.loader,self.orders,Model())
        event=self.store.append(self.scope,'weight',kind='message',actor='customer',
                                payload=dict(text='يلبس وزن 130',ad={'ad_id':'weight-ad'}))['id']
        out=runtime.turn(self.scope,event)
        self.assertEqual('continue',out['action']);self.assertTrue(out['recovered'])
        self.assertEqual(130,float(self.store.snapshot(self.scope)['data']['cart'][0]['size']))

    def test_contact_echo_does_not_need_new_source_evidence(self):
        self.run_turn('07701234567',contact={'phone':'07701234567'})
        self.run_turn('قياس 44',contact={'phone':'07701234567'})
        self.assertEqual('07701234567',self.store.snapshot(self.scope)['data']['contact']['phone'])

    def test_cancel_local_unexported_order_is_real_and_does_not_rebook(self):
        self.select()
        self.run_turn('حجزيلي 07701234567 بغداد حي المنصور قرب المدرسة',
                      contact=dict(phone='07701234567',address='بغداد حي المنصور قرب المدرسة'))
        event,out=self.run_turn('يي حبيبتي الغي الطلب')
        self.assertEqual('cancelled',out['action'])
        self.assertEqual('cancelled',self.db.execute('SELECT status FROM orders').fetchone()[0])
        self.assertEqual(out,self.runtime.turn(self.scope,event))
        self.assertFalse(self.db.execute("SELECT 1 FROM sales_v2_order_tasks WHERE status='queued'").fetchone())

    def test_tracking_existing_order_does_not_turn_into_new_sale(self):
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide)
        for index, text in enumerate(('عندي طلب يمكم صار كم يوم', 'شوكت يوصل طلبي',
                                      'حبيبتي طلب قبل يومين من طلبته')):
            result=entry.receive(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id=f'tracking-{index}',
                                 message_id='m',text=text)
            self.assertEqual('order_tracking',result['reason'])
        self.decide.assert_not_called()
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_generic_delivery_questions_do_not_stop_a_new_sale(self):
        from account_app.sales_runtime.recovery import grounded_plan
        entry = Entrypoint(self.db, lambda sid: self.products, lambda s, p: 5000, grounded_plan)
        for i, text in enumerate(('متى يتم توصيل الطلبات؟', 'شوكت يوصل الطلب إذا طلبت؟',
                                  'شوكت يوصل الطلب؟')):
            result = entry.receive(store_id='khuyoot', channel='chatwoot', account_id='a',
                                   sender_id=f'new-delivery-{i}', message_id='m', text=text)
            self.assertEqual('continue', result['action'], text)
            self.assertIn('delivery_time', [f['field'] for f in result['answer_plan']['facts']])
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_eta_during_new_checkout_keeps_the_cart_active(self):
        self.select()
        self.prompt()
        entry = Entrypoint(self.db, lambda sid: self.products, lambda s, p: 5000, self.decide)
        self.plan = dict(intent='continue')
        result = entry.receive(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                               sender_id=self.scope.sender, message_id='eta', text='شوكت يوصل الطلب؟')
        self.assertEqual('continue', result['action'])
        self.assertEqual('active', self.store.snapshot(self.scope)['data']['control_mode'])

    def test_unrelated_model_output_cannot_erase_a_declared_phone(self):
        self.select()
        self.run_turn('رقمي 07701234567')
        self.run_turn('عندج صورة من ورا؟', contact=dict(phone='ناقص'))
        self.assertEqual('07701234567', self.store.snapshot(self.scope)['data']['contact']['phone'])
        _, result = self.run_turn('بغداد حي المنصور قرب المدرسة')
        self.assertEqual('booked', result['action'])

    def test_second_unresolved_photo_stops_the_loop_and_keeps_declared_contact(self):
        writer = Mock(return_value=[dict(type='text', text='حبيبتي، دزيلي صورة أوضح للموديل.')])
        entry = Entrypoint(self.db, lambda sid: self.products, lambda s, p: 5000, self.decide,
                           writer=writer, media=lambda *args: dict(status='unknown', product_ids=[]))
        target = dict(store_id='khuyoot', channel='chatwoot', account_id='a', sender_id='unresolved')
        scope = Scope('khuyoot', 'chatwoot', 'a', 'unresolved')
        first = entry.receive(**target, message_id='image-1', media_kind='image', media_ref='image-1')
        self.assertEqual('media_unresolved', first['action'])
        part = entry.store.claim_reply(scope)
        self.assertIn('الصورة وصلتني', part['payload']['text'])
        entry.store.settle_reply(scope, part['id'], 'accepted', 'receipt')
        second = entry.receive(**target, message_id='image-2', media_kind='image', media_ref='image-2',
                               text='رقمي 07701234567')
        self.assertEqual('product_image_unresolved_after_retry', second['reason'])
        self.assertEqual('07701234567', entry.store.snapshot(scope)['data']['contact']['phone'])
        writer.assert_not_called()
        self.decide.assert_not_called()

    def test_clothing_size_is_not_a_weight_and_a_later_weight_is_remembered(self):
        self.products[0].update(sizes='وزن من 60 إلى 130 كيلو')
        self.plan = dict(intent='continue', changes=[dict(values=dict(product_id='P1', weight_kg=54))])
        event = self.store.append(self.scope, 'clothing-label', kind='message', actor='customer',
                                  payload=dict(text='آني قياسي ٥٤', ad={'ad_id':'120251717777850095'}))['id']
        outcome = self.runtime.turn(self.scope, event)
        state = self.store.snapshot(self.scope)['data']
        self.assertEqual('54', state['stated_clothing_size']['label'])
        self.assertFalse(state['cart'][0].get('size'))
        writer = Mock()
        part, = self.reply(event, outcome, writer)
        self.assertEqual('product.P1.weight_kg', part['question_field'])
        self.assertNotIn('54 كيلو', part['text'])
        writer.assert_not_called()
        self.run_turn('وزني ١٠٥')
        line, = self.store.snapshot(self.scope)['data']['cart']
        self.assertEqual(('105', 'weight'), (line['size'], line['size_source_kind']))

    def test_quote_requested_discount_matches_persisted_invoice(self):
        self.run_turn('اريد قطعتين ماروني 44',changes=[dict(values=dict(product_id='P1',size='44',color='ماروني',quantity=2))])
        self.run_turn('سويهن ب28 والتوصيل لبغداد')
        self.run_turn('07701234567',contact={'phone':'07701234567'})
        _,out=self.run_turn('بغداد حي المنصور قرب المدرسة',contact={'address':'بغداد حي المنصور قرب المدرسة'})
        self.assertEqual('booked',out['action'])
        total,items=self.db.execute('SELECT total_amount,order_items FROM orders').fetchone()
        self.assertEqual(28000,total)
        self.assertEqual(23000,sum(i['unit_price']*i['quantity'] for i in json.loads(items)))

    def test_delivered_checkout_details_hold_the_order_when_only_color_is_missing(self):
        self.products[0].update(price='17000', sizes='فري سايز من 55 إلى 130 كيلو', colors='نيلي، وردي')
        self.run_turn('اريد قطعتين لارج',changes=[dict(values=dict(product_id='P1',size='L',quantity=2))])
        event,out=self.run_turn('وشكد التوصيل')
        self.reply(event,out,Mock(return_value=[dict(type='text',text='التوصيل {{delivery_fee}}.')]),
                   policy={'delivery_fee':{'واسط':5000}})
        event,out=self.run_turn('سويهن ب30')
        self.reply(event,out,Mock(return_value=[dict(type='text',text=(
            'القطعتين {{subtotal}}، وبعد خصم {{discount}} يصيرن {{discounted_subtotal}} قبل التوصيل. لأي محافظة؟'))]))
        self.run_turn('لا فدوة')
        self.run_turn('07701234567',contact={'phone':'07701234567'})
        event,out=self.run_turn('واسط حي التجربة قرب المدرسة',contact={'address':'واسط حي التجربة قرب المدرسة'})
        self.assertEqual('booked',out['action'])
        self.assertTrue(out['held_for_color'])
        total,status,items=self.db.execute('SELECT total_amount,status,order_items FROM orders').fetchone()
        self.assertEqual((37000,COLOR_REVIEW_STATUS),(total,status))
        self.assertEqual([('', 'L', 2)],[(item['color'],item['size'],item['quantity']) for item in json.loads(items)])
        self.assertEqual(1,self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])
        task_id,kind=self.db.execute('SELECT id,kind FROM sales_v2_order_tasks').fetchone()
        self.assertEqual('confirmation',kind)
        from account_app.sales_runtime.delivery import deliver_confirmation
        send=Mock(return_value=dict(accepted=True,provider_id='confirmation'))
        self.assertEqual('accepted',deliver_confirmation(self.store,self.scope,task_id,send,lambda scope: True)['status'])
        send.assert_called_once()
        self.assertEqual(out,self.runtime.turn(self.scope,event))
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_explicit_price_limit_survives_contact_details_after_quote(self):
        self.run_turn('اريد قطعتين ماروني 44',changes=[dict(values=dict(product_id='P1',size='44',color='ماروني',quantity=2))])
        event,out=self.run_turn('ما احجز إلا ب20 والتوصيل لبغداد')
        self.reply(event,out,Mock(return_value=[dict(type='text',text=(
            'القطعتين {{subtotal}} والتوصيل {{delivery_fee}}، نخصم {{discount}} فيصير {{total}}. يناسبج المجموع؟'))]))
        self.run_turn('07701234567',contact={'phone':'07701234567'})
        _,out=self.run_turn('بغداد حي التجربة قرب المدرسة',contact={'address':'بغداد حي التجربة قرب المدرسة'})
        self.assertEqual('continue',out['action'])
        self.assertTrue(any(c['kind']=='price' for c in out['readiness']['conditions']))
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_known_choices_book_at_the_previously_shown_price_on_delivery_details(self):
        self.run_turn('اريد قطعتين ماروني 44',changes=[dict(values=dict(product_id='P1',size='44',color='ماروني',quantity=2))])
        event,out=self.run_turn('سويهن ب20 والتوصيل لبغداد')
        self.reply(event,out,Mock(return_value=[dict(type='text',text=(
            'القطعتين {{subtotal}} والتوصيل {{delivery_fee}}، نخصم {{discount}} فيصير {{total}}. يناسبج المجموع؟'))]))
        _,out=self.run_turn('07701234567 بغداد حي التجربة قرب المدرسة',
                            contact={'phone':'07701234567','address':'بغداد حي التجربة قرب المدرسة'})
        self.assertEqual('booked',out['action'])
        self.assertFalse(out['held_for_color'])
        self.assertEqual((27000,'new'),self.db.execute('SELECT total_amount,status FROM orders').fetchone())

    def test_complete_selection_and_contacts_book_on_first_turn(self):
        text='اريد ماروني قياس 44، 07701234567، بغداد حي التجربة قرب المدرسة'
        _,out=self.run_turn(text,changes=[dict(values=dict(product_id='P1',size='44',color='ماروني'))],
                            contact={'phone':'07701234567','address':'بغداد حي التجربة قرب المدرسة'})
        self.assertEqual('booked',out['action'])
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_large_discount_does_not_silently_book_unaccepted_price(self):
        self.run_turn('اريد قطعتين ماروني 44',changes=[dict(values=dict(product_id='P1',size='44',color='ماروني',quantity=2))])
        self.run_turn('سويهن ب20 والتوصيل لبغداد')
        self.run_turn('07701234567',contact={'phone':'07701234567'})
        _,out=self.run_turn('بغداد حي المنصور قرب المدرسة',contact={'address':'بغداد حي المنصور قرب المدرسة'})
        self.assertEqual('continue',out['action'])
        self.assertEqual(27000,out['quote']['total'])
        self.assertTrue(out['readiness']['conditions'])
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_explicit_counteroffer_acceptance_books_counteroffer_not_requested_price(self):
        self.run_turn('اريد قطعتين ماروني 44',changes=[dict(values=dict(product_id='P1',size='44',color='ماروني',quantity=2))])
        self.run_turn('سويهن ب20 والتوصيل لبغداد')
        self.run_turn('07701234567',contact={'phone':'07701234567'})
        event,out=self.run_turn('بغداد حي المنصور قرب المدرسة',contact={'address':'بغداد حي المنصور قرب المدرسة'})
        writer=Mock(return_value=[dict(type='text',text='حسابهن {{subtotal}} والتوصيل {{delivery_fee}}، نخصم {{discount}} فيصير {{total}}. يناسبج المجموع؟')])
        Responses(self.store,self.loader,writer).prepare(self.scope,event,out)
        part=self.store.claim_reply(self.scope)
        self.assertEqual(27000,part['payload']['quote_total'])
        self.store.settle_reply(self.scope,part['id'],'accepted','counteroffer')
        _,out=self.run_turn('اي تمام')
        self.assertEqual('booked',out['action'])
        self.assertEqual(27000,self.db.execute('SELECT total_amount FROM orders').fetchone()[0])

    def test_single_item_never_receives_multi_item_discount(self):
        self.run_turn('اريد ماروني44',changes=[dict(values=dict(product_id='P1',size='44',color='ماروني'))])
        _,out=self.run_turn('سويها ب16 والتوصيل لبغداد')
        self.assertEqual(0,out['quote']['discount'])
        self.assertEqual(17000,out['quote']['total'])
        self.assertTrue(out['readiness']['conditions'])

    def test_unknown_product_answers_store_policy_and_waits_for_a_photo(self):
        drafts = ('دزيلي صورة الموديل حبيبتي.',
                  'الدفع عند الاستلام بعد الفحص، والتوصيل {{delivery_fee}}.',
                  'حتى أحسبلج الدشداشتين، دزيلي صورة الموديل.',
                  'التوصيل {{delivery_fee}}، وحساب القطعتين يعتمد على الموديل بالصورة.',
                  'إي هالموديل متوفر، أي لون تحبين؟')
        writer=Mock(side_effect=[[dict(type='text',text=text)] for text in drafts])
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide,
                         writer=writer,media=lambda *args: dict(status='matched',product_ids=['P1']))
        kwargs=dict(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='unknown')
        scope=Scope('khuyoot','chatwoot','a','unknown')
        turns=('سلام عليكم','كيفية الدفع والتوصيل؟','اذا اطلب دشداشتين من هذا الموديل','شكد سعرهن وي التوصيل')
        for index,text in enumerate(turns):
            out=entry.receive(**kwargs,message_id=str(index),text=text,ad={'ad_id':'unmapped'} if index!=2 else {})
            self.assertEqual('continue',out['action'])
            part=entry.store.claim_reply(scope)
            entry.store.settle_reply(scope,part['id'],'accepted',str(index))
            if index==1:
                self.assertIn('التوصيل 5 آلاف',part['payload']['text'])
                self.assertNotIn('question_field',part['payload'])
                self.assertEqual({'payment','delivery_fee'},{f['field'] for f in writer.call_args.args[0]['facts']})
            elif index==3:
                self.assertNotIn('question_field',part['payload'])
                self.assertIn('التوصيل 5 آلاف',part['payload']['text'])
            else:
                self.assertEqual('product',part['payload']['question_field'])
        self.decide.assert_not_called()
        self.plan=dict(intent='continue',changes=[dict(values=dict(product_id='P1',quantity=2))])
        out=entry.receive(**kwargs,message_id='photo',media_kind='image',media_ref='synthetic')
        self.assertEqual('continue',out['action'])
        self.assertEqual('image',self.decide.call_args.args[0]['reference']['source'])
        state=entry.store.snapshot(scope)['data']
        self.assertEqual('P1',state['focus_product_id'])
        self.assertEqual(2,state['cart'][0]['quantity'])
        self.assertEqual('active',state['control_mode'])
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])

    def test_invalid_unknown_product_reply_is_retryable_without_human_handoff(self):
        writer=Mock(return_value=[dict(type='text',text='تم تثبيت الطلب')])
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide,writer=writer)
        kwargs=dict(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='retry',message_id='m',
                    ad={'ad_id':'unmapped'},text='اريد هذا الموديل')
        out=entry.receive(**kwargs)
        self.assertEqual('service_failure',out['action'])
        self.assertTrue(out['retryable'])
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])
        writer.return_value=[dict(type='text',text='دزيلي صورة القطعة عيوني.')]
        self.assertEqual('continue',entry.receive(**kwargs)['action'])

    def test_store_inspection_does_not_depend_on_product_description(self):
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide,
                         writer=Mock(return_value=[dict(type='text',text='إي عيوني الج حق الفحص بوجود المندوب قبل الدفع')]))
        self.plan=dict(intent='continue',answer=dict(facts=[dict(product_id='P1',field='description')]))
        out=entry.receive(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='inspection',
                          message_id='m',text='اذا ما رهمت ارجعها مع المندوب؟')
        self.assertEqual('continue',out['action'])
        self.assertEqual([],out['readiness']['conditions'])

    def test_daytime_guarantee_routes_to_service_without_carrier_promise(self):
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide)
        out=entry.receive(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='deadline',
                          message_id='m',text='اريده باجر الصبح ضروري')
        self.assertEqual('human',out['action'])
        self.assertEqual('delivery_deadline',out['reason'])

    def test_post_booking_question_is_answered_without_new_order(self):
        self.products[0]['fabric']='قطن'
        self.select()
        self.run_turn('حجزيلي 07701234567 بغداد حي المنصور قرب المدرسة',
                      contact=dict(phone='07701234567',address='بغداد حي المنصور قرب المدرسة'))
        _,out=self.run_turn('شنو قماشه؟',answer=dict(facts=[dict(product_id='P1',field='fabric')]))
        self.assertEqual('continue',out['action'])
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_decoder_handles_native_parsed_and_fenced_json(self):
        from account_app.sales_runtime.gateway import structured_content
        response=Mock()
        response.json.return_value={'choices':[{'message':{'content':'```json\n{"intent":"continue"}\n```'}}]}
        self.assertEqual('continue',structured_content(response)['intent'])
        response.json.return_value={'choices':[{'message':{'parsed':{'intent':'continue'},'content':None}}]}
        self.assertEqual('continue',structured_content(response)['intent'])

    def test_hidden_product_does_not_block_context_for_other_products(self):
        self.products[1]['status']='inactive'
        _,out=self.run_turn('هلا')
        self.assertEqual('continue',out['action'])
        context=self.decide.call_args.args[0]
        self.assertNotIn(self.products[1]['product_id'],context['available_fact_fields'])

    def test_internal_ids_and_identity_denials_are_not_customer_copy(self):
        for text in ('موديل P001 قياساته كذا','اني من المحل ارد بنفسي','منشأ الصنع مو محدد يمي بالمعلومات'):
            with self.subTest(text=text),self.assertRaises(ValueError):validate_copy([dict(type='text',text=text)],None)

    def test_side_questions_do_not_reset_already_asked_color(self):
        contexts=[]
        def writer(context):
            contexts.append(context)
            return [dict(type='text',text='يا لون عجبج؟' if context['question_field'] else 'التوصيل {{delivery_fee}} حبيبتي')]
        responses=Responses(self.store,self.loader,writer,lambda sid:dict(delivery_fee={'بغداد':5000}))
        for index,text in enumerate(('قياس 44','شكد التوصيل','اريد اعرف هم التوصيل')):
            event,out=self.run_turn(text,changes=[dict(values=dict(product_id='P1',size='44'))] if index==0 else [])
            responses.prepare(self.scope,event,out)
            part=self.store.claim_reply(self.scope)
            self.store.settle_reply(self.scope,part['id'],'accepted',str(index))
            if index:
                self.assertNotIn('question_field',part['payload'])
                self.assertIn('التوصيل 5 آلاف',part['payload']['text'])
        self.assertTrue(contexts[0]['question_field'].endswith('.color'))
        self.assertEqual([None,None],[context['question_field'] for context in contexts[1:]])

    def test_unavailable_child_age_offers_compatible_alternative_not_contacts(self):
        self.products[0].update(sizes='1 إلى 4 سنوات',product_type='sets',audience='boys')
        self.products[1].update(size_system='legacy',sizes='2 إلى 7 سنوات',product_type='sets',audience='boys')
        event,out=self.run_turn('اريد عمر 6 سنوات',changes=[dict(values=dict(product_id='P1',age_months=72))],
                                 answer=dict(question_field='contact.phone'))
        writer=Mock(return_value=[dict(type='text',text='أكو موديل ثاني متوفر لعمره حبيبتي، تحبين تشوفيه؟')])
        Responses(self.store,self.loader,writer).prepare(self.scope,event,out)
        context=writer.call_args.args[0]
        self.assertTrue(context['question_field'].startswith('alternative.'))
        self.assertEqual('B1',context['recommendations']['matches'][0]['product_id'])
        self.assertEqual('',self.store.snapshot(self.scope)['data']['cart'][0]['size'])

    def test_latest_age_and_weight_corrections_replace_their_own_values(self):
        self.products[0]['sizes']='1 إلى 7 سنوات'
        self.run_turn('عمر 4',changes=[dict(values=dict(product_id='P1',age_months=48))])
        self.run_turn('لا عمر 6',changes=[dict(values=dict(product_id='P1',age_months=72))])
        self.assertEqual('6 سنوات',self.store.snapshot(self.scope)['data']['cart'][0]['size'])
        line=self.store.snapshot(self.scope)['data']['cart'][0]['line_id']
        self.run_turn('احذف هذا الاختيار',changes=[dict(line_ref=line,remove=True,values={})])
        self.products[0]['sizes']='وزن 60 إلى 130 كيلو'
        self.run_turn('وزني 80',changes=[dict(values=dict(product_id='P1',weight_kg=80))])
        self.run_turn('لا وزني130',changes=[dict(values=dict(product_id='P1',weight_kg=130))])
        self.assertEqual('130',self.store.snapshot(self.scope)['data']['cart'][0]['size'])
        self.run_turn('اريد قياس60',changes=[dict(values=dict(product_id='P1',size='60'))])
        self.run_turn('وزني130',changes=[dict(values=dict(product_id='P1',weight_kg=130))])
        self.assertEqual('60',self.store.snapshot(self.scope)['data']['cart'][0]['size'])

    def test_changed_catalog_price_requires_new_counteroffer_acceptance(self):
        self.run_turn('اريد قطعتين ماروني44',changes=[dict(values=dict(product_id='P1',color='ماروني',size='44',quantity=2))])
        self.run_turn('سويهن ب28 والتوصيل لبغداد')
        self.products[0]['price']='20000'
        _,out=self.run_turn('07701234567',contact=dict(phone='07701234567'))
        self.assertTrue(any(c['kind']=='price' for c in out['readiness']['conditions']))
        _,out=self.run_turn('بغداد حي المنصور قرب المدرسة',contact=dict(address='بغداد حي المنصور قرب المدرسة'))
        self.assertEqual('continue',out['action'])
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_suspended_or_superseded_cancel_cannot_change_saved_order(self):
        self.select();self.run_turn('حجزيلي 07701234567 بغداد حي المنصور قرب المدرسة',
            contact=dict(phone='07701234567',address='بغداد حي المنصور قرب المدرسة'))
        cancel=self.event('الغي الطلب');newer=self.event('لا خلي الطلب')
        with self.assertRaisesRegex(ValueError,'turn_superseded'):
            self.orders.cancel(self.scope,'cancel-old',self.version(),cancel)
        staff=self.event('متابعة الموظف',actor='staff')
        self.store.set_control(self.scope,'pause',self.version(),staff,'human')
        with self.assertRaisesRegex(ValueError,'human_control_active'):
            self.orders.cancel(self.scope,'cancel-paused',self.version(),cancel)
        self.assertEqual('new',self.db.execute('SELECT status FROM orders').fetchone()[0])

    def test_writer_internal_product_name_uses_owner_category(self):
        self.products[0].update(product_name='تراك ولادي تطعيم',reply_category='تراك ولادي')
        event,out=self.run_turn('هلا',changes=[dict(values=dict(product_id='P1',size='44'))])
        writer=Mock(return_value=[dict(type='text',text='تراك ولادي تطعيم متوفر حبيبتي')])
        Responses(self.store,self.loader,writer).prepare(self.scope,event,out)
        text=json.loads(self.db.execute('SELECT payload FROM sales_v2_outbox').fetchone()[0])['text']
        self.assertNotIn('تطعيم',text);self.assertIn('تراك ولادي',text)

    def test_teaching_does_not_commit_or_override_prices(self):
        from account_app import product_questions
        from account_app.sales_runtime.catalog_answers import enrich
        product_questions.init(self.db);self.db.commit()
        self.db.execute("INSERT INTO product_questions(store_id,product_id,topic,question,question_key,answer,guidance,status,created_at,updated_at) VALUES('khuyoot','P1','price','سعر','k','1','وضحي العرض بلطف','answered','now','now')")
        result=enrich(self.db,'khuyoot',self.products)
        self.assertTrue(self.db.in_transaction)
        self.assertEqual('12000',result[0]['price'])
        self.assertEqual('وضحي العرض بلطف',result[0]['_staff_guidance'][0]['guidance'])
        self.db.rollback()

    def test_address_building_number_is_not_discount_request(self):
        from account_app.sales_runtime.negotiation import request
        self.assertIsNone(request('احجزيلي بغداد حي المنصور بيت ب12'))

    def test_unitless_legacy_range_uses_matching_explicit_catalog_weight(self):
        self.products[0].update(sizes='من 60 الى 120',description='تناسب أوزان من 60 إلى 120 كيلو')
        catalog=self.loader('khuyoot')
        self.assertEqual('weight',catalog.get('P1')['selection_mode'])
        from account_app.sales_runtime.facts import resolve
        self.assertIn('120',resolve(catalog,'P1','sizes',{})['value'])
        _,out=self.run_turn('وزني120',changes=[dict(values=dict(product_id='P1',weight_kg=120))])
        self.assertEqual('continue',out['action'])
        self.assertEqual('120',self.store.snapshot(self.scope)['data']['cart'][0]['size'])
        self.products[0]['description']='هذا موديل مريح'
        self.assertNotEqual('weight',self.loader('khuyoot').get('P1')['selection_mode'])

    def test_stale_catalog_choice_routes_to_service_without_model_or_order(self):
        self.select();self.products[0]['status']='inactive'
        with self.assertRaisesRegex(ServiceRequired,'cart_catalog_changed'):
            self.run_turn('هلا')

    def test_requested_written_discount_is_applied_to_multi_item_invoice(self):
        self.run_turn('اريد قطعتين ماروني44',changes=[dict(values=dict(product_id='P1',color='ماروني',size='44',quantity=2))])
        _,out=self.run_turn('نزلي ألفين والتوصيل لبغداد')
        self.assertEqual(2000,out['quote']['discount'])
        self.assertEqual(27000,out['quote']['total'])
        self.assertFalse(out['readiness']['conditions'])

    def test_discount_on_mixed_prices_never_makes_cheap_item_negative(self):
        self.products[1]['price']='100'
        _,out=self.run_turn('اريد قطعتين',changes=[dict(values=dict(product_id='P1',color='ماروني',size='44')),
            dict(new_line=True,values=dict(product_id='B1',color='أسود'))])
        _,out=self.run_turn('خصم 2000 والتوصيل لبغداد')
        self.assertEqual(2000,out['quote']['discount'])
        self.assertTrue(all(i['unit_price']>=0 for i in out['quote']['items']))
        self.assertEqual(10100,sum(i['unit_price']*i['quantity'] for i in out['quote']['items']))

    def reply(self, event, outcome, writer, policy=None):
        Responses(self.store, self.loader, writer, lambda store: policy or {}).prepare(self.scope, event, outcome)
        replies = []
        while part := self.store.claim_reply(self.scope):
            replies.append(part['payload'])
            self.store.settle_reply(self.scope, part['id'], 'accepted', str(part['id']))
        return replies

    def test_contacts_and_acknowledgment_cannot_fall_back_to_old_color_list(self):
        self.run_turn('اريد الدشداشة القديفة', changes=[dict(values=dict(product_id='P1'))])
        drafts = ['وصل العنوان عيني، شنو رقم الموبايل؟', 'وصل الرقم حبيبتي، يا لون تحبين؟', 'من عيوني.']
        writer = Mock(side_effect=[[dict(type='text',text=text)] for text in drafts])
        stale = dict(facts=[dict(product_id='P1',field='colors')])
        event,out = self.run_turn('بغداد حي المنصور قرب المدرسة', answer=stale,
                                  contact={'address':'بغداد حي المنصور قرب المدرسة'})
        reply = self.reply(event,out,writer)
        self.assertEqual(drafts[0],reply[0]['text'])
        self.assertIn('address',writer.call_args.args[0]['progress']['received'])
        self.assertEqual('contact.phone',reply[-1]['question_field'])
        event,out = self.run_turn('07701234567',answer=stale,contact={'phone':'07701234567'})
        reply = self.reply(event,out,writer)
        self.assertEqual(drafts[1],reply[0]['text'])
        self.assertEqual(['phone'],writer.call_args.args[0]['progress']['received'])
        self.assertTrue(reply[-1]['question_field'].endswith('.color'))
        event,out = self.run_turn('تمام',answer=stale)
        reply = self.reply(event,out,writer)
        self.assertEqual([{'type':'text','text':drafts[2]}],reply)
        self.assertTrue(writer.call_args.args[0]['progress']['acknowledgment'])
        self.assertTrue(writer.call_args.args[0]['action_results']['omit_color_list'])
        self.assertEqual([],writer.call_args.args[0]['facts'])

    def test_checkout_restatement_uses_the_two_selected_lines_and_missing_contacts(self):
        self.products[0]['colors'] = 'أسود، رصاصي'
        drafts = ['على اختيارج، اسود قياس 44 ورصاصي قياس 44.',
                  'إي عيوني، الرصاصي قياس 44 مثل الأسود.',
                  'صار حبيبتي، اسود قياس 44 ورصاصي قياس 44. شنو الرقم والعنوان حتى أثبتلج الحجز؟']
        writer = Mock(side_effect=[[dict(type='text',text=text)] for text in drafts])
        event,out = self.run_turn('اريد اسود ورصاصي قياس 44',changes=[
            dict(values=dict(product_id='P1',color='اسود',size='44')),
            dict(new_line=True,values=dict(product_id='P1',color='رصاصي',size='44'))])
        self.reply(event,out,writer)
        event,out = self.run_turn('حسب قياسي',answer=dict(facts=[dict(product_id='P1',field='sizes')]))
        self.assertIn('رصاصي قياس 44',self.reply(event,out,writer)[0]['text'])
        event,out = self.run_turn('ثبتيلي اسود ورصاصي قياس 44',
                                  answer=dict(facts=[dict(product_id='P1',field='sizes')]))
        reply = self.reply(event,out,writer)
        self.assertIn('اسود قياس 44',reply[0]['text'])
        self.assertIn('رصاصي قياس 44',reply[0]['text'])
        context = writer.call_args.args[0]
        self.assertTrue(context['progress']['recap_selection'])
        self.assertEqual([('اسود','44'),('رصاصي','44')],
                         [(row['color'],row['size']) for row in context['state']['cart']])
        self.assertEqual('contact.details',reply[-1]['question_field'])
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_verified_weight_advances_to_color_once_then_purchase_and_contact(self):
        self.products[0]['sizes'] = 'من وزن 60 إلى 130 كيلو'
        drafts = ['إي حبيبتي، 130 كيلو ضمن المجال المكتوب. يا لون عجبج؟',
                  'إي عيني، 130 كيلو الحد الأعلى لهالموديل.',
                  'الماروني على اختيارج، تحبين أثبتلج؟',
                  'شنو الرقم والعنوان للحجز؟']
        writer = Mock(side_effect=[[dict(type='text',text=text)] for text in drafts])
        for index in range(2):
            event,out = self.run_turn('يلبس وزن 130',changes=[dict(values=dict(product_id='P1',weight_kg=130))])
            reply = self.reply(event,out,writer)
            self.assertIn('130 كيلو',reply[0]['text'])
            self.assertNotIn('أزرق',reply[0]['text'])
            self.assertEqual(1,len(reply))
            self.assertEqual(index == 0,'question_field' in reply[0])
            self.assertEqual(dict(weight_kg=130,within_catalog_range=True),
                             writer.call_args.args[0]['progress']['fit'])
        event,out = self.run_turn('ماروني',changes=[dict(values=dict(product_id='P1',color='ماروني'))])
        self.assertEqual('purchase_intent',self.reply(event,out,writer)[-1]['question_field'])
        self.plan = dict(intent='withdraw')
        event = self.event('يلا تمام')
        out = self.runtime.turn(self.scope,event)
        self.assertEqual('contact.details',self.reply(event,out,writer)[-1]['question_field'])

    def test_counteroffer_explains_subtotal_delivery_discount_and_unchanged_total(self):
        self.products[0]['price'] = '17000'
        self.run_turn('اريد قطعتين ماروني 44',changes=[dict(values=dict(product_id='P1',color='ماروني',size='44',quantity=2))])
        drafts = ['أفهمج، حسابهن {{subtotal}} وننزل {{discount}} فيصير {{discounted_subtotal}} قبل التوصيل. لأي محافظة؟',
                  'أقدّر طلبج حبيبتي.',
                  'إي عيوني، الحساب على القطعتين. التوصيل لأي محافظة؟',
                  'القطعتين {{subtotal}} والتوصيل {{delivery_fee}}، ننزل {{discount}} ويصير كلهن {{total}}. يناسبج الحساب؟']
        writer = Mock(side_effect=[[dict(type='text',text=text)] for text in drafts])
        event,out = self.run_turn('سويهن ب30')
        self.assertEqual(32000,out['quote']['product_total'])
        self.assertEqual('quote.province',self.reply(event,out,writer)[-1]['question_field'])
        self.plan = dict(intent='withdraw')
        event = self.event('لا فدوة')
        out = self.runtime.turn(self.scope,event)
        self.assertEqual(32000,out['quote']['product_total'])
        objection = self.reply(event,out,writer)[0]['text']
        event,out = self.run_turn('غير طلبت 2')
        reply = self.reply(event,out,writer)[0]
        self.assertNotEqual(objection,reply['text'])
        self.assertIn('الحساب على القطعتين',reply['text'])
        self.assertEqual('quote.province',reply['question_field'])
        self.run_turn('التوصيل لبغداد')
        # A delivery answer can precede the next revised amount in the same negotiation.
        event,out = self.run_turn('بس احسبهن ب36 مع التوصيل')
        reply = self.reply(event,out,writer)[0]
        for amount in ('34 ألف','5 آلاف','ألفين','37 ألف'):
            self.assertIn(amount,reply['text'])
        self.assertEqual(37000,reply['quote_total'])
        self.assertEqual('price.accept',reply['question_field'])

    def test_unit_offer_and_generic_discount_are_recognized_only_on_eligible_cart(self):
        self.products[0]['price'] = '16000'
        self.run_turn('اريد قطعتين ماروني 44',changes=[dict(values=dict(product_id='P1',color='ماروني',size='44',quantity=2))])
        _,out = self.run_turn('بيها مجال تخفضولي السعر اذا اخذ اكثر من وحده')
        self.assertEqual(30000,out['quote']['product_total'])
        _,out = self.run_turn('سويهن الوحده 15 بالعدله')
        self.assertEqual(30000,out['quote']['product_total'])
        self.assertTrue(out['quote']['request_satisfied'])

    def test_free_size_keeps_requested_large_through_checkout(self):
        self.products[0]['sizes'] = 'فري سايز من 55 كيلو الى 130 كيلو'
        _, out = self.run_turn('اريد قياس لارج',changes=[dict(values=dict(product_id='P1',size='L',color='ماروني'))])
        self.assertIsNone(out['business_feedback'])
        self.assertEqual('L',self.store.snapshot(self.scope)['data']['cart'][0]['size'])
        self.run_turn('07701234567',contact={'phone':'07701234567'})
        _, out = self.run_turn('بغداد حي المنصور قرب المدرسة',contact={'address':'بغداد حي المنصور قرب المدرسة'})
        self.assertEqual('booked',out['action'])
        size,items = self.db.execute('SELECT size,order_items FROM orders').fetchone()
        self.assertEqual('L',size)
        self.assertEqual('L',json.loads(items)[0]['size'])

    def test_free_size_explanation_is_not_repeated_after_selection(self):
        self.products[0]['sizes'] = 'فري سايز من 55 كيلو الى 130 كيلو'
        event,out=self.run_turn('اريد قياس لارج',changes=[dict(values=dict(product_id='P1',size='L'))])
        first=self.reply(event,out,Mock(return_value=[dict(type='text',text='هو فري سايز عيوني. يا لون عجبج؟')]))
        self.assertEqual('P1',first[0]['size_explained_for'])
        event,out=self.run_turn('اريد ماروني',changes=[dict(values=dict(product_id='P1',color='ماروني'))])
        writer=Mock(return_value=[dict(type='text',text='صار الماروني لارج. شنو رقم الموبايل والعنوان؟')])
        self.reply(event,out,writer)
        proof=writer.call_args.args[0]['action_results']
        self.assertTrue(proof['omit_free_size_explanation'])
        with self.assertRaisesRegex(ValueError,'already explained'):
            validate_copy([dict(type='text',text='هو فري سايز')],None,proof)

    def test_free_size_does_not_add_an_unapproved_label(self):
        self.products[0]['sizes'] = 'فري سايز من 55 كيلو الى 130 كيلو'
        _, out = self.run_turn('اريد قياس XL',changes=[dict(values=dict(product_id='P1',size='XL',color='ماروني'))])
        self.assertEqual('size',out['business_feedback']['field'])
        self.assertEqual('',self.store.snapshot(self.scope)['data']['cart'][0]['size'])
        _, out = self.run_turn('اي اريد فري سايز',changes=[dict(values=dict(product_id='P1',size='فري سايز'))])
        self.assertFalse(out['readiness']['conditions'])
        self.assertEqual('فري سايز',self.store.snapshot(self.scope)['data']['cart'][0]['size'])

    def test_copy_does_not_repeat_choices_without_question_marks(self):
        for text in ('بس تحددين اللون حبيبتي','قوليلي قياسج','شكد عمر الطفل','اذا عندج نونو ثاني تحبين قياس أصغر؟'):
            with self.subTest(text=text),self.assertRaises(ValueError):
                validate_copy([dict(type='text',text=text)],'alternative.P1' if 'نونو' in text else None)

    def test_fit_and_shipping_claims_need_their_own_proof(self):
        for text in ('طلبج طلع','طلبج انشحن','قياس سنة كلش مناسب لعمر 11 شهر ويجي مضبوط عليها'):
            with self.subTest(text=text),self.assertRaises(ValueError):
                validate_copy([dict(type='text',text=text)],None,dict(order_saved=True))


if __name__=='__main__':unittest.main()
