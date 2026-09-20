"""Conversation regressions derived from the 2026-09-08 16:42 backup.

Model/network outputs are fixtures; routing, memory, collection, and storage are real.
"""
import copy
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
import account_app.app as m
REAL_MAIN_AI_ONCE = m._call_main_ai_once

CATALOG = [
    dict(product_id="F1", product_name="فستان دانتيل", category="فستان", colors="اسود", sizes="38 إلى 52", price="16000", fabric="لينن", stock="متوفر", status="active", image_url="https://images.test/dress1.jpg"),
    dict(product_id="F2", product_name="فستان انيقة", category="فستان", colors="جوزي", sizes="38 إلى 52", price="15000", fabric="باربي", stock="متوفر", status="active", image_url="https://images.test/dress2.jpg"),
]

class ConversationContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.old_db = m.DB_PATH
        m.DB_PATH = str(Path(cls.temp.name)/"context.db")
        m.init_db()
    @classmethod
    def tearDownClass(cls):
        m.DB_PATH = cls.old_db
        cls.temp.cleanup()
    def setUp(self):
        self.ctx = m.app.app_context(); self.ctx.push()
        self.token = m._current_store_id.set("al-fatena")
        self.db = m.get_db()
        self.sender = "al-fatena::context-" + uuid.uuid4().hex
        m.get_or_create_customer(self.db, self.sender, "page", "facebook")
        self.settings = dict(enabled=True, product_id="F1", product=CATALOG[0], send_image=False)
        for target, value in [("load_products_from_file", CATALOG), ("load_active_products", CATALOG), ("get_auto_product_settings", self.settings), ("is_ai_enabled", True), ("is_store_feature_enabled", False), ("send_telegram_message", False), ("send_webhook_result_to_facebook", True)]:
            patcher = patch.object(m, target, return_value=value); patcher.start(); self.addCleanup(patcher.stop)
        def model_fixture(ev, message_type, customer, history, products, matched, *args, **kwargs):
            # Deterministic provider fixture; validates routing, not language quality.
            if m.requests_alternative_photo(ev.get('text')):
                return {'reply': 'ما عندي تصوير إضافي', '_suppress_product_images': True}
            if matched:
                return {'reply': f"{matched['product_name']} قماشه {matched.get('fabric', '')} وسعره {int(matched['price']):,}", 'create_order': False, 'order': {}}
            return {'reply': 'أرسلي اسم الموديل', 'create_order': False, 'order': {}}
        patcher = patch.object(m, '_call_main_ai_once', side_effect=model_fixture)
        self.model_fixture = patcher.start(); self.addCleanup(patcher.stop)
        self.ev = dict(sender_id=self.sender, store_id="al-fatena", page_id="page", platform="facebook", text="", image_url=None, attachments=[], ref=None, ad_id=None, postback=None, quick_reply=None, timestamp=0, referral_source=None, referral_type=None)
    def tearDown(self):
        m._current_store_id.reset(self.token); self.ctx.pop()
    def event(self, text="", image=None, model_reply=None):
        ev = dict(self.ev, text=text, image_url=image)
        with patch.object(m, "extract_facebook_event", return_value=ev):
            if model_reply is not None:
                with patch.object(m, "_call_main_ai_once", return_value={"reply": model_reply, "order": {}, "create_order": False}):
                    return m.process_webhook(self.db, {}, use_debounce=False)
            return m.process_webhook(self.db, {}, use_debounce=False)
    def test_opening_duplicate_name_uses_existing_manual_binding(self):
        catalog = [dict(p, product_name='فستان') for p in CATALOG]
        with patch.object(m,'load_products_from_file',return_value=catalog), patch.object(m,'load_active_products',return_value=catalog):
            m.complete_customer_product_link(self.db,self.sender,catalog[1],'manual',source='manual_admin')
            result=self.event('ما هي مواصفات الفستان؟')
            self.assertNotIn('first_message_name_ambiguous',result.get('meta',{}))
            self.assertIn('15,000',result['reply'])
            self.assertFalse(m.reply_reasks_known_product(result['reply']))
            self.assertEqual(m.get_active_product_binding(self.db,self.sender)['product_id'],'F2')

    def test_opening_duplicate_name_uses_unique_ad_binding(self):
        catalog = [dict(p, product_name='فستان',ad_id=str(i+100)) for i,p in enumerate(CATALOG)]
        self.ev['ad_id']='101'
        with patch.object(m,'load_products_from_file',return_value=catalog), patch.object(m,'load_active_products',return_value=catalog):
            result=self.event('ما هي مواصفات الفستان؟')
            self.assertNotIn('first_message_name_ambiguous',result.get('meta',{}))
            self.assertIn('15,000',result['reply'])

    def test_fresh_ad_takes_precedence_over_previous_binding(self):
        catalog = [dict(p,product_name='فستان',ad_id=str(i+100)) for i,p in enumerate(CATALOG)]
        self.ev['ad_id']='101'
        with patch.object(m,'load_products_from_file',return_value=catalog), patch.object(m,'load_active_products',return_value=catalog):
            m.complete_customer_product_link(self.db,self.sender,catalog[0],'manual',source='manual_admin')
            result=self.event('ما هي مواصفات الفستان؟')
            self.assertIn('15,000',result['reply'])
            self.assertEqual(m.get_active_product_binding(self.db,self.sender)['product_id'],'F2')

    def test_ambiguous_opening_without_binding_still_asks(self):
        catalog = [dict(p,product_name='فستان') for p in CATALOG]
        with patch.object(m,'load_products_from_file',return_value=catalog), patch.object(m,'load_active_products',return_value=catalog):
            result=self.event('ما هي مواصفات الفستان؟')
            self.assertTrue(result['meta']['first_message_name_ambiguous'])
            self.assertIsNone(m.get_active_product_binding(self.db,self.sender))

    def test_explicit_other_product_overrides_opening_binding(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],'manual',source='manual_admin')
        result=self.event('اريد فستان انيقة')
        self.assertIn('15,000',result['reply'])
        self.assertEqual(m.get_active_product_binding(self.db,self.sender)['product_id'],'F2')

    def test_new_reask_variants_are_retried_with_known_context(self):
        ev=dict(self.ev,text='شكد السعر')
        for wrong in ['دزلي صورته حتى اعرف السعر','ارسلي صورتها','حددي الموديل حتى اجاوبج','يا موديل تقصدين؟']:
            with self.subTest(wrong=wrong), patch.object(m,'_call_main_ai_once',side_effect=[{'reply':wrong},{'reply':'سعره 16 ألف','create_order':False}]) as model:
                result=m.call_main_ai(ev,'text',{},[],CATALOG,CATALOG[0],None,'',[],customer_products=[CATALOG[0]])
                self.assertEqual(model.call_count,2)
                self.assertEqual(result['reply'],'سعره 16 ألف')
        for okay in ['شنو القياس المطلوب؟','دزلي رقم الهاتف','صورته موجودة بالمحادثة','دزلي العنوان حتى نكمل']:
            self.assertFalse(m.reply_reasks_known_product(okay))

    def test_missing_matched_record_recovers_only_valid_customer_selection(self):
        with patch.object(m,'_call_main_ai_once',return_value={'reply':'سعره 16 ألف'}) as model:
            m.call_main_ai(dict(self.ev,text='شكد السعر'),'text',{},[],CATALOG,None,None,'',[],customer_products=[CATALOG[0]])
            self.assertEqual(model.call_args.args[5]['product_id'],'F1')
        with patch.object(m,'_call_main_ai_once',return_value={'reply':'سعره 15 ألف'}) as model:
            m.call_main_ai(dict(self.ev,text='فستان انيقة'),'text',{},[],CATALOG,None,None,'',[],customer_products=[CATALOG[0]])
            self.assertEqual(model.call_args.args[5]['product_id'],'F2')

    def test_audit_product_context_survives_long_history_and_wording(self):
        skirt = dict(CATALOG[0], product_id='SK', product_name='تنورة', category='تنورة', price='12000')
        catalog = [skirt, dict(CATALOG[1], product_id='SU', product_name='سوت نيلي', category='سوت')]
        with patch.object(m, 'load_products_from_file', return_value=catalog), patch.object(m, 'load_active_products', return_value=catalog), patch('requests.sessions.Session.request', side_effect=AssertionError('Unexpected network')):
            m.complete_customer_product_link(self.db, self.sender, skirt, 'manual', source='manual_admin')
            for i in range(120):
                m.save_message(self.db, self.sender, 'incoming' if i % 2 == 0 else 'outgoing', 'text', 'متابعة سابقة '+str(i), None, None, None, {})
            phrases = ['ست نفس الي بالصوره يوصل',
                       'قطعه وحده بس اذا ماعجبتني ترجع بيد المندوب بدون ما ادفع شيء',
                       'قماشها شنو\nاكو تصوير حقيقي\nللقطعة',
                       'لعد شنو كاتبه نحدد لون تنوره', 'اي التنوره\nدريت صورتها',
                       'بس كوليلي شنو نوع القماش', 'واذا مو نفس الصوره ارجعهه وكروه ماادفع']
            for phrase in phrases:
                with self.subTest(phrase=phrase), patch.object(m, 'classify_contextual_selection') as selection, patch.object(m, '_call_main_ai_once', return_value={'reply':'التنورة قماشها باربي، والفحص عند الاستلام.', 'order':{}, 'create_order':False}) as model:
                    response = self.event(phrase)
                    self.assertEqual([p['product_id'] for p in m.load_customer_products(self.db,self.sender)],['SK'])
                    self.assertNotIn('مو واضح',response['reply'])
                    self.assertNotIn('ألغيت',response['reply'])
                    selection.assert_not_called()
                    self.assertEqual(model.call_args.args[5]['product_id'],'SK')
            self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],0)

    def test_context_spelling_and_generic_words_do_not_mask_new_category(self):
        skirt = dict(CATALOG[0], product_id='SK', product_name='تنورة', category='تنورة')
        suit = dict(CATALOG[1], product_id='SU', product_name='سوت نيلي', category='سوت')
        for phrase in ['تنوره', 'التنورة', 'شنو قماش القطعة', 'ست نفس الصوره؟']:
            self.assertEqual(m.select_customer_context_product(phrase,[skirt])['product_id'],'SK')
        for phrase in ['اريد سوت', 'اريد ست', 'السوت شكد سعره']:
            self.assertIsNone(m.select_customer_context_product(phrase,[skirt]))
        self.assertIsNone(m.select_customer_context_product('شكد سعر القطعة',[skirt,suit]))
        self.assertEqual(m.select_customer_context_product('شنو قماش التنوره',[skirt,suit])['product_id'],'SK')
        duplicate=dict(skirt, product_id='SK2', product_name='تنوره')
        self.assertIsNone(m.select_customer_context_product('تنوره',[skirt,duplicate]))

    def test_context_questions_do_not_revive_old_suggestions(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],'manual',source='manual_admin')
        m.remember_product_search_results(self.db,self.sender,[{'product':CATALOG[1], 'score':70}],{})
        with patch.object(m,'classify_contextual_selection',return_value=None) as classifier:
            self.assertIsNone(m.resolve_contextual_product_selection(self.db,dict(self.ev,text='بس كوليلي شنو نوع القماش'),CATALOG))
            self.assertIsNone(m.resolve_contextual_product_selection(self.db,dict(self.ev,text='لا حبي تسلمين'),CATALOG))
            classifier.assert_not_called()
            m.resolve_contextual_product_selection(self.db,dict(self.ev,text='هذا مو ذوقي'),CATALOG)
            self.assertEqual([p['product_id'] for p in classifier.call_args.args[2]],['F1'])
        self.assertEqual([p['product_id'] for p in m.load_customer_products(self.db,self.sender)],['F1'])

    def test_existing_conversation_can_switch_to_exact_category_name(self):
        skirt = dict(CATALOG[0], product_id='SK', product_name='تنورة', category='تنورة', price='12000')
        catalog = CATALOG + [skirt]
        with patch.object(m,'load_products_from_file',return_value=catalog), patch.object(m,'load_active_products',return_value=catalog):
            m.complete_customer_product_link(self.db,self.sender,CATALOG[0],'manual',source='manual_admin')
            with patch.object(m,'classify_contextual_selection',return_value=None), patch.object(m,'_call_main_ai_once',return_value={'reply':'التنورة متوفرة بسعر 12000','order':{},'create_order':False}) as model:
                self.event('اريد التنوره')
                self.assertEqual(model.call_args.args[5]['product_id'],'SK')
            self.assertEqual([p['product_id'] for p in m.load_customer_products(self.db,self.sender)],['SK'])
            self.assertIsNone(m.customer_product_target('اريد التنوره',[skirt,dict(skirt,product_id='SK2')]))
            self.assertIsNone(m.customer_product_target('اريد التنوره',[dict(skirt,stock='نفذ')]))

    def test_attached_return_condition_is_not_cancellation(self):
        for phrase in ['واذا مو نفس الصوره ارجعهه وكروه ماادفع', 'وإذا ماعجبني ارجعه', 'اذا ما صار ارجعه']:
            self.assertTrue(m.is_conditional_return_question(phrase))
            self.assertFalse(m.is_product_objection(phrase))
        for phrase in ['اريد الغي الطلب', 'وصلني غير موديل اريد ارجعه']:
            self.assertFalse(m.is_conditional_return_question(phrase))

    def test_demonstrative_keeps_known_product(self):
        m.complete_customer_product_link(self.db, self.sender, CATALOG[0], "manual")
        m.save_message(self.db,self.sender,"incoming","text","سلام عليكم",None,None,None,{})
        product, _, result = m.match_product(self.db, dict(self.ev, text="هو هذا الموديل"), CATALOG)
        self.assertEqual((product or {}).get("product_id"), "F1")
        self.assertFalse((result or {}).get("waiting_for_image"))

    def test_contextual_rejection_keeps_other_product_and_answers_next_question(self):
        for phrase, question in zip(['لا فستان دانتيل ماريده', 'دانتيل ماعجبني', 'هذا مو ذوقي'],
                                    ['شكد سعره؟', 'شنو السعر؟', 'كم السعر؟']):
            with self.subTest(phrase=phrase):
                self.recognize()
                decision = {'action': 'change', 'reject_ids': ['F1'], 'select_ids': [], 'focus_id': 'F2'}
                with patch.object(m, 'classify_contextual_selection', return_value=decision) as classify:
                    result = self.event(phrase, model_reply='تمام، نخلي فستان انيقة. شنو القياس المطلوب؟')
                self.assertIn('انيقة', result['reply'])
                self.assertNotIn('حدديه', result['reply'])
                self.assertEqual([p['product_id'] for p in m.load_customer_products(self.db, self.sender)], ['F2'])
                self.assertIsNone(m.pending_product_choice(self.db, self.sender))
                self.assertTrue(classify.call_args.args[3])
                self.assertIn('15,000', self.event(question)['reply'])
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)

    def test_contextual_reverse_selection_restores_only_explicit_choice(self):
        self.recognize()
        decision = {'action': 'change', 'reject_ids': ['F2'], 'select_ids': ['F1'], 'focus_id': 'F1'}
        with patch.object(m, 'classify_contextual_selection', return_value=decision):
            result = self.event('أريد دانتيل مو انيقة', model_reply='تمام، نكمل على دانتيل. شنو القياس المطلوب؟')
        self.assertIn('دانتيل', result['reply'])
        self.assertEqual([p['product_id'] for p in m.load_customer_products(self.db, self.sender)], ['F1'])
        self.assertIn('16,000', self.event('شكد سعره؟')['reply'])

    def test_ambiguous_or_invalid_selection_keeps_context(self):
        self.recognize()
        before = [dict(r) for r in self.db.execute('SELECT * FROM customer_product_interests WHERE sender_id=?', (self.sender,))]
        for attempt, decision in enumerate([None, {'action': 'clarify'},
                         {'action': 'change', 'reject_ids': ['UNKNOWN'], 'select_ids': [], 'focus_id': ''},
                         {'action': 'change', 'reject_ids': ['F2'], 'select_ids': ['F2'], 'focus_id': 'F2'}]):
            with patch.object(m, 'classify_contextual_selection', return_value=decision):
                result = m.resolve_product_choice(self.db, dict(self.ev, text='ماعجبني'), CATALOG)
            if attempt == 0:
                self.assertIn('دانتيل', result['reply'])
                self.assertIn('انيقة', result['reply'])
            else:
                self.assertEqual(result['reply'], '')
                self.assertTrue(result['meta']['needs_human'])
            self.assertNotIn('صورة', result['reply'])
            after = [dict(r) for r in self.db.execute('SELECT * FROM customer_product_interests WHERE sender_id=?', (self.sender,))]
            self.assertEqual(before, after)
            self.assertIsNotNone(m.pending_product_choice(self.db, self.sender))

    def test_negated_dislike_does_not_remove_a_product(self):
        self.recognize()
        with patch.object(m, 'classify_contextual_selection', return_value={'action': 'none'}):
            m.resolve_contextual_product_selection(self.db, dict(self.ev, text='مو ماعجبني بس أسأل عن السعر'), CATALOG)
        self.assertEqual([p['product_id'] for p in m.load_customer_products(self.db, self.sender)], ['F2'])

    def test_selection_model_receives_context_and_only_known_ids(self):
        with patch.object(m, 'OPENROUTER_KEY', 'test'), patch.object(m.requests, 'post') as post:
            post.return_value.json.return_value = {'choices': [{'message': {'content': '{"action":"change","reject_ids":["F1"],"select_ids":[],"focus_id":"F2"}'}}]}
            result = m.classify_contextual_selection(self.db, 'ماريده', [{'product_id': 'F1'}, {'product_id': 'F2'}],
                [{'direction': 'outgoing', 'text': 'تريدين دانتيل ويا انيقة؟'}], None)
        self.assertEqual(result['focus_id'], 'F2')
        content = post.call_args.kwargs['json']['messages'][-1]['content']
        self.assertIn('تريدين دانتيل ويا انيقة؟', content)
        self.assertIn('ماريده', content)
    def test_default_applies_to_product_question_not_only_greeting(self):
        with patch.object(m,"generate_first_message_reply",return_value=("أرسلي صورة", "")) as first:
            result = self.event("ما هي مواصفات الفستان؟", model_reply="الفستان دانتيل بسعر 16000")
        first.assert_not_called()
        self.assertIn("دانتيل", result["reply"])

    def test_opening_ad_questions_bind_exact_skirt_name_before_default(self):
        skirt = dict(CATALOG[0], product_id='SK1', product_name='تنورة', category='تنورة', price='19000')
        catalog = CATALOG + [skirt]
        for question in ['شنو قياسات التنورة المتوفرة؟', '1. شكد سعر التنورة', 'شلون أطلب التنورة وكم التوصيل؟', 'شكد سعر التنوره؟']:
            with self.subTest(question=question):
                self.sender = 'al-fatena::ad-name-' + uuid.uuid4().hex
                self.ev['sender_id'] = self.sender
                m.get_or_create_customer(self.db, self.sender, 'page', 'facebook')
                with patch.object(m, 'load_products_from_file', return_value=catalog), patch.object(m, 'load_active_products', return_value=catalog), patch.object(m, 'generate_first_message_reply') as greeting, patch.object(m, '_call_main_ai_once', return_value={'reply':'التنورة متوفرة، شنو القياس المطلوب؟', 'order':{}, 'create_order':False}) as model:
                    result = self.event(question)
                    self.assertEqual([p['product_id'] for p in m.load_customer_products(self.db, self.sender)], ['SK1'])
                greeting.assert_not_called()
                self.assertEqual(model.call_args.args[5]['product_id'], 'SK1')
                self.assertIn('التنورة', result['reply'])
                self.assertNotIn('صورة', result['reply'])
                self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],0)

    def test_duplicate_opening_names_ask_without_default_binding(self):
        catalog = [dict(CATALOG[0], product_id='SK1', product_name='تنورة'), dict(CATALOG[1], product_id='SK2', product_name='تنوره')]
        with patch.object(m, 'load_products_from_file', return_value=catalog), patch.object(m, 'load_active_products', return_value=catalog), patch.object(m, '_call_main_ai_once') as model:
            result = self.event('شكد سعر التنورة؟')
            self.assertEqual(m.load_customer_products(self.db, self.sender), [])
        model.assert_not_called()
        self.assertTrue(result['meta']['first_message_name_ambiguous'])

    def test_opening_name_normalization_scope_negation_and_full_names(self):
        skirt = dict(CATALOG[0], product_id='SK1', product_name='تنورة', store_id='al-fatena')
        pleated = dict(skirt, product_id='SK2', product_name='تنورة بليسي')
        for question in ['شنو قياسات التَّنُّورَة؟', 'كم التوصيل للتنوره؟']:
            self.assertEqual(m.first_message_named_products(question, [skirt]), [skirt])
        self.assertEqual(m.first_message_named_products('شكد سعر التنورة البليسي؟', [skirt, pleated]), [pleated])
        self.assertEqual(m.first_message_named_products('شكد سعر التنورة؟', [pleated]), [])
        self.assertEqual(m.first_message_named_products('شنو سعر التنورة؟', [dict(skirt,store_id='khuyoot')]), [])
        for question in ['التنورة ماريدها', 'لا اريد التنورة', 'مو التنورة', 'اريد غير التنورة', 'المتنورة', 'ما عجبني التنورة']:
            self.assertEqual(m.first_message_named_products(question, [skirt]), [])
        self.assertEqual(m.first_message_named_products('التنورة وفستان دانتيل متوفرات؟', [skirt, CATALOG[0]]), [skirt, CATALOG[0]])
        self.assertEqual(m.first_message_named_products('شكد سعر التنورة؟', [dict(skirt,stock='نفذ')])[0]['stock'], 'نفذ')

    def test_opening_availability_and_delivery_questions_are_not_rejections(self):
        product = dict(CATALOG[0], product_id='BAG', product_name='حقيبة جلد')
        for text in ['الحقيبة الجلد متوفرة لو لا؟', 'الحقيبة الجلد موجودة أم لا؟', 'كم سعر الحقيبة الجلد بدون التوصيل؟', 'شكد سعر حقيبة جلد من غير أجور الشحن؟']:
            self.assertEqual(m.first_message_named_products(text, [product]), [product])
        for text in ['لا اريد الحقيبة الجلد بدون توصيل', 'أريد الطلب بدون حقيبة جلد', 'حقيبة جلد ما عجبني']:
            self.assertEqual(m.first_message_named_products(text, [product]), [])

    def test_opening_greeting_burst_still_binds_ad_name(self):
        product = dict(CATALOG[0], product_id='BAG', product_name='حقيبة جلد')
        catalog=CATALOG+[product]
        m.save_message(self.db,self.sender,'incoming','text','السلام عليكم',None,None,None,{})
        with patch.object(m,'load_products_from_file',return_value=catalog), patch.object(m,'load_active_products',return_value=catalog), patch.object(m,'_call_main_ai_once',return_value={'reply':'الحقيبة متوفرة، شنو اللون المطلوب؟','order':{},'create_order':False}) as model:
            self.event('شنو قياسات الحقيبة الجلد المتوفرة؟')
            self.assertEqual([p['product_id'] for p in m.load_customer_products(self.db,self.sender)],['BAG'])
        self.assertEqual(model.call_args.args[5]['product_id'],'BAG')
    def test_specs_use_conversation_instead_of_dumping_internal_size_notes(self):
        product = dict(CATALOG[0], sizes="قياسات هذا المنتج من 38 إلى 52. اعتمد الجدول التالي عند الرد على الزبونة: وزن 55–60 كيلو = قياس 38")
        self.settings["product"] = product
        expected = "الفستان دانتيل أسود وقماشه لينن، سعره 16,000 د.ع. شنو القياس اللي تلبسينه؟"
        with patch.object(m, "_call_main_ai_once", return_value={"reply": expected, "order": {}, "create_order": False}) as model:
            result = self.event("ما هي مواصفات الفستان؟")
        model.assert_called_once()
        self.assertEqual(result["reply"], expected)
        self.assertNotIn("اعتمد الجدول", result["reply"])
        self.assertIsNone(m.known_product_question_reply("شنو القياسات؟", product))
        self.assertIsNone(m.known_product_question_reply("ما هي مواصفات الفستان؟", product))

    def test_photo_followed_by_text_in_same_burst_is_matched(self):
        m.save_message(self.db,self.sender,"incoming","image","","https://images.test/customer.jpg",None,None,{})
        self.settings["enabled"] = False
        with patch.object(m,"match_customer_image_with_catalog",return_value={"product_found":True,"product_id":"F2","confidence":100}) as vision:
            self.event("هذة الون والقياس 44",model_reply="قياس 44 متوفر")
        vision.assert_called_once()
        self.assertEqual(m.load_customer_products(self.db,self.sender)[0]["product_id"],"F2")

    def recognize(self, text="", product_id="F2"):
        with patch.object(m, "match_customer_image_with_catalog", return_value={"product_found": True, "product_id": product_id, "confidence": 100}):
            result = self.event(text, image="https://images.test/" + uuid.uuid4().hex + ".jpg", model_reply="الموديل متوفر")
            if result.get("meta", {}).get("auto_reply"):
                result["reply"] = result["meta"]["auto_reply"]["reply"]
            return result

    def test_different_photo_asks_add_or_replace_without_changing_old_choice(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        result = self.recognize()
        self.assertTrue(result["meta"]["product_choice_pending"])
        self.assertEqual("الموديل متوفر",result["reply"])
        self.assertNotIn("لو",result["reply"])
        self.assertEqual([p["product_id"] for p in m.load_customer_products(self.db,self.sender)],["F2"])
        result = self.event("بس هذا")
        self.assertEqual(result["meta"]["product_choice_resolved"],"replace")
        self.assertEqual([p["product_id"] for p in m.load_customer_products(self.db,self.sender)],["F2"])
        self.assertIn("باربي",self.event("شنو القماش؟")["reply"])

    def test_adding_photo_keeps_both_products_after_next_message(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        self.recognize()
        result = self.event("ضيفيه ويا السابق")
        self.assertEqual(result["meta"]["product_choice_resolved"],"add")
        self.assertEqual({p["product_id"] for p in m.load_customer_products(self.db,self.sender)},{"F1","F2"})
        # Contextual follow-up uses the last explicitly selected model.
        self.assertIn("15,000",self.event("شكد سعره؟")["reply"])
        self.assertEqual({p["product_id"] for p in m.load_customer_products(self.db,self.sender)},{"F1","F2"})

    def test_ambiguous_yes_does_not_choose_add_or_replace(self):
        first = self.recognize()
        self.assertTrue(first["meta"].get("product_choice_pending"), first)
        result = self.event("اي", model_reply="تفضلي عيني")
        self.assertNotIn("أثبتلج", result["reply"])
        self.assertIsNotNone(m.pending_product_choice(self.db,self.sender))
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?",(self.sender,)).fetchone()[0],0)

    def test_explicit_addition_in_caption_needs_no_extra_choice(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        result = self.recognize("ضيفي هذا ويا السابق")
        self.assertFalse(result["meta"].get("product_choice_pending"))
        self.assertEqual({p["product_id"] for p in m.load_customer_products(self.db,self.sender)},{"F1","F2"})

    def test_new_image_matches_default_without_asking_choice(self):
        result = self.recognize(product_id="F1")
        self.assertFalse(result["meta"].get("product_choice_pending"))
        self.assertEqual(m.load_customer_products(self.db,self.sender)[0]["product_id"],"F1")
        self.assertIn("لينن",self.event("شنو القماش؟")["reply"])

    def test_auto_off_and_no_link_asks_for_photo(self):
        self.settings["enabled"] = False
        with patch.object(m,"generate_first_message_reply",return_value=("أرسلي صورة الموديل", "")):
            result = self.event("سلام عليكم")
        self.assertIn("صورة",result["reply"])
        self.assertEqual(m.load_customer_products(self.db,self.sender),[])

    def test_auto_off_keeps_manual_product_and_answers(self):
        self.settings["enabled"] = False
        m.complete_customer_product_link(self.db,self.sender,CATALOG[1],"manual")
        self.assertIn("باربي",self.event("شنو القماش؟")["reply"])

    def test_failed_album_preserves_recognized_and_existing_product_links(self):
        self.settings["enabled"] = False
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        ev = dict(self.ev, text="بس هذا", image_url="https://images.test/a.jpg", attachments=[{"type":"image","url":"https://images.test/a.jpg"},{"type":"image","url":"https://images.test/b.jpg"}])
        responses = [(CATALOG[1],"image_recognition",{"product_found":True,"product_id":"F2"}),(None,None,{"product_found":False}),(None,None,{"product_found":False})]
        with patch.object(m,"_match_single_product",side_effect=responses) as match:
            product, _, result = m.match_product(self.db,ev,CATALOG)
        self.assertIsNone(product)
        self.assertEqual(match.call_count,2)
        self.assertEqual({p["product_id"] for p in m.load_customer_products(self.db,self.sender)}, {"F1", "F2"})

    def test_main_model_reasking_known_product_is_retried(self):
        with patch.object(m,"_call_main_ai_once",side_effect=[{"reply":"دزيلي صورة الموديل"},{"reply":"يرجى الفحص عند الاستلام"}]) as model:
            result=m.call_main_ai(dict(self.ev,text="الفحص شلون؟"),"text",{},[],CATALOG,CATALOG[0],None,"",[])
        self.assertEqual(model.call_count,2)
        self.assertNotIn("صورة",result["reply"])

    def test_unknown_model_reply_cannot_repeat_with_known_product(self):
        with patch.object(m,"_call_main_ai_once",return_value={"reply":"دزيلي صورة الموديل"}) as model:
            result=m.call_main_ai(dict(self.ev,text="الفحص شلون؟"),"text",{},[],CATALOG,CATALOG[0],None,"",[])
        self.assertEqual(model.call_count,2)
        self.assertTrue(result["failed"])
        self.assertEqual(result["reply"],"")

    def test_collects_text_before_and_after_image_within_burst(self):
        for text,kind,image in [("أريد بس هذا","text",None),("","image","https://images.test/a.jpg"),("قياس 44","text",None)]:
            last=m.save_message(self.db,self.sender,"incoming",kind,text,image,None,None,{})
        ev=m.collect_unanswered_event(self.db,dict(self.ev,text="قياس 44"),last)
        self.assertEqual(ev["image_url"],"https://images.test/a.jpg")
        self.assertIn("أريد بس هذا",ev["text"])
        self.assertIn("قياس 44",ev["text"])
        self.assertEqual(m.detect_message_type(ev),"image")

    def test_async_worker_does_not_drop_photo_before_latest_text(self):
        self.settings["enabled"]=False
        def body(text,image=None):
            return {"object":"page","entry":[{"id":"page","_store_id":"al-fatena","messaging":[{"sender":{"id":self.sender},"message":{"mid":uuid.uuid4().hex,"text":text,"attachments":[{"type":"image","payload":{"url":image}}] if image else []}}]}]}
        photo=body("","https://images.test/async.jpg"); text=body("هذة الون والقياس 44")
        calls=[]
        def during_wait(_):
            calls.append(1)
            if len(calls)==1:
                m._process_manychat_webhook_async(text,self.sender,"facebook")
        with patch.object(m,"DEBOUNCE_DELAY",1), patch.object(m.time,"sleep",side_effect=during_wait), patch.object(m,"match_customer_image_with_catalog",return_value={"product_found":True,"product_id":"F2"}) as vision, patch.object(m,"_call_main_ai_once",return_value={"reply":"قياس 44 متوفر","create_order":False,"order":{}}), patch.object(m,"send_manychat_messages",return_value=True):
            m._process_manychat_webhook_async(photo,self.sender,"facebook")
        self.assertEqual(vision.call_count,1, {"waits":len(calls), "messages":[dict(r) for r in self.db.execute("SELECT sender_id,direction,message_type,text FROM messages WHERE sender_id LIKE ?",("%"+self.sender.split("::",1)[-1],))], "event":m.extract_facebook_event(photo)})
        self.assertTrue(vision.call_args.args[0].startswith("data:image/png;base64,"))
        self.assertEqual(m.load_customer_products(self.db,self.sender)[0]["product_id"],"F2")
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='incoming'",(self.sender,)).fetchone()[0],2)

    def test_manual_link_resolves_pending_selection(self):
        self.recognize()
        self.assertIsNotNone(m.pending_product_choice(self.db,self.sender))
        m.complete_customer_product_link(self.db,self.sender,CATALOG[1],"manual",source="manual_admin")
        self.assertIsNone(m.pending_product_choice(self.db,self.sender))
        self.assertIn("باربي",self.event("شنو القماش؟")["reply"])

    def test_text_followup_does_not_recollect_photo_resolved_by_later_manual_link(self):
        photo = m.save_message(self.db, self.sender, 'incoming', 'image', 'شنو نوع القماش',
                               'https://images.test/resolved.jpg', None, None, {})
        self.db.execute("UPDATE messages SET created_at='2026-09-16T08:00:00+03:00' WHERE id=?", (photo,))
        m.complete_customer_product_link(self.db, self.sender, CATALOG[0], 'manual', source='manual_admin')
        self.db.execute("UPDATE customer_product_interests SET last_seen_at='2026-09-16T08:01:00+03:00' WHERE sender_id=?", (self.sender,))
        latest = m.save_message(self.db, self.sender, 'incoming', 'text', 'شنو القياسات', None, None, None, {})
        ev = m.collect_unanswered_event(self.db, dict(self.ev, text='شنو القياسات'), latest)
        self.assertIsNone(ev['image_url'])
        self.assertEqual(ev['attachments'], [])
        self.assertIn('شنو نوع القماش', ev['text'])
        self.assertIn('شنو القياسات', ev['text'])

    def test_newer_or_same_time_photo_is_not_hidden_by_an_older_link(self):
        m.complete_customer_product_link(self.db, self.sender, CATALOG[0], 'manual', source='manual_admin')
        self.db.execute("UPDATE customer_product_interests SET last_seen_at='2026-09-16T08:00:00+03:00' WHERE sender_id=?", (self.sender,))
        photo = m.save_message(self.db, self.sender, 'incoming', 'image', '', 'https://images.test/newer.jpg', None, None, {})
        self.db.execute("UPDATE messages SET created_at='2026-09-16T08:00:00+03:00' WHERE id=?", (photo,))
        latest = m.save_message(self.db, self.sender, 'incoming', 'text', 'شكد سعره', None, None, None, {})
        ev = m.collect_unanswered_event(self.db, dict(self.ev, text='شكد سعره'), latest)
        self.assertEqual(ev['image_url'], 'https://images.test/newer.jpg')

    def test_image_pause_does_not_send_canned_shipping_reply(self):
        review = m.create_human_review(self.db, self.ev, 'image unresolved', notify_telegram=False)
        m.set_customer_ai_enabled(self.db, self.sender, False, reason='image_unresolved')
        with patch.object(m, '_call_main_ai_once') as model:
            result = self.event('توصيل اشكد')
        self.assertEqual(result['reply'], '')
        self.assertEqual(result['meta']['reason'], 'ai_disabled_for_conversation')
        self.assertFalse(m.is_customer_ai_enabled(self.db, self.sender))
        self.assertTrue(m.has_pending_human_review(self.db, self.sender))
        model.assert_not_called()
        result = self.event('شكد سعره')
        self.assertEqual(result['reply'], '')

    def test_manual_pause_still_blocks_even_independent_shipping_question(self):
        m.set_customer_ai_enabled(self.db, self.sender, False)
        result = self.event('توصيل اشكد')
        self.assertEqual(result['reply'], '')
        self.assertEqual(result['meta']['reason'], 'ai_disabled_for_conversation')

    def test_request_for_real_photo_does_not_resend_same_catalog_image(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        for text in ["ماكو غير هاي الصورة؟", "الفستان حطي وصوريه ودزيه", "تدزين نفس الصوره"]:
            result=self.event(text)
            self.assertIn("ما عندي تصوير إضافي",result["reply"])
            self.assertFalse(result.get("send_image"))
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='outgoing' AND message_type='image'",(self.sender,)).fetchone()[0],0)

    def test_store_configuration_and_image_choice_matrix(self):
        original_sender, original_ev = self.sender, self.ev
        cases=0
        for store in ["default","al-fatena","khuyoot","golden-threads"]:
            for auto in [False,True]:
                for linked in [False,True]:
                    for mode in ["same","replace","add","ask","failed","text_only"]:
                        with self.subTest(store=store,auto=auto,linked=linked,mode=mode):
                            token=m._current_store_id.set(store)
                            try:
                                self.sender=store+"::matrix-"+uuid.uuid4().hex
                                self.ev=dict(original_ev,sender_id=self.sender,store_id=store)
                                self.settings["enabled"]=auto
                                m.get_or_create_customer(self.db,self.sender,"page","facebook")
                                if linked:m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
                                if mode=="text_only":
                                    with patch.object(m,"generate_first_message_reply",return_value=("أرسلي صورة الموديل", "")):
                                        result=self.event("شنو القماش؟")
                                    self.assertEqual("لينن" in result["reply"],auto or linked)
                                elif mode=="failed":
                                    with patch.object(m,"_match_single_product",return_value=(None,None,{"product_found":False})) as match:
                                        result=self.event("",image="https://images.test/fail.jpg")
                                    self.assertEqual(match.call_count,1)
                                    self.assertEqual(result["reply"],"")
                                    self.assertFalse(m.is_customer_ai_enabled(self.db,self.sender))
                                else:
                                    caption={"same":"","replace":"بس هذا","add":"ضيفيه ويا السابق","ask":""}[mode]
                                    result=self.recognize(caption,"F1" if mode=="same" else "F2")
                                    pending=mode=="ask" and (linked or auto)
                                    self.assertEqual(bool(result["meta"].get("product_choice_pending")),pending)
                                    if pending:self.event("بس هذا")
                                    ids={p["product_id"] for p in m.load_customer_products(self.db,self.sender)}
                                    expected={"F1"} if mode=="same" else {"F1","F2"} if mode=="add" and (auto or linked) else {"F2"}
                                    self.assertEqual(ids,expected)
                                    self.assertTrue(m.is_customer_ai_enabled(self.db,self.sender))
                                self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?",(self.sender,)).fetchone()[0],0)
                                cases+=1
                            finally:m._current_store_id.reset(token)
        self.sender,self.ev=original_sender,original_ev
        self.assertEqual(cases,96)

    def test_dashboard_collects_image_before_caption_and_offers_choice(self):
        m.save_message(self.db,self.sender,"incoming","image","","https://images.test/customer.jpg",None,None,{})
        m.save_message(self.db,self.sender,"incoming","text","قياس 44",None,None,None,{})
        client=m.app.test_client()
        with client.session_transaction() as session: session["dashboard_authenticated"]=True
        with patch.object(m,"match_customer_image_with_catalog",return_value={"product_found":True,"product_id":"F2","confidence":100}), patch.object(m,"call_main_ai", return_value={"reply":"قياس 44 متوفر", "intent":"product_question"}) as main:
            response=client.post(f"/api/conversations/{self.sender}/ask_ai",json={"allow_empty":True})
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.get_json()["reply"],"قياس 44 متوفر")
        self.assertEqual(main.call_args.args[5]["product_id"],"F2")

    def test_dashboard_uses_enabled_default_for_faq(self):
        m.save_message(self.db,self.sender,"incoming","text","شنو القماش",None,None,None,{})
        client=m.app.test_client()
        with client.session_transaction() as session: session["dashboard_authenticated"]=True
        with patch.object(m,"call_main_ai",return_value={"reply":"لينن","intent":"product","confidence":100}) as main:
            response=client.post(f"/api/conversations/{self.sender}/ask_ai",json={"allow_empty":True})
        self.assertEqual(response.status_code,200)
        self.assertEqual(main.call_args.args[5]["product_id"],"F1")

    def test_labelled_photos_take_precedence_over_catalog_sheet_numbers(self):
        expected={"product_found":True,"product_id":"F2","confidence":100}
        with patch.object(m,"is_store_feature_enabled",return_value=True), patch.object(m,"confirm_with_vision",return_value=expected) as direct, patch.object(m.requests,"post") as request:
            result=m.match_customer_image_with_catalog("https://images.test/customer.jpg",CATALOG)
        self.assertEqual(result["product_id"],"F2")
        self.assertEqual(result["reference_source"],"labelled_product_images")
        self.assertEqual(len(direct.call_args.args[1]),2)
        request.assert_not_called()

    def test_failed_direct_comparison_cannot_guess_a_catalog_number(self):
        with patch.object(m,"is_store_feature_enabled",return_value=True), patch.object(m,"confirm_with_vision",return_value={"product_found":False,"product_id":""}), patch.object(m.requests,"post") as request:
            result=m.match_customer_image_with_catalog("https://images.test/customer.jpg",CATALOG)
        self.assertFalse(result["product_found"])
        request.assert_not_called()

    def test_new_photo_discussion_defers_choice_until_booking(self):
        self.recognize()
        price=self.event("شكد سعره؟")["reply"]
        self.assertIn("15,000",price)
        self.assertNotIn("دانتيل",price)
        fabric=self.event("شنو القماش؟")["reply"]
        self.assertIn("باربي",fabric)
        result=self.event("ثبتي الطلب")
        self.assertIn("أثبتلج",result["reply"])
        self.assertIn("دانتيل",result["reply"])
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?",(self.sender,)).fetchone()[0],0)
        self.event("بس هذا")
        self.assertIsNone(m.pending_product_choice(self.db,self.sender))

    def test_model_cannot_confirm_ambiguous_cart_during_discussion(self):
        self.recognize()
        with patch.object(m,"_call_main_ai_once",return_value={"reply":"تم تثبيت الطلب","create_order":True,"order":{}}):
            result=m.call_main_ai(dict(self.ev,text="اي"),"text",{},[],CATALOG,CATALOG[1],None,"",[])
        self.assertFalse(result["create_order"])
        self.assertIn("أثبتلج",result["reply"])

    def test_album_links_both_products_and_preserves_image_order_in_reply(self):
        self.settings['enabled'] = False
        urls = ['https://images.test/first.jpg', 'https://images.test/second.jpg']
        ev = dict(self.ev, image_url=urls[0], attachments=[{'type': 'image', 'url': u} for u in urls])
        responses = [(p, 'image_recognition', {'product_found': True}) for p in CATALOG]
        with patch.object(m, '_match_single_product', side_effect=responses) as match:
            product, _, result = m.match_product(self.db, ev, CATALOG)
        self.assertEqual([c.args[1]['image_url'] for c in match.call_args_list], urls)
        self.assertEqual([r['product_id'] for r in result['images']], ['F1', 'F2'])
        remembered = m.load_customer_products(self.db, self.sender)
        self.assertEqual({p['product_id'] for p in remembered}, {'F1', 'F2'})
        self.assertEqual(m.select_customer_context_product('الصورة الأولى', remembered)['product_id'], 'F1')
        self.assertEqual(m.select_customer_context_product('الصورة الثانية', remembered)['product_id'], 'F2')
        with patch.object(m, '_call_main_ai_once', return_value={'reply': 'الصورة الأولى متوفرة والثانية متوفرة', 'create_order': False, 'order': {}}) as model:
            m.auto_reply_after_product_link(self.db, self.sender, product, event=ev)
        self.assertEqual([r['product_id'] for r in model.call_args.args[6]['images']], ['F1', 'F2'])
        self.assertIn('image_index', model.call_args.args[7])
        self.assertNotIn('لا تذكر موديلين إلا', model.call_args.args[7])

    def test_repeated_product_keeps_both_image_positions(self):
        self.settings['enabled'] = False
        ev = dict(self.ev, attachments=[{'type': 'image', 'url': 'https://images.test/a.jpg'}, {'type': 'image', 'url': 'https://images.test/b.jpg'}])
        with patch.object(m, '_match_single_product', return_value=(CATALOG[0], 'image_recognition', {'product_found': True})):
            m.match_product(self.db, ev, CATALOG)
        remembered = m.load_customer_products(self.db, self.sender)
        self.assertEqual(len(remembered), 1)
        self.assertEqual(remembered[0]['image_positions'], [1, 2])
        self.assertEqual(m.select_customer_context_product('الصورة الثانية', remembered)['product_id'], 'F1')

    def test_unmatched_first_photo_does_not_shift_second_photo_to_first(self):
        self.settings['enabled'] = False
        ev = dict(self.ev, attachments=[{'type': 'image', 'url': 'https://images.test/a.jpg'}, {'type': 'image', 'url': 'https://images.test/b.jpg'}])
        with patch.object(m, '_match_single_product', side_effect=[(None, None, {'product_found': False}), (CATALOG[1], 'image_recognition', {'product_found': True})]):
            m.match_product(self.db, ev, CATALOG)
        remembered = m.load_customer_products(self.db, self.sender)
        self.assertIsNone(m.select_customer_context_product('الصورة الأولى', remembered))
        self.assertEqual(m.select_customer_context_product('الصورة الثانية', remembered)['product_id'], 'F2')
        with patch.object(m, '_call_main_ai_once', return_value={'reply': 'الثانية متوفرة', 'create_order': False, 'order': {}}) as model:
            m.call_main_ai(dict(self.ev, text='الثانية متوفرة؟'), 'text', {}, [], CATALOG, CATALOG[1], None, '', [], customer_products=remembered)
        self.assertIn('https://images.test/b.jpg', model.call_args.args[7])

    def test_color_only_does_not_switch_known_dress_to_pink_abaya(self):
        products=[dict(CATALOG[0],product_id='P017',product_name='فستان رباط',colors='جوزي'),
                  dict(CATALOG[1],product_id='P016',product_name='عباء لون وردي',colors='وردي')]
        self.assertIsNone(m._text_match_product('لون جوزي', products))
        self.assertIsNone(m.customer_product_target('لون جوزي', products))
        self.assertEqual(m.customer_product_target('اريد عباء لون وردي', products)['product_id'],'P016')

    def test_global_refusal_stops_choice_loop_without_human_or_order(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],'manual')
        m.complete_customer_product_link(self.db,self.sender,CATALOG[1],'manual',preserve_existing=True)
        with patch.object(m,'classify_contextual_selection') as classifier, patch.object(m,'create_human_review') as review:
            result=m.resolve_product_choice(self.db,dict(self.ev,text='ما ااريد بعد'),CATALOG)
        self.assertTrue(result['meta']['purchase_declined'])
        self.assertNotIn('موديل',result['reply'])
        classifier.assert_not_called();review.assert_not_called()
        self.assertFalse(m.customer_declines_purchase('ما اريد السوت اريد الفستان'))

    def test_second_unresolved_choice_creates_one_review_without_repeating_question(self):
        ev=dict(self.ev,text='هذا لو هذا')
        with patch.object(m,'send_telegram_message',return_value=True) as notify:
            first=m.product_clarification_or_review(self.db,ev,'تقصدين الفستان الأول لو الثاني؟')
            second=m.product_clarification_or_review(self.db,ev,'تقصدين الفستان الأول لو الثاني؟')
            third=m.product_clarification_or_review(self.db,ev,'تقصدين الفستان الأول لو الثاني؟')
        self.assertTrue(first['reply'])
        self.assertEqual(second['reply'],'')
        self.assertTrue(second['meta']['needs_human'])
        self.assertEqual(second['meta']['human_review_id'],third['meta']['human_review_id'])
        self.assertEqual(notify.call_count,1)
        self.assertTrue(m.is_customer_ai_enabled(self.db,self.sender))

    def test_multi_product_contact_does_not_get_blocked_before_model(self):
        # The pre-provider guard must allow an address/refusal in a known basket.
        with patch.object(m,'OPENROUTER_KEY',''), patch.object(m,'is_product_detail_followup',return_value=False):
            for text in ['دزيت الج','ثنيهم واحد اسود والثاني جوزي','بغداد العامرية']:
                result=REAL_MAIN_AI_ONCE(dict(self.ev,text=text),'text',{},[],CATALOG,None,None,'',[],customer_products=CATALOG)
                self.assertEqual(result.get('failure_reason'),'no_api_key')
                self.assertNotIn('حدديه',result.get('reply',''))
        self.assertEqual(m.image_selection_intent('حبيبتي ثنيهم واحد اسود والثاني جوزي'),'add')

    def test_contact_preserves_multi_item_draft_and_weight_type(self):
        for product in CATALOG:
            m.complete_customer_product_link(self.db,self.sender,product,'manual',preserve_existing=True)
        items=[dict(product_id=p['product_id'],color='اسود',size='38',size_type='size',weight='50',quantity=1) for p in CATALOG]
        m.save_message(self.db,self.sender,'outgoing','text','دزي الرقم',None,None,None,{'checkout_draft':{'items':items,'address':'المقدادية','notes':'واتساب فقط'}})
        result={'order':{'items':[dict(i,size_type='weight',weight='') for i in items]}}
        m.restore_checkout_draft(self.db,dict(self.ev,text='07701234567'),result,None)
        self.assertEqual(result['order']['items'],items)
        self.assertEqual(result['order']['notes'],'واتساب فقط')
        self.assertEqual(result['order']['address'],'المقدادية')

    def test_known_price_recovers_without_human_when_model_cannot_answer(self):
        with patch.object(m,'_call_main_ai_once',return_value={'reply':'','failed':True,'failure_reason':'provider_http_402'}):
            result=m.call_main_ai(dict(self.ev,text='شكد السعر؟'),'text',{},[],CATALOG,CATALOG[0],None,'',[])
        self.assertFalse(result['requires_human'])
        self.assertIn('16,000',result['reply'])
        self.assertTrue(result['_catalog_fallback'])

    def test_unknown_compound_question_is_not_hidden_by_price_fallback(self):
        with patch.object(m,'_call_main_ai_once',return_value={'reply':'','requires_human':True,'handoff_reason':'تفصيل غير متوفر'}):
            result=m.call_main_ai(dict(self.ev,text='شكد السعر وشنو الطول'),'text',{},[],CATALOG,CATALOG[0],None,'',[])
        self.assertTrue(result['failed'])
        self.assertTrue(result['requires_human'])
        self.assertFalse(result.get('_catalog_fallback'))
