"""Production director routing/settings and isolated writer boundaries."""
import json
import unittest
from unittest.mock import patch,Mock
from account_app import sales_director as d
from account_app.tests import test_sales_recovery_nov09 as fixtures
from account_app import checkout,sales_context,sales_playbook,product_variants


def response(value):
    r=Mock();r.json.return_value={'choices':[{'finish_reason':'stop','message':{'content':json.dumps(value,ensure_ascii=False)}}]}
    return r


class WriterTests(unittest.TestCase):
    def test_iraqi_black_alias_is_the_same_catalog_color_without_changing_quantity(self):
        for alias in ('سودة','سوده','سودا','سوداء'):
            self.assertEqual(product_variants.colors(alias),['أسود'])
            draft=dict(order={'items':[dict(product_id='K',color=alias,size='44',quantity=2)]})
            normalized=d.canonical_cart_colors(draft,[dict(product_id='K',colors='اسود جوزي')])
            self.assertEqual(normalized['order']['items'][0],dict(product_id='K',color='أسود',size='44',quantity=2))
            self.assertEqual(draft['order']['items'][0]['color'],alias)
        self.assertEqual(product_variants.colors('جوزية'),['جوزي'])
        self.assertEqual(product_variants.colors('نيلية'),['نيلي'])
        self.assertEqual(product_variants.colors('لبنية عمر 3 سنوات'),[])
    def test_side_answer_does_not_repeat_unanswered_contact_request(self):
        history=[dict(direction='outgoing',text='دزيلي رقم الموبايل والعنوان.')]
        draft={'reply':'قماشه فرو ومبطن. بس دزيلي رقم الموبايل والعنوان حتى أثبت الطلب.'}
        result=d.avoid_contact_nag(draft,history,'شنو قماشه؟')
        self.assertIn('قماشه فرو',result['reply']);self.assertNotIn('رقم',result['reply'])
        self.assertEqual(d.avoid_contact_nag(draft,history,'تمام ثبتي'),draft)
        self.assertEqual(d.avoid_contact_nag(draft,history,'07700000001'),draft)
    def test_color_replacement_with_weight_and_unchanged_second_line(self):
        history=[dict(direction='incoming',text='اريد ازرق لوزن 130 وماروني لوزن 95 قطعة من كل لون'),
                 dict(direction='incoming',text='لا خلي الرصاصي بدل الماروني نفس وزن 95 والازرق يبقى 130')]
        product=dict(product_id='F',product_name='سوت',sizes='من وزن 60 إلى 130 كيلو',colors='ازرق ماروني رصاصي')
        context=sales_context.advance({},history)
        self.assertEqual(set(sales_playbook.customer_selection(context)['colors']),{'أزرق','رصاصي'})
        plan=sales_playbook.weight_cart_choices(product,context)
        self.assertEqual({(i['color'],i['size'],i['quantity']) for i in plan['items']},{('أزرق','130',1),('رصاصي','95',1)})
        self.assertFalse(checkout.chosen_color_error(plan['items'],history,[product]))
        self.assertIsNone(product_variants.color_replacement('خلي الرصاصي بدل الماروني إذا موجود'))
        self.assertEqual(product_variants.color_replacement('لا اعتمدي الاسود بدل النيلي يبقى 44 والجوزي 46'),('أسود','نيلي'))
        numbered=[dict(direction='incoming',text=t) for t in ('اريد نيلي قياس 44',
            'وخلي جوزي قياس 46 قطعة ثانية','لا اعتمدي الاسود بدل النيلي يبقى 44 والجوزي 46')]
        chosen=sales_playbook.customer_selection(sales_context.advance({},numbered))
        self.assertEqual(set(chosen['colors']),{'أسود','جوزي'})
        items=[dict(product_id='K',color='أسود',size='44',quantity=1),dict(product_id='K',color='جوزي',size='46',quantity=1)]
        self.assertFalse(checkout.chosen_color_error(items,numbered,[dict(product_id='K',colors='اسود نيلي جوزي')]))

    def test_only_text_can_change_and_decisions_are_preserved(self):
        draft=dict(reply='نعم، اللون الماروني قياس 48 متوفر. أرسلي العنوان فقط.',order={'items':[{'size':'48'}]},create_order=False,
            sales_plan={'next_step':'collect_contact'})
        with patch.object(d.ai_transport,'post',return_value=response({'reply_parts':['إي حبيبتي الماروني قياس 48 متوفر، باقي بس العنوان.']})):
            result=d.rewrite(draft,model=d.WRITER_MODEL,key='test-only',url='https://provider.test',gender='female')
        self.assertIn('حبيبتي',result['reply']);self.assertEqual(result['order'],draft['order']);self.assertFalse(result['create_order'])
        self.assertEqual(draft['reply'],'نعم، اللون الماروني قياس 48 متوفر. أرسلي العنوان فقط.')

    def test_writer_cannot_change_numbers_add_phone_booking_or_action_fields(self):
        draft=dict(reply='ماروني قياس 48 متوفر، باقي العنوان.',create_order=False,order={})
        for value in ({'reply_parts':['ماروني قياس 46 متوفر، باقي العنوان.']},
                      {'reply_parts':['ماروني قياس 48 متوفر، دزيلي الرقم والعنوان.']},
                      {'reply_parts':['تم تثبيت ماروني قياس 48.']},
                      {'reply_parts':['ماروني قياس 48 متوفر، باقي العنوان.'],'create_order':True}):
            with self.subTest(value=value),patch.object(d.ai_transport,'post',return_value=response(value)):
                result=d.rewrite(draft,model=d.WRITER_MODEL,key='test',url='https://provider.test',gender='female')
                self.assertEqual(result['reply'],draft['reply']);self.assertIn('_sales_writer_failure',result)

    def test_refusal_wait_and_handoff_do_not_gain_a_sales_question(self):
        for text in ('براحتك، أنتظر حتى تكملي اختيار القطعة الثانية.','شكراً لك، بالخدمة.','الأسود غير متوفر.'):
            with self.subTest(text=text):
                self.assertTrue(d.rewrite_issues({'reply':text},[text+' شكد قياس تلبسين؟']))
        with patch.object(d.ai_transport,'post') as provider:
            self.assertEqual(d.rewrite({'reply':'','requires_human':True},model=d.WRITER_MODEL,key='test',url='https://provider.test',gender='female')['reply'],'')
            provider.assert_not_called()


class DirectorPipelineTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.SalesPipelineNov09Tests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.SalesPipelineNov09Tests.tearDownClass.__func__)
    setUp=fixtures.SalesPipelineNov09Tests.setUp
    tearDown=fixtures.SalesPipelineNov09Tests.tearDown
    save=fixtures.SalesPipelineNov09Tests.save

    def enable(self):
        self.m.set_store_setting(self.db,'sales_director_enabled','1','baraah-kids')

    def test_settings_store_scope_validation_and_partial_save_preserve_ai_state(self):
        client=self.m.app.test_client()
        with client.session_transaction() as s:s['dashboard_authenticated']=True;s['csrf_token']='test'
        headers={'X-CSRF-Token':'test'}
        url='/api/settings/ai?store_id=baraah-kids'
        before=client.get(url).json['ai_enabled']
        r=client.post(url,json={'sales_director_enabled':True,'director_model':'openai/gpt-6.1-terra','writer_model':'google/gemini-3.8-flash'},headers=headers)
        self.assertEqual(r.status_code,200,r.json)
        saved=client.get(url).json
        self.assertTrue(saved['sales_director_enabled']);self.assertEqual(saved['ai_enabled'],before)
        page=client.get('/settings/ai?store_id=baraah-kids')
        self.assertEqual(page.status_code,200)
        for field in ('salesDirectorEnabled','directorModelInput','writerModelInput'):
            self.assertIn('id="'+field+'"',page.get_data(as_text=True))
        self.assertFalse(client.get('/api/settings/ai?store_id=khuyoot').json['sales_director_enabled'])
        self.assertEqual(client.post(url,json={'director_model':'Terra'},headers=headers).status_code,400)
        self.assertEqual(client.get(url).json['director_model'],'openai/gpt-6.1-terra')
        self.assertEqual(client.post(url,json={'sales_director_enabled':False},headers=headers).status_code,200)
        self.assertFalse(client.get(url).json['sales_director_enabled'])

    def test_director_bypasses_faq_and_final_legacy_rewrite(self):
        self.enable()
        result={'reply':'القماش دبل إيطالي. باقي بس العنوان.','_sales_director':d.DIRECTOR_MODEL,'create_order':False,'order':{}}
        ev=dict(self.ev,text='شنو قماشه؟')
        with patch.object(self.m,'_call_main_ai_once',return_value=result) as provider:
            actual=self.m.call_main_ai(ev,'text',{},[],[self.product],self.product,None,'',[],customer_products=[self.product])
        self.assertEqual(actual['reply'],result['reply']);self.assertTrue(provider.call_args.args[0]['_director_mode'])
        self.assertEqual(self.m.owner_result_surface(actual,ev,{},[])['reply'],result['reply'])

    def test_real_generation_selects_both_configured_models_and_restores_context(self):
        self.enable()
        self.save('incoming','اريد نيلي قياس 44')
        self.save('incoming','لا جوزي بدل النيلي نفس القياس')
        self.m.save_message(self.db,self.sender,'outgoing','text','أعرضلج هالاختيار.',None,None,None,
            {'director_state':{'plan':{'recommended_product_ids':['K1'],'next_step':'choose'}}})
        self.m.set_store_setting(self.db,'ai_director_model','openai/gpt-6.1-sol','baraah-kids')
        self.m.set_store_setting(self.db,'ai_writer_model','google/gemini-3.8-flash','baraah-kids')
        draft=dict(reply='الجوزي قياس 44 متوفر. أرسلي رقم الهاتف والعنوان.',create_order=False,order={},sales_plan={'next_step':'collect_contact'})
        with patch.object(self.m.ai_transport,'post',side_effect=[response(draft),response({'reply_parts':['الجوزي قياس 44 متوفر، دزيلي رقم الموبايل والعنوان.']})]) as provider:
            result=self.m.call_main_ai(dict(self.ev,text='تمام'), 'text',{},self.m.load_history(self.db,self.sender,limit=100),[self.product],self.product,None,'',[],customer_products=[self.product])
        self.assertEqual(len(provider.call_args_list),2,result)
        self.assertEqual([c.kwargs['json']['model'] for c in provider.call_args_list],['openai/gpt-6.1-sol','google/gemini-3.8-flash'])
        self.assertIn('جوزي',str(provider.call_args_list[0].kwargs['json']['messages']))
        self.assertIn('recommended_product_ids',str(provider.call_args_list[0].kwargs['json']['messages']))
        self.assertIn('44',result['reply']);self.assertEqual(result['_sales_writer'],d.WRITER_MODEL)

    def test_complete_director_order_is_saved_once_and_confirmation_cannot_be_rewritten(self):
        self.enable()
        self.save('incoming','اريد نيلي قياس 44 قطعة وحدة')
        self.save('outgoing','دزيلي الرقم والعنوان حتى أثبت الطلب.')
        text='07700000001 بغداد حي الاختبار قرب مدرسة التجربة'
        generated=dict(reply='الاختيار جاهز.',_sales_director=d.DIRECTOR_MODEL,create_order=True,
            order={'items':[dict(product_id='K1',color='نيلي',size='44',quantity=1)],
                   'phone':'07700000001','province':'بغداد','address':'بغداد حي الاختبار قرب مدرسة التجربة'})
        def features(name,default,db=None):return name=='sales_director_enabled'
        with patch.object(self.m,'extract_facebook_event',return_value=dict(self.ev,text=text)), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'is_customer_ai_enabled',return_value=True), \
             patch.object(self.m,'is_store_feature_enabled',side_effect=features), \
             patch.object(self.m,'_call_main_ai_once',return_value=generated):
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT,result)
        order=self.db.execute('SELECT * FROM orders WHERE sender_id=?',(self.sender,)).fetchall()
        self.assertEqual(len(order),1)
        self.assertEqual(order[0]['total_amount'],21000)

    def test_weight_only_cart_completes_from_evidence_without_asking_garment_size(self):
        self.enable()
        self.product.update(sizes='من وزن 60 إلى 130 كيلو',colors='ازرق ماروني رصاصي',
            price='15000 او بكج قطعتين ب 25000',offer='قطعتين ب 25 الف')
        turns=['اريد ازرق لوزن 130 وماروني لوزن 95 قطعة من كل لون',
            'لا خلي الرصاصي بدل الماروني نفس وزن 95 والازرق يبقى 130','تمام وافقت ثبتي القطعتين بالعرض']
        for text in turns:self.save('incoming',text)
        self.save('outgoing','دزيلي رقم الموبايل والعنوان.')
        text='07700000001 بغداد حي الاختبار قرب مدرسة التجربة'
        def features(name,default,db=None):return name=='sales_director_enabled'
        with patch.object(self.m,'extract_facebook_event',return_value=dict(self.ev,text=text)), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'is_customer_ai_enabled',return_value=True), \
             patch.object(self.m,'is_store_feature_enabled',side_effect=features), \
             patch.object(self.m,'_call_main_ai_once',side_effect=AssertionError('Ready weighted cart must not need a model decision')):
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT,result)
        order=self.db.execute('SELECT * FROM orders WHERE sender_id=?',(self.sender,)).fetchone()
        lines=json.loads(order['order_items'])
        self.assertEqual({(i['color'],i['size'],i.get('size_type')) for i in lines},{('أزرق','130','weight'),('رصاصي','95','weight')})
        self.assertEqual(order['total_amount'],30000)

    def test_director_failure_stays_failed_and_writer_is_not_called(self):
        self.enable()
        with patch.object(self.m,'OPENROUTER_KEY','synthetic'), \
             patch.object(self.m.reply_consultant,'enabled',return_value=False), \
             patch.object(self.m.ai_transport,'post',side_effect=RuntimeError('synthetic failure')) as provider:
            result=self.m.call_main_ai(dict(self.ev,text='شكد سعره؟'),'text',{},[],[self.product],self.product,None,'',[],customer_products=[self.product])
        self.assertTrue(result.get('failed'),result);self.assertFalse(result.get('create_order'))
        self.assertEqual(provider.call_count,1)

    def test_silent_human_request_precedes_director_and_store_control_is_separate(self):
        self.enable()
        with patch.object(self.m,'_call_main_ai_once') as provider:
            result=self.m.call_main_ai(dict(self.ev,text='اريد موظفة'),'text',{},[],[self.product],self.product,None,'',[],customer_products=[self.product])
        provider.assert_not_called();self.assertEqual(result['reply'],'')

    def test_missing_fact_consultation_keeps_the_selected_director_mode(self):
        self.enable()
        ev=dict(self.ev,text='بيه ليكرا؟',_defer_product_handoff=True)
        with self.m.reply_consultant.scope(True) as state,patch.object(self.m,'_call_main_ai_once') as provider:
            result=self.m._call_main_ai_primary(ev,'text',{},[],[self.product],self.product,None,'',[],customer_products=[self.product])
            self.assertTrue(state.data['ev']['_director_mode'])
        provider.assert_not_called();self.assertTrue(result.get('_needs_fact_review'))

    def test_partial_saved_cart_does_not_append_contact_request_to_side_answer(self):
        self.enable()
        self.save('incoming','اريد نيلي قياس 44 قطعة وحدة')
        self.m.save_message(self.db,self.sender,'outgoing','text','دزيلي الرقم والعنوان.',None,None,None,
            {'checkout_draft':{'items':[dict(product_id='K1',color='نيلي',size='44',quantity=1)]}})
        generated=dict(reply='قماشه دبل إيطالي.',_sales_director=d.DIRECTOR_MODEL,create_order=False,order={})
        def features(name,default,db=None):return name=='sales_director_enabled'
        with patch.object(self.m,'extract_facebook_event',return_value=dict(self.ev,text='شنو قماشه؟')), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'is_customer_ai_enabled',return_value=True), \
             patch.object(self.m,'is_store_feature_enabled',side_effect=features), \
             patch.object(self.m,'_call_main_ai_once',return_value=generated):
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertNotIn('رقم',result['reply']);self.assertNotIn('عنوان',result['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],0)

    def test_second_piece_then_color_correction_saves_the_directors_two_lines(self):
        self.enable()
        for text in ('اريد نيلي قياس 44','وخلي جوزي قياس 46 قطعة ثانية',
                     'لا اعتمدي الاسود بدل النيلي يبقى 44 والجوزي 46','تمام موافقة ثبتي القطعتين'):
            self.save('incoming',text)
        self.save('outgoing','دزيلي الرقم والعنوان.')
        generated=dict(reply='وصلت التفاصيل.',_sales_director=d.DIRECTOR_MODEL,create_order=True,
            order={'items':[dict(product_id='K1',color='أسود',size='44',quantity=1),
                            dict(product_id='K1',color='جوزي',size='46',quantity=1)]})
        def features(name,default,db=None):return name=='sales_director_enabled'
        with patch.object(self.m,'extract_facebook_event',return_value=dict(self.ev,text='07700000001 بغداد حي الاختبار قرب مدرسة التجربة')), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'is_customer_ai_enabled',return_value=True), \
             patch.object(self.m,'is_store_feature_enabled',side_effect=features), \
             patch.object(self.m,'_call_main_ai_once',return_value=generated):
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT,result)
        order=self.db.execute('SELECT * FROM orders WHERE sender_id=?',(self.sender,)).fetchone()
        self.assertEqual(order['total_amount'],37000)
        self.assertEqual({(i['color'],i['size']) for i in json.loads(order['order_items'])},{('أسود','44'),('جوزي','46')})

    def test_long_context_last_correction_survives_side_questions(self):
        self.enable()
        history=[dict(direction='incoming',text='اريد النيلي قياس 44')]
        for i in range(12):
            history.extend([dict(direction='outgoing',text='قماشه دبل إيطالي.'),dict(direction='incoming',text='تمام، شكد التوصيل؟')])
        history.extend([dict(direction='incoming',text='لا اعتمدي 46 بدل 44 والجوزي بدل النيلي'),
                        dict(direction='incoming',text='هذا رقمي 07700000001')])
        for row in history:self.save(row['direction'],row['text'])
        draft=dict(reply='الجوزي قياس 46، باقي العنوان.',create_order=False,order={})
        with patch.object(self.m.ai_transport,'post',side_effect=[response(draft),response({'reply_parts':['صار بعيوني، الجوزي قياس 46، باقي العنوان.']})]) as provider:
            result=self.m.call_main_ai(dict(self.ev,text='وشنو قماشه؟'),'text',{'phone':'07700000001'},history,[self.product],self.product,None,'',[],customer_products=[self.product])
        sent=provider.call_args_list[0].kwargs['json']['messages'][1]['content']
        self.assertIn('46',sent);self.assertIn('جوزي',sent)
        self.assertIn('46',result['reply']);self.assertNotIn('رقم',result['reply'])
