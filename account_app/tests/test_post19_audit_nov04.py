"""Behavioral replays of failures observed after PR19; customer details are synthetic."""
import json
from unittest import TestCase
from unittest.mock import patch

from account_app import (catalog_profile, catalog_truth, checkout, color_choices,
    conversation_quality, owner_sales, product_links, product_variants,
    sales_context, sales_playbook, sales_recovery, sales_voice)
from account_app.tests import test_last50_checkout_nov01 as fixtures


class Post19Facts(TestCase):
    def test_inspection_acceptance_keeps_the_offered_color_and_size(self):
        context = sales_context.advance({}, [dict(direction='incoming', text='وزني 85'),
            dict(direction='incoming', text='اريد اكسين لارج لو 3 اكس'),
            dict(direction='outgoing', text='متوفر اللون الأخضر.'),
            dict(direction='outgoing', text='تحبين أثبتلج حجز عليه بقياس 3XL؟'),
            dict(direction='incoming', text='اي بس من استلمه راح اقيسه'),
            dict(direction='outgoing', text='إلكم حق الفحص قبل الدفع'),
            dict(direction='incoming', text='تمام')])
        selected = sales_playbook.customer_selection(context)
        self.assertEqual((selected['size'],selected['color']),('3XL','أخضر'))
        self.assertFalse(checkout.accepts_inspection_offer('اي بس غيري اللون للاسود'))
        self.assertTrue(sales_playbook.checkout_consent(context))
        withdrawn=sales_context.advance(context,[dict(direction='incoming',text='لا شكرا ما اريد')])
        self.assertFalse(sales_playbook.checkout_consent(withdrawn))
    def test_shipping_inclusive_two_piece_questions_are_not_fee_only(self):
        for text in ('يعني ثنين مع التوصيل شكد', 'شكد 2 معه توصيل', 'كم سعر القطعتين مع التوصيل'):
            self.assertTrue(checkout.delivered_total_inquiry(text), text)
        self.assertFalse(checkout.delivered_total_inquiry('كم تبلغ تكلفة التوصيل؟'))
        self.assertFalse(checkout.delivered_total_inquiry('اذا التوصيل مجاني ثبتي'))

    def test_currency_abbreviation_cannot_be_cut_into_half_a_sentence(self):
        answer = 'التوصيل 5,000 د.ع لكل المحافظات. بس دزيلي رقم الموبايل والعنوان.'
        result = sales_recovery.checkout_reply(answer, 'باقي العنوان.', 'شكد التوصيل؟')
        self.assertIn('5 آلاف', result)
        self.assertNotIn('د.', result)

    def test_catalog_prices_become_one_natural_offer(self):
        text = catalog_truth.conversational('سعر الموديل: 12 ألف عرض 2 قطع: 20 ألف سعر 2 قطع: 20 ألف')
        self.assertEqual(text.count('20'), 1)
        self.assertIn('القطعتين بـ 20', text)
        self.assertNotIn('قطع:', text)

    def test_spoken_size_typo_and_accepting_52_keep_the_chosen_label(self):
        for text in ('قياس اكس لاج', 'قياس لكس لاج'):
            self.assertEqual(product_variants.requested(text)['sizes'], ['XL'])
        self.assertEqual(product_variants.size_choice_text('52\nهاي هيا'), 'قياس 52')
        self.assertTrue(checkout.explicit_purchase('اثيبت'))

    def test_relative_birthday_is_one_current_age(self):
        text = 'كلبي بعدله شهرين يلا يصير سنه ونص'
        self.assertEqual(product_variants.child_ages(text), [16/12])
        product=dict(product_id='K',product_name='تراك',price='12000',colors='أخضر',sizes='2 إلى 7 سنوات',stock='متوفر')
        result=owner_sales.reply(dict(text=text),product,{}, {},[])
        self.assertTrue(result is None or 'بنيتها خشنة' not in result.get('reply',''))

    def test_explicit_garment_age_replaces_preference_without_repeating_body_age(self):
        context = sales_context.advance({}, [dict(direction='incoming', text='عمر سنة و5 اشهر'),
            dict(direction='outgoing', text='القياس المتوفر من سنتين'),
            dict(direction='incoming', text='ممكن صور مال سنتين'),
            dict(direction='outgoing', text='قطعة من كل لون؟')])
        selected = sales_playbook.customer_selection(context)
        self.assertEqual(product_variants.child_age(selected['size']), 2)
        p = dict(product_id='K', product_name='تراك', price='12000', colors='أخضر برتقالي', sizes='من 2 إلى 7 سنوات', stock='متوفر')
        context = sales_context.advance(context, [dict(direction='incoming', text='الأخضر والبرتقالي')])
        reply = color_choices.reply(p, 'الأخضر والبرتقالي', context)
        self.assertNotIn('مو ضمن', reply['reply'])
        self.assertIn('سنتين', reply['reply'])

    def test_catalogue_keeps_requested_boys_age_across_browse_turns(self):
        def p(pid, name, sizes, **kw):
            return dict(product_id=pid, product_name=name, sizes=sizes, stock='متوفر', status='active', price='12000', image_url='https://images.test/'+pid+'.jpg', **kw)
        products = [p('A','تراك ولادي','من 2 إلى 7 سنوات'), p('B','طقم ولادي','من 1 إلى 4 سنوات'),
                    p('C','فستان','من 3 إلى 9 سنوات')]
        context = sales_context.advance({}, [dict(direction='incoming', text='عمر 7 سنوات'),
            dict(direction='incoming', text='ولادي'), dict(direction='outgoing', text='دزي البقية؟')])
        self.assertEqual([p['product_id'] for p in catalog_profile.contextual_candidates('بعد شنو الموجود',products,context)], ['A'])
        self.assertEqual(product_links._audience(products[2]), 'girls')
        source = dict(products[0], product_type='sets', store_id='baraah-kids')
        alternative = dict(products[1], store_id='baraah-kids')
        self.assertEqual([p['product_id'] for p in product_links.catalog_alternatives(source,[source,alternative],age_years=1.4)], ['B'])

    def test_stock_wait_is_not_purchase_and_shipping_notes_need_real_action(self):
        self.assertEqual(sales_strategy_block('اذا توفر قياسها اريد هذا الحليبي'), 'awaiting_stock')
        for text in ('سجلت يمي ملاحظة التوصيل وقت المغرب حتى يرتبها المندوب وياج',
                     'راح نثبت ملاحظتك يم شركة التوصيل بخصوص وقت العصر'):
            self.assertTrue(conversation_quality.unsupported_claim(text))
        self.assertEqual(owner_sales.human_reason('وهاي حجز وياهم',post_order=True),'post_order_action')
        self.assertEqual(owner_sales.human_reason('وصلو الي المغرب لان بالدوام',post_order=True),'post_order_delivery_schedule')
        self.assertTrue(owner_sales.human_reason('لان قماشها مو زين وخياطة مالتها مخربط'))
        self.assertFalse(owner_sales.human_reason('اخاف الخياطة مو زينة؟'))
        self.assertTrue(sales_recovery.claims_sent_models('هسه أدزلك الموديلات ثواني بس'))

    def test_album_wording_respects_male_or_unknown_buyer(self):
        self.assertIn('عجبك', owner_sales.surface('أي واحد عجبج أكثر؟','male',warm=False))
        self.assertNotIn('عجبج', owner_sales.surface('أي واحد عجبج أكثر؟','',warm=False))


def sales_strategy_block(text):
    from account_app.sales_strategy import purchase_block_reason
    return purchase_block_reason(text)


class Post19Checkout(TestCase):
    setUpClass = classmethod(fixtures.Last50PipelineTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.Last50PipelineTests.tearDownClass.__func__)
    setUp, tearDown = fixtures.Last50PipelineTests.setUp, fixtures.Last50PipelineTests.tearDown
    link, save, process = fixtures.Last50PipelineTests.link, fixtures.Last50PipelineTests.save, fixtures.Last50PipelineTests.process
    remember_photo, order = fixtures.Last50PipelineTests.remember_photo, fixtures.Last50PipelineTests.order
    data = fixtures.Last50PipelineTests.data

    def test_inspection_acceptance_requests_missing_contacts_then_only_missing_field(self):
        self.link('أخضر')
        self.product['sizes']='وزن 70 إلى 130 كيلو'
        for direction,text in [('incoming','وزني 85'),('outgoing','الأخضر متوفر.'),
                ('outgoing','أثبتلج حجز عليه؟')]:
            self.save(direction,text)
        reply,_=self.process('اي بس من استلمه راح اقيسه')
        self.assertIn('رقم',reply['reply'])
        self.assertIn('العنوان',reply['reply'])
        self.assertIn('الفحص',reply['reply'])
        reply,_=self.process('دهوك قرب دهوك مول')
        self.assertIn('رقم',reply['reply'])
        self.assertNotIn('اختياراتج محفوظة',reply['reply'])
        reply,_=self.process('07701234567')
        self.assertIsNotNone(self.order(),reply)

    def test_contacts_then_supported_size_correction_book_without_extra_consent(self):
        self.link()
        for direction,text in [('incoming','ثبتي'),('outgoing','دزيلي الرقم والعنوان'),
                ('incoming','07701234567'),('incoming','صلاح الدين الشرقاط اسديرة عليا'),
                ('incoming','اريد قياس 54 اذا اكو'),('outgoing','54 مو متوفر، تحبين 52؟')]:
            self.save(direction,text)
        result,_=self.process('52\nهاي هيا')
        self.assertIsNotNone(self.order(),result)
        self.assertEqual(self.order()['size'],'52')
        self.assertIn('تم تثبيت',result['reply'])

    def test_weight_band_in_model_cart_persists_declared_kg(self):
        self.link('أخضر')
        self.product['sizes']='وزن 70 إلى 130 كيلو'
        self.save('incoming','وزني 85 اريد الأخضر ثبتي')
        data=self.data();data['items'][0].update(color='أخضر',size='من 70 الى 130 كيلو',size_type='weight',weight='85')
        result={'order':data}
        created,reply=self.m.create_order_if_valid(self.db,self.sender,result,self.product)
        self.assertTrue(created,reply)
        item=json.loads(self.order()['order_items'])[0]
        self.assertEqual((item['size'],item['weight']),('85','85'))

    def test_two_pieces_total_and_recovered_cart_total_use_offer(self):
        self.link()
        self.product.update(price='12000',offer='قطعتين ب 20 الف')
        result,_=self.process('يعني ثنين مع التوصيل شكد')
        self.assertIn('25 ألف',result['reply'])
        self.assertIsNone(self.order())
        data=self.data(True);data['items'][1]=dict(data['items'][0],color='اسود')
        reply=self.m.recover_factual_sales_reply(dict(self.ev,text='سعره وشكد التوصيل'),self.product,order=data)
        self.assertIn('25',reply['reply'])
        self.assertNotIn('17',reply['reply'])

    def test_typo_purchase_survives_product_link_callback(self):
        self.link()
        self.save('incoming','قياس 44 أسود')
        self.save('outgoing','دزيلي الرقم والعنوان')
        self.save('incoming','07701234567 بغداد حي الاختبار')
        self.save('incoming','اثيبت')
        self.remember_photo('اسود')
        self.save('incoming','هاي')
        candidate={'order':self.data()}
        self.assertTrue(self.m.checkout_requested(self.db,dict(self.ev,text='هاي'),candidate))

    def test_known_unavailable_age_does_not_turn_into_yes_in_stock(self):
        self.link()
        self.product['sizes']='من 2 إلى 7 سنوات'
        self.save('incoming','عمر 14 سنة')
        self.save('outgoing','14 مو ضمن قياسات هالموديل')
        result,model=self.process('يعني ممتوفر عدكم نهائي')
        model.assert_not_called()
        self.assertIn('14',result['reply'])
        self.assertNotIn('إي متوفر',result['reply'])

    def test_general_specs_quote_the_offer_once(self):
        self.link()
        self.product.update(price='12000', offer='عرض خاص القطعة ب12 الف قطعتين ب20 الف', fabric='تركي شتوي')
        result,_=self.process('ما هي مواصفات القطعة؟')
        self.assertEqual(result['reply'].count('12'),1)
        self.assertEqual(result['reply'].count('20'),1)
        self.assertFalse(self.m.reply_reasks_known_product('باقي نحدد قياس هالموديل بشكل واضح حتى أثبت الطلب.'))

    def test_catalogue_page_filters_recipient_and_age_before_attaching_photos(self):
        self.link()
        products=[dict(self.product,product_id='B',product_name='تراك ولادي',sizes='من 2 إلى 7 سنوات',image_url='https://images.test/b.jpg'),
                  dict(self.product,product_id='G',product_name='فستان',sizes='من 3 إلى 7 سنوات',image_url='https://images.test/g.jpg')]
        self.save('incoming','ولادي عمر 7 سنوات')
        with patch.object(self.m,'customer_address_gender',return_value=''):
            result=self.m.saved_catalog_page(self.db,dict(self.ev,text='بعد شنو الموجود'),products,1,start=True)
        self.assertEqual(result['meta']['catalog_page']['product_ids'],['B'])
        self.assertEqual(len(result['image_urls']),1)
        self.assertNotIn('عجبج',result['reply'])

    def test_missing_promised_catalogue_image_is_a_silent_review(self):
        self.link()
        result=self.m.missing_catalog_images_reply(self.db,self.ev)
        self.assertEqual(result['reply'],'')
        self.assertTrue(result['meta']['needs_human'])
        self.assertTrue(self.m.has_pending_human_review(self.db,self.sender))
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))

    def test_post_booking_schedule_or_addition_goes_to_real_review(self):
        self.link()
        self.save('incoming','ثبتي الاسود قياس 44')
        self.m.create_order_if_valid(self.db,self.sender,{'order':self.data()},self.product)
        original=dict(self.order())
        for text in ('وصلو الي المغرب لان بالدوام','وهاي حجز وياهم','اي صحيح رفضت لان ما اجه قياسي'):
            result,model=self.process(text)
            model.assert_not_called()
            self.assertEqual(result['reply'],'')
            self.assertTrue(result['meta']['needs_human'])
        self.assertEqual(dict(self.order()),original)

    def test_paused_conversation_keeps_cancellation_pending_without_executing_it(self):
        self.link()
        self.save('incoming','ثبتي الاسود قياس 44')
        self.m.create_order_if_valid(self.db,self.sender,{'order':self.data()},self.product)
        self.ev['text']='عيني الغي ماعجبهم'
        with patch.object(self.m,'extract_facebook_event',return_value=self.ev), \
             patch.object(self.m,'is_ai_enabled',return_value=False), \
             patch.object(self.m,'_call_main_ai_once') as provider:
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        provider.assert_not_called()
        self.assertEqual(result['reply'],'')
        self.assertEqual(self.order()['status'],'new')
        self.assertTrue(self.m.order_actions.pending(self.db,self.sender))
        self.m.close_human_attention(self.db,self.sender)
        self.assertTrue(self.m.has_pending_human_review(self.db,self.sender))
        self.m.set_customer_ai_enabled(self.db,self.sender,False)
        result=self.m.complete_customer_product_link(self.db,self.sender,self.product,'manual',source='manual_admin')
        self.assertEqual(result['closed_reviews'],0)
        self.assertTrue(self.m.has_pending_human_review(self.db,self.sender))
        self.assertFalse(self.m.is_customer_ai_enabled(self.db,self.sender))
