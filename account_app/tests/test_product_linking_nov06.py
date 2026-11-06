"""Owner-managed catalog linking, isolated from customer/fulfillment delivery."""
import json
import unittest
from account_app import app as m,catalog_profile,product_links,sales_context
from account_app.tests import test_product_editor_links_oct24 as editor


class ProductLinkingApi(unittest.TestCase):
    setUp=editor.ProductEditorLinkTests.setUp
    tearDown=editor.ProductEditorLinkTests.tearDown
    create=editor.ProductEditorLinkTests.create
    saved=editor.ProductEditorLinkTests.saved

    def payload(self,store='default',**values):
        response=self.client.get('/api/products/manage?store_id='+store)
        return dict(product_ids=['A','B'],revision=response.json['revision'],changes=dict(audience='boys',product_type='tracksuits',
            occasions=['home','winter'],recommendation_groups=['ولادي بيتي']),**values)

    def seed(self,store='default'):
        for code in ('A','B'):
            response=self.client.post('/api/products/manage?store_id='+store,headers=self.headers,json=dict(product_id=code,
                product_name='تراك '+code,price='12000',stock='متوفر',status='active',sizes='2 إلى 7 سنوات',ad_id=code+'-ad'))
            self.assertEqual(response.status_code,201,response.json)

    def test_all_five_stores_classify_only_selected_products_and_keep_commercial_facts(self):
        for store in ('default','khuyoot','al-fatena','baraah-kids','golden-threads'):
            with self.subTest(store=store):
                self.seed(store)
                before=json.loads(self.catalog.read_text(encoding='utf8'))
                response=self.client.post('/api/products/manage/classify?store_id='+store,headers=self.headers,json=self.payload(store))
                self.assertEqual(response.status_code,200,response.json)
                after=json.loads(self.catalog.read_text(encoding='utf8'))
                for old,new in zip(before,after):
                    for field in ('price','sizes','image_url','ad_id','stock','store_id','related_product_ids'):
                        self.assertEqual(old.get(field),new.get(field),field)
                    if old['store_id']!=store:self.assertEqual(old,new)
                rows=self.client.get('/api/products/manage?store_id='+store).json['products']
                self.assertTrue(all(p['audience']=='boys' and p['product_type']=='tracksuits' for p in rows))
                self.assertTrue(all(p['occasions']==['home','winter'] for p in rows))
                self.assertIn('ولادي بيتي',rows[0]['recommendation_groups'])

    def test_draft_preview_applies_to_both_sides_without_saving_anything(self):
        self.seed();before=self.catalog.read_bytes()
        data=self.payload(source_id='A',age_months=36,occasion='home')
        response=self.client.post('/api/products/manage/classify-preview',headers=self.headers,json=data)
        self.assertEqual(response.status_code,200,response.json)
        self.assertEqual([p['product_id'] for p in response.json['products']],['B'])
        self.assertTrue(response.json['draft']);self.assertEqual(self.catalog.read_bytes(),before)
        self.assertEqual(m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0],0)
        self.assertEqual(m.get_db().execute('SELECT COUNT(*) FROM messages').fetchone()[0],0)

    def test_empty_or_malformed_changes_are_atomic_and_never_edit_price_or_another_store(self):
        self.seed();before=self.catalog.read_bytes()
        for changes in ({},{'price':'1'},{'audience':[]},{'recommendation_enabled':'false'},
                {'occasions':['unknown']},{'recommendation_groups':['bad\nname']},{'product_type':'missing'}):
            data=self.payload();data['changes']=changes
            result=self.client.post('/api/products/manage/classify',headers=self.headers,json=data)
            self.assertEqual(result.status_code,400,result.json)
            self.assertEqual(self.catalog.read_bytes(),before)
        data=self.payload();data['product_ids']=['A','foreign']
        self.assertEqual(self.client.post('/api/products/manage/classify',headers=self.headers,json=data).status_code,400)
        self.assertEqual(self.catalog.read_bytes(),before)

    def test_stale_catalogue_revision_requires_refresh_and_csrf_is_mandatory(self):
        self.seed();data=self.payload()
        self.client.put('/api/products/manage/A',headers=self.headers,json=dict(price='18000'))
        before=self.catalog.read_bytes()
        self.assertEqual(self.client.post('/api/products/manage/classify',headers=self.headers,json=data).status_code,409)
        self.assertEqual(self.catalog.read_bytes(),before)
        for path in ('classify','classify-preview'):
            self.assertEqual(self.client.post('/api/products/manage/'+path,json=self.payload()).status_code,403)

    def test_group_modes_and_new_categories_preserve_size_data(self):
        self.seed()
        self.client.post('/api/products/manage/classify',headers=self.headers,json=self.payload())
        for mode,wanted in (('add',['ولادي بيتي','جديد']),('remove',['ولادي بيتي']),('replace',['جديد'])):
            data=self.payload();data.update(changes={'recommendation_groups':['جديد']},recommendation_groups_mode=mode)
            self.assertEqual(self.client.post('/api/products/manage/classify',headers=self.headers,json=data).status_code,200)
            self.assertEqual(self.saved()['A']['recommendation_groups'],wanted)
        self.assertEqual(self.saved()['A']['sizes'],'2 إلى 7 سنوات')

    def test_manual_category_and_renamed_group_are_resolved_by_the_store_registry(self):
        self.seed()
        path='/api/products/manage/classifications'
        category=self.client.post(path,headers=self.headers,json=dict(kind='category',name='تراكات البيت',family='tracksuits')).json['id']
        group=self.client.post(path,headers=self.headers,json=dict(kind='group',name='الاسم القديم')).json['id']
        self.assertEqual(self.client.put(path+'/group/'+group,headers=self.headers,json=dict(name='تراكات مشتركة')).status_code,200)
        data=self.payload();data['changes'].update(product_type=category,recommendation_groups=['الاسم القديم'])
        response=self.client.post('/api/products/manage/classify',headers=self.headers,json=data)
        self.assertEqual(response.status_code,200,response.json)
        rows=self.client.get('/api/products/manage').json['products']
        for product in rows:
            self.assertEqual((product['product_type'],product['product_family'],product['product_type_label']),
                             (category,'tracksuits','تراكات البيت'))
            self.assertEqual(product['recommendation_groups'],['تراكات مشتركة'])
            self.assertEqual(product['sizes'],'2 إلى 7 سنوات')
        data=self.payload();data.update(changes={},source_id='A',age_months=36,occasion='home')
        result=self.client.post('/api/products/manage/classify-preview',headers=self.headers,json=data)
        self.assertEqual([p['product_id'] for p in result.json['products']],['B'])

    def test_preview_respects_home_outing_and_does_not_accept_age_and_size_together(self):
        self.seed();self.client.post('/api/products/manage/classify',headers=self.headers,json=self.payload())
        self.client.put('/api/products/manage/B',headers=self.headers,json=dict(occasions=['outing']))
        data=self.payload();data.update(changes={},source_id='A',occasion='home')
        result=self.client.post('/api/products/manage/classify-preview',headers=self.headers,json=data)
        self.assertEqual(result.status_code,200,result.json);self.assertEqual(result.json['products'],[])
        data['occasion']='outing'
        self.assertEqual(len(self.client.post('/api/products/manage/classify-preview',headers=self.headers,json=data).json['products']),1)
        data.update(age_months=36,size='44')
        self.assertEqual(self.client.post('/api/products/manage/classify-preview',headers=self.headers,json=data).status_code,400)

    def test_product_staff_has_both_pages_and_previews_but_reply_only_cannot_change_classification(self):
        self.seed()
        self.client.post('/api/staff',headers=self.headers,json=dict(username='link-products',name='اختبار',password='isolated-password-123',permissions=['products'],active=True))
        staff=self.client.get('/api/staff').json['accounts'][0]
        with self.client.session_transaction() as session:session.update(staff_id=staff['id'],staff_version=1)
        self.assertEqual(self.client.get('/product-links').status_code,200)
        self.assertEqual(self.client.post('/api/products/manage/classify-preview',headers=self.headers,json=self.payload(source_id='A')).status_code,200)
        source=self.client.get('/api/products/manage').json['products'][0]
        self.assertEqual(self.client.post('/api/products/recommendations/preview',headers=self.headers,json={'product':source}).status_code,200)
        data=self.payload()
        m.get_db().execute('UPDATE staff_accounts SET permissions=? WHERE id=?',(json.dumps(['reply']),staff['id']));m.get_db().commit()
        self.assertEqual(self.client.get('/product-links').status_code,403)
        self.assertEqual(self.client.post('/api/products/manage/classify',headers=self.headers,json=data).status_code,403)


class ProductLinkingEligibility(unittest.TestCase):
    def product(self,pid,**fields):
        return dict(dict(product_id=pid,product_name='تراك',store_id='default',audience='boys',product_type='tracksuits',
            price='12000',stock='متوفر',status='active',sizes='2 إلى 7 سنوات',occasions=['home','winter'],
            recommendation_groups=['مشترك'],image_url='https://images.test/'+pid+'.jpg'),**fields)

    def test_group_never_overrides_store_audience_use_type_age_stock_or_disabled_state(self):
        source=self.product('A');good=self.product('B')
        bad=[self.product('C',audience='girls'),self.product('D',occasions=['outing']),self.product('E',product_type='sets'),
            self.product('F',sizes='8 إلى 10 سنوات'),self.product('G',stock_quantity='0'),self.product('H',store_id='khuyoot'),
            self.product('I',recommendation_enabled=False),self.product('J',status='inactive')]
        self.assertEqual([p['product_id'] for p in product_links.eligible_related_products(source,[source,*bad,good],age_years=3)],['B'])

    def test_natural_iraqi_requests_and_followup_keep_type_use_audience_and_age(self):
        rows=[self.product('A'),self.product('B',occasions=['outing','winter']),self.product('C',audience='girls')]
        question='اريد تراك ولادي بيتي شتوي عمر 3 سنوات'
        self.assertEqual([p['product_id'] for p in catalog_profile.discovery_candidates(question,rows)],['A'])
        context=sales_context.advance({},[dict(direction='incoming',text=question)])
        self.assertEqual([p['product_id'] for p in catalog_profile.contextual_candidates('غيره',rows,context)],['A'])
        self.assertEqual([p['product_id'] for p in catalog_profile.contextual_candidates('طلعه بدل البيتي',rows,context)],['B'])

    def test_explicit_new_use_and_type_can_replace_source_constraints(self):
        source=self.product('A');out=self.product('B',product_type='sets',occasions=['outing'])
        result=product_links.catalog_alternatives(source,[source,out],age_years=3,request='اريد سوت للطلعه')
        self.assertEqual([p['product_id'] for p in result],['B'])

    def test_track_suit_and_dress_are_distinct_and_legacy_identity_still_works(self):
        for text,kind in (('تراك ولادي','tracksuits'),('سوت بناتي','sets'),('فساتين طلعه','dresses'),('دشداشة بيتية','dishdasha')):
            self.assertEqual(catalog_profile.query_type(text),kind)
            self.assertEqual(catalog_profile.effective_family({'product_name':text}),kind)
        self.assertEqual(catalog_profile.query_type('مو فستان اريد تراك'),'tracksuits')

    def test_main_product_search_uses_the_same_audience_type_and_usage_constraints(self):
        rows=[self.product('A',product_name='موديل A'),self.product('B',audience='girls'),
              self.product('C',occasions=['outing','winter']),self.product('D',recommendation_enabled=False)]
        result=m.search_available_products_for_request('اريد تراك ولادي بيتي شتوي عمر 3 سنوات',rows)
        self.assertEqual([r['product']['product_id'] for r in result['matches']],['A'])

    def test_main_search_reads_manual_bag_classification_without_a_product_name_keyword(self):
        bag=self.product('A',product_name='موديل 22',product_type='bags',audience='women',size_system='none',
                         sizes='',occasions=['outing'])
        result=m.search_available_products_for_request('عندكم حقائب للطلعه',[bag])
        self.assertEqual([r['product']['product_id'] for r in result['matches']],['A'])

    def test_recorded_winter_fabric_supports_old_products_without_guessing_home_or_outing(self):
        legacy=self.product('A',occasions=[],fabric='قماش تركي شتوي')
        self.assertIn('winter',catalog_profile.product_occasions(legacy))
        self.assertNotIn('home',catalog_profile.product_occasions(legacy))
        self.assertEqual(catalog_profile.discovery_candidates('تراك بيتي',[legacy]),[])
