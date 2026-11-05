"""Managed vocabulary and shoe/bag sales, with all customer delivery intercepted."""
import json
import unittest
from unittest.mock import patch
from account_app import catalog_profile,product_links,product_variants,sales_playbook,owner_sales,customer_copy,child_availability,sales_context
from account_app.tests import test_product_editor_links_oct24 as editor
from account_app.tests import test_last50_checkout_nov01 as pipeline
from account_app import app as m

URL='/api/products/manage/classifications'


class ManualCategoriesApi(unittest.TestCase):
    setUp=editor.ProductEditorLinkTests.setUp
    tearDown=editor.ProductEditorLinkTests.tearDown
    create=editor.ProductEditorLinkTests.create
    saved=editor.ProductEditorLinkTests.saved

    def category(self,name='حقائب يد',family='bags',size_system='none'):
        response=self.client.post(URL,headers=self.headers,json=dict(kind='category',name=name,family=family,size_system=size_system))
        self.assertEqual(response.status_code,201,response.json)
        return response.json['id']

    def test_empty_categories_and_groups_are_available_before_the_first_product(self):
        category=self.category()
        response=self.client.post(URL,headers=self.headers,json=dict(kind='group',name='حقائب الدوام'))
        self.assertEqual(response.status_code,201,response.json)
        data=self.client.get('/api/products/manage').json
        self.assertEqual(data['products'],[])
        self.assertEqual(data['catalog_schema']['product_types'][category],'حقائب يد')
        self.assertIn('حقائب الدوام',[r['name'] for r in data['catalog_schema']['groups']])
        self.assertEqual(self.client.get('/product-categories').status_code,200)
        self.assertIn('إدارة الفئات والمجموعات',self.client.get('/products').text)

    def test_custom_category_persists_family_and_overrides_spoofed_metadata(self):
        category=self.category()
        response=self.create('B1',product_name='حقيبة اختبار',product_type=category,product_family='shoes',product_type_label='اسم مزيف',size_system='none')
        self.assertEqual(response.status_code,201,response.json)
        product=self.saved()['B1']
        self.assertEqual((product['product_type'],product['product_family'],product['product_type_label']),(category,'bags','حقائب يد'))
        self.assertEqual(customer_copy.reply_category(product),'حقائب يد')
        self.assertEqual(self.client.put('/api/products/manage/B1',headers=self.headers,json=dict(product_type='')).status_code,200)
        self.assertEqual(self.saved()['B1']['product_family'],'')

    def test_store_isolation_and_unknown_category_rejection(self):
        category=self.category()
        self.assertNotIn(category,[r['id'] for r in self.client.get(URL+'?store_id=khuyoot').json['categories']])
        response=self.client.post('/api/products/manage?store_id=khuyoot',headers=self.headers,json=dict(product_id='X',product_name='حقيبة',product_type=category))
        self.assertEqual(response.status_code,400,response.json)
        self.assertEqual(self.client.put(URL+'/category/'+category+'?store_id=khuyoot',headers=self.headers,json=dict(name='اسم آخر')).status_code,404)

    def test_names_are_unique_and_mutations_require_csrf(self):
        self.category()
        response=self.client.post(URL,headers=self.headers,json=dict(kind='category',name='حقائب يد',family='bags'))
        self.assertEqual(response.status_code,400)
        for name in ('','فئة\nأخرى','a'*61):
            self.assertEqual(self.client.post(URL,headers=self.headers,json=dict(kind='group',name=name)).status_code,400)
        self.assertEqual(self.client.post(URL,json=dict(kind='group',name='جديد')).status_code,403)
        for fields in (dict(kind=[]),dict(family=[]),dict(size_system=[])):
            data=dict(kind='category',name='فئة جديدة',family='other');data.update(fields)
            self.assertEqual(self.client.post(URL,headers=self.headers,json=data).status_code,400)
        self.assertEqual(self.client.put(URL+'/category/shoes',headers=self.headers,json=dict(family='bags')).status_code,400)

    def test_rename_and_archive_preserve_products_but_prevent_new_assignments(self):
        category=self.category()
        self.assertEqual(self.create('B1',product_type=category,size_system='none').status_code,201)
        response=self.client.put(URL+'/category/'+category,headers=self.headers,json=dict(name='حقائب المدرسة',active=False))
        self.assertEqual(response.status_code,200,response.json)
        current=self.client.get('/api/products/manage').json['products'][0]
        self.assertEqual(current['product_type_label'],'حقائب المدرسة')
        self.assertEqual(self.client.put('/api/products/manage/B1',headers=self.headers,json=dict(price='17000')).status_code,200)
        self.assertEqual(self.create('B2',product_type=category).status_code,400)
        self.assertEqual(self.client.delete(URL+'/category/'+category,headers=self.headers).status_code,400)
        self.assertEqual(self.client.put(URL+'/category/'+category,headers=self.headers,json=dict(family='shoes',size_system='shoe')).status_code,400)

    def test_group_rename_keeps_the_links_after_products_are_saved_again(self):
        response=self.client.post(URL,headers=self.headers,json=dict(kind='group',name='دوام'))
        group=response.json['id']
        for pid in ('B1','B2'):self.assertEqual(self.create(pid,recommendation_groups=['دوام'],product_type='bags',size_system='none').status_code,201)
        self.assertEqual(self.client.put(URL+'/group/'+group,headers=self.headers,json=dict(name='حقائب الدوام')).status_code,200)
        for pid in ('B1','B2'):self.client.put('/api/products/manage/'+pid,headers=self.headers,json=dict(price='15000'))
        data=self.client.get('/api/products/manage').json
        self.assertEqual([p['recommendation_groups'] for p in data['products']],[['حقائب الدوام'],['حقائب الدوام']])
        self.assertEqual(len(data['catalog_schema']['groups']),1)
        candidates=product_links.eligible_related_products(data['products'][0],data['products'])
        self.assertEqual([p['product_id'] for p in candidates],['B2'])

    def test_product_staff_can_manage_vocabulary_but_reply_only_staff_cannot(self):
        response=self.client.post('/api/staff',headers=self.headers,json=dict(username='products-test',name='اختبار',password='isolated-password-123',permissions=['products'],active=True))
        self.assertEqual(response.status_code,200,response.json)
        staff=self.client.get('/api/staff').json['accounts'][0]
        with self.client.session_transaction() as session:session.update(staff_id=staff['id'],staff_version=1)
        self.assertEqual(self.client.post(URL,headers=self.headers,json=dict(kind='group',name='مسموح')).status_code,201)
        db=m.get_db();db.execute('UPDATE staff_accounts SET permissions=? WHERE id=?',(json.dumps(['reply']),staff['id']));db.commit()
        self.assertEqual(self.client.get('/product-categories').status_code,403)
        self.assertEqual(self.client.post(URL,headers=self.headers,json=dict(kind='group',name='ممنوع')).status_code,403)


class AccessorySalesFacts(unittest.TestCase):
    def test_shoe_sizes_never_gain_adult_weight_labels(self):
        shoe=dict(product_type='shoes',size_system='shoe',sizes='36 إلى 40',audience='women')
        self.assertEqual(product_variants.owner_adult_labels(shoe),())
        self.assertTrue(product_variants.order_choice_error(shoe,'size','XL'))
        self.assertTrue(product_variants.order_choice_error(shoe,'size','44'))
        self.assertEqual(product_variants.order_choice_error(shoe,'size','38'),'')
        with self.assertRaises(ValueError):catalog_profile.normalize(dict(shoe,size_system='weight'),strict=True)

    def test_bag_does_not_ask_for_clothing_size_or_body_weight(self):
        bag=dict(product_type='bags',size_system='none',colors='اسود، بيج',sizes='')
        for answer in (sales_playbook.price_next_step(bag,{},{}),owner_sales.next_step(bag,{},{})):
            self.assertIn('لون',answer)
            self.assertNotIn('قياس',answer);self.assertNotIn('وزن',answer)

    def test_recommendation_group_does_not_mix_bags_with_shoes(self):
        source=dict(product_id='B1',product_type='bags',size_system='none',sizes='',price='15000',stock='متوفر',recommendation_groups=['دوام'],store_id='default')
        bag=dict(source,product_id='B2');shoe=dict(source,product_id='S1',product_type='shoes',size_system='shoe',sizes='36 إلى 40')
        self.assertEqual([p['product_id'] for p in product_links.eligible_related_products(source,[source,shoe,bag])],['B2'])
        self.assertEqual([p['product_id'] for p in catalog_profile.discovery_candidates('اريد حقيبة',[source,shoe])],['B1'])

    def test_child_shoe_age_requires_foot_size_without_human_pause_or_guessed_fit(self):
        shoe=dict(product_type='shoes',size_system='shoe',sizes='26 إلى 32',stock='متوفر')
        result=child_availability.reply(shoe,[shoe],'ابني عمره 5 سنوات')
        self.assertIn('قياس الحذاء',result['reply']);self.assertFalse(result.get('requires_human'))
        self.assertNotIn('يناسب',result['reply'])
        self.assertIsNone(child_availability.reply(shoe,[shoe],'عمره 5 سنوات قياس 28'))

    def test_half_shoe_size_is_not_truncated_or_inferred_from_an_integer_range(self):
        shoe=dict(product_type='shoes',size_system='shoe',sizes='37.5، 38، 39')
        self.assertEqual(product_variants.requested('قياس 37.5')['sizes'],['37.5'])
        self.assertEqual(product_variants.order_choice_error(shoe,'size','37.5'),'')
        self.assertTrue(product_variants.order_choice_error(dict(shoe,sizes='37.5'),'size','37'))
        self.assertTrue(product_variants.order_choice_error(dict(shoe,sizes='36 إلى 40'),'size','37.5'))
        context=sales_context.advance({},[dict(direction='outgoing',text='شنو قياس الحذاء المطلوب؟'),dict(direction='incoming',text='37.5')])
        self.assertEqual(sales_playbook.customer_selection(context)['size'],'37.5')


class AccessoryCheckout(unittest.TestCase):
    setUpClass=classmethod(pipeline.Last50PipelineTests.setUpClass.__func__)
    tearDownClass=classmethod(pipeline.Last50PipelineTests.tearDownClass.__func__)
    setUp=pipeline.Last50PipelineTests.setUp
    tearDown=pipeline.Last50PipelineTests.tearDown
    link=pipeline.Last50PipelineTests.link
    save=pipeline.Last50PipelineTests.save
    process=pipeline.Last50PipelineTests.process
    order=pipeline.Last50PipelineTests.order

    def setup_item(self,kind):
        self.link('اسود، بيج')
        self.product.update(product_type=kind,product_family=kind,product_name='حقيبة اختبار' if kind=='bags' else 'حذاء اختبار',
            size_system='none' if kind=='bags' else 'shoe',sizes='' if kind=='bags' else '36 إلى 40',price='15000',offer='',audience='women')
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')

    def test_bag_books_after_color_phone_and_address_without_asking_a_size(self):
        self.setup_item('bags');self.save('incoming','اريد الحقيبة الاسود احجزيها')
        self.save('outgoing','دزيلي رقم الموبايل والعنوان حتى أثبتلج الحجز')
        result,_=self.process('07701234567')
        self.assertIn('العنوان',result['reply']);self.assertNotIn('قياس',result['reply'])
        result,model=self.process('بغداد حي الاختبار قرب مدرسة الاختبار')
        model.assert_not_called();self.assertEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        item=json.loads(self.order()['order_items'])[0]
        self.assertEqual(item['color'],'أسود');self.assertEqual(item.get('size') or '','')

    def test_shoe_price_asks_foot_size_then_selected_size_books(self):
        self.setup_item('shoes')
        result,model=self.process('شكد سعره')
        model.assert_not_called();self.assertIn('قياس الحذاء',result['reply']);self.assertNotIn('وزن',result['reply'])
        self.save('incoming','اريد اسود قياس 38 احجزي');self.save('outgoing','دزيلي الرقم والعنوان حتى أثبتلج الحجز')
        result,model=self.process('07701234567 بغداد حي الاختبار قرب مدرسة الاختبار')
        model.assert_not_called();self.assertEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(json.loads(self.order()['order_items'])[0]['size'],'38')

    def test_unavailable_shoe_size_does_not_book_or_get_replaced(self):
        self.setup_item('shoes');self.save('incoming','احجزي اسود قياس 44')
        result,_=self.process('07701234567 بغداد حي الاختبار قرب مدرسة الاختبار')
        self.assertIsNone(self.order());self.assertNotIn('تم تثبيت الطلب',result['reply'])

    def test_half_shoe_size_books_the_exact_declared_variant(self):
        self.setup_item('shoes');self.product['sizes']='37.5، 38، 39'
        self.save('incoming','احجزي اسود قياس 37.5');self.save('outgoing','دزيلي الرقم والعنوان')
        result,model=self.process('07701234567 بغداد حي الاختبار قرب مدرسة الاختبار')
        model.assert_not_called();self.assertEqual(result['reply'],self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(json.loads(self.order()['order_items'])[0]['size'],'37.5')

    def test_bag_color_choice_requests_contacts_without_waiting_for_a_garment_size(self):
        self.setup_item('bags')
        result,_=self.process('اريد الاسود')
        self.assertIn('رقم',result['reply']);self.assertIn('العنوان',result['reply'])
        self.assertNotIn('قياس',result['reply']);self.assertNotIn('وزن',result['reply']);self.assertIsNone(self.order())

    def test_bag_alternative_uses_the_group_without_requesting_a_size(self):
        self.setup_item('bags');self.product.update(recommendation_groups=['دوام'],image_url='https://images.test/bag1.jpg')
        other=dict(self.product,product_id='B2',product_name='حقيبة بديلة',image_url='https://images.test/bag2.jpg')
        catalog=[self.product,other]
        self.save('incoming','هاي عجبتني');self.save('outgoing','شنو اللون اللي يعجبج؟')
        with patch.object(self.m,'load_active_products',return_value=catalog),patch.object(self.m,'load_products_from_file',return_value=catalog):
            result,model=self.process('دزيلي موديل ثاني')
        model.assert_not_called();self.assertNotIn('قياس',result['reply']);self.assertNotIn('عمر',result['reply'])
        self.assertIn('B2',((result.get('meta') or {}).get('catalog_page') or {}).get('product_ids') or [])


if __name__=='__main__':unittest.main()
