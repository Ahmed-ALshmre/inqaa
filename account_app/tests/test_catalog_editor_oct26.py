"""Structured catalog facts round-trip and constrain real recommendation paths."""
import copy
import io
import json
import unittest
from account_app import catalog_profile as profile, product_links, product_variants
from account_app.tests import test_product_editor_links_oct24 as fixture


def dress(pid, **fields):
    return dict(dict(product_id=pid, product_name='فستان '+pid, store_id='default', price='13000',
        stock='متوفر', status='active', product_type='dresses', audience='girls', colors='وردي',
        size_system='age', age_min_months=12, age_max_months=84, sizes='12 إلى 84 أشهر',
        recommendation_groups=['عيد ميلاد'], occasions=['birthday']), **fields)


class CatalogProfileTests(unittest.TestCase):
    def test_age_range_keeps_exact_months_and_uses_existing_variant_checks(self):
        p = profile.normalize(dress('A', age_min_months=6, age_max_months=18), strict=True)
        self.assertEqual(p['sizes'], '6 إلى 18 أشهر')
        checks = product_variants.assess(p, 'عمر سنة ونص')
        self.assertTrue(checks[0]['supported'])
        self.assertFalse(product_variants.assess(p, 'عمر سنتين')[0]['supported'])

    def test_legacy_discrete_sizes_are_never_expanded_to_a_range(self):
        p = profile.normalize(dict(product_id='A', sizes='38، 42، 46'))
        self.assertEqual(p['size_system'], 'legacy')
        self.assertEqual(p['sizes'], '38، 42، 46')
        self.assertIsNone(p['age_min_months'])

    def test_invalid_metadata_rejected_without_mutating_input(self):
        original = dress('A')
        for change in (dict(age_min_months=25, age_max_months=12), dict(age_min_months=True),
                       dict(age_max_months=18.5), dict(age_max_months=float('nan')),
                       dict(audience='invented'), dict(occasions=['unknown']),
                       dict(recommendation_groups='birthday'), dict(recommendation_priority=True),
                       dict(recommendation_priority=8), dict(recommendation_enabled='false')):
            p = dict(original, **change); before = copy.deepcopy(p)
            with self.subTest(change=change), self.assertRaises(ValueError): profile.normalize(p, strict=True)
            for key in p:
                if key != 'age_max_months': self.assertEqual(p[key], before[key])

    def test_adult_weight_and_children_age_are_distinct(self):
        p = profile.normalize(dress('A', audience='women', size_system='weight', weight_min_kg=60,
                                    weight_max_kg=90), strict=True)
        self.assertEqual(p['sizes'], 'وزن 60 إلى 90 كيلو')
        self.assertEqual(product_variants.size_data(p['sizes'])['weights'], [(60, 90)])
        for p in (dress('A', audience='women'), dress('A', size_system='weight', weight_min_kg=60, weight_max_kg=90)):
            with self.assertRaises(ValueError): profile.normalize(p, strict=True)

    def test_no_size_is_explicit_and_clears_previous_size(self):
        self.assertEqual(profile.normalize(dress('A', size_system='none'), strict=True)['sizes'], '')

    def test_structured_classification_wins_over_ambiguous_legacy_names(self):
        a = dress('A', related_product_ids=['B'], product_name='موديل 1', category='قديم')
        b = dress('B', product_name='موديل 2', category='مختلف')
        self.assertEqual(product_links._audience(a), 'girls')
        self.assertEqual(product_links._category(a), 'dresses')
        self.assertEqual([p['product_id'] for p in product_links.eligible_related_products(a, [a,b], age_years=4)], ['B'])

    def test_generic_legacy_children_category_never_mixes_dress_with_set(self):
        a=dress('A',category='اطفال',product_type='')
        b=dress('B',category='اطفال',product_type='')
        c=dress('C',category='اطفال',product_type='',product_name='طقم بناتي')
        self.assertEqual([p['product_id'] for p in product_links.eligible_related_products(a,[a,b,c],age_years=4)],['B'])

    def test_unknown_type_never_becomes_a_verified_related_or_budget_alternative(self):
        from account_app import sales_decision
        a=dress('A',category='اطفال',product_type='',product_name='موديل أ',price='18000')
        b=dress('B',category='اطفال',product_type='',product_name='موديل ب',price='13000')
        self.assertFalse(product_links.eligible_related_products(a,[a,b],age_years=4))
        self.assertFalse(sales_decision.budget_alternatives('اريد ارخص عمر 4 سنوات',{},[a,b],a,5000))

    def test_shared_groups_link_without_crossing_audience_type_or_store(self):
        a, b = dress('A'), dress('B')
        good = product_links.eligible_related_products(a, [a,b], age_years=4)
        self.assertEqual([p['product_id'] for p in good], ['B'])
        for changed in (dict(audience='boys'), dict(product_type='sets'), dict(store_id='other'),
                        dict(age_max_months=24, sizes='12 إلى 24 أشهر'), dict(stock_quantity=0),
                        dict(stock='نفذ'), dict(recommendation_enabled=False)):
            self.assertFalse(product_links.eligible_related_products(a, [a,dict(b,**changed)], age_years=4), changed)

    def test_explicit_unisex_child_can_be_offered_for_a_boy_but_not_woman(self):
        a = dress('A', audience='boys'); b = dress('B', audience='children')
        self.assertTrue(product_links.eligible_related_products(a,[a,b], age_years=3))
        b['audience'] = 'women'
        self.assertFalse(product_links.eligible_related_products(a,[a,b], age_years=3))

    def test_priority_sorts_valid_candidates_only_without_mutation(self):
        a, b, c = dress('A'), dress('B', recommendation_priority=1), dress('C', recommendation_priority=5)
        original = copy.deepcopy([a,b,c])
        self.assertEqual([p['product_id'] for p in product_links.eligible_related_products(a,[a,b,c])],['C','B'])
        self.assertEqual([a,b,c],original)

    def test_discovery_checks_audience_all_ages_occasion_and_opt_out(self):
        items = [dress('A'), dress('B',audience='boys'), dress('C',occasions=['school']),
                 dress('D',recommendation_enabled=False), dress('E', age_min_months=60, sizes='60 إلى 84 أشهر')]
        answer = profile.discovery_candidates('اريد بناتي لعيد ميلاد عمر 4 سنوات',items)
        self.assertEqual([p['product_id'] for p in answer],['A'])
        self.assertFalse(profile.discovery_candidates('اريد بناتي عمر 4 سنوات وعمر 9 سنوات',items))

    def test_clothing_discovery_requires_the_actual_requested_size(self):
        a = dress('A', audience='women', size_system='clothing', sizes='38، 40')
        b = dress('B', audience='women', size_system='clothing', sizes='42، 44')
        self.assertEqual([p['product_id'] for p in profile.discovery_candidates('نسائي قياس 44',[a,b])],['B'])

    def test_budget_suggestions_use_audience_type_and_opt_out_even_without_legacy_category(self):
        from account_app import sales_decision
        a=dress('A',price='18000',category='')
        b=dress('B',price='13000',category='')
        self.assertEqual([p['product_id'] for p in sales_decision.budget_alternatives(
            'اريد ارخص عمر سنة ونص',{},[a,b],a,5000)],['B'])
        for change in (dict(recommendation_enabled=False),dict(audience='boys'),dict(product_type='sets')):
            self.assertFalse(sales_decision.budget_alternatives('اريد ارخص عمر سنة ونص',{},
                [a,dict(b,**change)],a,5000))


class CatalogEditorApiTests(unittest.TestCase):
    setUp = fixture.ProductEditorLinkTests.setUp
    tearDown = fixture.ProductEditorLinkTests.tearDown
    create = fixture.ProductEditorLinkTests.create
    saved = fixture.ProductEditorLinkTests.saved

    def test_new_fields_round_trip_with_canonical_age_and_old_facts(self):
        data = dress('A', lining='مبطن بالكامل', order_name='طلب خاص')
        self.assertEqual(self.create('A', **{k:v for k,v in data.items() if k != 'product_id'}).status_code,201)
        saved = self.saved()['A']
        for key in profile.FIELDS:
            if key in data: self.assertEqual(saved[key], data[key])
        self.assertEqual(saved['sizes'],'12 إلى 84 أشهر')
        listing = self.client.get('/api/products/manage').json['products'][0]
        self.assertEqual(listing['catalog_meta']['audience'],'girls')
        self.assertEqual(listing['lining'],'مبطن بالكامل')
        self.client.put('/api/products/manage/A',headers=self.headers,json=dict(occasions=['eid']))
        self.assertEqual(self.saved()['A']['order_name'],'طلب خاص')

    def test_invalid_create_or_update_does_not_write_or_lose_legacy_data(self):
        self.create('A', sizes='38، 42', notes='معلومة قديمة')
        before = self.catalog.read_bytes()
        for change in (dict(audience='strange'),dict(size_system='age',age_min_months=30,age_max_months=12),
                       dict(recommendation_groups=['صيف',123]),dict(recommendation_enabled='false')):
            response=self.client.put('/api/products/manage/A',headers=self.headers,json=change)
            self.assertEqual(response.status_code,400,response.json)
            self.assertEqual(self.catalog.read_bytes(),before)

    def test_legacy_product_save_preserves_exact_discrete_options(self):
        self.create('A',sizes='38، 42، 46',image_colors={},related_product_ids=[])
        self.client.put('/api/products/manage/A',headers=self.headers,json={'product_name':'اسم معدل'})
        self.assertEqual(self.saved()['A']['sizes'],'38، 42، 46')

    def seed_group(self):
        for pid in ('A','B','C'):
            fields=dress(pid, recommendation_enabled=pid!='C')
            self.assertEqual(self.create(pid, **{k:v for k,v in fields.items() if k!='product_id'}).status_code,201)

    def test_preview_uses_unsaved_groups_and_never_writes_catalog_orders_or_messages(self):
        self.seed_group();before=self.catalog.read_bytes()
        response=self.client.post('/api/products/recommendations/preview',headers=self.headers,
            json=dict(product=dress('A'),audience='girls',age_months=48,occasion='birthday'))
        self.assertEqual(response.status_code,200,response.json)
        self.assertEqual([p['product_id'] for p in response.json['products']],['B'])
        self.assertEqual(self.catalog.read_bytes(),before)
        from account_app.app import get_db
        db=get_db()
        self.assertEqual(db.execute('SELECT COUNT(*) FROM orders').fetchone()[0],0)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM messages').fetchone()[0],0)

    def test_preview_rejects_cross_store_unknown_links_and_invalid_age(self):
        self.seed_group()
        for change in (dict(age_months=-1),dict(age_months=1.5),dict(audience='other'),
                       dict(product=dress('A',related_product_ids=['outside']))):
            payload=dict(product=dress('A'),age_months=48);payload.update(change)
            self.assertEqual(self.client.post('/api/products/recommendations/preview',headers=self.headers,
                                             json=payload).status_code,400)

    def test_import_validates_new_fields_and_preserves_groups(self):
        items=[dress('A'),dress('B')]
        def upload(data):
            return self.client.post('/api/import/products',headers=self.headers,
                data={'file':(io.BytesIO(json.dumps(data).encode()),'products.json')})
        self.assertEqual(upload(items).status_code,200)
        self.assertEqual(self.saved()['A']['recommendation_groups'],['عيد ميلاد'])
        before=self.catalog.read_bytes();items[0]['audience']='invalid'
        self.assertEqual(upload(items).status_code,400)
        self.assertEqual(self.catalog.read_bytes(),before)

    def test_page_renders_real_schema_sections_without_duplicate_controls(self):
        from html.parser import HTMLParser
        class Parser(HTMLParser):
            def __init__(self): super().__init__();self.ids=[]
            def handle_starttag(self,tag,attrs):
                if dict(attrs).get('id'):self.ids.append(dict(attrs)['id'])
        page=self.client.get('/products');self.assertEqual(page.status_code,200)
        self.assertEqual(self.client.get('/products/').status_code,200)
        parser=Parser();parser.feed(page.text)
        self.assertEqual(len(parser.ids),len(set(parser.ids)))
        self.assertTrue({'panel-basics','panel-fit','panel-knowledge','panel-media','panel-links',
                         'productAudience','productGroups','previewRecommendationsBtn'}.issubset(parser.ids))
        self.assertNotIn('databaseImportInput',parser.ids)


if __name__=='__main__':unittest.main()
