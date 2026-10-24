"""Manual links use current catalog facts; no customer/provider/transport calls."""
import copy
from html.parser import HTMLParser
from pathlib import Path
import shutil
import subprocess
import unittest

from account_app import product_links


def garment(pid):
    return dict(product_id=pid, product_name='فستان نسائي ' + pid, store_id='default',
                category='فساتين نسائية', sizes='38، 40، 42، 44', price='18000',
                stock='متوفر', status='active')


class ProductLinkTests(unittest.TestCase):
    def setUp(self):
        self.source = dict(garment('A'), related_product_ids=['B'])
        self.other = garment('B')

    def candidates(self, candidate=None, **kwargs):
        return product_links.eligible_related_products(self.source, [self.source, candidate or self.other], **kwargs)

    def test_normalization_does_not_mutate_catalog_and_ignores_invalid_legacy_values(self):
        self.assertEqual(product_links.validate_related_ids([' B ', 'B', ''], 'A', 'default', [self.other]), ['B'])
        self.assertNotIn('related_product_ids', self.other)
        self.assertEqual(product_links.normalize_related_ids('B'), [])
        self.assertEqual(product_links.normalize_related_ids(None), [])

    def test_self_cross_store_unknown_and_wrong_types_are_rejected(self):
        foreign = dict(self.other, product_id='X', store_id='other')
        for value in [['A'], ['X'], ['MISSING'], 'B', {'id':'B'}, [1], [False]]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                product_links.validate_related_ids(value, 'A', 'default', [self.source, self.other, foreign])

    def test_matching_model_is_an_option_never_a_cart_line(self):
        result = self.candidates(size='44')
        self.assertEqual([p['product_id'] for p in result], ['B'])
        self.assertNotIn('quantity', result[0])

    def test_local_id_cannot_be_overridden_by_same_id_in_foreign_store(self):
        result = product_links.eligible_related_products(self.source, [self.other, dict(self.other, store_id='other', stock='نفذ')])
        self.assertEqual([p['product_id'] for p in result], ['B'])

    def test_unlinked_or_stale_ids_never_produce_suggestions(self):
        self.source['related_product_ids'] = ['MISSING']
        self.assertEqual(self.candidates(), [])

    def test_link_cannot_override_current_size_stock_store_or_category(self):
        for change in [dict(sizes='38، 40'), dict(stock='نفذ'), dict(stock=''), dict(status='inactive'),
                       dict(stock_quantity=0), dict(stock_quantity='٠'), dict(stock_quantity='nan'),
                       dict(category='تنانير نسائية'), dict(store_id='other'), dict(price='غير معروف')]:
            with self.subTest(change=change):
                self.assertEqual(self.candidates(dict(self.other, **change), size='44'), [])

    def test_inactive_link_remains_editable_but_is_never_suggested(self):
        other = dict(self.other, status='inactive')
        self.assertEqual(product_links.validate_related_ids(['B'], 'A', 'default', [other]), ['B'])
        self.assertFalse(self.candidates(other))

    def test_unknown_nonoverlapping_and_weight_only_sizes_fail_closed(self):
        for sizes in ['', 'وزن 50 إلى 100 كيلو', '46، 48، 50']:
            with self.subTest(sizes=sizes):
                self.assertFalse(self.candidates(dict(self.other, sizes=sizes)))
                self.assertFalse(self.candidates(dict(self.other, sizes=sizes), size='44'))

    def test_letter_size_and_category_family_are_supported(self):
        self.source['sizes'] = 'M / L / XL'
        self.assertTrue(self.candidates(dict(self.other, sizes='L / XL', category='فستان نسائي'), size='XL'))
        self.assertFalse(self.candidates(dict(self.other, sizes='S / M'), size='XL'))

    def test_boys_request_rejects_girls_and_unknown_audience(self):
        self.source.update(product_name='سوت ولادي', category='سوت ولادي', sizes='3 إلى 8 سنوات')
        for category in ['سوت بناتي', 'سوت أطفال']:
            other = dict(self.other, product_name=category, category=category, sizes='3 إلى 8 سنوات')
            self.assertFalse(self.candidates(other, age_years=6, audience='boys'))
            self.assertFalse(self.candidates(other, age_years=6))

    def test_matching_child_age_and_audience_and_source_range_overlap(self):
        self.source.update(product_name='سوت ولادي', category='سوت ولادي', sizes='3 إلى 8 سنوات')
        other = dict(self.other, product_name='سوت ولادي مختلف', category='طقم ولادي', sizes='5 إلى 9 سنوات')
        self.assertTrue(self.candidates(other, age_years=6))
        self.assertFalse(self.candidates(other, age_years=3))
        self.assertFalse(self.candidates(dict(other, sizes=''), age_years=6))

    def test_womens_source_rejects_childrens_dress(self):
        child = dict(self.other, product_name='فستان بناتي', category='فستان بناتي', sizes='3 إلى 8 سنوات')
        self.assertFalse(self.candidates(child))

    def test_unknown_source_audience_does_not_confidently_offer_girls_product(self):
        self.source.update(product_name='فستان', category='فساتين', sizes='')
        child = dict(self.other, product_name='فستان بناتي', category='فستان بناتي', sizes='3 إلى 8 سنوات')
        self.assertFalse(self.candidates(child))

    def test_english_women_does_not_accidentally_match_men(self):
        self.source.update(product_name='Womens dress', category='women dresses')
        self.assertTrue(self.candidates(dict(self.other, product_name='Women dress', category='women dress')))
        self.assertFalse(self.candidates(dict(self.other, product_name='Men dress', category='men dress')))

    def test_explicit_age_overrides_a_broad_overlap(self):
        self.source.update(product_name='سوت بناتي', category='سوت بناتي', sizes='1 إلى 7 سنوات')
        other = dict(self.other, product_name='سوت بناتي مختلف', category='سوت بناتي', sizes='1 إلى 3 سنوات')
        self.assertTrue(self.candidates(other))
        self.assertFalse(self.candidates(other, age_years=7))

    def test_order_is_preserved_at_most_two_without_mutation(self):
        self.source['related_product_ids'] = ['C','B','D','C']
        catalog = [self.source, self.other, garment('C'), garment('D')]
        original = copy.deepcopy(catalog)
        result = product_links.eligible_related_products(self.source, catalog, limit=20)
        self.assertEqual([p['product_id'] for p in result], ['C','B'])
        result[0]['product_name'] = 'changed'
        self.assertEqual(catalog, original)

    def test_invalid_age_and_unknown_audience_do_not_become_fit_claims(self):
        for kwargs in [dict(age_years='unknown'), dict(age_years=float('nan')), dict(age_years=-1), dict(audience='unknown')]:
            with self.subTest(kwargs=kwargs):
                self.assertFalse(self.candidates(**kwargs))


class ProductEditorBoundaryTests(unittest.TestCase):
    def test_real_template_has_unique_editor_controls_and_exact_manual_cue_values(self):
        class Controls(HTMLParser):
            def __init__(self):
                super().__init__()
                self.ids, self.cues = [], []
            def handle_starttag(self, tag, attrs):
                values = dict(attrs)
                if values.get('id'):
                    self.ids.append(values['id'])
                if tag == 'input' and values.get('name') == 'sales_cues':
                    self.cues.append(values.get('value'))
        parser = Controls()
        parser.feed((Path(__file__).resolve().parents[1] / 'templates/products.html').read_text(encoding='utf-8'))
        self.assertEqual(len(parser.ids), len(set(parser.ids)))
        self.assertTrue({'relatedProductsSearch', 'relatedProductsOptions', 'relatedProductsSelected',
                         'relatedProductsCount', 'productSalesCues'}.issubset(parser.ids))
        self.assertEqual(set(parser.cues), {'limited_stock', 'high_demand', 'frequent_bookings', 'many_purchases'})

    @unittest.skipUnless(shutil.which('node'), 'Node is required for editor boundary verification')
    def test_editor_search_preserves_selection_and_roundtrips_cues_without_html_injection(self):
        root = Path(__file__).resolve().parents[1]
        script = r'''
const fs=require('fs'), vm=require('vm'), assert=require('assert');
class Element {
 constructor(){this.value='';this.children=[];this.listeners={};this.style={};this.checked=false;}
 append(...x){this.children.push(...x);} replaceChildren(...x){this.children=x;}
 addEventListener(k,v){this.listeners[k]=v;} setAttribute(k,v){this[k]=v;}
 reset(){for(const x of nodes.values()){x.value='';x.checked=false;}for(const x of cues)x.checked=false;}
}
const nodes=new Map(), cues=['limited_stock','high_demand','frequent_bookings','many_purchases'].map(value=>Object.assign(new Element(),{value}));
const document={getElementById(id){if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);},createElement(){return new Element();},addEventListener(){},querySelectorAll(s){return s.endsWith(':checked')?cues.filter(x=>x.checked):cues;}};
const sandbox={document,window:{location:{search:'?store_id=default'}},URLSearchParams,Set,console};vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(process.argv[1],'utf8'),sandbox);
vm.runInContext(`allProducts=[
{product_id:'A',store_id:'default',product_name:'الأصل',related_product_ids:['B'],sales_cues:['high_demand'],stock_quantity:0,status:'active'},
{product_id:'B',store_id:'default',product_name:'موديل <img onerror=x>',category:'فساتين',sizes:'44',status:'active'},
{product_id:'C',store_id:'default',product_name:'الثالث',sizes:'46',status:'inactive'},
{product_id:'X',store_id:'other',product_name:'متجر آخر',status:'active'}];editProduct('A');`,sandbox);
assert.strictEqual(document.getElementById('relatedProductsOptions').children.length,2);
assert.strictEqual(document.getElementById('productStockQuantity').value,'0');
assert.strictEqual(document.getElementById('relatedProductsSelected').children[0].textContent,'موديل <img onerror=x> ×');
document.getElementById('relatedProductsSearch').value='46';vm.runInContext('renderRelatedProducts()',sandbox);
const row=document.getElementById('relatedProductsOptions').children[0];row.children[0].checked=true;row.children[0].listeners.change();
let data=JSON.parse(vm.runInContext('JSON.stringify(collectPayload())',sandbox));
assert.deepStrictEqual(data.related_product_ids,['B','C']);assert.deepStrictEqual(data.sales_cues,['high_demand']);
vm.runInContext('newProduct()',sandbox);data=JSON.parse(vm.runInContext('JSON.stringify(collectPayload())',sandbox));
assert.deepStrictEqual(data.related_product_ids,[]);assert.deepStrictEqual(data.sales_cues,[]);
'''
        result = subprocess.run([shutil.which('node'), '-e', script, str(root / 'static/js/products.js')],
                                capture_output=True, text=True, encoding='utf-8', timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
