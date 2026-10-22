import unittest
from account_app.pricing import quote, money, expand_bundles, terms, offer_tiers
from account_app import sales_answers, sales_playbook

class BundlePricingTests(unittest.TestCase):
    def setUp(self):
        self.product = dict(product_id="B", price="بكج 3 قطع سعره ب 25 الف", delivery="توصيل مجاني")

    def test_arabic_and_formatted_prices(self):
        for value in ("٢٥ ألف", "25,000", "25000", "٢٥٬٠٠٠"):
            self.assertEqual(money(value), 25000)
        with self.assertRaises(ValueError):
            money("3 قطع 25 الف")

    def test_two_bundles_and_exact_allocation(self):
        result = quote([dict(product_id="B", quantity=6)], [self.product], 5000)
        self.assertEqual(result["product_total"], 50000)
        self.assertEqual(result["delivery_fee"], 0)
        self.assertEqual(sum(i["quantity"] * i["unit_price"] for i in result["items"]), 50000)
        self.assertEqual(sum(i["quantity"] for i in result["items"]), 6)

    def test_incomplete_bundle_is_rejected(self):
        with self.assertRaises(ValueError):
            quote([dict(product_id="B", quantity=2)], [self.product], 5000)

    def test_one_package_with_three_colors_expands_once(self):
        rows = expand_bundles([dict(product_id='B', quantity=1, color='وردي، تركوازي، أزرق ملكي', size='سنتين', notes='بكج 3 قطع')], [self.product])
        self.assertEqual([r['color'] for r in rows], ['وردي', 'تركوازي', 'أزرق ملكي'])
        self.assertEqual(expand_bundles(rows, [self.product]), rows)
        self.assertEqual(quote(rows, [self.product], 5000)['product_total'], 25000)

    def test_package_notes_on_individual_lines_do_not_multiply_pieces(self):
        rows = [dict(product_id='B', quantity=1, color=c, size='سنتين', notes='بكج 3 قطع') for c in ['وردي','تركوازي','أزرق ملكي']]
        self.assertEqual(sum(r['quantity'] for r in expand_bundles(rows, [self.product])), 3)

    def test_mixed_cart_keeps_regular_shipping(self):
        result = quote([dict(product_id="B", quantity=3), dict(product_id="R", quantity=1)],
                       [self.product, dict(product_id="R", price="18000")], 5000)
        self.assertEqual(result["product_total"], 43000)
        self.assertEqual(result["delivery_fee"], 5000)

    def test_two_piece_offer_from_catalog_formats(self):
        products = [
            dict(price='سعر القطعه ب 12000', bundle_contents='بكج قطعتين ب 20 الف سعر القطعه الواحده 12 الف', expected=20000),
            dict(price='9000', bundle_contents='قطعتين ب 16 الف', expected=16000),
            dict(price='15000', offer='عرض سعر القطعتين ب 25 الف', expected=25000),
            dict(price='10000', offer='عرض خاص سعر القطعة ب 10 قطعتين ب 18 الف', expected=18000),
        ]
        for index, product in enumerate(products):
            with self.subTest(index=index):
                product['product_id'] = 'P'
                regular = terms(product)[1]
                self.assertEqual(offer_tiers(product), [(2, product['expected'])])
                for count, expected in [(1, regular), (2, product['expected']),
                                        (3, regular + product['expected']), (4, 2 * product['expected'])]:
                    priced = quote([dict(product_id='P', quantity=count)], [product], 5000)
                    self.assertEqual(priced['product_total'], expected)
                    self.assertEqual(sum(i['quantity'] * i['unit_price'] for i in priced['items']), expected)

    def test_numeric_price_for_package_sku_is_one_sale_unit(self):
        for product in [
            dict(product_id='P', product_name='بكج برنص', price='65000', notes='بكج 6 قطع برنص عدد 2 ومنشفه وخاولي وكفوف عدد 2'),
            dict(product_id='P', product_name='بكج ولادي', price='39000', notes='يباع فقط بكج 3قطع لا يباع مفصول', bundle_contents='قمصلة فروه تراك و طقم جينز'),
        ]:
            with self.subTest(name=product['product_name']):
                self.assertEqual(terms(product)[0], 1)
                self.assertEqual(quote([dict(product_id='P', quantity=1)], [product], 5000)['product_total'], int(product['price']))

    def test_unspecified_offer_does_not_create_discount(self):
        product = dict(product_id='P', price='15000', offer='عرض بدل 20 الف هسه ب 15 الف فقط')
        self.assertEqual(offer_tiers(product), [])
        self.assertEqual(quote([dict(product_id='P', quantity=2)], [product], 5000)['product_total'], 30000)

    def test_conflicting_offer_prices_stop_checkout(self):
        product = dict(product_id='P', price='15000', offer='قطعتين ب 25 الف',
                       bundle_contents='قطعتين ب 24 الف')
        with self.assertRaisesRegex(ValueError, 'متعارضة'):
            quote([dict(product_id='P', quantity=2)], [product], 5000)

    def test_factual_replies_use_offer_total(self):
        product = dict(product_id='P', price='12000', offer='عرض سعر القطعتين ب 20 الف')
        self.assertIn('20,000', ' '.join(sales_answers.product_fragments('عرض القطعتين والقماش شنو؟', product)[0]))
        self.assertIn('20,000', ' '.join(sales_playbook.price_preview(product, [('', 5000)])))
        self.assertEqual(sales_answers.missing_answers('العرض 20 الف، القماش قطن', 'سعر القطعتين والقماش؟', dict(product, fabric='قطن')), [])
        facts, _ = sales_answers.product_fragments('شكد 3 قطع وشنو القماش؟', dict(product, fabric='قطن'))
        self.assertIn('سعر 3 قطع: 32,000 د.ع.', facts)
        self.assertIn('مجموع القطع الصحيح', sales_answers.missing_answers(
            'السعر 36 الف والقماش قطن', 'شكد 3 قطع وشنو القماش؟', dict(product, fabric='قطن')))
