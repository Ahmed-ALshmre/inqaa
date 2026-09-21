import unittest
from account_app.pricing import quote, money, expand_bundles

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
