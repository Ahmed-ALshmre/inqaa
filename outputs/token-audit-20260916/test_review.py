from pathlib import Path
p=Path('account_app/tests/test_checkout_regressions.py');s=p.read_text(encoding='utf-8').replace('            self.assertEqual([p["product_id"] for p in self.m.load_customer_products(self.db, self.sender)], [CATALOG[0]["product_id"]])','            self.assertEqual(self.m.load_customer_products(self.db, self.sender), [])',1);p.write_text(s,encoding='utf-8')
