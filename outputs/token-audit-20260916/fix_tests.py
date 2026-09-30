from pathlib import Path
p=Path('account_app/tests/test_checkout_regressions.py');s=p.read_text(encoding='utf-8').replace('        self.assertEqual(ai.call_args.args[5]["product_id"], selected["product_id"])','''        if fallback:
            ai.assert_not_called()
            vision.assert_not_called()
            review.assert_called_once()
            self.assertEqual([p["product_id"] for p in self.m.load_customer_products(self.db, self.sender)], [CATALOG[0]["product_id"]])
            return
        self.assertEqual(ai.call_args.args[5]["product_id"], selected["product_id"])''',1);p.write_text(s,encoding='utf-8')
p=Path('account_app/tests/test_conversation_context.py');s=p.read_text(encoding='utf-8').replace('self.assertEqual(vision.call_args.args[0],"https://images.test/async.jpg")','self.assertTrue(vision.call_args.args[0].startswith("data:image/png;base64,"))');p.write_text(s,encoding='utf-8')
