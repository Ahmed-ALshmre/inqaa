import unittest
from unittest.mock import patch, mock_open
import json
from account_app import baraah
from account_app.tests.test_chatwoot import ChatwootTests
import account_app.app as m


class BaraahTests(unittest.TestCase):
    setUp = ChatwootTests.setUp
    tearDown = ChatwootTests.tearDown
    post = ChatwootTests.post

    def test_facebook_and_instagram_route_to_childrens_store(self):
        with patch.dict(m.os.environ, CHATWOOT_ACCOUNT_ID="184605"):
            for inbox in (139094, 139098):
                self.payload.update(id=inbox, account={"id": 184605}, inbox={"id": inbox})
                self.payload["conversation"]["id"] = inbox
                with patch.object(m, "_process_manychat_webhook_async") as process:
                    self.assertEqual(self.post().status_code, 200)
                    self.assertEqual(process.call_args.args[1], f"baraah-kids::cw-184605-{inbox}")

    def test_same_inbox_number_in_other_account_is_not_bound(self):
        self.payload["inbox"]["id"] = 139094
        self.assertEqual(self.post().status_code, 422)

    def test_seed_preserves_edits_and_other_stores(self):
        with m.app.app_context():
            db = m.get_db()
            db.execute("UPDATE app_settings SET value='custom description' WHERE key=?",
                       ("store:baraah-kids:store_description",))
            baraah.seed(db, m.now_baghdad_iso())
            self.assertEqual(m.get_app_setting("store_description", db=db, store_id=baraah.STORE_ID), "custom description")
            self.assertEqual(len([s for s in m.list_stores(db) if s['store_id'] == baraah.STORE_ID]), 1)
            self.assertTrue(m.get_store(db, 'default'))

    def test_prompts_do_not_inherit_womens_products_or_global_instructions(self):
        with m.app.app_context():
            db = m.get_db()
            db.execute("INSERT INTO ai_instructions(content,active) VALUES('legacy-women-only',1)")
            token = m._current_store_id.set(baraah.STORE_ID)
            try:
                instructions, rules = m.load_ai_config(db)
                self.assertIn('ملابس الأطفال', instructions)
                self.assertNotIn('legacy-women-only', instructions)
                self.assertNotIn('الدشداشة النسائية', instructions)
                self.assertIn('ملابس الأطفال', m.get_app_setting('prompt_main_rules', db=db))
            finally:
                m._current_store_id.reset(token)

    def test_supported_store_is_visible_and_can_be_created_idempotently(self):
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        response = self.client.post('/api/stores', json={'store_id':baraah.STORE_ID, 'name':baraah.STORE_NAME})
        self.assertEqual(response.status_code, 201)
        stores = self.client.get('/api/stores').get_json()['stores']
        store = next(s for s in stores if s['store_id'] == baraah.STORE_ID)
        self.assertEqual(store['name'], baraah.STORE_NAME)
        self.assertTrue(store['webhook_url'].endswith('/chatwoot/webhook/baraah-kids'))

    def test_catalog_never_falls_back_to_other_stores(self):
        products = [{"product_id":"women", "store_id":"default", "product_name":"سوت نسائي"},
                    {"product_id":"kids", "store_id":baraah.STORE_ID, "product_name":"طقم أطفال"}]
        with patch("builtins.open", mock_open(read_data=json.dumps(products))):
            self.assertEqual([p['product_id'] for p in m.load_products_from_file(store_id=baraah.STORE_ID)], ['kids'])
        with patch("builtins.open", mock_open(read_data=json.dumps(products[:1]))):
            self.assertEqual(m.load_products_from_file(store_id=baraah.STORE_ID), [])

    def test_environment_binding_keeps_precedence(self):
        with patch.dict(m.os.environ, CHATWOOT_ACCOUNT_ID="184605", CHATWOOT_INBOX_STORES=json.dumps({"184605:139094":"khuyoot"})):
            self.payload.update(account={"id":184605}, inbox={"id":139094})
            with patch.object(m, "_process_manychat_webhook_async") as process:
                self.assertEqual(self.post().status_code, 200)
                self.assertTrue(process.call_args.args[1].startswith('khuyoot::'))
