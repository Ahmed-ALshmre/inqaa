"""Product editor round trips for owner-selected sales links and cues."""
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import account_app.app as m


class ProductEditorLinkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(m.__file__).resolve().parents[1])
        root = Path(self.temp.name)
        self.catalog = root / "products.json"
        self.catalog.write_text("[]", encoding="utf-8")
        self.uploads = root / "uploads"
        self.uploads.mkdir()
        self.paths = patch.multiple(m, DB_PATH=str(root / "sales.db"),
                                    PRODUCTS_FILE=str(self.catalog), UPLOADS_DIR=str(self.uploads))
        self.paths.start()
        m.init_db()
        self.ctx = m.app.app_context()
        self.ctx.push()
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session["dashboard_authenticated"] = True
            session["csrf_token"] = "test"
        self.headers = {"X-CSRF-Token": "test"}

    def tearDown(self):
        self.ctx.pop()
        self.paths.stop()
        self.temp.cleanup()

    def create(self, code, **fields):
        payload = dict(product_id=code, product_name="فستان " + code,
                       store_id="default", status="active", stock="متوفر", price="16000")
        payload.update(fields)
        return self.client.post("/api/products/manage", json=payload, headers=self.headers)

    def saved(self):
        return {p["product_id"]: p for p in json.loads(self.catalog.read_text(encoding="utf-8"))}

    def test_manual_links_and_sales_cues_round_trip_reciprocally(self):
        self.assertEqual(self.create("P1").status_code, 201)
        self.assertEqual(self.create("P2").status_code, 201)
        result = self.client.put("/api/products/manage/P1", headers=self.headers, json={
            "related_product_ids": ["P2"], "sales_cues": ["limited_stock", "high_demand"]})
        self.assertEqual(result.status_code, 200, result.get_json())
        saved = self.saved()
        self.assertEqual(saved["P1"]["related_product_ids"], ["P2"])
        self.assertEqual(saved["P2"]["related_product_ids"], ["P1"])
        self.assertEqual(saved["P1"]["sales_cues"], ["limited_stock", "high_demand"])
        visible = {p["product_id"]: p for p in self.client.get("/api/products/manage").json["products"]}
        self.assertEqual(visible["P1"]["related_product_ids"], ["P2"])
        self.assertEqual(visible["P1"]["sales_cues"], ["limited_stock", "high_demand"])

    def test_bad_link_or_cue_does_not_change_catalog(self):
        self.create("P1")
        before = self.catalog.read_bytes()
        for update in ({"related_product_ids": ["P1"]},
                       {"related_product_ids": ["missing"]},
                       {"related_product_ids": [42]},
                       {"sales_cues": ["many_purchases", "invented"]}):
            result = self.client.put("/api/products/manage/P1", headers=self.headers, json=update)
            self.assertEqual(result.status_code, 400, result.get_json())
            self.assertEqual(self.catalog.read_bytes(), before)

    def test_delete_and_import_clean_links(self):
        self.create("P1")
        self.create("P2")
        self.client.put("/api/products/manage/P1", headers=self.headers,
                        json={"related_product_ids": ["P2"]})
        self.assertEqual(self.client.delete("/api/products/manage/P1", headers=self.headers).status_code, 200)
        self.assertEqual(self.saved()["P2"]["related_product_ids"], [])
        imported = [dict(product_id="P1", product_name="فستان 1",
                         related_product_ids=["P2"], sales_cues=["many_purchases"]),
                    dict(product_id="P2", product_name="فستان 2")]
        response = self.client.post("/api/import/products", headers=self.headers,
                                    data={"file": (io.BytesIO(json.dumps(imported).encode()), "products.json")})
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(self.saved()["P2"]["related_product_ids"], ["P1"])
        self.assertEqual(self.saved()["P1"]["sales_cues"], ["many_purchases"])

    def test_explicit_similar_dress_request_uses_only_owner_link_and_candidate_photo(self):
        first = dict(product_id='P1', product_name='فستان دانتيل', store_id='default',
                     category='فستان', status='active', stock='متوفر', price='16000',
                     sizes='38 إلى 52', related_product_ids=['P2'],
                     image_url='https://images.test/first.png')
        second = dict(product_id='P2', product_name='فستان رباط', store_id='default',
                      category='فستان', status='active', stock='متوفر', price='15000',
                      sizes='38 إلى 52', fabric='باربي',
                      image_url='https://images.test/second.png')
        result = m.owner_linked_discovery_reply(
            {'text': 'أريد أشوف فستان ثاني قريب منه'}, first, [first, second], {})
        self.assertEqual(result['image_product_ids'], ['P2'])
        self.assertFalse(result['create_order'])
        self.assertIn('اختيارج فد شي راقي', result['reply'])
        self.assertIn('فستان رباط', result['reply'])
        images = m.requested_reply_images(result, [first, second], first,
                                          'أريد أشوف فستان ثاني قريب منه', result['reply'])
        self.assertEqual(images, ['https://images.test/second.png'])

    def test_only_delivered_related_offer_is_resolved_for_photo_followup(self):
        db = m.get_db()
        sender = 'default::synthetic-linked-photo'
        m.get_or_create_customer(db, sender, 'page', 'facebook')
        m.save_message(db, sender, 'outgoing', 'text', 'عدنا فستان ثاني',
                       None, None, None, {'related_product_suggestion': {
                           'source_product_id': 'P1', 'product_ids': ['P2']},
                           '_delivery_status': 'accepted'})
        self.assertEqual(m.last_related_suggestion_ids(db, sender), ['P2'])

    def test_assistant_suggestion_does_not_replace_customer_selection(self):
        chosen = {"product_id": "P1", "product_name": "فستان أسود", "related_product_ids": ["P2"]}
        suggested = {"product_id": "P2", "product_name": "فستان كريمي"}
        self.assertTrue(m.is_unselected_related_suggestion(suggested, chosen, "شكد سعر الأسود؟"))
        self.assertFalse(m.is_unselected_related_suggestion(suggested, chosen, "أريد فستان كريمي"))
        self.assertEqual(m.named_related_suggestions("عدنا فستان كريمي", chosen, [chosen, suggested]), ["P2"])
