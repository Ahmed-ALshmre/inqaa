import json
import unittest
import uuid
from unittest.mock import patch

from account_app.app import (
    app,
    bind_customer_to_product,
    create_order_if_valid,
    complete_customer_product_link,
    get_active_product_binding,
    get_auto_product_settings,
    get_db,
    infer_explicit_customer_gender,
    load_products_from_file,
    reject_current_binding,
    _should_send_image,
    should_use_auto_product,
    update_customer_intelligence,
)


class SalesCoreTests(unittest.TestCase):
    def setUp(self):
        self.sender_id = "test-suite-" + uuid.uuid4().hex
        self.ctx = app.app_context()
        self.ctx.push()
        self.db = get_db()
        self.db.execute(
            "INSERT INTO customers(sender_id, lead_score, lead_stage) VALUES (?,0,'new')",
            (self.sender_id,),
        )
        self.db.commit()

    def tearDown(self):
        self.db.execute("DELETE FROM orders WHERE sender_id=?", (self.sender_id,))
        self.db.execute("DELETE FROM customer_product_interests WHERE sender_id=?", (self.sender_id,))
        self.db.execute("DELETE FROM customers WHERE sender_id=?", (self.sender_id,))
        self.db.commit()
        self.ctx.pop()

    def test_gender_is_explicit_and_correctable(self):
        self.assertEqual(infer_explicit_customer_gender("اني رجل واشتري لزوجتي"), "male")
        self.assertEqual(infer_explicit_customer_gender("أنا بنت وأريد سوت"), "female")
        self.assertEqual(infer_explicit_customer_gender("أريدها لزوجتي"), "")
        update_customer_intelligence(self.db, self.sender_id, "اني رجل")
        state = update_customer_intelligence(self.db, self.sender_id, "مو رجل، اني بنية")
        self.assertEqual(state["gender"], "female")

    def test_purchase_intent_becomes_hot(self):
        update_customer_intelligence(self.db, self.sender_id, "شكد السعر وهل متوفر؟")
        state = update_customer_intelligence(self.db, self.sender_id, "اريد احجز وهذا رقمي 07701234567")
        self.assertEqual(state["lead_stage"], "hot")
        self.assertGreaterEqual(state["lead_score"], 60)

    def test_booking_fields_are_extracted_and_correctable(self):
        update_customer_intelligence(
            self.db, self.sender_id,
            "اني من بغداد رقمي ٠٧٧٠١٢٣٤٥٦٧ عنواني المنصور قرب الداودي",
        )
        row = self.db.execute(
            "SELECT phone,province,address FROM customers WHERE sender_id=?", (self.sender_id,)
        ).fetchone()
        self.assertEqual(row["phone"], "07701234567")
        self.assertEqual(row["province"], "بغداد")
        self.assertIn("المنصور", row["address"])
        update_customer_intelligence(self.db, self.sender_id, "غيرت رقمي إلى 07801234567")
        row = self.db.execute("SELECT phone FROM customers WHERE sender_id=?", (self.sender_id,)).fetchone()
        self.assertEqual(row["phone"], "07801234567")

    def test_product_binding_can_change_from_any_source(self):
        products = load_products_from_file()
        bind_customer_to_product(self.db, self.sender_id, products[0], source="image_recognition")
        self.assertTrue(reject_current_binding(self.db, self.sender_id, "مو هذا", allow_any_source=True))
        bind_customer_to_product(self.db, self.sender_id, products[1], match_method="customer_correction")
        self.assertEqual(get_active_product_binding(self.db, self.sender_id)["product_id"], "P002")

    def test_new_customer_gets_default_product_for_text_or_image(self):
        self.assertTrue(should_use_auto_product(
            self.db, self.sender_id,
            {"text": "مرحبا", "image_url": "", "ref": "", "ad_id": ""},
            "text", [],
        ))
        self.assertTrue(should_use_auto_product(
            self.db, self.sender_id,
            {"text": "", "image_url": "/tmp/customer.jpg", "ref": "", "ad_id": ""},
            "image", [],
        ))

    def test_ad_customer_also_gets_default_product(self):
        self.assertTrue(should_use_auto_product(
            self.db, self.sender_id,
            {"text": "مرحبا", "image_url": "", "ref": "campaign", "ad_id": "123"},
            "text", [],
        ))

    def test_stale_default_product_is_repaired(self):
        old_id = self.db.execute(
            "SELECT value FROM app_settings WHERE key='auto_product_id'"
        ).fetchone()
        old_id = old_id[0] if old_id else ""
        try:
            self.db.execute(
                "INSERT INTO app_settings(key,value) VALUES('auto_product_id','P999') "
                "ON CONFLICT(key) DO UPDATE SET value='P999'"
            )
            self.db.commit()
            settings = get_auto_product_settings(self.db)
            self.assertTrue(settings["enabled"])
            self.assertEqual(settings["product_id"], "P001")
        finally:
            self.db.execute(
                "INSERT INTO app_settings(key,value) VALUES('auto_product_id',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (old_id,),
            )
            self.db.commit()

    def test_explicit_image_request_can_resend_auto_product_image(self):
        product = load_products_from_file()[0]
        binding = bind_customer_to_product(
            self.db, self.sender_id, product, source="auto_default_product"
        )
        self.db.execute(
            "UPDATE customer_product_interests SET image_sent=1 WHERE id=?",
            (binding["id"],),
        )
        self.db.commit()
        self.assertTrue(_should_send_image(
            self.db, self.sender_id, product,
            {"text": "ممكن ترسلين صورته؟", "ref": "", "ad_id": ""},
        ))

    def test_recognized_image_supersedes_default_binding(self):
        products = load_products_from_file()
        complete_customer_product_link(
            self.db, self.sender_id, products[0], "auto_default_product",
            source="auto_default_product",
        )
        complete_customer_product_link(
            self.db, self.sender_id, products[2], "image_recognition",
            source="image_recognition",
        )
        active = get_active_product_binding(self.db, self.sender_id)
        self.assertEqual(active["product_id"], "P003")
        old = self.db.execute(
            "SELECT status FROM customer_product_interests WHERE sender_id=? AND product_id='P001'",
            (self.sender_id,),
        ).fetchone()
        self.assertEqual(old["status"], "superseded")

    @patch("account_app.app.send_order_to_telegram", return_value=True)
    @patch("account_app.app.save_booking_to_file")
    def test_multi_item_order_is_structured(self, _save, _telegram):
        result, _ = create_order_if_valid(self.db, self.sender_id, {"order": {
            "phone": "07701234567", "province": "بغداد", "address": "المنصور",
            "items": [
                {"product_id": "P001", "color": "أسود", "size": "70 كيلو", "quantity": 2},
                {"product_id": "P003", "color": "زيتي", "size": "90 كيلو", "quantity": 1},
            ],
        }}, None)
        self.assertTrue(result)
        row = self.db.execute("SELECT order_items FROM orders WHERE sender_id=?", (self.sender_id,)).fetchone()
        self.assertEqual(len(json.loads(row[0])), 2)

    def test_incomplete_order_is_rejected(self):
        result, reply = create_order_if_valid(self.db, self.sender_id, {"order": {
            "phone": "07701234567", "address": "المنصور",
            "items": [{"product_id": "P001"}],
        }}, None)
        self.assertIsNone(result)
        self.assertIn("المحافظة", reply)


if __name__ == "__main__":
    unittest.main()
