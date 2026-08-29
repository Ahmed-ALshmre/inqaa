import json
import io
import os
import shutil
import tempfile
import unittest
import uuid
from unittest.mock import patch

from account_app.app import (
    app,
    bind_customer_to_product,
    clean_telegram_text,
    create_human_review,
    create_order_if_valid,
    format_order_for_telegram,
    complete_customer_product_link,
    get_active_product_binding,
    get_auto_product_settings,
    get_db,
    infer_explicit_customer_gender,
    extract_store_name_from_manychat,
    is_recent_duplicate_incoming,
    load_products_from_file,
    reject_current_binding,
    save_message,
    _should_send_image,
    should_use_auto_product,
    update_customer_intelligence,
    ensure_store,
    get_app_setting,
    set_store_setting,
    current_store_id,
    get_store_settings,
    send_catalog_to_customer,
    manychat_api_key_for_page,
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
        self.db.execute("DELETE FROM human_reviews WHERE sender_id=?", (self.sender_id,))
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

    def test_ai_settings_are_isolated_between_stores(self):
        store_a = "test-store-" + uuid.uuid4().hex
        store_b = "test-store-" + uuid.uuid4().hex
        try:
            ensure_store(self.db, store_a, "متجر أ", webhook_key=store_a)
            ensure_store(self.db, store_b, "متجر ب", webhook_key=store_b)
            set_store_setting(self.db, "ai_main_model", "model/a", store_a)
            set_store_setting(self.db, "ai_main_model", "model/b", store_b)
            self.assertEqual(get_app_setting("ai_main_model", "", self.db, store_a), "model/a")
            self.assertEqual(get_app_setting("ai_main_model", "", self.db, store_b), "model/b")
        finally:
            self.db.execute("DELETE FROM app_settings WHERE key LIKE ?", (f"store:{store_a}:%",))
            self.db.execute("DELETE FROM app_settings WHERE key LIKE ?", (f"store:{store_b}:%",))
            self.db.execute("DELETE FROM stores WHERE store_id IN (?,?)", (store_a, store_b))
            self.db.commit()

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

    def test_telegram_text_removes_links(self):
        cleaned = clean_telegram_text(
            "تفاصيل المنتج https://example.com/product/1\nwww.example.com/image.jpg"
        )
        self.assertNotIn("http", cleaned)
        self.assertNotIn("www.", cleaned)

    @patch("account_app.app.send_telegram_photo", return_value=True)
    @patch("account_app.app.send_telegram_message", return_value=True)
    def test_human_review_sends_only_simple_alert(self, send_message, send_photo):
        candidates = [
            {"product_id": f"P{i:03d}", "product_name": f"منتج {i}", "image_url": f"https://example.com/{i}.jpg"}
            for i in range(1, 8)
        ]
        create_human_review(
            self.db,
            {"sender_id": self.sender_id, "text": "هذا موجود؟", "image_url": "https://example.com/customer.jpg"},
            "لم يتم التعرف تلقائياً",
            candidates,
        )
        self.assertEqual(send_message.call_count, 1)
        self.assertEqual(send_photo.call_count, 0)
        notification = send_message.call_args.args[0]
        self.assertNotIn("https://", notification)
        self.assertIn("تحتاج تدخلك", notification)
        self.assertNotIn("P001", notification)
        self.assertNotIn("هذا موجود", notification)

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
        self.db.execute(
            "UPDATE customers SET name='زبونة الاختبار' WHERE sender_id=?",
            (self.sender_id,),
        )
        self.db.commit()
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
        booking = _save.call_args.args[0]
        self.assertEqual(booking["product_total"], 56000)
        self.assertEqual(booking["delivery_fee"], 5000)
        self.assertEqual(booking["total_amount"], 56000)

    def test_incomplete_order_is_rejected(self):
        result, reply = create_order_if_valid(self.db, self.sender_id, {"order": {
            "phone": "07701234567", "address": "المنصور",
            "items": [{"product_id": "P001"}],
        }}, None)
        self.assertIsNone(result)
        self.assertIn("المحافظة", reply)

    def test_duplicate_customer_event_is_detected_semantically(self):
        save_message(
            self.db, self.sender_id, "incoming", "text",
            "مرحبا، شكد السعر؟", None, None, None, {},
        )
        self.assertTrue(is_recent_duplicate_incoming(
            self.db, self.sender_id, "مرحبا شكد السعر", "",
        ))
        self.assertFalse(is_recent_duplicate_incoming(
            self.db, self.sender_id, "أريد قياس مختلف", "",
        ))

    def test_booking_telegram_message_matches_importer_shape(self):
        message = format_order_for_telegram({
            "phone": "07701234567",
            "province": "بغداد",
            "address": "المنصور قرب السوق",
            "total_amount": 40000,
            "items": [
                {"product_name": "فستان انيقه", "quantity": 1, "color": "أسود", "size": "40"},
                {"product_name": "فستان انيقه", "quantity": 1, "color": "أسود", "size": "42"},
            ],
        })
        self.assertEqual(message, (
            "فستان انيقه × 2\n"
            "بغداد / المنصور قرب السوق\n"
            "07701234567\n"
            "40000\n"
            "القياسات: 40، 42"
        ))
        self.assertNotIn("http", message)
        self.assertNotIn("Sender", message)

        weight_message = format_order_for_telegram({
            "phone": "07800000000",
            "province": "بغداد",
            "address": "حي الجهاد / قرب السوق",
            "total_amount": 38000,
            "items": [
                {"product_id": "P001", "product_name": "سوت ملكي", "quantity": 1, "size": "75"},
                {"product_id": "P003", "product_name": "دشداشة أم السوتاج", "quantity": 1, "size": "75"},
            ],
        })
        self.assertEqual(weight_message, (
            "سوت ملكي + دشداشة أم السوتاج\n"
            "بغداد / حي الجهاد / قرب السوق\n"
            "07800000000\n"
            "38000\n"
            "الوزن: 75"
        ))

    def test_store_name_is_extracted_from_manychat(self):
        self.assertEqual(
            extract_store_name_from_manychat({"store_name": "متجر الفاتنة"}),
            "متجر الفاتنة",
        )
        self.assertEqual(
            extract_store_name_from_manychat({"custom_fields": {"page_name": "لمسة ستور"}}),
            "لمسة ستور",
        )


class MultiStoreIntegrationTests(unittest.TestCase):
    """End-to-end regression checks for the two ManyChat storefronts."""

    def setUp(self):
        self.marker = "multi-store-test-" + uuid.uuid4().hex
        self.lamsa_sender = f"default::{self.marker}"
        self.khuyoot_sender = f"khuyoot::{self.marker}"
        self.ctx = app.app_context()
        self.ctx.push()
        self.db = get_db()
        ensure_store(self.db, "default", "لمسة ستور", webhook_key="lamsa-store")
        ensure_store(self.db, "khuyoot", "خيوط", webhook_key="khuyoot")
        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["dashboard_authenticated"] = True

    def tearDown(self):
        for table in (
            "orders", "human_reviews", "messages", "customer_product_interests",
            "customer_instructions", "conversation_history", "customers",
        ):
            try:
                self.db.execute(f"DELETE FROM {table} WHERE sender_id IN (?,?)", (
                    self.lamsa_sender, self.khuyoot_sender,
                ))
            except Exception:
                pass
        self.db.execute("DELETE FROM app_settings WHERE key LIKE ?", (f"%{self.marker}%",))
        self.db.commit()
        self.ctx.pop()

    @patch("account_app.app.threading.Thread")
    def test_webhooks_namespace_same_manychat_customer_by_store(self, thread_cls):
        payload = {
            "subscriber_id": self.marker,
            "first_name": "زبونة",
            "last_input_text": "أريد هذا الموديل",
        }
        lamsa = self.client.post("/manychat/webhook/lamsa-store", json=payload)
        khuyoot = self.client.post("/manychat/webhook/khuyoot", json=payload)
        unknown = self.client.post("/manychat/webhook/not-configured", json=payload)

        self.assertEqual(lamsa.status_code, 200)
        self.assertEqual(khuyoot.status_code, 200)
        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(lamsa.get_json()["store_id"], "default")
        self.assertEqual(khuyoot.get_json()["store_id"], "khuyoot")
        rows = self.db.execute(
            "SELECT sender_id,store_id,store_name FROM customers WHERE sender_id IN (?,?) ORDER BY store_id",
            (self.lamsa_sender, self.khuyoot_sender),
        ).fetchall()
        self.assertEqual({row["sender_id"] for row in rows}, {self.lamsa_sender, self.khuyoot_sender})
        self.assertEqual({row["store_id"] for row in rows}, {"default", "khuyoot"})
        self.assertEqual(thread_cls.call_count, 2)

    def test_ai_dashboard_action_uses_conversation_store_context(self):
        for sender_id, store_id in ((self.lamsa_sender, "default"), (self.khuyoot_sender, "khuyoot")):
            self.db.execute(
                "INSERT INTO customers(sender_id,store_id,lead_score,lead_stage) VALUES(?,?,0,'new')",
                (sender_id, store_id),
            )
        self.db.commit()
        seen = []

        def fake_products(_db):
            sid = current_store_id()
            return [{"store_id": sid, "product_id": "P-SAME", "product_name": sid}]

        def fake_ai(*args, **kwargs):
            seen.append({
                "store_id": current_store_id(),
                "store_name": get_store_settings()["name"],
                "products": [item["store_id"] for item in args[4]],
            })
            return {"reply": "اختبار", "intent": "test", "confidence": 100}

        with patch("account_app.app.is_ai_enabled", return_value=True), \
             patch("account_app.app.load_active_products", side_effect=fake_products), \
             patch("account_app.app.call_main_ai", side_effect=fake_ai):
            for sender_id in (self.lamsa_sender, self.khuyoot_sender):
                response = self.client.post(
                    f"/api/conversations/{sender_id}/ask_ai",
                    json={"text": "هل متوفر؟"},
                )
                self.assertEqual(response.status_code, 200)

        self.assertEqual([item["store_id"] for item in seen], ["default", "khuyoot"])
        self.assertEqual([item["products"] for item in seen], [["default"], ["khuyoot"]])
        self.assertEqual([item["store_name"] for item in seen], ["لمسة ستور", "خيوط"])

    def test_catalog_product_images_are_sent_only_from_current_store(self):
        products = {
            "default": [{
                "store_id": "default", "product_id": "P-SAME", "product_name": "لمسة",
                "stock": "متوفر", "image_url": "https://images.test/lamsa.jpg",
            }],
            "khuyoot": [{
                "store_id": "khuyoot", "product_id": "P-SAME", "product_name": "خيوط",
                "stock": "متوفر", "image_url": "https://images.test/khuyoot.jpg",
            }],
        }
        sent = []

        def fake_load(_db):
            return products[current_store_id()]

        def fake_send(sender_id, messages, platform):
            sent.append((current_store_id(), sender_id, messages))
            return True

        with patch("account_app.app.load_active_products", side_effect=fake_load), \
             patch("account_app.app.send_manychat_messages", side_effect=fake_send):
            for sid, sender in (("default", self.lamsa_sender), ("khuyoot", self.khuyoot_sender)):
                with app.test_request_context(f"/?store_id={sid}"):
                    send_catalog_to_customer(self.db, sender)

        self.assertEqual(sent[0][2], [{"type": "image", "url": "https://images.test/lamsa.jpg"}])
        self.assertEqual(sent[1][2], [{"type": "image", "url": "https://images.test/khuyoot.jpg"}])

    def test_catalog_uploads_use_separate_store_directories(self):
        catalog_root = tempfile.mkdtemp(prefix="lamsa-catalog-test-")
        try:
            with patch("account_app.app.CATALOG_IMAGE_DIR", catalog_root):
                for sid, filename, content in (
                    ("default", "lamsa.jpg", b"lamsa-image"),
                    ("khuyoot", "khuyoot.jpg", b"khuyoot-image"),
                ):
                    response = self.client.post(
                        f"/api/settings/catalog_images?store_id={sid}",
                        data={"images": (io.BytesIO(content), filename)},
                        content_type="multipart/form-data",
                    )
                    self.assertEqual(response.status_code, 200)

                lamsa = self.client.get("/api/settings/catalog_images?store_id=default").get_json()
                khuyoot = self.client.get("/api/settings/catalog_images?store_id=khuyoot").get_json()
                lamsa_managed = [item for item in lamsa["images"] if item["managed"]]
                khuyoot_managed = [item for item in khuyoot["images"] if item["managed"]]
                self.assertEqual(len(lamsa_managed), 1)
                self.assertEqual(len(khuyoot_managed), 1)
                self.assertTrue(lamsa_managed[0]["filename"].startswith("lamsa_"))
                self.assertTrue(khuyoot_managed[0]["filename"].startswith("khuyoot_"))
                self.assertTrue(os.path.isfile(os.path.join(catalog_root, lamsa_managed[0]["filename"])))
                self.assertTrue(os.path.isfile(os.path.join(catalog_root, "khuyoot", khuyoot_managed[0]["filename"])))
        finally:
            shutil.rmtree(catalog_root, ignore_errors=True)

    def test_same_product_code_can_be_bound_independently_per_store(self):
        for sender_id, store_id in ((self.lamsa_sender, "default"), (self.khuyoot_sender, "khuyoot")):
            self.db.execute(
                "INSERT INTO customers(sender_id,store_id,lead_score,lead_stage) VALUES(?,?,0,'new')",
                (sender_id, store_id),
            )
        self.db.commit()
        for sender_id, store_id, name in (
            (self.lamsa_sender, "default", "موديل لمسة"),
            (self.khuyoot_sender, "khuyoot", "موديل خيوط"),
        ):
            with app.test_request_context(f"/?store_id={store_id}"):
                complete_customer_product_link(
                    self.db, sender_id,
                    {"store_id": store_id, "product_id": "P-SAME", "product_name": name},
                    "manual", source="manual",
                )
        rows = self.db.execute(
            "SELECT sender_id,product_id,product_name FROM customer_product_interests "
            "WHERE sender_id IN (?,?) AND status='active' ORDER BY sender_id",
            (self.lamsa_sender, self.khuyoot_sender),
        ).fetchall()
        by_sender = {row["sender_id"]: (row["product_id"], row["product_name"]) for row in rows}
        self.assertEqual(by_sender[self.lamsa_sender], ("P-SAME", "موديل لمسة"))
        self.assertEqual(by_sender[self.khuyoot_sender], ("P-SAME", "موديل خيوط"))

    def test_store_ai_and_sales_settings_are_isolated(self):
        key = f"test_setting_{self.marker}"
        set_store_setting(self.db, key, "قيمة لمسة", "default")
        set_store_setting(self.db, key, "قيمة خيوط", "khuyoot")
        self.assertEqual(get_app_setting(key, "", self.db, "default"), "قيمة لمسة")
        self.assertEqual(get_app_setting(key, "", self.db, "khuyoot"), "قيمة خيوط")

    def test_manychat_api_key_is_selected_by_store(self):
        with patch.dict(os.environ, {
            "MANYCHAT_API_KEY_LAMSA": "lamsa-private-key",
            "MANYCHAT_API_KEY_KHUYOOT": "khuyoot-private-key",
            "MANYCHAT_API_KEY": "fallback-key",
        }, clear=False):
            self.assertEqual(manychat_api_key_for_page("", "default"), "lamsa-private-key")
            self.assertEqual(manychat_api_key_for_page("", "khuyoot"), "khuyoot-private-key")
            self.assertEqual(manychat_api_key_for_page("", "unknown-store"), "fallback-key")

    @patch("account_app.app.requests.post")
    def test_manychat_sender_prefix_chooses_correct_store_key(self, post):
        response = post.return_value
        response.ok = True
        response.status_code = 200
        response.json.return_value = {"status": "success"}
        with patch.dict(os.environ, {
            "MANYCHAT_API_KEY_LAMSA": "lamsa-private-key",
            "MANYCHAT_API_KEY_KHUYOOT": "khuyoot-private-key",
        }, clear=False):
            from account_app.app import send_manychat_messages_detailed
            send_manychat_messages_detailed(
                self.khuyoot_sender,
                [{"type": "text", "text": "اختبار"}],
            )
        headers = post.call_args.kwargs["headers"]
        payload = post.call_args.kwargs["json"]
        self.assertEqual(headers["Authorization"], "Bearer khuyoot-private-key")
        self.assertEqual(payload["subscriber_id"], self.marker)


if __name__ == "__main__":
    unittest.main()
