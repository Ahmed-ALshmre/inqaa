import copy
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from account_app.checkout import contact_fields, phone_number, order_line_error, unsupported_order_action


CATALOG = [
    dict(product_id="F1", product_name="فستان دانتيل", category="فستان", colors="اسود", sizes="38 إلى 52", price="16000", stock="متوفر", status="active"),
    dict(product_id="F2", product_name="فستان انيقة", category="فستان", colors="جوزي", sizes="38 إلى 52", price="15000", stock="متوفر", status="active"),
    dict(product_id="S1", product_name="سوت ملكي", colors="اسود", sizes="38 إلى 52", price="18000", stock="متوفر", status="active"),
]


class ContactRegressionTests(unittest.TestCase):
    def test_address_and_phone_arrive_separately(self):
        contact = contact_fields("السليمانية\nمرتفعات السليمانية زون 3\nH611")
        contact.update(contact_fields("٠٧٧٠١٢٣٤٥٦٧", contact))
        self.assertEqual(contact["phone"], "07701234567")
        self.assertIn("H611", contact["address"])
        self.assertEqual(contact["province"], "السليمانية")

    def test_iraqi_phone_formats_and_short_number(self):
        for text in ["+964 770 123 4567", "009647701234567", "٠٧٧٠١٢٣٤٥٦٧", "0770-123-4567"]:
            self.assertEqual(phone_number(text), "07701234567")
        self.assertFalse(phone_number("0770123456"))
        self.assertFalse(phone_number("1077012345679"))

    def test_customer_address_without_address_label(self):
        for text, province in [("كوت الموفقيه", "واسط"), ("الانبار قضاء حديثه بروانه", "الأنبار"), ("بغداد حي اور", "بغداد")]:
            with self.subTest(text=text):
                contact = contact_fields(text)
                self.assertEqual(contact["province"], province)
                self.assertEqual(contact["address"], text)
        self.assertIn("مستشفى", contact_fields("العنوان مستشفى اليرموك")["address"])

    def test_questions_and_province_only_do_not_become_address(self):
        for text in ["شكد توصيل بغداد", "بغداد", "صلاح الدين", "اريد فستان اسود بغداد", "السليمانية"]:
            self.assertNotIn("address", contact_fields(text))
        self.assertNotIn("province", contact_fields("شكد توصيل بغداد"))

    def test_actions_reported_in_fatena_are_blocked(self):
        for reply in ["لغيت القميص وهسه الطلب صار بس الفستان والسوت الملكي", "من عيوني شلت القميص", "عدلت القياس", "أثبتلج القطعة على هذا القياس؟", "تم إلغاء الطلب"]:
            self.assertTrue(unsupported_order_action(reply), reply)
        self.assertFalse(unsupported_order_action("حقج تفحصين القطعة قبل الدفع"))


class CheckoutRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import account_app.app as module
        cls.m = module
        cls.temp = tempfile.TemporaryDirectory()
        cls.old_path = module.DB_PATH
        module.DB_PATH = str(Path(cls.temp.name) / "checkout.db")
        module.init_db()

    @classmethod
    def tearDownClass(cls):
        cls.m.DB_PATH = cls.old_path
        cls.temp.cleanup()

    def setUp(self):
        self.ctx = self.m.app.app_context(); self.ctx.push()
        self.token = self.m._current_store_id.set("al-fatena")
        self.sender = "al-fatena::checkout-" + uuid.uuid4().hex
        self.db = self.m.get_db()
        self.db.execute("INSERT INTO customers(sender_id,store_id) VALUES(?,?)", (self.sender, "al-fatena"))
        self.db.commit()
        self.patches = [patch.object(self.m, "load_products_from_file", return_value=copy.deepcopy(CATALOG)),
                        patch.object(self.m, "load_active_products", return_value=copy.deepcopy(CATALOG)),
                        patch.object(self.m, "send_order_to_telegram", return_value=True),
                        patch.object(self.m, "save_booking_to_file"),
                        patch.object(self.m, "send_telegram_message", return_value=False)]
        for p in self.patches: p.start()
        self.ev = dict(sender_id=self.sender, page_id="test", platform="facebook", text="", image_url=None,
                       ad_id=None, ref=None, store_id="al-fatena", attachments=[], timestamp=0, postback=None, quick_reply=None, referral_source=None, referral_type=None)

    def tearDown(self):
        for p in reversed(self.patches): p.stop()
        self.m._current_store_id.reset(self.token); self.ctx.pop()

    def data(self, multiple=False):
        items = [dict(product_id="F1", product_name="فستان دانتيل", color="اسود", size="44", size_type="size", quantity=1)]
        if multiple:
            items.append(dict(product_id="S1", product_name="سوت ملكي", color="اسود", size="46", size_type="size", quantity=1))
        return dict(phone="07701234567", province="بغداد", address="حي اور", items=items)

    def incoming(self, text):
        self.ev["text"] = text
        self.m.save_message(self.db, self.sender, "incoming", "text", text, None, None, None, {})

    def test_greeting_and_generic_dress_never_bind_default(self):
        for text in ["سلام عليكم", "فستان", "ما هي مواصفات الفستان؟", "اريد فستان اسود قياس ٤٤"]:
            self.ev["text"] = text
            self.assertFalse(self.m.should_use_auto_product(self.db, self.sender, self.ev, "text", []))
            self.assertIsNone(self.m.customer_product_target(text, CATALOG))
        self.assertEqual(self.m.customer_product_target("اريد فستان دانتيل", CATALOG)["product_id"], "F1")

    def test_search_results_are_not_selected_products(self):
        matches = [{"product": p, "score": 70} for p in CATALOG]
        self.m.remember_product_search_results(self.db, self.sender, matches, {})
        self.assertEqual(self.m.load_customer_products(self.db, self.sender), [])
        self.assertIsNone(self.m.get_active_product_binding(self.db, self.sender))
        self.assertEqual(self.db.execute("SELECT count(*) FROM customer_product_interests WHERE sender_id=? AND status='suggested'", (self.sender,)).fetchone()[0], 3)

    def test_search_does_not_overwrite_manual_choice(self):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source="manual_admin")
        self.m.remember_product_search_results(self.db, self.sender, [{"product": CATALOG[0]}], {})
        self.assertEqual(self.m.get_active_product_binding(self.db, self.sender)["source"], "manual_admin")

    def test_legacy_guesses_and_renamed_product_are_not_reused(self):
        for method in ["catalog_search", "auto_default_product", "reply_product_context"]:
            self.m.remember_customer_product(self.db, self.sender, CATALOG[0], method)
            self.assertEqual(self.m.load_customer_products(self.db, self.sender), [])
        renamed = dict(CATALOG[0], product_name="عباء")
        self.m.remember_customer_product(self.db, self.sender, renamed, "manual")
        self.assertEqual(self.m.load_customer_products(self.db, self.sender), [])

    def test_contact_survives_paused_ai_without_sending_reply(self):
        self.m.set_customer_ai_enabled(self.db, self.sender, False)
        for text in ["٠٧٧٠١٢٣٤٥٦٧", "كوت الموفقيه"]:
            self.ev["text"] = text
            with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "create_human_review", return_value=1):
                result = self.m.process_webhook(self.db, {}, use_debounce=False)
            self.assertEqual(result["reply"], "")
        row = self.db.execute("SELECT phone,province,address FROM customers WHERE sender_id=?", (self.sender,)).fetchone()
        self.assertEqual(tuple(row), ("07701234567", "واسط", "كوت الموفقيه"))

    def test_single_checkout_merges_contact_and_returns_one_receipt(self):
        self.m.update_customer_intelligence(self.db, self.sender, "بغداد حي اور")
        self.m.update_customer_intelligence(self.db, self.sender, "٠٧٧٠١٢٣٤٥٦٧")
        result = {"order": {"items": self.data()["items"]}}
        created, reply = self.m.create_order_if_valid(self.db, self.sender, result, CATALOG[0])
        self.assertTrue(created)
        self.assertIn("21,000", reply)
        self.assertIn("قياس 44", reply)
        self.assertEqual(self.m.approved_reply_parts(result, reply), [reply])

    def test_new_contact_triggers_checkout_despite_false_model_flag(self):
        self.m.save_message(self.db, self.sender, "outgoing", "text", "دزيلي رقم الموبايل والعنوان حتى أثبت الحجز", None, None, None, {})
        self.ev["text"] = "07701234567"
        self.assertTrue(self.m.checkout_requested(self.db, self.ev, {"create_order": False, "order": self.data()}))

    def test_unknown_item_is_not_silently_dropped(self):
        data = self.data(True); data["items"][1]["product_id"] = "other-store"
        created, _ = self.m.create_order_if_valid(self.db, self.sender, {"order": data}, CATALOG[0])
        self.assertFalse(created)
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?", (self.sender,)).fetchone()[0], 0)

    def test_unresolved_size_is_not_chosen_for_customer(self):
        data = self.data(); data["items"][0]["size"] = "٤٤ او ٤٦"
        created, reply = self.m.create_order_if_valid(self.db, self.sender, {"order": data}, CATALOG[0])
        self.assertFalse(created); self.assertIn("قياس", reply)

    def test_multi_piece_proposal_requires_exact_confirmation(self):
        result = {"order": self.data(True)}
        created, reply = self.m.create_order_if_valid(self.db, self.sender, result, None)
        self.assertFalse(created); self.assertIn("39,000", reply)
        self.m.saved_checkout_reply(self.db, self.ev, reply, {}, {"checkout_proposal": result["_checkout_proposal"]})
        self.incoming("اي حياتي")
        accepted = self.m.accept_checkout_proposal(self.db, self.ev, [], CATALOG)
        self.assertTrue(accepted["meta"]["order_created"])
        order = self.db.execute("SELECT order_items FROM orders WHERE sender_id=?", (self.sender,)).fetchone()
        self.assertEqual(len(json.loads(order[0])), 2)
        self.assertIsNone(self.m.accept_checkout_proposal(self.db, self.ev, [], CATALOG))

    def test_yes_with_correction_does_not_confirm_stale_cart(self):
        result = {"order": self.data(True)}
        _, reply = self.m.create_order_if_valid(self.db, self.sender, result, None)
        self.m.saved_checkout_reply(self.db, self.ev, reply, {}, {"checkout_proposal": result["_checkout_proposal"]})
        self.incoming("عوفي السوت"); self.incoming("اي حياتي")
        self.assertIsNone(self.m.accept_checkout_proposal(self.db, self.ev, [], CATALOG))
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?", (self.sender,)).fetchone()[0], 0)

    def test_price_change_requires_new_summary(self):
        result = {"order": self.data(True)}
        _, reply = self.m.create_order_if_valid(self.db, self.sender, result, None)
        self.m.saved_checkout_reply(self.db, self.ev, reply, {}, {"checkout_proposal": result["_checkout_proposal"]})
        self.incoming("تمام")
        changed = copy.deepcopy(CATALOG); changed[0]["price"] = "19000"
        accepted = self.m.accept_checkout_proposal(self.db, self.ev, [], changed)
        self.assertTrue(accepted["meta"]["checkout_changed"])

    def test_prior_order_complaint_does_not_enter_new_sale(self):
        self.ev["text"] = "عيني صارلي سبوع من حجزت والطلب ماوصل"
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "is_customer_ai_enabled", return_value=True), patch.object(self.m, "create_human_review", return_value=1), patch.object(self.m, "call_main_ai") as ai:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        ai.assert_not_called()
        self.assertTrue(result["meta"]["existing_order_followup"])

    def test_full_webhook_collects_burst_then_creates_one_receipt(self):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source="image_recognition")
        self.m.save_message(self.db, self.sender, "outgoing", "text", "دزيلي رقم الموبايل والعنوان حتى أثبت الحجز", None, None, None,
                            {"checkout_draft": {"items": self.data()["items"]}})
        self.incoming("07701234567")
        self.incoming("بغداد حي اور")
        self.ev["text"] = "كم يوم ويوصل الطلب"
        model_result = {"reply": "دزيلي الموبايل والعنوان حتى أثبت الحجز؟", "create_order": False, "order": {}}
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "is_customer_ai_enabled", return_value=True), patch.object(self.m, "is_store_feature_enabled", return_value=False), patch.object(self.m, "call_main_ai", return_value=model_result) as ai:
            response = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertIn("07701234567", ai.call_args.args[0]["text"])
        self.assertIn("تم تثبيت الطلب", response["reply"])
        self.assertEqual(response["reply_parts"], [response["reply"]])
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?", (self.sender,)).fetchone()[0], 1)

    def test_ai_cannot_settle_unresolved_size_from_history(self):
        self.incoming("اريد السوت ٤٤ او ٤٦")
        data = self.data(True)
        created, reply = self.m.create_order_if_valid(self.db, self.sender, {"order": data}, None)
        self.assertFalse(created); self.assertIn("أكثر من قياس", reply)
        self.incoming("السوت قياس 46")
        result = {"order": data}
        _, reply = self.m.create_order_if_valid(self.db, self.sender, result, None)
        self.assertIn("هذه القطع المقترحة", reply)

    def test_manual_checkout_rejects_incomplete_shelan_order(self):
        client = self.m.app.test_client()
        with client.session_transaction() as session:
            session["dashboard_authenticated"] = True
        data = self.data(); data["items"][0].update(size="", color="")
        with patch.object(self.m, "send_text_to_facebook") as send:
            response = client.post(f"/api/conversations/{self.sender}/create_order", json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("لون", response.get_json()["error"])
        send.assert_not_called()

    def test_actual_post_order_reply_cannot_claim_removed_shirt(self):
        self.db.execute("INSERT INTO orders(sender_id,store_id,status,created_at,order_items) VALUES(?,?,'new',?,?)",
                        (self.sender, "al-fatena", self.m.now_baghdad_iso(), json.dumps(self.data(True)["items"])))
        self.db.commit()
        self.ev["text"] = "عوفي القميص اريد بس الفستان والسوت"
        order = self.m.get_latest_customer_order(self.db, self.sender)
        with patch.object(self.m, "call_main_ai", return_value={"reply": "لغيت القميص وهسه الطلب صار بس الفستان والسوت الملكي"}), patch.object(self.m, "is_store_feature_enabled", return_value=False), patch.object(self.m, "create_human_review", return_value=1):
            response = self.m.handle_post_order_message(self.db, self.ev, {}, [], CATALOG, [], order)
        self.assertTrue(response["meta"]["needs_human"])
        self.assertEqual(response["reply"], "")
        self.assertEqual(response["reply_parts"], [])
        self.assertEqual(self.m.get_latest_customer_order(self.db, self.sender)["order_items"], order["order_items"])

    def test_ambiguous_linked_models_ask_before_answering(self):
        self.assertIsNone(self.m.select_customer_context_product("فستان", CATALOG[:2]))
        self.ev["text"] = "شكد سعر الفستان"
        result = self.m.call_main_ai(self.ev, "text", {}, [], CATALOG, None, None, "", [], customer_products=CATALOG[:2])
        self.assertFalse(result["create_order"])
        self.assertIn("أكثر من موديل", result["reply"])

    def test_missing_catalog_product_cannot_be_reused_by_memory(self):
        self.m.remember_customer_product(self.db, self.sender, {"product_id": "gone", "product_name": "قديم"}, "manual")
        self.assertIsNone(self.m.get_active_product_binding(self.db, self.sender))
        self.assertEqual(self.m.load_customer_products(self.db, self.sender), [])

    def test_new_category_does_not_reuse_old_dress(self):
        self.assertIsNone(self.m.select_customer_context_product("السوت شكد سعره", CATALOG[:1]))
        self.assertEqual(self.m.select_customer_context_product("شكد سعره", CATALOG[:1])["product_id"], "F1")

    def test_receipt_answers_delivery_time_from_store_setting(self):
        with patch.object(self.m, "get_delivery_settings", return_value={"delivery_time": "من يوم إلى يومين"}):
            text = self.m.checkout_receipt(self.data(), self.data()["items"], CATALOG, 5000)
        self.assertIn("مدة التوصيل: من يوم إلى يومين", text)


if __name__ == "__main__":
    unittest.main()
