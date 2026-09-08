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
        self.assertEqual(reply, "تم تثبيت الطلب. يرجى فحص الطلب بحضور المندوب والتأكد من الموديل والقياس. إذا لم يطابق الطلب يرجع مع المندوب بدون دفع. أهم شي تفحصين الطلب قبل دفع المبلغ.")
        self.assertEqual(self.m.approved_reply_parts(result, reply), [reply])

    def test_new_contact_triggers_checkout_despite_false_model_flag(self):
        self.m.save_message(self.db, self.sender, "outgoing", "text", "دزيلي رقم الموبايل والعنوان حتى أثبت الحجز", None, None, None, {})
        self.ev["text"] = "07701234567"
        self.assertTrue(self.m.checkout_requested(self.db, self.ev, {"create_order": False, "order": self.data()}))

    def test_model_failure_does_not_disable_next_linked_product_question(self):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source="manual_admin")
        self.ev['text'] = 'شنو تفاصيل هذا الموديل؟'
        with patch.object(self.m, 'extract_facebook_event', return_value=self.ev), patch.object(self.m, 'is_ai_enabled', return_value=True), patch.object(self.m, 'is_store_feature_enabled', return_value=False), patch.object(self.m, 'call_main_ai', side_effect=[
                {'failed': True, 'reply': '', 'failure_reason': 'exception:Timeout'},
                {'reply': 'السعر 16000 دينار', 'create_order': False}]):
            failed = self.m.process_webhook(self.db, {}, use_debounce=False)
            self.assertEqual(failed['reply'], '')
            self.assertTrue(self.m.has_pending_human_review(self.db, self.sender))
            self.assertTrue(self.m.is_customer_ai_enabled(self.db, self.sender))
            self.ev['text'] = 'شكد السعر؟'
            answered = self.m.process_webhook(self.db, {}, use_debounce=False)
            self.assertEqual(answered['reply'], 'السعر 16000 دينار')
            self.assertTrue(self.m.is_customer_ai_enabled(self.db, self.sender))

    def test_explicit_conversation_pause_is_still_respected(self):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source="manual_admin")
        self.m.set_customer_ai_enabled(self.db, self.sender, False)
        self.ev['text'] = 'شكد السعر؟'
        with patch.object(self.m, 'extract_facebook_event', return_value=self.ev), patch.object(self.m, 'is_ai_enabled', return_value=True), patch.object(self.m, 'call_main_ai') as model:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertEqual(result['reply'], '')
        model.assert_not_called()

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
        self.assertEqual(response["reply"], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
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

    def test_dashboard_ai_suggestion_creates_order_only_on_send(self):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source="manual_admin")
        self.incoming("ثبتي الطلب")
        client = self.m.app.test_client()
        with client.session_transaction() as session:
            session["dashboard_authenticated"] = True
        result = {"reply": "تم تثبيت الطلب", "create_order": True, "order": self.data()}
        with patch.object(self.m, "call_main_ai", return_value=result), patch.object(self.m, "is_ai_enabled", return_value=True):
            response = client.post(f"/api/conversations/{self.sender}/ask_ai", json={"allow_empty": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?", (self.sender,)).fetchone()[0], 0)
        with patch.object(self.m, "send_text_via_manychat_detailed", return_value={"ok": True}) as send:
            response = client.post(f"/api/conversations/{self.sender}/send", json={"text": result["reply"]})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?", (self.sender,)).fetchone()[0], 1)
        self.assertEqual(send.call_args.args[1], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)

    def cache_dashboard_draft(self, result):
        self.m.ai_reply_draft_table(self.db)
        last_id = self.db.execute("SELECT COALESCE(MAX(id),0) FROM messages WHERE sender_id=?", (self.sender,)).fetchone()[0]
        self.db.execute("INSERT OR REPLACE INTO ai_reply_drafts VALUES(?,?,?,?,?)", (self.sender, result["reply"], json.dumps({"result": result, "product_id": "F1"}), last_id, self.m.now_baghdad_iso()))
        self.db.commit()

    def test_dashboard_incomplete_order_cannot_send_success_claim(self):
        self.cache_dashboard_draft({"reply": "تم تثبيت الطلب", "create_order": True, "order": {}})
        reply, meta, error = self.m.apply_dashboard_ai_checkout(self.db, self.sender, "تم تثبيت الطلب")
        self.assertIsNone(error)
        self.assertFalse(meta["order_created"])
        self.assertNotIn("تم تثبيت", reply)
        self.assertIn("أحتاج", reply)

    def test_dashboard_stale_draft_is_rejected(self):
        self.cache_dashboard_draft({"reply": "تم تثبيت الطلب", "create_order": True, "order": self.data()})
        self.incoming("لا تثبتين")
        _, _, error = self.m.apply_dashboard_ai_checkout(self.db, self.sender, "تم تثبيت الطلب")
        self.assertTrue(error)
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?", (self.sender,)).fetchone()[0], 0)

    def test_new_image_ignores_old_ad_and_uses_product_image_fallback(self):
        products = copy.deepcopy(CATALOG)
        products[0]["ad_id"] = "old-ad"
        ev = dict(self.ev, image_url="https://example.test/customer.jpg", ad_id="old-ad", text="شكد سعره")
        with patch.object(self.m, "match_customer_image_with_catalog", return_value={"product_found": False}), patch.object(self.m, "confirm_with_vision", return_value={"product_id": "F2", "product_found": True}) as vision, patch.object(self.m, "is_store_feature_enabled", return_value=True):
            product, method, _ = self.m.match_product(self.db, ev, products)
        vision.assert_called_once()
        self.assertEqual(product["product_id"], "F2")
        self.assertEqual(method, "image_recognition")

    def test_explicit_model_overrides_ad(self):
        products = copy.deepcopy(CATALOG); products[0]["ad_id"] = "old-ad"
        with patch.object(self.m, "_text_match_product", return_value=products[1]):
            product, method, _ = self.m.match_product(self.db, dict(self.ev, text="فستان انيقة", ad_id="old-ad"), products)
        self.assertEqual(product["product_id"], "F2")
        self.assertEqual(method, "text")

    def test_resume_existing_booking_request_creates_order(self):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source="manual_admin")
        self.incoming("ثبتي الطلب")
        self.m.set_customer_ai_enabled(self.db, self.sender, False)
        client = self.m.app.test_client()
        with client.session_transaction() as session:
            session["dashboard_authenticated"] = True
        result = {"reply": "تم تثبيت الطلب", "create_order": True, "order": self.data()}
        with patch.object(self.m, "call_main_ai", return_value=result), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "send_webhook_result_to_facebook", return_value=True):
            response = client.post(f"/api/conversations/{self.sender}/ai", json={"enabled": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?", (self.sender,)).fetchone()[0], 1)

    def test_old_reviewed_image_does_not_silence_new_question(self):
        self.m.save_message(self.db, self.sender, "incoming", "image", "", "https://example.test/old.jpg", None, None, {})
        self.m.save_message(self.db, self.sender, "outgoing", "text", "تفضلي", None, None, None, {})
        self.ev["text"] = "القماش شنو؟"
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "is_customer_ai_enabled", return_value=True), patch.object(self.m, "is_store_feature_enabled", return_value=False), patch.object(self.m, "call_main_ai", return_value={"reply": "أي موديل تقصدين؟", "create_order": False}) as ai:
            response = self.m.process_webhook(self.db, {}, use_debounce=False)
        ai.assert_called_once()
        self.assertEqual(response["reply"], "أي موديل تقصدين؟")

    def test_unmatched_image_pauses_silently_for_human(self):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source="manual_admin")
        self.ev.update(text="", image_url="https://example.test/new.jpg")
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "is_store_feature_enabled", return_value=False), patch.object(self.m, "match_product", return_value=(None, None, {})), patch.object(self.m, "create_human_review", return_value=77):
            response = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, self.sender))
        self.assertTrue(response["meta"]["ai_paused"])
        self.assertEqual(response["reply"], "")
        self.assertIsNone(self.m.get_active_product_binding(self.db, self.sender))

    def test_dashboard_send_accepts_confirmed_cart_once(self):
        result = {"order": self.data(True)}
        _, reply = self.m.create_order_if_valid(self.db, self.sender, result, None)
        self.m.saved_checkout_reply(self.db, self.ev, reply, {}, {"checkout_proposal": result["_checkout_proposal"]})
        self.incoming("تمام")
        self.cache_dashboard_draft({"reply": "تم تثبيت الطلب", "create_order": True, "order": self.data(True)})
        reply, meta, error = self.m.apply_dashboard_ai_checkout(self.db, self.sender, "تم تثبيت الطلب")
        self.assertIsNone(error)
        self.assertTrue(meta["order_created"])
        self.assertTrue(meta["cart_confirmed"])
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?", (self.sender,)).fetchone()[0], 1)

    def test_vision_request_contains_customer_and_product_images(self):
        with patch.object(self.m, "OPENROUTER_KEY", "test-key"), patch.object(self.m, "is_store_feature_enabled", return_value=True), patch.object(self.m, "_image_ref_for_openrouter", side_effect=lambda x: x), patch.object(self.m.requests, "post") as post:
            post.return_value.json.return_value = {"choices": [{"message": {"content": "F1"}}]}
            result = self.m.confirm_with_vision("https://example.test/customer.jpg", [dict(CATALOG[0], image_url="https://example.test/product.jpg")])
        images = [x["image_url"]["url"] for x in post.call_args.kwargs["json"]["messages"][1]["content"] if x["type"] == "image_url"]
        self.assertEqual(images, ["https://example.test/customer.jpg", "https://example.test/product.jpg"])
        self.assertEqual(result["product_id"], "F1")

    def simulate_customer_photo(self, product_index, fallback=False):
        self.m.bind_customer_to_product(self.db, self.sender, CATALOG[0], source="manual_admin")
        selected = CATALOG[product_index]
        self.ev.update(text="بس هذا" if product_index else "", image_url="https://images.test/customer-model.jpg", ad_id="old-ad")
        recognized = {"product_found": True, "product_id": selected["product_id"], "confidence": 100}
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "is_store_feature_enabled", return_value=True), patch.object(self.m, "match_customer_image_with_catalog", return_value={"product_found": False} if fallback else recognized), patch.object(self.m, "confirm_with_vision", return_value=recognized) as vision, patch.object(self.m, "call_main_ai", return_value={"reply": "الموديل متوفر، شنو القياس المطلوب؟", "create_order": False}) as ai, patch.object(self.m, "send_webhook_result_to_facebook", return_value=True) as send, patch.object(self.m, "create_human_review") as review:
            response = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertEqual(ai.call_args.args[5]["product_id"], selected["product_id"])
        self.assertTrue(response["meta"]["auto_reply"]["sent"])
        send.assert_called_once()
        review.assert_not_called()
        self.assertEqual(vision.call_count, int(fallback))
        self.assertEqual([p["product_id"] for p in self.m.load_customer_products(self.db, self.sender)], [selected["product_id"]])
        self.assertTrue(self.m.is_customer_ai_enabled(self.db, self.sender))
        self.assertEqual(self.db.execute("SELECT count(*) FROM customer_product_interests WHERE sender_id=? AND product_id=?", (self.sender, selected["product_id"])).fetchone()[0], 1)
        # Continue the actual webhook path with a follow-up question.
        self.ev.update(text="شنو القياسات؟", image_url=None, ad_id=None, attachments=[])
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "is_store_feature_enabled", return_value=False), patch.object(self.m, "call_main_ai", return_value={"reply": "القياسات من 38 إلى 52", "create_order": False}) as ai, patch.object(self.m, "create_human_review") as review:
            followup = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertEqual(ai.call_args.args[5]["product_id"], selected["product_id"])
        self.assertEqual(followup["reply"], "القياسات من 38 إلى 52")
        review.assert_not_called()

    def test_photo_same_model_continues_without_duplicate_binding(self):
        self.simulate_customer_photo(0)

    def test_photo_different_model_relinks_and_continues(self):
        self.simulate_customer_photo(1)

    def test_photo_same_model_fallback_continues(self):
        self.simulate_customer_photo(0, fallback=True)

    def test_photo_different_model_fallback_replaces_old_binding(self):
        self.simulate_customer_photo(1, fallback=True)

    def test_catalog_sends_and_records_only_images(self):
        products = [dict(CATALOG[0], image_url="https://images.test/catalog.jpg")]
        with patch.object(self.m, "load_active_products", return_value=products), patch.object(self.m, "send_manychat_messages", return_value=True) as send:
            sent, reply, images = self.m.send_catalog_to_customer(self.db, self.sender)
        self.assertTrue(sent)
        self.assertEqual(reply, "")
        self.assertTrue(images)
        self.assertTrue(all(m["type"] == "image" and "text" not in m for m in send.call_args.args[1]))
        rows = self.db.execute("SELECT message_type,text FROM messages WHERE sender_id=? AND direction='outgoing'", (self.sender,)).fetchall()
        self.assertTrue(all(r["message_type"] == "image" and not r["text"] for r in rows))

    def test_background_catalog_keeps_customer_store(self):
        observed = []
        def send(*args):
            observed.append(self.m.current_store_id())
            return True, "", []
        token = self.m._current_store_id.set("default")
        try:
            with patch.object(self.m, "send_catalog_to_customer", side_effect=send):
                self.m.send_catalog_to_customer_background(self.sender)
            self.assertEqual(self.m.current_store_id(), "default")
        finally:
            self.m._current_store_id.reset(token)
        self.assertEqual(observed, ["al-fatena"])

    def test_enabled_default_greeting_uses_product_without_asking_for_photo(self):
        self.ev["text"] = "سلام عليكم"
        settings = {"enabled": True, "product_id": "F1", "product": CATALOG[0], "send_image": False}
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "get_auto_product_settings", return_value=settings), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "is_store_feature_enabled", return_value=False), patch.object(self.m, "generate_first_message_reply") as first, patch.object(self.m, "_call_main_ai_once") as model:
            response = self.m.process_webhook(self.db, {}, use_debounce=False)
            self.assertEqual(self.m.load_customer_products(self.db, self.sender)[0]["product_id"], "F1")
        first.assert_not_called()
        model.assert_not_called()
        self.assertIn("فستان دانتيل", response["reply"])
        self.assertNotIn("صورة", response["reply"])

    def test_existing_enabled_auto_binding_is_visible_to_ai(self):
        self.m.remember_customer_product(self.db, self.sender, CATALOG[0], "auto_default_product")
        with patch.object(self.m, "get_auto_product_settings", return_value={"enabled": True, "product_id": "F1"}):
            self.assertEqual(self.m.load_customer_products(self.db, self.sender)[0]["product_id"], "F1")
        with patch.object(self.m, "get_auto_product_settings", return_value={"enabled": False, "product_id": "F1"}):
            self.assertEqual(self.m.load_customer_products(self.db, self.sender), [])

    def test_default_is_not_restored_after_customer_photo_or_rejection(self):
        with patch.object(self.m, "get_auto_product_settings", return_value={"enabled": True, "product_id": "F1"}):
            self.m.save_message(self.db, self.sender, "incoming", "image", "", "https://example.test/customer.jpg", None, None, {})
            self.assertFalse(self.m.should_use_auto_product(self.db, self.sender, {"text": "سلام عليكم"}, "text", []))

    def test_vision_auth_failure_is_not_classified_as_unknown_product(self):
        import requests
        response = requests.Response(); response.status_code = 401
        with patch.object(self.m, "OPENROUTER_KEY", "test"), patch.object(self.m, "is_store_feature_enabled", return_value=True), patch.object(self.m.requests, "post", side_effect=requests.HTTPError(response=response)):
            result = self.m.confirm_with_vision("https://example.test/customer.jpg", [])
        self.assertTrue(result["service_error"])
        self.assertEqual(result["error_code"], "authentication")
        self.assertEqual(result["http_status"], 401)

    def test_image_service_failure_is_silent(self):
        self.ev.update(text="", image_url="https://example.test/customer.jpg")
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "match_product", return_value=(None, None, {"service_error": True, "error_code": "authentication"})), patch.object(self.m, "create_human_review", return_value=88) as review:
            response = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertEqual(response["reply"], "")
        self.assertTrue(response["meta"]["ai_paused"])
        self.assertIn("authentication", review.call_args.args[2])

    def test_vision_accepts_plain_and_social_image_references_without_ui_filter(self):
        references = ["https://images.test/plain.jpg", "https://scontent.xx.fbcdn.net/screenshot.jpg", "https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=sample"]
        for reference in references:
            with self.subTest(reference=reference), patch.object(self.m, "OPENROUTER_KEY", "test"), patch.object(self.m, "is_store_feature_enabled", return_value=True), patch.object(self.m.requests, "post") as post:
                post.return_value.json.return_value = {"choices": [{"message": {"content": "F1"}}]}
                result = self.m.confirm_with_vision(reference, [dict(CATALOG[0], image_url="https://images.test/product.jpg")])
            content = post.call_args.kwargs["json"]["messages"][1]["content"]
            self.assertEqual(content[1]["image_url"]["url"], reference)
            self.assertTrue(any(self.m.SCREENSHOT_MATCH_GUIDANCE in part.get("text", "") for part in content))
            self.assertEqual(result["product_id"], "F1")

    def test_catalog_also_receives_general_image_matching_guidance(self):
        with patch.object(self.m, "OPENROUTER_KEY", "test"), patch.object(self.m, "is_store_feature_enabled", return_value=True), patch.object(self.m, "_resolve_catalog_image_paths", return_value=["catalog.png"]), patch.object(self.m, "_file_to_data_url", return_value="data:image/png;base64,TEST"), patch.object(self.m.requests, "post") as post:
            post.return_value.json.return_value = {"choices": [{"message": {"content": "F1"}}]}
            result = self.m.match_customer_image_with_catalog("https://images.test/plain.jpg", CATALOG)
        content = post.call_args.kwargs["json"]["messages"][0]["content"]
        self.assertIn(self.m.SCREENSHOT_MATCH_GUIDANCE, content[0]["text"])
        self.assertIn("F1", content[0]["text"])
        self.assertEqual(result["product_id"], "F1")

    def test_image_second_attempt_success_continues_without_review(self):
        ev = dict(self.ev, image_url="https://example.test/customer.jpg")
        with patch.object(self.m, "_match_single_product", side_effect=[(None, None, {"product_found": False}), (CATALOG[1], "image_recognition", {"product_found": True, "product_id": "F2"})]) as match, patch.object(self.m, "create_human_review") as review:
            product, _, result = self.m.match_product(self.db, ev, CATALOG)
        self.assertEqual(match.call_count, 2)
        self.assertEqual(product["product_id"], "F2")
        self.assertEqual(len(result["images"][0]["attempts"]), 2)
        review.assert_not_called()

    def test_image_two_failures_then_silent_handoff(self):
        self.ev.update(text="", image_url="https://example.test/customer.jpg")
        with patch.object(self.m, "extract_facebook_event", return_value=self.ev), patch.object(self.m, "is_ai_enabled", return_value=True), patch.object(self.m, "_match_single_product", return_value=(None, None, {"product_found": False})) as match, patch.object(self.m, "create_human_review", return_value=99) as review, patch.object(self.m, "call_main_ai") as ai:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertEqual(match.call_count, 2)
        review.assert_called_once()
        ai.assert_not_called()
        self.assertEqual(result["reply"], "")
        self.assertTrue(result["meta"]["ai_paused"])
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='outgoing'", (self.sender,)).fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
