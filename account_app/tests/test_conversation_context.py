"""Conversation regressions derived from the 2026-09-08 16:42 backup.

Model/network outputs are fixtures; routing, memory, collection, and storage are real.
"""
import copy
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
import account_app.app as m

CATALOG = [
    dict(product_id="F1", product_name="فستان دانتيل", category="فستان", colors="اسود", sizes="38 إلى 52", price="16000", fabric="لينن", stock="متوفر", status="active", image_url="https://images.test/dress1.jpg"),
    dict(product_id="F2", product_name="فستان انيقة", category="فستان", colors="جوزي", sizes="38 إلى 52", price="15000", fabric="باربي", stock="متوفر", status="active", image_url="https://images.test/dress2.jpg"),
]

class ConversationContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.old_db = m.DB_PATH
        m.DB_PATH = str(Path(cls.temp.name)/"context.db")
        m.init_db()
    @classmethod
    def tearDownClass(cls):
        m.DB_PATH = cls.old_db
        cls.temp.cleanup()
    def setUp(self):
        self.ctx = m.app.app_context(); self.ctx.push()
        self.token = m._current_store_id.set("al-fatena")
        self.db = m.get_db()
        self.sender = "al-fatena::context-" + uuid.uuid4().hex
        m.get_or_create_customer(self.db, self.sender, "page", "facebook")
        self.settings = dict(enabled=True, product_id="F1", product=CATALOG[0], send_image=False)
        for target, value in [("load_products_from_file", CATALOG), ("load_active_products", CATALOG), ("get_auto_product_settings", self.settings), ("is_ai_enabled", True), ("is_store_feature_enabled", False), ("send_telegram_message", False), ("send_webhook_result_to_facebook", True)]:
            patcher = patch.object(m, target, return_value=value); patcher.start(); self.addCleanup(patcher.stop)
        self.ev = dict(sender_id=self.sender, store_id="al-fatena", page_id="page", platform="facebook", text="", image_url=None, attachments=[], ref=None, ad_id=None, postback=None, quick_reply=None, timestamp=0, referral_source=None, referral_type=None)
    def tearDown(self):
        m._current_store_id.reset(self.token); self.ctx.pop()
    def event(self, text="", image=None, model_reply=None):
        ev = dict(self.ev, text=text, image_url=image)
        with patch.object(m, "extract_facebook_event", return_value=ev):
            if model_reply is not None:
                with patch.object(m, "_call_main_ai_once", return_value={"reply": model_reply, "order": {}, "create_order": False}):
                    return m.process_webhook(self.db, {}, use_debounce=False)
            return m.process_webhook(self.db, {}, use_debounce=False)
    def test_demonstrative_keeps_known_product(self):
        m.complete_customer_product_link(self.db, self.sender, CATALOG[0], "manual")
        m.save_message(self.db,self.sender,"incoming","text","سلام عليكم",None,None,None,{})
        product, _, result = m.match_product(self.db, dict(self.ev, text="هو هذا الموديل"), CATALOG)
        self.assertEqual((product or {}).get("product_id"), "F1")
        self.assertFalse((result or {}).get("waiting_for_image"))
    def test_default_applies_to_product_question_not_only_greeting(self):
        with patch.object(m,"generate_first_message_reply",return_value=("أرسلي صورة", "")) as first:
            result = self.event("ما هي مواصفات الفستان؟", model_reply="الفستان دانتيل بسعر 16000")
        first.assert_not_called()
        self.assertIn("دانتيل", result["reply"])
    def test_photo_followed_by_text_in_same_burst_is_matched(self):
        m.save_message(self.db,self.sender,"incoming","image","","https://images.test/customer.jpg",None,None,{})
        self.settings["enabled"] = False
        with patch.object(m,"match_customer_image_with_catalog",return_value={"product_found":True,"product_id":"F2","confidence":100}) as vision:
            self.event("هذة الون والقياس 44",model_reply="قياس 44 متوفر")
        vision.assert_called_once()
        self.assertEqual(m.load_customer_products(self.db,self.sender)[0]["product_id"],"F2")

    def recognize(self, text="", product_id="F2"):
        with patch.object(m, "match_customer_image_with_catalog", return_value={"product_found": True, "product_id": product_id, "confidence": 100}):
            return self.event(text, image="https://images.test/" + uuid.uuid4().hex + ".jpg", model_reply="الموديل متوفر")

    def test_different_photo_asks_add_or_replace_without_changing_old_choice(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        result = self.recognize()
        self.assertTrue(result["meta"]["product_choice_pending"])
        self.assertIn("فستان انيقة",result["reply"])
        self.assertIn("فستان دانتيل",result["reply"])
        self.assertEqual([p["product_id"] for p in m.load_customer_products(self.db,self.sender)],["F1"])
        result = self.event("بس هذا")
        self.assertEqual(result["meta"]["product_choice_resolved"],"replace")
        self.assertEqual([p["product_id"] for p in m.load_customer_products(self.db,self.sender)],["F2"])
        self.assertIn("باربي",self.event("شنو القماش؟")["reply"])

    def test_adding_photo_keeps_both_products_after_next_message(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        self.recognize()
        result = self.event("ضيفيه ويا السابق")
        self.assertEqual(result["meta"]["product_choice_resolved"],"add")
        self.assertEqual({p["product_id"] for p in m.load_customer_products(self.db,self.sender)},{"F1","F2"})
        # Contextual follow-up uses the last explicitly selected model.
        self.assertIn("15,000",self.event("شكد سعره؟")["reply"])
        self.assertEqual({p["product_id"] for p in m.load_customer_products(self.db,self.sender)},{"F1","F2"})

    def test_ambiguous_yes_does_not_choose_add_or_replace(self):
        first = self.recognize()
        self.assertTrue(first["meta"].get("product_choice_pending"), first)
        result = self.event("اي")
        self.assertTrue(result["meta"].get("product_choice_pending"), result)
        self.assertIsNotNone(m.pending_product_choice(self.db,self.sender))
        self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?",(self.sender,)).fetchone()[0],0)

    def test_explicit_addition_in_caption_needs_no_extra_choice(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        result = self.recognize("ضيفي هذا ويا السابق")
        self.assertNotIn("product_choice_pending",result["meta"])
        self.assertEqual({p["product_id"] for p in m.load_customer_products(self.db,self.sender)},{"F1","F2"})

    def test_new_image_matches_default_without_asking_choice(self):
        result = self.recognize(product_id="F1")
        self.assertNotIn("product_choice_pending",result["meta"])
        self.assertEqual(m.load_customer_products(self.db,self.sender)[0]["product_id"],"F1")
        self.assertIn("لينن",self.event("شنو القماش؟")["reply"])

    def test_auto_off_and_no_link_asks_for_photo(self):
        self.settings["enabled"] = False
        with patch.object(m,"generate_first_message_reply",return_value=("أرسلي صورة الموديل", "")):
            result = self.event("سلام عليكم")
        self.assertIn("صورة",result["reply"])
        self.assertEqual(m.load_customer_products(self.db,self.sender),[])

    def test_auto_off_keeps_manual_product_and_answers(self):
        self.settings["enabled"] = False
        m.complete_customer_product_link(self.db,self.sender,CATALOG[1],"manual")
        self.assertIn("باربي",self.event("شنو القماش؟")["reply"])

    def test_failed_album_does_not_commit_partial_product_links(self):
        self.settings["enabled"] = False
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        ev = dict(self.ev, text="بس هذا", image_url="https://images.test/a.jpg", attachments=[{"type":"image","url":"https://images.test/a.jpg"},{"type":"image","url":"https://images.test/b.jpg"}])
        responses = [(CATALOG[1],"image_recognition",{"product_found":True,"product_id":"F2"}),(None,None,{"product_found":False}),(None,None,{"product_found":False})]
        with patch.object(m,"_match_single_product",side_effect=responses) as match:
            product, _, result = m.match_product(self.db,ev,CATALOG)
        self.assertIsNone(product)
        self.assertEqual(match.call_count,3)
        self.assertEqual([p["product_id"] for p in m.load_customer_products(self.db,self.sender)],["F1"])

    def test_main_model_reasking_known_product_is_retried(self):
        with patch.object(m,"_call_main_ai_once",side_effect=[{"reply":"دزيلي صورة الموديل"},{"reply":"يرجى الفحص عند الاستلام"}]) as model:
            result=m.call_main_ai(dict(self.ev,text="الفحص شلون؟"),"text",{},[],CATALOG,CATALOG[0],None,"",[])
        self.assertEqual(model.call_count,2)
        self.assertNotIn("صورة",result["reply"])

    def test_unknown_model_reply_cannot_repeat_with_known_product(self):
        with patch.object(m,"_call_main_ai_once",return_value={"reply":"دزيلي صورة الموديل"}) as model:
            result=m.call_main_ai(dict(self.ev,text="الفحص شلون؟"),"text",{},[],CATALOG,CATALOG[0],None,"",[])
        self.assertEqual(model.call_count,2)
        self.assertTrue(result["failed"])
        self.assertEqual(result["reply"],"")

    def test_collects_text_before_and_after_image_within_burst(self):
        for text,kind,image in [("أريد بس هذا","text",None),("","image","https://images.test/a.jpg"),("قياس 44","text",None)]:
            last=m.save_message(self.db,self.sender,"incoming",kind,text,image,None,None,{})
        ev=m.collect_unanswered_event(self.db,dict(self.ev,text="قياس 44"),last)
        self.assertEqual(ev["image_url"],"https://images.test/a.jpg")
        self.assertIn("أريد بس هذا",ev["text"])
        self.assertIn("قياس 44",ev["text"])
        self.assertEqual(m.detect_message_type(ev),"image")

    def test_async_worker_does_not_drop_photo_before_latest_text(self):
        self.settings["enabled"]=False
        def body(text,image=None):
            return {"object":"page","entry":[{"id":"page","_store_id":"al-fatena","messaging":[{"sender":{"id":self.sender},"message":{"mid":uuid.uuid4().hex,"text":text,"attachments":[{"type":"image","payload":{"url":image}}] if image else []}}]}]}
        photo=body("","https://images.test/async.jpg"); text=body("هذة الون والقياس 44")
        calls=[]
        def during_wait(_):
            calls.append(1)
            if len(calls)==1:
                m._process_manychat_webhook_async(text,self.sender,"facebook")
        with patch.object(m,"DEBOUNCE_DELAY",1), patch.object(m.time,"sleep",side_effect=during_wait), patch.object(m,"match_customer_image_with_catalog",return_value={"product_found":True,"product_id":"F2"}) as vision, patch.object(m,"_call_main_ai_once",return_value={"reply":"قياس 44 متوفر","create_order":False,"order":{}}), patch.object(m,"send_manychat_messages",return_value=True):
            m._process_manychat_webhook_async(photo,self.sender,"facebook")
        self.assertEqual(vision.call_count,1, {"waits":len(calls), "messages":[dict(r) for r in self.db.execute("SELECT sender_id,direction,message_type,text FROM messages WHERE sender_id LIKE ?",("%"+self.sender.split("::",1)[-1],))], "event":m.extract_facebook_event(photo)})
        self.assertEqual(vision.call_args.args[0],"https://images.test/async.jpg")
        self.assertEqual(m.load_customer_products(self.db,self.sender)[0]["product_id"],"F2")
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='incoming'",(self.sender,)).fetchone()[0],2)

    def test_manual_link_resolves_pending_selection(self):
        self.recognize()
        self.assertIsNotNone(m.pending_product_choice(self.db,self.sender))
        m.complete_customer_product_link(self.db,self.sender,CATALOG[1],"manual",source="manual_admin")
        self.assertIsNone(m.pending_product_choice(self.db,self.sender))
        self.assertIn("باربي",self.event("شنو القماش؟")["reply"])

    def test_request_for_real_photo_does_not_resend_same_catalog_image(self):
        m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
        for text in ["ماكو غير هاي الصورة؟", "الفستان حطي وصوريه ودزيه", "تدزين نفس الصوره"]:
            result=self.event(text)
            self.assertIn("ما عندي تصوير إضافي",result["reply"])
            self.assertFalse(result.get("send_image"))
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='outgoing' AND message_type='image'",(self.sender,)).fetchone()[0],0)

    def test_store_configuration_and_image_choice_matrix(self):
        original_sender, original_ev = self.sender, self.ev
        cases=0
        for store in ["default","al-fatena","khuyoot","golden-threads"]:
            for auto in [False,True]:
                for linked in [False,True]:
                    for mode in ["same","replace","add","ask","failed","text_only"]:
                        with self.subTest(store=store,auto=auto,linked=linked,mode=mode):
                            token=m._current_store_id.set(store)
                            try:
                                self.sender=store+"::matrix-"+uuid.uuid4().hex
                                self.ev=dict(original_ev,sender_id=self.sender,store_id=store)
                                self.settings["enabled"]=auto
                                m.get_or_create_customer(self.db,self.sender,"page","facebook")
                                if linked:m.complete_customer_product_link(self.db,self.sender,CATALOG[0],"manual")
                                if mode=="text_only":
                                    with patch.object(m,"generate_first_message_reply",return_value=("أرسلي صورة الموديل", "")):
                                        result=self.event("شنو القماش؟")
                                    self.assertEqual("لينن" in result["reply"],auto or linked)
                                elif mode=="failed":
                                    with patch.object(m,"_match_single_product",return_value=(None,None,{"product_found":False})) as match:
                                        result=self.event("",image="https://images.test/fail.jpg")
                                    self.assertEqual(match.call_count,2)
                                    self.assertEqual(result["reply"],"")
                                    self.assertFalse(m.is_customer_ai_enabled(self.db,self.sender))
                                else:
                                    caption={"same":"","replace":"بس هذا","add":"ضيفيه ويا السابق","ask":""}[mode]
                                    result=self.recognize(caption,"F1" if mode=="same" else "F2")
                                    pending=mode=="ask" and (linked or auto)
                                    self.assertEqual(bool(result["meta"].get("product_choice_pending")),pending)
                                    if pending:self.event("بس هذا")
                                    ids={p["product_id"] for p in m.load_customer_products(self.db,self.sender)}
                                    expected={"F1"} if mode=="same" else {"F1","F2"} if mode=="add" and (auto or linked) else {"F2"}
                                    self.assertEqual(ids,expected)
                                    self.assertTrue(m.is_customer_ai_enabled(self.db,self.sender))
                                self.assertEqual(self.db.execute("SELECT count(*) FROM orders WHERE sender_id=?",(self.sender,)).fetchone()[0],0)
                                cases+=1
                            finally:m._current_store_id.reset(token)
        self.sender,self.ev=original_sender,original_ev
        self.assertEqual(cases,96)

    def test_dashboard_collects_image_before_caption_and_offers_choice(self):
        m.save_message(self.db,self.sender,"incoming","image","","https://images.test/customer.jpg",None,None,{})
        m.save_message(self.db,self.sender,"incoming","text","قياس 44",None,None,None,{})
        client=m.app.test_client()
        with client.session_transaction() as session: session["dashboard_authenticated"]=True
        with patch.object(m,"match_customer_image_with_catalog",return_value={"product_found":True,"product_id":"F2","confidence":100}), patch.object(m,"call_main_ai") as main:
            response=client.post(f"/api/conversations/{self.sender}/ask_ai",json={"allow_empty":True})
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.get_json()["intent"],"product_choice")
        self.assertIn("دانتيل",response.get_json()["reply"])
        main.assert_not_called()

    def test_dashboard_uses_enabled_default_for_faq(self):
        m.save_message(self.db,self.sender,"incoming","text","شنو القماش",None,None,None,{})
        client=m.app.test_client()
        with client.session_transaction() as session: session["dashboard_authenticated"]=True
        with patch.object(m,"call_main_ai",return_value={"reply":"لينن","intent":"product","confidence":100}) as main:
            response=client.post(f"/api/conversations/{self.sender}/ask_ai",json={"allow_empty":True})
        self.assertEqual(response.status_code,200)
        self.assertEqual(main.call_args.args[5]["product_id"],"F1")

    def test_labelled_photos_take_precedence_over_catalog_sheet_numbers(self):
        expected={"product_found":True,"product_id":"F2","confidence":100}
        with patch.object(m,"is_store_feature_enabled",return_value=True), patch.object(m,"confirm_with_vision",return_value=expected) as direct, patch.object(m.requests,"post") as request:
            result=m.match_customer_image_with_catalog("https://images.test/customer.jpg",CATALOG)
        self.assertEqual(result["product_id"],"F2")
        self.assertEqual(result["reference_source"],"labelled_product_images")
        self.assertEqual(len(direct.call_args.args[1]),2)
        request.assert_not_called()

    def test_failed_direct_comparison_cannot_guess_a_catalog_number(self):
        with patch.object(m,"is_store_feature_enabled",return_value=True), patch.object(m,"confirm_with_vision",return_value={"product_found":False,"product_id":""}), patch.object(m.requests,"post") as request:
            result=m.match_customer_image_with_catalog("https://images.test/customer.jpg",CATALOG)
        self.assertFalse(result["product_found"])
        request.assert_not_called()
