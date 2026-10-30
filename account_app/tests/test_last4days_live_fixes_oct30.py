"""Regression tests for the 6 live failure patterns identified in the Oct 26-30 audit."""
import json
import unittest
from unittest.mock import patch

from account_app import (
    checkout,
    owner_sales,
    product_links,
    sales_context,
    sales_playbook,
    sales_strategy,
)
from account_app.tests import test_stale_detail_checkout_oct28 as fixtures


class Last4DaysUnitTests(unittest.TestCase):
    def test_iraqi_districts_and_neighborhoods_resolve_province_and_address(self):
        cases = [
            ("طوزخورماتو محله 108 زقاق 33 دار 25", "صلاح الدين"),
            ("العنوان ناحيه سلام قلعه قصاب", "ميسان"),
            ("الكرادة داخل قرب مستشفى الراهبات", "بغداد"),
            ("دوره ميكانيك شارع الكنيسة", "بغداد"),
            ("الفلوجة حي الجولان قرب الجامع", "الأنبار"),
            ("الشطرة حي المعلمين قرب المدرسة", "ذي قار"),
        ]
        for text, expected_province in cases:
            fields = checkout.contact_fields(text)
            self.assertEqual(fields["province"], expected_province, text)
            self.assertTrue(fields["address"], text)

    def test_confirmation_recognizes_iraqi_enthusiastic_fillers(self):
        for phrase in (
            "اي اي بسرعه زاد",
            "اي والله ثبتي فدوه",
            "نعم اكيد ثبتيه بلا زحمه",
            "تمام ماشي اعتمديه",
        ):
            self.assertTrue(checkout.is_confirmation(phrase), phrase)

    def test_equivalent_letter_and_numeric_size_is_resolved_not_wiped(self):
        context = sales_context.advance(
            {},
            [
                {"direction": "outgoing", "text": "شكد قياس تلبسين؟"},
                {"direction": "incoming", "text": "اكس لارج او ٤٢"},
            ],
        )
        selected = sales_playbook.customer_selection(context)
        self.assertEqual(selected["size"], "XL")

        product = {
            "product_id": "F1",
            "product_name": "فستان سهرة",
            "sizes": "L XL XXL",
            "colors": "ماروني",
            "price": "35000",
            "stock": "متوفر",
        }
        step = sales_playbook.price_next_step(product, context, {"province": "بغداد"})
        self.assertNotIn("شكد قياس", step)
        self.assertIn("حجز", step)

    def test_two_numeric_sizes_with_or_remain_unresolved(self):
        context = sales_context.advance(
            {},
            [
                {"direction": "outgoing", "text": "شكد قياس تلبسين؟"},
                {"direction": "incoming", "text": "قياس 42 لو 44"},
            ],
        )
        selected = sales_playbook.customer_selection(context)
        self.assertIsNone(selected["size"])

    def test_question_rotation_prevents_identical_consecutive_questions(self):
        product = {
            "product_id": "P1",
            "product_name": "فستان تجريبي",
            "sizes": "38 40 42 44",
            "colors": "اسود",
            "price": "25000",
            "stock": "متوفر",
        }
        history = [{"direction": "outgoing", "text": "سعره 25 ألف، شكد قياس تلبسين؟"}]
        step1 = sales_playbook.price_next_step(product, {}, {}, history=history)
        self.assertNotEqual(step1, "شكد قياس تلبسين؟")
        self.assertIn("قياس", step1)

        step2 = owner_sales.next_step(product, {}, {}, history=history)
        self.assertNotEqual(step2, "شكد قياس تلبسين؟")
        self.assertIn("قياس", step2)

    def test_out_of_stock_owner_sales_does_not_repeat_same_sentence_on_affirmative_reply(self):
        product = {
            "product_id": "OUT1",
            "product_name": "فستان نافد",
            "sizes": "38 40 42",
            "colors": "اسود",
            "price": "25000",
            "stock": "نفذ",
        }
        history = [
            {"direction": "outgoing", "text": "للأسف هذا الموديل ما متوفر، تحبين أقترحلج غيره؟"},
            {"direction": "incoming", "text": "أي نعم"},
        ]
        reply = owner_sales.reply(
            {"text": "أي نعم"},
            product,
            {"gender": "female"},
            {},
            history,
        )
        self.assertIsNone(reply)

    def test_customer_behavior_analysis_and_psychological_tactics(self):
        explorer = sales_strategy.analyze_customer_behavior(
            {"text": "بيش ذني الموديلات؟", "_image_matches": [{"product_id": "A"}, {"product_id": "B"}]},
            {"gender": "female"},
            history=[],
        )
        self.assertIn("Multi-Model Explorer", explorer)
        self.assertIn("الإغلاق التخييري المزدوج", explorer)

        quality = sales_strategy.analyze_customer_behavior(
            {"text": "شنو نوع القماش ومبطن لو شفاف؟"},
            {"gender": "female"},
            history=[],
        )
        self.assertIn("الجودة", quality)
        self.assertIn("عكس المخاطرة", quality)

        value = sales_strategy.analyze_customer_behavior(
            {"text": "شكد السعر ويه التوصيل واكو عرض اذا اخذ قطعتين؟"},
            {"gender": "female"},
            history=[],
        )
        self.assertIn("حساس للسعر ويبحث عن القيمة", value)
        self.assertIn("تأطير القيمة", value)

        decisive = sales_strategy.analyze_customer_behavior(
            {"text": "قياس 42 اللون الاسود"},
            {"gender": "female"},
            history=[],
        )
        self.assertIn("عملي وحاسم سريع الاستجابة", decisive)
        self.assertIn("الإغلاق الافتراضي السلس", decisive)

        guide = sales_strategy.guidance(
            {"text": "شنو نوع القماش؟", "_sales_context": {"selected_size": "42"}},
            {"gender": "female"},
            history=[],
        )
        self.assertIn("[تحليل سلوك الزبون وتكتيك البيع النفسي المخطط لهذه الرسالة]", guide)

    def test_correlate_multi_products_links_models_and_suggests_matching_alternatives(self):
        m1 = {
            "product_id": "M1",
            "product_name": "سوت كريب ملكي",
            "category": "سوت نسائي",
            "sizes": "38 40 42 44",
            "colors": "اسود ماروني",
            "price": "25000",
            "stock": "متوفر",
            "status": "active",
        }
        m2 = {
            "product_id": "M2",
            "product_name": "سوت لينن صيفي",
            "category": "سوت نسائي",
            "sizes": "38 40 42",
            "colors": "بيج",
            "price": "22000",
            "stock": "نفذ",
            "status": "active",
        }
        alt = {
            "product_id": "M3",
            "product_name": "سوت باربي راقي",
            "category": "سوت نسائي",
            "sizes": "38 40 42 44",
            "colors": "اسود نيلي",
            "price": "26000",
            "stock": "متوفر",
            "status": "active",
            "image_url": "https://example.com/m3.jpg",
        }
        corr = product_links.correlate_multi_products(
            [m1, m2],
            [m1, m2, alt],
            size="42",
            request="بيش ذني واريد قياس 42",
        )
        self.assertEqual(len(corr["available_models"]), 1)
        self.assertEqual(len(corr["unavailable_models"]), 1)
        self.assertIn("sets", corr["shared_categories"])
        self.assertEqual(len(corr["suggested_alternatives"]), 1)
        self.assertEqual(corr["suggested_alternatives"][0]["product_id"], "M3")
        self.assertIn("متوفر", corr["coaching"])


class Last4DaysPipelineTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.StaleDetailCheckoutTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.StaleDetailCheckoutTests.tearDownClass.__func__)
    tearDown = fixtures.StaleDetailCheckoutTests.tearDown
    save = fixtures.StaleDetailCheckoutTests.save
    order_count = fixtures.StaleDetailCheckoutTests.order_count
    assert_confirmation = fixtures.StaleDetailCheckoutTests.assert_confirmation

    def setUp(self):
        fixtures.StaleDetailCheckoutTests.setUp(self)
        self.product.update(
            product_name="فستان باربي",
            colors="ماروني اسود",
            sizes="من 60 كيلو الى 85 كيلو",
            price="18000",
            stock="متوفر",
        )
        self.m.bind_customer_to_product(self.db, self.sender, self.product, source="image_recognition")

    def process(self, text):
        result, model = fixtures.StaleDetailCheckoutTests.process(self, text)
        with patch.object(self.m, "send_text_to_facebook", return_value=True), \
             patch.object(self.m, "send_image_to_facebook", return_value=True):
            self.m.send_webhook_result_to_facebook(result, self.sender)
        return result, model

    def test_weight_only_product_with_prior_dress_size_and_valid_weight_books(self):
        """Reproduces default::cw-184605-14009: dress size 40/42 + valid weight 70kg + color + contact."""
        self.save("outgoing", "سعره 18 ألف، شكد قياسج؟")
        self.save("incoming", "اني البس ٤٠ او ٤٢")
        self.save("incoming", "اني وزني ٧٠")
        self.save("incoming", "ردت اللون الماروني")
        result, model = self.process("طوزخورماتو محله 108 زقاق 33 دار 25 07701234567")
        model.assert_not_called()
        self.assert_confirmation(result)
        row = self.db.execute("SELECT * FROM orders WHERE sender_id=?", (self.sender,)).fetchone()
        self.assertEqual(row["province"], "صلاح الدين")
        items = json.loads(row["order_items"])
        self.assertEqual(items[0]["color"], "ماروني")
        self.assertEqual(items[0]["size_type"], "weight")
        self.assertEqual(items[0]["weight"], "70")

    def test_enthusiastic_confirmation_books_past_old_unanswered_lining_question(self):
        """Reproduces al-fatena::...-6bca2f79e0aca8fdd8a47b771eca85bc."""
        self.product.update(
            product_name="فستان مناسبات",
            colors="وردي",
            sizes="38 40 42 44",
            price="35000",
            stock="متوفر",
        )
        self.m.bind_customer_to_product(self.db, self.sender, self.product, source="image_recognition")
        self.save("incoming", "مبطن؟")
        self.save("incoming", "قياس 42")
        self.save("incoming", "بغداد الكرادة داخل قرب مستشفى الراهبات 07701234567")
        self.m.capture_customer_contact(
            self.db, self.sender, "بغداد الكرادة داخل قرب مستشفى الراهبات 07701234567"
        )
        self.save("outgoing", "تحبين هسه أثبتلج الحجز على قياس 42؟")
        result, model = self.process("اي اي بسرعه زاد")
        model.assert_not_called()
        self.assert_confirmation(result)
        self.assertEqual(self.order_count(), 1)

    def test_out_of_stock_consent_sends_available_catalog_alternatives(self):
        """Reproduces al-fatena::...-cc5ec52611410ca282da93a1ef81143a."""
        self.product.update(
            product_name="سوت نافد",
            category="سوت نسائي",
            colors="اسود",
            sizes="38 40 42 44",
            price="25000",
            stock="نفذ",
            status="active",
        )
        self.m.bind_customer_to_product(self.db, self.sender, self.product, source="image_recognition")
        alt_product = dict(
            product_id="ALT_1",
            product_name="سوت ملكي متوفر",
            category="سوت نسائي",
            colors="اسود ماروني",
            sizes="38 40 42 44",
            price="25000",
            stock="متوفر",
            status="active",
            image_url="https://example.com/alt1.jpg",
        )
        for name in ("load_products_from_file", "load_active_products"):
            p = patch.object(self.m, name, return_value=[self.product, alt_product])
            p.start()
            self.patches.append(p)
        self.save("outgoing", "للأسف هذا الموديل ما متوفر، تحبين أقترحلج غيره؟")
        result, model = self.process("أي نعم")
        model.assert_not_called()
        self.assertNotIn("للأسف هذا الموديل ما متوفر", result["reply"])
        self.assertIn("سوت ملكي متوفر", result["reply"])

    def test_multi_product_out_of_stock_consent_sends_catalog_alternatives(self):
        """Customer sent 2 out-of-stock photos and replied 'أي نعم' to 'تحبين أقترحلج غيرهن؟'."""
        self.product.update(
            product_name="سوت نافد 1",
            category="سوت نسائي",
            colors="اسود",
            sizes="38 40 42 44",
            price="25000",
            stock="نفذ",
            status="active",
        )
        second_out = dict(
            product_id="OUT_2",
            product_name="سوت نافد 2",
            category="سوت نسائي",
            colors="بيج",
            sizes="38 40 42",
            price="24000",
            stock="نفذ",
            status="active",
            image_url="https://example.com/out2.jpg",
        )
        alt_product = dict(
            product_id="ALT_MULTI_1",
            product_name="سوت ملكي بديل",
            category="سوت نسائي",
            colors="اسود ماروني",
            sizes="38 40 42 44",
            price="25000",
            stock="متوفر",
            status="active",
            image_url="https://example.com/alt_multi1.jpg",
        )
        self.m.complete_customer_product_link(
            self.db, self.sender, self.product, "image_recognition", preserve_existing=True
        )
        self.m.complete_customer_product_link(
            self.db, self.sender, second_out, "image_recognition", preserve_existing=True
        )
        for name in ("load_products_from_file", "load_active_products"):
            p = patch.object(self.m, name, return_value=[self.product, second_out, alt_product])
            p.start()
            self.patches.append(p)
        self.save("outgoing", "للأسف هالموديلات ما متوفرة، تحبين أقترحلج غيرهن؟")
        result, model = self.process("أي نعم")
        model.assert_not_called()
        self.assertIn("سوت ملكي بديل", result["reply"])
