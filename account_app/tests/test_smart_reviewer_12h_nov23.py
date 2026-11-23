import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import account_app.app as app_module


class SmartReviewer12hTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(setattr, app_module, 'DB_PATH', app_module.DB_PATH)
        app_module.DB_PATH = str(Path(self.tmp.name) / 'smart_reviewer.db')
        app_module.init_db()
        self.ctx = app_module.app.app_context()
        self.ctx.push()
        self.addCleanup(self.ctx.pop)
        self.db = app_module.get_db()
        app_module.save_smart_reviewer_settings(self.db, {'enabled': True, 'interval_minutes': 30})
        app_module.save_followup_settings(self.db, {
            'enabled': True, 'min_interest_score': 30, 'max_per_day': 2,
            'contact_start_hour': 0, 'contact_end_hour': 23,
        })
        app_module.set_store_setting(self.db, 'ai_enabled', '1', 'default')

    def _seed_sender(self, sender_id, messages, *, lead_score=70, ai_enabled=True, pending_review=False):
        now = datetime.now(app_module.BAGHDAD_TZ).isoformat()
        self.db.execute(
            "INSERT OR REPLACE INTO customers (sender_id, name, store_id, lead_score, first_seen_at, last_seen_at) VALUES (?, ?, 'default', ?, ?, ?)",
            (sender_id, 'زبونة اختبار', lead_score, now, now),
        )
        if not ai_enabled:
            app_module.set_customer_ai_enabled(self.db, sender_id, False, reason='staff_manual', commit=False)
        for direction, text, hours_ago, raw_payload in messages:
            ts = (datetime.now(app_module.BAGHDAD_TZ) - timedelta(hours=hours_ago)).isoformat()
            self.db.execute(
                "INSERT INTO messages (sender_id, direction, message_type, text, created_at, store_id, raw_payload) VALUES (?, ?, 'text', ?, ?, 'default', ?)",
                (sender_id, direction, text, ts, json.dumps(raw_payload) if raw_payload else None),
            )
        if pending_review:
            self.db.execute(
                "INSERT INTO human_reviews (sender_id, store_id, message_text, reason, status, created_at) VALUES (?, 'default', 'سؤال', 'manual', 'pending', ?)",
                (sender_id, now),
            )
        self.db.commit()

    def test_filters_ai_on_only_ai_replied_only_and_12h_window(self):
        # 1. Valid: within 12h (2h ago), AI replied last, customer stopped replying
        self._seed_sender('s-valid', [
            ('incoming', 'شكد سعر الروب؟', 2.2, None),
            ('outgoing', 'سعر الروب 18 ألف حبيبتي، يا لون يعجبج؟', 2.0, {'sender_type': 'ai'}),
        ])
        # 2. Too old: 14h ago (> 12h cutoff)
        self._seed_sender('s-old', [
            ('incoming', 'شكد سعره؟', 14.2, None),
            ('outgoing', 'سعره 18 ألف', 14.0, {'sender_type': 'ai'}),
        ])
        # 3. Conversation AI disabled (staff handling manually)
        self._seed_sender('s-ai-off', [
            ('incoming', 'اريد اسود', 2.2, None),
            ('outgoing', 'متوفر اسود', 2.0, {'sender_type': 'ai'}),
        ], ai_enabled=False)
        # 4. Staff replied last (not AI)
        self._seed_sender('s-staff-replied', [
            ('incoming', 'متوفر قياس 44؟', 2.2, None),
            ('outgoing', 'اي عيوني متوفر', 2.0, {'sender_type': 'staff'}),
        ])
        # 5. Customer replied last (no AI reply to latest turn)
        self._seed_sender('s-unanswered', [
            ('incoming', 'مرحبا', 2.0, None),
        ])
        # 6. Pending human review
        self._seed_sender('s-review', [
            ('incoming', 'شكد طوله؟', 2.2, None),
            ('outgoing', 'خل أتأكدلج', 2.0, {'sender_type': 'ai'}),
        ], pending_review=True)

        fake_resp = Mock()
        fake_resp.raise_for_status.return_value = None
        fake_resp.json.return_value = {
            'choices': [{'message': {'content': json.dumps({
                'tag': 'مهتم بالروب',
                'needs_followup': True,
                'followup_message': 'هلا حبيبتي، بعدج حابة تختارين لون للروب قبل ما تخلص القطع؟ 🌷',
            }, ensure_ascii=False)}}]
        }

        with patch.object(app_module.ai_transport, 'post', return_value=fake_resp) as post_mock, \
             patch.object(app_module, 'OPENROUTER_KEY', 'test-key'):
            app_module.run_smart_reviewer_cycle(self.db)

        # ONLY s-valid should have been reviewed by the model!
        self.assertEqual(1, post_mock.call_count)
        tag_row = self.db.execute("SELECT tag, needs_followup FROM customer_tags WHERE sender_id='s-valid'").fetchone()
        self.assertIsNotNone(tag_row)
        self.assertEqual('مهتم بالروب', tag_row['tag'])
        self.assertEqual(1, tag_row['needs_followup'])

        # Make the scheduled followup due and send it, verifying dashboard tag gets marked
        past = (datetime.now(app_module.BAGHDAD_TZ) - timedelta(minutes=5)).isoformat()
        self.db.execute("UPDATE followups SET scheduled_at=? WHERE sender_id='s-valid'", (past,))
        self.db.commit()
        with patch.object(app_module, 'send_reply_via_manychat', return_value=True), \
             patch.object(app_module, 'build_followup_message', return_value='هلا حبيبتي، بعدج حابة تختارين لون للروب؟ 🌷'):
            res = app_module.send_due_followups(self.db)
        self.assertEqual(1, res['sent'])
        tag_after = self.db.execute("SELECT followup_sent FROM customer_tags WHERE sender_id='s-valid'").fetchone()
        self.assertEqual(1, tag_after['followup_sent'])

    def test_skips_entire_store_when_store_ai_disabled(self):
        app_module.set_store_setting(self.db, 'ai_enabled', '0', 'default')
        self._seed_sender('s-store-off', [
            ('incoming', 'شكد سعره؟', 2.2, None),
            ('outgoing', 'سعره 18 ألف', 2.0, {'sender_type': 'ai'}),
        ])
        with patch.object(app_module.ai_transport, 'post') as post_mock, \
             patch.object(app_module, 'OPENROUTER_KEY', 'test-key'):
            app_module.run_smart_reviewer_cycle(self.db)
        self.assertEqual(0, post_mock.call_count)

    def test_vision_tiebreaker_resolves_conflicting_batch_matches(self):
        cands = [
            {'product_id': 'P013', 'product_name': 'سوت فرو', 'image_urls': ['https://example.test/p13.jpg']},
            {'product_id': 'P016', 'product_name': 'روب فرو', 'image_urls': ['https://example.test/p16.jpg']},
            {'product_id': 'P017', 'product_name': 'شال', 'image_urls': ['https://example.test/p17.jpg']},
        ]
        # Force 2 batches on the first call, 1 batch on the tiebreaker call
        batch_calls = []

        def fake_batches(payload, prefix, groups, suffix=()):
            batch_calls.append(len(groups))
            if len(groups) == 3:
                return [
                    dict(payload, messages=[{'role': 'user', 'content': list(prefix) + groups[0] + list(suffix)}]),
                    dict(payload, messages=[{'role': 'user', 'content': list(prefix) + groups[1] + groups[2] + list(suffix)}]),
                ]
            return [dict(payload, messages=[{'role': 'user', 'content': list(prefix) + [x for g in groups for x in g] + list(suffix)}])]

        def make_resp(pid):
            r = Mock()
            r.raise_for_status.return_value = None
            r.json.return_value = {'choices': [{'message': {'content': json.dumps({'product_id': pid, 'observed_color': 'أسود'})}}]}
            return r

        with patch.object(app_module.vision_payload, 'request_batches', side_effect=fake_batches), \
             patch.object(app_module.vision_payload, 'image_url', side_effect=lambda u, **kw: u), \
             patch.object(app_module, '_image_ref_for_openrouter', side_effect=lambda u: u), \
             patch.object(app_module, 'OPENROUTER_KEY', 'test-key'), \
             patch.object(app_module.ai_transport, 'post', side_effect=[make_resp('P013'), make_resp('P016'), make_resp('P016')]) as post_mock:
            res = app_module.confirm_with_vision('https://example.test/cust.jpg', cands, catalog_model='google/gemini-2.5-flash')

        self.assertTrue(res['product_found'])
        self.assertEqual('P016', res['product_id'])
        self.assertEqual([3, 2], batch_calls)
        self.assertEqual(3, post_mock.call_count)

    def test_multi_product_photos_passes_all_matched_products_and_prices_to_writer(self):
        from account_app.sales_runtime.cart import Catalog
        from account_app.sales_runtime.response import Responses
        from account_app.sales_runtime.store import Scope, Store

        db = app_module.get_db()
        store = Store(db)
        scope = Scope('default', 'chatwoot', '1', 'default::multi-photo-test')
        ev_id = store.append(scope, 'm-1', kind='message', actor='customer',
                             payload={'text': 'ممكن الأسعار', 'media_kind': 'image'})['id']
        snap = store.snapshot(scope)
        catalog = Catalog('default', [
            {'store_id': 'default', 'product_id': 'P013', 'product_name': 'تراك كاروهات فرو', 'status': 'active',
             'price': 17000, 'offer': 'قطعتين 30 الف', 'colors': 'أسود، جوزي', 'sizes': 'L-XXL', 'availability': 'متوفر'},
            {'store_id': 'default', 'product_id': 'P015', 'product_name': 'شال كيبور', 'status': 'active',
             'price': 22000, 'offer': '3 قطع ب22 الف', 'colors': 'أسود، بيجي', 'sizes': 'فري سايز', 'availability': 'متوفر'},
        ])
        captured_ctx = {}

        def fake_writer(ctx):
            captured_ctx.update(ctx)
            return [
                dict(type='text', text='وصلتني الصور عيوني، تراك الكاروهات الفرو متوفر والقطعة بـ 17 ألف والقطعتين بـ 30 ألف.'),
                dict(type='text', text='والشال الكيبور متوفر بعرض 3 قطع بـ 22 ألف.'),
                dict(type='text', text='وإذا حابة تاخذيهم سوية يوصلونج بطلب واحد وبنفس التوصيل، تحبين نثبتلج الاثنين لو نبدي بواحد منهم؟'),
            ]

        prepared = Responses(store, lambda _s: catalog, fake_writer).prepare(
            scope, ev_id,
            dict(action='media_unresolved', state_version=snap['version'],
                 multiple_images=True, photo_stage='product-detail', unreadable_photo=False,
                 matched_product_ids=['P013', 'P015']),
        )
        self.assertEqual('queued', prepared['action'])
        self.assertEqual(3, prepared['parts'])
        matched = captured_ctx['photo_clarification']['matched_products']
        self.assertEqual(2, len(matched))
        self.assertEqual(460, captured_ctx['copy_budget'])
        self.assertTrue(all(item['price_summary'] for item in matched))

    def test_pending_input_recovers_ad_id_after_staff_reply_or_multi_day_pause(self):
        from account_app.sales_runtime import live as sales_live

        db = app_module.get_db()
        sender = 'baraah-kids::zn-sumaya-test'
        old_ts = (datetime.now(app_module.BAGHDAD_TZ) - timedelta(days=3)).isoformat()
        now_ts = datetime.now(app_module.BAGHDAD_TZ).isoformat()
        db.execute(
            "INSERT INTO messages (sender_id, direction, message_type, text, ad_id, created_at, store_id) VALUES (?, 'incoming', 'text', 'مرحبا', '120252058171770095', ?, 'baraah-kids')",
            (sender, old_ts),
        )
        staff_id = db.execute(
            "INSERT INTO messages (sender_id, direction, message_type, text, created_at, store_id, raw_payload) VALUES (?, 'outgoing', 'text', 'ممكن توضيح', ?, 'baraah-kids', '{\"sender_type\":\"staff\"}')",
            (sender, now_ts),
        ).lastrowid
        inc_id = db.execute(
            "INSERT INTO messages (sender_id, direction, message_type, text, created_at, store_id) VALUES (?, 'incoming', 'text', 'اربيل قرية الكوري', ?, 'baraah-kids')",
            (sender, now_ts),
        ).lastrowid
        db.commit()
        row = {
            'sender_id': sender, 'store_id': 'baraah-kids', 'channel': 'zernio',
            'account_id': 'acc1', 'first_incoming_id': 1, 'handled_id': 0,
        }
        event, _ = sales_live.pending_input(app_module, db, row, inc_id, staff_id)
        self.assertEqual('120252058171770095', event['ad'].get('ad_id'))


if __name__ == '__main__':
    unittest.main()
