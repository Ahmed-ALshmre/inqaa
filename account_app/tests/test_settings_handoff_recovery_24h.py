import sqlite3
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import account_app.app as app_module


class HandoffRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmpdir.name) / "test.db")
        self.orig_db_path = app_module.DB_PATH
        app_module.DB_PATH = self.db_path
        app_module.init_db()
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        now = datetime.now(app_module.sales_intelligence.BAGHDAD)
        recent_iso = (now - timedelta(hours=2)).isoformat()
        old_iso = (now - timedelta(hours=30)).isoformat()

        # Enable store AI globally
        app_module.set_setting(self.conn, "ai_enabled", "1")

        # 1. Eligible recent customer stopped due to credit error (human_reviews pending + ai disabled)
        self.conn.execute("INSERT INTO customers(sender_id, store_id, name) VALUES('s_credit', 'default', 'نور الولاية')")
        self.conn.execute("INSERT INTO messages(sender_id, store_id, direction, message_type, text, created_at) VALUES('s_credit', 'default', 'incoming', 'text', 'أربعة ب40', ?)", (recent_iso,))
        self.conn.execute("INSERT INTO customer_ai_settings(sender_id, enabled, updated_at) VALUES('s_credit', 0, ?)", (recent_iso,))
        self.conn.execute("INSERT INTO human_reviews(sender_id, store_id, reason, status, created_at) VALUES('s_credit', 'default', 'http_402 insufficient credits', 'pending', ?)", (recent_iso,))

        # 2. Eligible recent customer stopped due to unresolvable review
        self.conn.execute("INSERT INTO customers(sender_id, store_id, name) VALUES('s_human', 'default', 'أم علي')")
        self.conn.execute("INSERT INTO messages(sender_id, store_id, direction, message_type, text, created_at) VALUES('s_human', 'default', 'incoming', 'text', 'اريد توصيل اليوم ساعة 5', ?)", (recent_iso,))
        self.conn.execute("INSERT INTO customer_ai_settings(sender_id, enabled, updated_at) VALUES('s_human', 0, ?)", (recent_iso,))
        self.conn.execute("INSERT INTO human_reviews(sender_id, store_id, reason, status, created_at) VALUES('s_human', 'default', 'delivery_deadline', 'pending', ?)", (recent_iso,))

        # 3. Old customer (>24h) -> must be excluded
        self.conn.execute("INSERT INTO customers(sender_id, store_id, name) VALUES('s_old', 'default', 'قديمة')")
        self.conn.execute("INSERT INTO messages(sender_id, store_id, direction, message_type, text, created_at) VALUES('s_old', 'default', 'incoming', 'text', 'مرحبا', ?)", (old_iso,))
        self.conn.execute("INSERT INTO customer_ai_settings(sender_id, enabled, updated_at) VALUES('s_old', 0, ?)", (old_iso,))

        # 4. Recent customer already answered by staff -> must be excluded
        self.conn.execute("INSERT INTO customers(sender_id, store_id, name) VALUES('s_answered', 'default', 'مجاوب عليها')")
        self.conn.execute("INSERT INTO messages(sender_id, store_id, direction, message_type, text, created_at) VALUES('s_answered', 'default', 'incoming', 'text', 'شكد سعره', ?)", (recent_iso,))
        self.conn.execute("INSERT INTO messages(sender_id, store_id, direction, message_type, text, created_at) VALUES('s_answered', 'default', 'outgoing', 'text', 'سعره 15 الف', ?)", (now.isoformat(),))
        self.conn.execute("INSERT INTO customer_ai_settings(sender_id, enabled, updated_at) VALUES('s_answered', 0, ?)", (recent_iso,))

        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        app_module.DB_PATH = self.orig_db_path
        with app_module._HANDOFF_RECOVERY_LOCK:
            app_module._HANDOFF_RECOVERY_RUNS.clear()
        self.tmpdir.cleanup()

    def test_collect_24h_candidates_and_process_resolved_vs_needs_human(self):
        with app_module.app.app_context():
            cands = app_module.collect_handoff_recovery_candidates(self.conn, "default", hours=24)
            sender_ids = [c["sender_id"] for c in cands]
            self.assertEqual(set(sender_ids), {"s_credit", "s_human"})

            def fake_resume(db, sender_id, payload):
                if sender_id == "s_credit":
                    return {"ok": True, "auto_reply": {"sent": True}}
                return {"ok": True, "needs_human": True, "reason": "sales_runtime_review", "error": "تحتاج مراجعة الموظف"}

            with patch.object(app_module, "resume_pending_ai_reply", side_effect=fake_resume):
                # Process s_credit -> resolved & handed back to main AI (enabled=1) & review closed
                c_credit = next(c for c in cands if c["sender_id"] == "s_credit")
                st1, _ = app_module.process_single_handoff_recovery(self.conn, "default", c_credit)
                self.assertEqual("resolved", st1)
                self.assertTrue(app_module.is_customer_ai_enabled(self.conn, "s_credit"))
                rev_st = self.conn.execute("SELECT status FROM human_reviews WHERE sender_id='s_credit'").fetchone()[0]
                self.assertEqual("reviewed", rev_st)

                # Process s_human -> remains needs_human & AI disabled
                c_human = next(c for c in cands if c["sender_id"] == "s_human")
                st2, _ = app_module.process_single_handoff_recovery(self.conn, "default", c_human)
                self.assertEqual("needs_human", st2)
                self.assertFalse(app_module.is_customer_ai_enabled(self.conn, "s_human"))

    def test_worker_waits_between_conversations_and_tracks_top_counters(self):
        sleeps = []
        with app_module.app.app_context():
            cands = app_module.collect_handoff_recovery_candidates(self.conn, "default", hours=24)
            run = {
                "run_id": "test1", "store_id": "default", "status": "running",
                "started_at": app_module.now_baghdad_iso(), "finished_at": None,
                "interval_seconds": 60, "next_run_at": None,
                "total": len(cands), "processed": 0, "resolved": 0,
                "remaining_human": 0, "skipped": 0, "error": "", "items": cands,
            }
            stop_ev = threading.Event()

            def fake_resume(db, sender_id, payload):
                if sender_id == "s_credit":
                    return {"ok": True, "auto_reply": {"sent": True}}
                return {"ok": True, "needs_human": True, "error": "تحتاج موظف"}

            main_tid = threading.get_ident()
            fake_now = [1000.0]
            def fake_time():
                return fake_now[0]
            def fake_sleep(dt):
                if threading.get_ident() == main_tid:
                    sleeps.append(dt)
                    fake_now[0] += dt

            with patch.object(app_module, "resume_pending_ai_reply", side_effect=fake_resume), \
                 patch.object(app_module.time, "time", side_effect=fake_time), \
                 patch.object(app_module.time, "sleep", side_effect=fake_sleep):
                app_module._run_handoff_recovery_worker("default", run, stop_ev)

            self.assertEqual("completed", run["status"])
            self.assertEqual(2, run["total"])
            self.assertEqual(2, run["processed"])
            self.assertEqual(1, run["resolved"])
            self.assertEqual(1, run["remaining_human"])
            # Verified 60 seconds total waited between item 1 and item 2
            self.assertAlmostEqual(60.0, sum(sleeps), delta=1.0)

    def test_start_endpoint_returns_json_without_name_error(self):
        with app_module.app.test_request_context(
            "/api/settings/handoff-recovery/start?store_id=default",
            method="POST",
            json={"hours": 24, "interval_seconds": 60},
        ):
            with patch.object(app_module, "_run_handoff_recovery_worker"):
                resp = app_module.api_handoff_recovery_start.__wrapped__()
                data = resp.get_json()
                self.assertTrue(data["ok"])
                self.assertEqual(2, data["run"]["total"])
                self.assertTrue(data["run"]["run_id"])


if __name__ == "__main__":
    unittest.main()
