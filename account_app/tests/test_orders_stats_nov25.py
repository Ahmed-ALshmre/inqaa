import sqlite3
import tempfile
import unittest
from pathlib import Path

import account_app.app as app_module


class OrdersStatsAndDateFilterTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmpdir.name) / "orders_test.db")
        self.orig_db = app_module.DB_PATH
        app_module.DB_PATH = self.db_path
        app_module.init_db()
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

        # 4 customers in baraah-kids on 2026-11-25 (1 message has empty store_id to test fallback via customers)
        for sid in ("b1", "b2", "b3", "b4"):
            self.conn.execute(
                "INSERT INTO customers(sender_id, store_id, name, first_seen_at) VALUES(?, 'baraah-kids', ?, '2026-11-25T09:00:00+03:00')",
                (sid, sid),
            )
            msg_store = "" if sid == "b4" else "baraah-kids"
            self.conn.execute(
                "INSERT INTO messages(sender_id, store_id, direction, message_type, text, created_at) VALUES(?, ?, 'incoming', 'text', 'مرحبا', '2026-11-25T10:00:00+03:00')",
                (sid, msg_store),
            )

        # Orders on 2026-11-25 in baraah-kids:
        # b1 -> 1 active order (25,000)
        # b2 -> 1 active order (15,000) + 1 cancelled Arabic ('ملغي', 20,000)
        # b3 -> 1 cancelled English ('cancelled', 30,000)
        self.conn.execute(
            "INSERT INTO orders(sender_id, store_id, customer_name, status, total_amount, created_at) VALUES('b1', 'baraah-kids', 'B1', 'new', 25000, '2026-11-25T11:00:00+03:00')"
        )
        self.conn.execute(
            "INSERT INTO orders(sender_id, store_id, customer_name, status, total_amount, created_at) VALUES('b2', 'baraah-kids', 'B2', 'confirmed', 15000, '2026-11-25T12:00:00+03:00')"
        )
        self.conn.execute(
            "INSERT INTO orders(sender_id, store_id, customer_name, status, total_amount, created_at) VALUES('b2', 'baraah-kids', 'B2', 'ملغي', 20000, '2026-11-25T12:30:00+03:00')"
        )
        self.conn.execute(
            "INSERT INTO orders(sender_id, store_id, customer_name, status, total_amount, created_at) VALUES('b3', 'baraah-kids', 'B3', 'cancelled', 30000, '2026-11-25T13:00:00+03:00')"
        )

        # Yesterday's order (2026-11-24) -> must NOT appear when filtering 2026-11-25..2026-11-25
        self.conn.execute(
            "INSERT INTO customers(sender_id, store_id, name, first_seen_at) VALUES('b_old', 'baraah-kids', 'Old', '2026-11-24T08:00:00+03:00')"
        )
        self.conn.execute(
            "INSERT INTO orders(sender_id, store_id, customer_name, status, total_amount, created_at) VALUES('b_old', 'baraah-kids', 'Old', 'new', 18000, '2026-11-24T15:00:00+03:00')"
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        app_module.DB_PATH = self.orig_db
        self.tmpdir.cleanup()

    def test_today_filter_stats_conversion_and_revenue(self):
        res = app_module._orders_payload(
            self.conn, date_from="2026-11-25", date_to="2026-11-25", store_id="baraah-kids"
        )
        self.assertEqual(4, res["total"])
        self.assertEqual(2, res["active_orders"])
        self.assertEqual(2, res["cancelled_count"])
        self.assertEqual(1, res["new_count"])
        self.assertEqual(40000, res["total_revenue"])  # 25k + 15k (excludes 20k + 30k cancelled)
        self.assertEqual(4, res["people_count"])       # b1, b2, b3, b4
        self.assertEqual(2, res["ordering_people_count"])  # b1, b2 (excludes b3 cancelled)
        self.assertEqual(50.0, res["conversion_rate"])

    def test_yesterday_and_invalid_date_range(self):
        yest = app_module._orders_payload(
            self.conn, date_from="2026-11-24", date_to="2026-11-24", store_id="baraah-kids"
        )
        self.assertEqual(1, yest["active_orders"])
        self.assertEqual(18000, yest["total_revenue"])
        self.assertEqual(100.0, yest["conversion_rate"])

        with self.assertRaises(ValueError):
            app_module._orders_payload(self.conn, date_from="2026-11-26", date_to="2026-11-24")


if __name__ == "__main__":
    unittest.main()
