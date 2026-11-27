import sqlite3
import unittest

from account_app import checkout, sales_recovery
from account_app.sales_runtime import copy_guard
from account_app.sales_runtime.contact import capture as capture_contact
from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.sales_runtime.store import Scope, Store


class TestNov27BookingAndSalesFixes(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.execute("""CREATE TABLE orders(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id TEXT, sender_id TEXT, customer_name TEXT, phone TEXT,
            province TEXT, address TEXT, product_id TEXT, product_name TEXT,
            color TEXT, size TEXT, notes TEXT, status TEXT, order_items TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            product_total INTEGER, delivery_fee INTEGER, total_amount INTEGER,
            is_paid INTEGER DEFAULT 0
        )""")
        self.store = Store(self.db)
        self.store.migrate()
        self.scope = Scope('baraah-kids', 'zernio', 'acc1', 'cust1')
        self.products = [
            {
                'product_id': 'P019',
                'name': 'طقم نزلات',
                'price_iqd': 20000,
                'colors': ['أحمر', 'وردي'],
                'sizes': 'من سنة إلى 5 سنوات',
                'selection_mode': 'age',
                'min_age_months': 12,
                'max_age_months': 60,
                'status': 'active',
                'stock_status': 'available',
            },
        ]

    def test_turij_and_baghdad_colloquial_districts_extract_province(self):
        # Um Zuzu: "طوريج حي ضباط" must resolve to كربلاء
        res = checkout.contact_fields('طوريج حي ضباط')
        self.assertEqual('كربلاء', res.get('province'))
        self.assertEqual('طوريج حي ضباط', res.get('address'))
        # Colloquial Baghdad districts without "ال"
        self.assertEqual('بغداد', checkout.contact_fields('زعفرانية شارع المعهد').get('province'))
        self.assertEqual('بغداد', checkout.contact_fields('بنوك قرب السوق').get('province'))

    def test_superseded_phone_bubble_is_recovered_on_followup_turn(self):
        # Zahrat Ayloul: address saved on E1 (completed), then E2 ("٠٧٨٠٧٣٧٨٠٢٩")
        # gets superseded by E3 ("دزيته الج") before E2 saves contact.
        e1 = self.store.append(self.scope, 'm1', kind='message', actor='customer', payload=dict(text='كربلاء المقدسة حي العامل مقابيل المستوصف'))
        capture_contact(self.store, self.scope, e1['id'], None)
        snap = self.store.snapshot(self.scope)
        self.store.apply(self.scope, f"turn-v3:{e1['id']}:done", snap['version'], {}, lambda s: dict(outcome=dict(action='continue')))

        e2 = self.store.append(self.scope, 'm2', kind='message', actor='customer', payload=dict(text='٠٧٨٠٧٣٧٨٠٢٩'))
        e3 = self.store.append(self.scope, 'm3', kind='message', actor='customer', payload=dict(text='دزيته الج'))
        captured = capture_contact(self.store, self.scope, e3['id'], None)
        self.assertIn('phone', captured.get('fields', []))
        final_contact = self.store.snapshot(self.scope)['data']['contact']
        self.assertEqual('07807378029', final_contact.get('phone'))
        self.assertEqual('كربلاء', final_contact.get('province'))

    def test_single_burst_booking_with_eta_question_not_hijacked_by_order_tracking(self):
        # Um Aba Al-Fadl: sends product + address + phone + "شوكت يوصل طلب" in one burst
        # before cart exists -> must NOT route to order_tracking handoff.
        seen = []

        def decide(context):
            seen.append(context)
            return dict(intent='buy', changes=[], contact={}, answer={})

        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, prov: 5000, decide)
        burst = (
            'سلام عليكم\nاريد فستان احمر\nعمر 3\n'
            'العنوان نجف مجمعات سكنيه مجمع ابو تراب عمود رقم 1\n'
            'تمام\n07844504936\nشوكت يوصل طلب\nشكرا'
        )
        res = entry.receive(
            store_id='baraah-kids', channel='zernio', account_id='acc1',
            sender_id='um-aba-alfadl', message_id='burst1', text=burst,
        )
        self.assertNotEqual('human', res.get('action'))
        self.assertNotEqual('order_tracking', res.get('reason'))

    def test_phantom_photo_promises_are_blocked_by_copy_guard(self):
        phantom_replies = [
            'تدللين من عيوني حبيبتي. هسة أحضرلج صور التراكات والسوتات الشتوية وادزهن الج.',
            'ثواني وأدزهن الج حبيبتي، موديلات تخبل ودافية.',
            'هسه اجهزلج صور الموديلات البناتية الحلوة المتوفرة لعمر سنة ونص.',
            'ثواني وأدزلج أحلى الموديلات المتوفرة لعمر البنوتة.',
            'دقايق وما نطوّل عليج ونكون يمج بأحلى الموديلات.',
            'لحظات وكل التفاصيل تكون يمج حتى تشوفين.',
        ]
        for txt in phantom_replies:
            with self.subTest(txt=txt):
                self.assertTrue(sales_recovery.promises_photo_attachment(txt))
                with self.assertRaises(ValueError):
                    copy_guard.validate_copy([dict(type='text', text=txt)], None, {})


if __name__ == '__main__':
    unittest.main()
