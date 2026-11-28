import sqlite3
import unittest

from account_app import checkout, sales_recovery
from account_app.sales_runtime import copy_guard
from account_app.sales_runtime.contact import capture as capture_contact
from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.sales_runtime.orders import Orders, migrate_tasks
from account_app.sales_runtime.runtime import Runtime
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
                'store_id': 'baraah-kids',
                'product_id': 'P019',
                'product_name': 'طقم نزلات',
                'name': 'طقم نزلات',
                'price': '20000',
                'price_iqd': 20000,
                'colors': 'أحمر، وردي',
                'sizes': 'من 1 إلى 5 سنوات',
                'size_system': 'age',
                'selection_mode': 'age',
                'age_min_months': 12,
                'age_max_months': 60,
                'status': 'active',
                'stock': 'متوفر',
                'ad_id': '120251829540240095',
                'ad_ids': ['120251829540240095'],
            },
            {
                'store_id': 'baraah-kids',
                'product_id': 'P024',
                'product_name': 'تراك بناتي سحاب',
                'name': 'تراك بناتي سحاب',
                'price': '12000 او قطعتين ب 20000',
                'price_iqd': 12000,
                'colors': 'وردي، أحمر',
                'sizes': 'من 2 إلى 8 سنوات',
                'size_system': 'age',
                'selection_mode': 'age',
                'age_min_months': 24,
                'age_max_months': 96,
                'status': 'active',
                'stock': 'متوفر',
                'ad_id': '120252087323640095',
                'ad_ids': ['120252087323640095'],
            },
            {
                'store_id': 'baraah-kids',
                'product_id': 'P012',
                'product_name': 'تراك ولادي',
                'name': 'تراك ولادي',
                'price': '15000',
                'price_iqd': 15000,
                'colors': 'أسود، أخضر',
                'sizes': 'من 1 إلى 3 سنوات',
                'size_system': 'age',
                'selection_mode': 'age',
                'age_min_months': 12,
                'age_max_months': 36,
                'status': 'active',
                'stock': 'متوفر',
                'ad_id': '120252094682870095',
                'ad_ids': ['120252094682870095'],
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
        self.assertEqual('بغداد', checkout.contact_fields('عنواني لشعب شارع الصحه رقمي07717419444').get('province'))
        self.assertEqual('بغداد', checkout.contact_fields('الشعب قرب السوق').get('province'))

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
        # Fake booking claim mentioning province adoption before order_saved must be blocked
        with self.assertRaisesRegex(ValueError, 'reply.booking'):
            copy_guard.validate_fulfillment(
                [dict(type='text', text='وبالنسبة لطلب البنوتات اعتمدت محافظة بغداد لتثبيت حجزه وتدللين عيوني.')],
                {'order_saved': False},
            )

    def test_e2e_um_zuzu_single_burst_with_delivery_preference_books_order(self):
        # End-to-end booking test: cart ready + customer sends "طوريج حي ضباط\n07734560441\nمثل كتلج اريده جمعه أو سبت"
        migrate_tasks(self.db)
        from account_app.sales_runtime.cart import Catalog as CartCatalog
        loader = lambda sid: CartCatalog(sid, self.products)
        orders = Orders(self.store, loader, lambda s, p: 5000)
        decide = lambda ctx: dict(intent='continue', changes=[], contact={}, answer={})
        rt = Runtime(self.store, loader, orders, decide, lambda sid: {'delivery_fee': {'كربلاء': 5000}, 'delivery_time': '24 ساعة إلى يومين'})
        # Seed cart with 2 tracksuits (pink + red, 8 years = 96 months)
        e0 = self.store.append(self.scope, 'init-umzuzu', kind='message', actor='customer', payload=dict(text='اريد الوردي والاحمر عمر 8'))
        snap = self.store.snapshot(self.scope)
        def seed_cart(st):
            st['focus_product_id'] = 'P024'
            st['cart'] = [
                dict(line_id='L1', product_id='P024', color='وردي', size_system='age', age_months=96, quantity=1),
                dict(line_id='L2', product_id='P024', color='أحمر', size_system='age', age_months=96, quantity=1),
            ]
            return dict(seeded=True)
        self.store.apply(self.scope, 'seed-umzuzu', snap['version'], {}, seed_cart)
        e1 = self.store.append(
            self.scope, 'umzuzu-details', kind='message', actor='customer',
            payload=dict(text='طوريج حي ضباط\n07734560441\nمثل كتلج\nاريده جمعه أو سبت'),
        )
        outcome = rt.turn(self.scope, e1['id'])
        self.assertEqual('booked', outcome['action'])
        row = self.db.execute('SELECT phone, province, address, total_amount FROM orders').fetchone()
        self.assertEqual(('07734560441', 'كربلاء', 'طوريج حي ضباط', 25000), row)

    def test_e2e_zahrat_ayloul_superseded_phone_then_dazetah_books_order(self):
        # End-to-end booking test: address saved on E1, phone sent on E2 (superseded), E3 "دزيته الج" -> MUST BOOK!
        migrate_tasks(self.db)
        from account_app.sales_runtime.cart import Catalog as CartCatalog
        loader = lambda sid: CartCatalog(sid, self.products)
        orders = Orders(self.store, loader, lambda s, p: 5000)
        decide = lambda ctx: dict(intent='continue', changes=[], contact={}, answer={})
        rt = Runtime(self.store, loader, orders, decide, lambda sid: {'delivery_fee': {'كربلاء': 5000}, 'delivery_time': '24 ساعة إلى يومين'})
        scope2 = Scope('baraah-kids', 'zernio', 'acc1', 'zahra-e2e')
        self.store.append(scope2, 'z0', kind='message', actor='customer', payload=dict(text='اي احجزيلي'))
        snap = self.store.snapshot(scope2)
        def seed_cart(st):
            st['focus_product_id'] = 'P019'
            st['cart'] = [dict(line_id='L1', product_id='P019', color='أحمر', size_system='age', age_months=18, quantity=1)]
            return dict(seeded=True)
        self.store.apply(scope2, 'seed-zahra', snap['version'], {}, seed_cart)
        e1 = self.store.append(scope2, 'z1', kind='message', actor='customer', payload=dict(text='كربلاء المقدسة حي العامل مقابيل المستوصف'))
        out1 = rt.turn(scope2, e1['id'])
        self.assertEqual('continue', out1['action'])
        # Customer sends phone, then immediately sends "دزيته الج" before z2 finishes
        self.store.append(scope2, 'z2', kind='message', actor='customer', payload=dict(text='٠٧٨٠٧٣٧٨٠٢٩'))
        e3 = self.store.append(scope2, 'z3', kind='message', actor='customer', payload=dict(text='دزيته الج'))
        out3 = rt.turn(scope2, e3['id'])
        self.assertEqual('booked', out3['action'])
        row = self.db.execute("SELECT phone, province, total_amount FROM orders WHERE sender_id='zahra-e2e'").fetchone()
        self.assertEqual(('07807378029', 'كربلاء', 25000), row)

    def test_new_ad_click_overrides_old_image_focus_while_repeated_ad_does_not(self):
        from account_app.sales_runtime.cart import Catalog as CartCatalog
        loader = lambda sid: CartCatalog(sid, self.products)
        orders = Orders(self.store, loader, lambda s, p: 5000)
        rt = Runtime(self.store, loader, orders, lambda ctx: dict(intent='continue', changes=[], contact={}, answer={}), lambda sid: {})
        scope3 = Scope('baraah-kids', 'zernio', 'acc1', 'ad-switch')
        self.store.append(scope3, 'a1', kind='message', actor='customer', payload=dict(text='مرحبا', ad={'ad_id': '120251829540240095'}))
        snap = self.store.snapshot(scope3)
        self.store.apply(scope3, 'img-focus', snap['version'], {}, lambda st: st.update(focus_product_id='P024', focus_source='image') or dict(ok=True))
        state = self.store.snapshot(scope3)['data']
        # Repeated old ad_id -> ignored so image focus P024 stays
        self.store.append(scope3, 'a2', kind='message', actor='customer', payload=dict(text='شكد السعر', ad={'ad_id': '120251829540240095'}))
        ref_old = rt.reference_for(scope3, loader('baraah-kids'), state, dict(text='شكد السعر', ad={'ad_id': '120251829540240095'}))
        self.assertEqual('none', ref_old['status'])
        # Brand-new ad_id (120252094682870095 -> P012) -> honored!
        self.store.append(scope3, 'a3', kind='message', actor='customer', payload=dict(text='شكد السعر', ad={'ad_id': '120252094682870095'}))
        ref_new = rt.reference_for(scope3, loader('baraah-kids'), state, dict(text='شكد السعر', ad={'ad_id': '120252094682870095'}))
        self.assertEqual('known', ref_new['status'])
        self.assertEqual('P012', ref_new['product_id'])


if __name__ == '__main__':
    unittest.main()
