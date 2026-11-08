"""Durable test-only tools for the experimental dual-model sales agent.

No imports of the Flask app, production database, messaging, or fulfillment.
The evaluator supplies catalog snapshots and synthetic customer events.
"""
import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from account_app import dual_sales, product_variants


class SalesSandbox:
    def __init__(self, path, cases, confirmation_text):
        self.path = Path(path)
        if self.path.exists():
            raise ValueError('Refusing to overwrite an existing test database')
        self.cases = {c['id']: c for c in cases}
        self.latest = {}
        self.confirmation = confirmation_text
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE carts(case_id TEXT PRIMARY KEY, version INTEGER, data TEXT);
                CREATE TABLE offers(case_id TEXT PRIMARY KEY, discount INTEGER, evidence TEXT, accepted TEXT, presented TEXT);
                CREATE TABLE orders(id INTEGER PRIMARY KEY,case_id TEXT UNIQUE,version INTEGER,data TEXT);
                CREATE TABLE media(id INTEGER PRIMARY KEY,case_id TEXT,data TEXT,status TEXT DEFAULT 'queued');
                CREATE TABLE reviews(id INTEGER PRIMARY KEY,case_id TEXT,data TEXT);
                CREATE TABLE understanding(case_id TEXT PRIMARY KEY,data TEXT);
            ''')
            for cid in self.cases:
                db.execute('INSERT INTO carts VALUES(?,0,?)', (cid, json.dumps(
                    dict(items=[], phone=None, province=None, address=None))))
                db.execute('INSERT INTO offers VALUES(?,0,NULL,NULL,NULL)', (cid,))
                db.execute('INSERT INTO understanding VALUES(?,?)',(cid,'{}'))

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=20)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def current(self, db, cid):
        row = db.execute('SELECT * FROM carts WHERE case_id=?', (cid,)).fetchone()
        order = db.execute('SELECT * FROM orders WHERE case_id=?', (cid,)).fetchone()
        offer = db.execute('SELECT * FROM offers WHERE case_id=?', (cid,)).fetchone()
        state=dict(cart_version=row['version'], cart=json.loads(row['data']),
                    order=dict(json.loads(order['data']), order_id=order['id'],
                               order_version=order['version'], status='new') if order else None,
                    offer=dict(offer),review_pending=bool(db.execute('SELECT 1 FROM reviews WHERE case_id=?',(cid,)).fetchone()),
                    conversation_understanding=json.loads(db.execute('SELECT data FROM understanding WHERE case_id=?',(cid,)).fetchone()['data']))
        q=self.priced(state,cid)
        state['price_presented']=state['offer']['presented']==q['monetary_fingerprint']
        state['price_accepted']=state['offer']['accepted']==q['monetary_fingerprint']
        return state

    def remember_understanding(self,cid,understanding):
        """Durable semantic notes, separate from verified cart and money."""
        with self.connect() as db:
            data=dict(understanding,last_customer_message=self.latest.get(cid,''),source='model_summary_verify_against_customer')
            db.execute('UPDATE understanding SET data=? WHERE case_id=?',(json.dumps(data,ensure_ascii=False),cid))

    def settle_media(self,cid,queue_ids,*,displayed):
        with self.connect() as db:
            for mid in queue_ids:
                db.execute('UPDATE media SET status=? WHERE case_id=? AND id=?',('displayed_in_sandbox' if displayed else 'cancelled',cid,mid))

    def priced(self, state, cid, **options):
        c = self.cases[cid]
        policy = c['policy']
        fee = policy['التوصيل_لبغداد'] if state['cart'].get('province') == 'بغداد' else policy['التوصيل_لبقية_المحافظات']
        return dual_sales.negotiated_quote(state['cart']['items'], c['products'], fee,
                    previous_discount=state['offer']['discount'], **options)

    def mark_presented(self, cid, priced_quote):
        with self.connect() as db:
            db.execute('UPDATE offers SET presented=? WHERE case_id=?', (priced_quote['monetary_fingerprint'],cid))

    def execute(self, name, args, cid):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            s = self.current(db, cid)
            products = self.cases[cid]['products']
            by_id = {p['product_id']: p for p in products}
            if name == 'get_state':
                return dict(ok=True, **s)
            if name == 'catalog_lookup':
                rows = products
                if args.get('ad_id'):
                    rows = [p for p in rows if args['ad_id'] in [p.get('ad_id'), *(p.get('ad_ids') or [])]]
                elif args.get('query'):
                    rows = [p for p in rows if args['query'] in json.dumps(p, ensure_ascii=False)]
                return dict(ok=True, products=rows)
            if name == 'update_cart':
                cart = {k: args[k] for k in ('items','phone','province','address')}
                for field in ('phone','province','address'):
                    if cart[field] is None:
                        cart[field] = s['cart'].get(field)
                if any(i.get('product_id') not in by_id for i in cart['items']):
                    return dict(ok=False, error='unknown_product')
                # Pricing validates all quantities before persisting a draft.
                check = dict(s, cart=cart)
                self.priced(check, cid)
                if sum(i['quantity'] for i in cart['items']) <= 1:
                    db.execute('UPDATE offers SET discount=0,evidence=NULL WHERE case_id=?', (cid,))
                db.execute('UPDATE carts SET version=version+1,data=? WHERE case_id=?',
                           (json.dumps(cart, ensure_ascii=False), cid))
                return dict(ok=True, **self.current(db, cid))
            if name == 'quote_cart':
                return dict(ok=True, **self.priced(s, cid))
            if name == 'quote_product':
                if args.get('product_id') not in by_id:
                    return dict(ok=False,error='unknown_product')
                p=self.cases[cid]['policy']
                province=args.get('province') or s['cart'].get('province')
                if not province and p['التوصيل_لبغداد']!=p['التوصيل_لبقية_المحافظات']:
                    return dict(ok=False,error='province_required')
                fee=p['التوصيل_لبغداد'] if province=='بغداد' else p['التوصيل_لبقية_المحافظات']
                items=[dict(product_id=args['product_id'],color=None,size=None,quantity=args['quantity'])]
                q=dual_sales.negotiated_quote(items,products,fee)
                return dict(ok=True,preview_only=True,**q)
            if name == 'negotiate_price':
                text = self.latest.get(cid, '')
                clean = text.replace('أ','ا').replace('إ','ا')
                evidence = args.get('request_text') or ''
                if not evidence or evidence not in text or not re.search(r'خصم|تخفيض|سوي|خلي|حسبي|تحسب|تنزل|نزلي|نزل|اقل|ارخص|بي مجال|اكو مجال|جبر.{0,8}خاطر', clean):
                    return dict(ok=False, error='customer_discount_request_required')
                base = self.priced(s, cid)
                target, reduction = args.get('requested_total'), args.get('requested_discount')
                if (target is None) == (reduction is None):
                    return dict(ok=False, error='specify_total_or_discount')
                if target is not None:
                    dual_sales._dinars(target)
                    reduction = max(0, base['product_total'] + base['delivery_fee'] - target)
                q = self.priced(s, cid, requested_discount=reduction, customer_requested=True)
                # Store the absolute concession; future calls cannot accumulate it.
                db.execute('UPDATE offers SET discount=?,evidence=?,accepted=NULL WHERE case_id=?',
                           (q['negotiated_discount'], evidence, cid))
                return dict(ok=True, **q, price_accepted=False)
            if name == 'accept_price':
                q = self.priced(s, cid)
                text = self.latest.get(cid, '')
                clean = text.replace('أ','ا').replace('إ','ا')
                evidence = args.get('evidence') or ''
                explicit = re.search(r'موافق|تمام|زين|ثبتي|ثبت|احجزي|احجز|اي\b|إي\b', text)
                contact_reply = (re.search(r'07\d{9}|بغداد|المنصور|عنواني', text)
                                 and s['price_presented'])
                buying_known_quote=bool(s['price_presented'] and re.search(r'(?:اريد|أريد|اخذ|آخذ)\s+(?!(?:اعرف|أعرف|اسال|أسأل|استفسر|سعر|تفاصيل))\S+',evidence))
                reopening_price = re.search(r'نزلي|نزّل|سوي(?:هن|هم|ها)|خلي(?:هن|هم|ها)|(?:اريد|ممكن|بس)\s*خصم|خصم\s*(?:الف|ألف|الفين|\d)',clean)
                if (not evidence or evidence not in text or not (explicit or contact_reply or buying_known_quote)
                        or reopening_price or re.search(r'ما\s*(?:اخذ|اريد)|ماريد|الا\s*ب|بشرط|بسعر', clean)):
                    return dict(ok=False, error='clear_acceptance_required')
                # An explicit agreement to 52 is not agreement to a 53 quote.
                # Contact data must not override a different stated total.
                numeric = clean.translate(str.maketrans('٠١٢٣٤٥٦٧٨٩','0123456789'))
                agreed = re.search(r'(?:موافق[هة]?|تمام|زين|ثبتي|ثبت)\s*(?:على|بـ?)\s*(\d+)', numeric)
                if agreed:
                    amount = int(agreed[1])
                    amount = amount*1000 if amount < 1000 and q['total_amount'] >= 1000 else amount
                    if amount != q['total_amount']:
                        return dict(ok=False,error='different_customer_total',quote=q)
                if not dual_sales.offer_matches(args.get('quote_fingerprint'), q):
                    return dict(ok=False, error='stale_price_quote', quote=q)
                db.execute('UPDATE offers SET accepted=? WHERE case_id=?', (q['monetary_fingerprint'], cid))
                return dict(ok=True, quote=q, price_accepted=True)
            if name in ('commit_order', 'update_order'):
                if args.get('cart_version') != s['cart_version']:
                    return dict(ok=False, error='stale_cart_version')
                q = self.priced(s, cid)
                order = s['order']
                if name == 'commit_order' and order:
                    if order.get('quote_fingerprint') == q['quote_fingerprint']:
                        return dict(ok=True, already_saved=True, order=order)
                    return dict(ok=False, error='use_update_order')
                if name == 'update_order' and (not order or order['order_id'] != args.get('order_id') or order['order_version'] != args.get('order_version')):
                    return dict(ok=False, error='stale_order')
                # A requested color edit at the same accepted price preserves the
                # monetary agreement; other changes need a freshly accepted quote.
                same_price_edit = bool(name == 'update_order' and order
                    and order['total_amount'] == q['total_amount']
                    and re.search(r'بدل|غير', self.latest.get(cid,'')))
                if not same_price_edit and not s['price_accepted']:
                    return dict(ok=False, error='price_acceptance_required', quote=q)
                cart = s['cart']
                missing = [k for k in ('phone','province','address') if not cart.get(k)]
                if missing:
                    return dict(ok=False, error='missing_contact', fields=missing)
                if not re.fullmatch(r'07\d{9}', cart['phone']):
                    return dict(ok=False, error='invalid_phone')
                if not cart['items']:
                    return dict(ok=False, error='empty_cart')
                for row in cart['items']:
                    p = by_id[row['product_id']]
                    for field in ('color','size'):
                        if not row.get(field) or product_variants.order_choice_error(p, field, row[field]):
                            return dict(ok=False, error='invalid_variant', field=field)
                    count = sum(i['quantity'] for i in cart['items'] if i['product_id'] == row['product_id'])
                    stock = str(p.get('stock_quantity') or '')
                    if stock.isdigit() and count > int(stock):
                        return dict(ok=False, error='insufficient_stock')
                payload = dict(cart, product_total=q['product_total'], negotiated_discount=q['negotiated_discount'],
                               delivery_fee=q['delivery_fee'], total_amount=q['total_amount'],
                               quote_fingerprint=q['quote_fingerprint'])
                if name == 'commit_order':
                    db.execute('INSERT INTO orders(case_id,version,data) VALUES(?,1,?)', (cid, json.dumps(payload,ensure_ascii=False)))
                else:
                    db.execute('UPDATE orders SET version=version+1,data=? WHERE case_id=?', (json.dumps(payload,ensure_ascii=False),cid))
                return dict(ok=True, action='created' if name == 'commit_order' else 'updated',
                            order=self.current(db,cid)['order'],
                            confirmation_text=self.confirmation if name == 'commit_order' else None)
            if name == 'send_product_images':
                p = by_id.get(args.get('product_id'))
                available = (p or {}).get('image_colors') or []
                chosen = [c for c in available if c in (args.get('colors') or available)]
                if not chosen:
                    return dict(ok=False, error='no_matching_images')
                data = dict(product_id=p['product_id'], colors=chosen)
                row=db.execute('INSERT INTO media(case_id,data) VALUES(?,?)', (cid,json.dumps(data,ensure_ascii=False)))
                return dict(ok=True, queued_for_test=True,queue_id=row.lastrowid,image_colors=chosen, external_delivery=False)
            if name in ('handoff_to_staff','request_order_action'):
                db.execute('INSERT INTO reviews(case_id,data) VALUES(?,?)', (cid,json.dumps(args,ensure_ascii=False)))
                return dict(ok=True, status='pending', action_applied=False)
            return dict(ok=False, error='unknown_tool')
