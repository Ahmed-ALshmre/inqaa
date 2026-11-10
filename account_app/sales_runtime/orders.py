"""Atomic booking into the existing orders table, without history extraction."""
from datetime import datetime
from zoneinfo import ZoneInfo

from .cart import CartService
from .store import Conflict, encode


CONFIRMATION = ('تم تثبيت الطلب. يرجى فحص الطلب بحضور المندوب والتأكد من الموديل والقياس. '
                'إذا لم يطابق الطلب يرجع مع المندوب بدون دفع. أهم شي فحص الطلب قبل دفع المبلغ.')


def migrate_tasks(db):
    if db.in_transaction:
        raise Conflict('migration_requires_clean_transaction')
    db.executescript('''
        CREATE TABLE IF NOT EXISTS sales_v2_order_tasks (
            id INTEGER PRIMARY KEY, scope TEXT NOT NULL, order_id INTEGER NOT NULL,
            kind TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
            UNIQUE(order_id,kind));
    ''')
    columns = {row[1] for row in db.execute('PRAGMA table_info(sales_v2_order_tasks)')}
    if 'provider_id' not in columns:
        db.execute('ALTER TABLE sales_v2_order_tasks ADD COLUMN provider_id TEXT')
    db.commit()


class Orders:
    def __init__(self, store, catalog_loader, delivery_fee, confirmation=CONFIRMATION):
        self.store, self.catalog_loader = store, catalog_loader
        self.delivery_fee, self.confirmation = delivery_fee, confirmation

    def cancel(self,scope,key,version,event_id):
        from .contracts import ServiceRequired
        def handle(state):
            from .intent import explicit_cancellation
            if state['control_mode']!='active':raise Conflict('human_control_active')
            latest=self.store.db.execute("SELECT MAX(id) FROM sales_v2_events WHERE scope=? AND kind<>'receipt'",(scope.key,)).fetchone()[0]
            if latest!=event_id:raise Conflict('turn_superseded')
            source=self.store.evidence(scope,event_id)
            if source['actor']!='customer' or not explicit_cancellation(source['payload'].get('text','')):
                raise Conflict('explicit_cancel_required')
            order_id=state.get('order_id')
            row=self.store.db.execute('SELECT status FROM orders WHERE id=? AND sender_id=? AND store_id=?',
                                     (order_id,scope.sender,scope.store)).fetchone()
            if not row:raise ServiceRequired('order_cancellation_requires_lookup')
            if row[0]=='cancelled':return dict(order_id=order_id,cancelled=True)
            pending=self.store.db.execute("SELECT status FROM sales_v2_order_tasks WHERE order_id=? AND kind='fulfillment'",(order_id,)).fetchone()
            if row[0]!='new' or not pending or pending[0]!='queued':
                raise ServiceRequired('order_cancellation_requires_fulfillment_review')
            self.store.db.execute("UPDATE orders SET status='cancelled' WHERE id=?",(order_id,))
            self.store.db.execute("UPDATE sales_v2_order_tasks SET status='superseded' WHERE order_id=? AND status='queued'",(order_id,))
            state.update(order_status='cancelled',purchase_event=None,conditions=[])
            return dict(order_id=order_id,cancelled=True)
        return self.store.apply(scope,key,version,dict(event_id=event_id,action='cancel_order'),handle)

    def commit(self, scope, key, version):
        # All callbacks here must be local reads; no network and no commits.
        def handle(state):
            if state.get('order_id'):
                return dict(order_id=state['order_id'], already_booked=True)
            catalog = self.catalog_loader(scope.store)
            cart = CartService(self.store, catalog)
            readiness = cart.readiness(scope)
            if not readiness['ready']:
                raise Conflict('checkout_not_ready:' + ','.join(readiness['missing_fields']))
            quote = cart.quote(scope, self.delivery_fee(scope.store, state['contact']['province']))
            if state.get('price_request') and not quote.get('request_satisfied'):
                from .contracts import ServiceRequired
                raise ServiceRequired('quoted_price_changed_before_booking')
            # When a numeric piece count is present, respect it across all lines.
            for pid in {line['product_id'] for line in state['cart']}:
                quantity = catalog.get(pid).get('stock_quantity')
                if quantity is not None and str(quantity).isdigit():
                    requested = sum(line['quantity'] for line in state['cart'] if line['product_id']==pid)
                    if requested > int(quantity):
                        raise Conflict('checkout_quantity_exceeds_stock')
            items = []
            for line in quote['items']:
                product = catalog.get(line['product_id'])
                from ..product_albums import stock_state
                if stock_state(product) != 'available':
                    raise Conflict('checkout_stock_not_confirmed')
                item = dict(line, product_name=product.get('product_name', ''), quantity_unit='piece')
                system = line['size_system']
                if system == 'age':
                    # Do not silently turn month counts into clothing sizes.
                    item.update(size=f"{line['age_months']} شهر", size_type='age')
                elif system == 'weight':
                    item.update(size_type='weight', weight=line['weight_kg'])
                else:
                    item['size_type'] = 'none' if system == 'none' else 'size'
                item['order_name'] = product.get('order_name') or item['product_name']
                items.append(item)
            contact = state['contact']
            now = datetime.now(ZoneInfo('Asia/Baghdad')).isoformat()
            names = '، '.join(i['product_name'] for i in items)
            cursor = self.store.db.execute('''INSERT INTO orders
                (sender_id,store_id,customer_name,phone,province,address,product_id,product_name,
                 color,size,notes,status,order_items,created_at,product_total,delivery_fee,total_amount)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,'new',?,?,?,?,?)''',
                (scope.sender, scope.store, contact.get('customer_name', ''), contact['phone'],
                 contact['province'], contact['address'], ', '.join(i['product_id'] for i in items), names,
                 items[0].get('color', '') if len(items) == 1 else 'متعدد',
                 items[0].get('size', '') if len(items) == 1 else 'متعدد', '', encode(items), now,
                 quote['product_total'], quote['delivery_fee'], quote['total']))
            order_id = cursor.lastrowid
            booking = dict(id=order_id, sender_id=scope.sender, store_id=scope.store,
                           created_at=now, items=items, **contact,
                           product_total=quote['product_total'], delivery_fee=quote['delivery_fee'],
                           total_amount=quote['total'], _booking_actor='ai')
            # Durable tasks survive later state versions, unlike a disposable reply draft.
            for kind, payload in [('confirmation', dict(text=self.confirmation)),
                                  ('fulfillment', booking)]:
                self.store.db.execute('''INSERT INTO sales_v2_order_tasks(scope,order_id,kind,payload)
                    VALUES(?,?,?,?)''', (scope.key, order_id, kind, encode(payload)))
            state['order_id'] = order_id
            return dict(order_id=order_id, total=quote['total'], catalog_revision=catalog.revision)
        return self.store.apply(scope, key, version, dict(action='commit_order'), handle)
