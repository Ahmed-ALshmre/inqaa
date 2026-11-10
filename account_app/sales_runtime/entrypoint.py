"""Explicit integration boundary for normalized Chatwoot/Zernio events.

Channel adapters determine actor/account/store from trusted routing metadata,
never from customer text. Existing webhook processing is not enabled by import.
"""
from .cart import Catalog
from .orders import Orders, migrate_tasks, CONFIRMATION
from .runtime import Runtime
from .store import Store, Scope, Conflict
from .contracts import DecisionRejected, DecisionUnavailable


class Entrypoint:
    def __init__(self, db, load_products, delivery_fee, decide, confirmation=CONFIRMATION, media=None, writer=None):
        self.store = Store(db)
        self.load_catalog = lambda sid: Catalog(sid, load_products(sid))
        self.runtime = Runtime(self.store, self.load_catalog,
                               Orders(self.store, self.load_catalog, delivery_fee, confirmation), decide)
        self.media = media
        from .response import Responses
        self.responses = Responses(self.store,self.load_catalog,writer) if writer else None

    def migrate(self):
        self.store.migrate()
        migrate_tasks(self.store.db)
        from .. import product_questions
        product_questions.init(self.store.db)
        self.store.db.commit()

    def receive(self, **kwargs):
        try:
            return self._receive(**kwargs)
        except (DecisionRejected,DecisionUnavailable) as exc:
            scope=Scope(kwargs['store_id'],kwargs['channel'],kwargs['account_id'],kwargs['sender_id'])
            row=self.store.db.execute('SELECT id FROM sales_v2_events WHERE scope=? AND external_id=?',
                                      (scope.key,kwargs['message_id'])).fetchone()
            if not row:raise
            return self.record_failure(scope,row[0],exc)

    def record_failure(self,scope,event_id,error):
        import time
        from datetime import datetime
        from zoneinfo import ZoneInfo
        rejected=isinstance(error,DecisionRejected)
        category='invalid_decision' if rejected else 'provider_unavailable'
        snap=self.store.snapshot(scope)
        if not rejected:
            # An outage must not stale an already committed turn/writer input.
            with self.store.transaction():
                self.store.db.execute('''INSERT INTO sales_v2_incidents
                    (scope,event_id,category,error_code,created_at) VALUES(?,?,?,?,?)
                    ON CONFLICT(scope,event_id,category) DO UPDATE SET error_code=excluded.error_code''',
                    (scope.key,event_id,category,str(error),time.time()))
            return dict(action='service_failure',error_code=str(error),review_id=None,retryable=True,reply='')
        latest=self.store.db.execute("SELECT MAX(id) FROM sales_v2_events WHERE scope=? AND kind<>'receipt'",(scope.key,)).fetchone()[0]
        if latest!=event_id:return dict(action='superseded',reply='')
        def handle(state):
            review_id=None
            if rejected:
                review_id=self.store.db.execute('''INSERT INTO human_reviews
                    (sender_id,store_id,message_text,reason,status,created_at) VALUES(?,?,?,?,'pending',?)''',
                    (scope.sender,scope.store,self.store.evidence(scope,event_id)['payload'].get('text',''),
                     'مراجعة تقنية بعد استنفاد تصحيح القرار: '+str(error),
                     datetime.now(ZoneInfo('Asia/Baghdad')).isoformat())).lastrowid
                state['control_mode']='human'
            self.store.db.execute('''INSERT INTO sales_v2_incidents
                (scope,event_id,category,error_code,review_id,created_at) VALUES(?,?,?,?,?,?)''',
                (scope.key,event_id,category,str(error),review_id,time.time()))
            return dict(action='needs_review' if rejected else 'service_failure',error_code=str(error),
                        review_id=review_id,retryable=not rejected,reply='')
        return self.store.apply(scope,f'failure:{event_id}:{category}',snap['version'],
                                dict(category=category),handle)['result']

    def _receive(self, *, store_id, channel, account_id, sender_id, message_id,
                text='', ad=None, media_kind='text', actor='customer', ai_enabled=True, media_ref=None):
        if channel not in {'chatwoot', 'zernio'}:
            raise ValueError('unsupported_channel')
        if media_kind not in {'text', 'image', 'sticker', 'video', 'audio'}:
            raise ValueError('unsupported_media')
        scope = Scope(store_id, channel, account_id, sender_id)
        event = self.store.append(scope, message_id, kind='message', actor=actor,
                                  payload=dict(text=text, ad=ad or {}, media_kind=media_kind, media_ref=media_ref))
        if actor == 'staff':
            self.store.set_control(scope, f"staff:{event['id']}", self.store.snapshot(scope)['version'],
                                   event['id'], 'human')
            return dict(action='human', event_id=event['id'])
        if actor != 'customer' or not ai_enabled:
            return dict(action='skipped', event_id=event['id'])
        if self.store.snapshot(scope)['data']['control_mode'] != 'active':
            return dict(action='human', event_id=event['id'])
        # A sticker is social context, never a request for product recognition.
        # Image/video interpretation must be supplied by an explicit media adapter.
        if media_kind in {'image', 'video', 'audio'}:
            if self.media is None:
                return dict(action='needs_media_adapter', event_id=event['id'])
            saved = self.store.snapshot(scope)
            if str(event['id']) not in saved['data'].get('media_observations', {}):
                catalog = self.load_catalog(store_id)
                observed = self.media(scope, media_kind, media_ref, list(catalog.products.values()))
                if not isinstance(observed, dict) or observed.get('status') not in {'matched','ambiguous','unknown'}:
                    raise ValueError('invalid_media_result')
                candidates = observed.get('product_ids', [])
                if not isinstance(candidates,list) or len(candidates)>10:
                    raise ValueError('invalid_media_candidates')
                for product_id in candidates:
                    catalog.get(product_id)
                if observed['status']=='matched' and len(candidates)!=1:
                    raise ValueError('media_match_requires_one_product')
                clean = dict(status=observed['status'], product_ids=candidates)
                def remember(state):
                    state.setdefault('media_observations', {})[str(event['id'])] = clean
                    return dict(media=clean)
                self.store.apply(scope,f"media:{event['id']}",saved['version'],dict(event_id=event['id']),remember)
        outcome = self.runtime.turn(scope, event['id'])
        if self.responses:
            reply=self.responses.prepare(scope,event['id'],outcome)
            if reply['action']=='missing_fact':
                return self.handoff_fact(scope,event['id'],outcome['state_version'],reply)
            return dict(outcome,reply=reply)
        return outcome

    def handoff_fact(self, scope, event_id, version, missing):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from ..product_questions import question_key
        def handle(state):
            question=self.store.evidence(scope,event_id)['payload'].get('text','')
            product=self.load_catalog(scope.store).get(missing['product_id'])
            now=datetime.now(ZoneInfo('Asia/Baghdad')).isoformat()
            review=self.store.db.execute('''INSERT INTO human_reviews
                (sender_id,store_id,message_text,reason,status,created_at)
                VALUES(?,?,?,?,'pending',?)''',(scope.sender,scope.store,question,
                    'معلومة منتج ناقصة: '+missing['field'],now)).lastrowid
            self.store.db.execute('''INSERT INTO product_questions
                (store_id,product_id,product_name,topic,question,question_key,sender_id,review_id,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(store_id,product_id,topic,question_key)
                DO UPDATE SET status='pending',updated_at=excluded.updated_at,review_id=excluded.review_id''',
                (scope.store,product['product_id'],product.get('product_name',''),missing['field'],question,
                 question_key(question),scope.sender,review,now,now))
            state['control_mode']='human'
            state['pending_questions'].append(dict(event_id=event_id,review_id=review,**missing))
            return dict(action='human',review_id=review,reply='')
        return self.store.apply(scope,f'handoff:{event_id}',version,dict(missing=missing),handle)['result']

    def prepare_fulfillment(self, scope, task_id):
        """Transfer a committed task to the existing Menger queue using LOCAL SQL.

        This does not contact Menger. Call only in an enabled delivery worker;
        isolated tests never run the existing external worker.
        """
        import json
        from .. import menger
        if scope.store in menger.TELEGRAM_ONLY_STORES:
            raise Conflict('telegram_only_notification_adapter_required')
        with self.store.transaction():
            row = self.store.db.execute('''SELECT order_id,payload,status FROM sales_v2_order_tasks
                WHERE id=? AND scope=? AND kind='fulfillment' ''', (task_id, scope.key)).fetchone()
            if not row:
                raise Conflict('fulfillment_task_not_in_scope')
            if row[2] == 'prepared':
                return dict(order_id=row[0], replayed=True)
            if row[2] != 'queued':
                raise Conflict('fulfillment_task_not_queued')
            booking = json.loads(row[1])
            order = self.store.db.execute('''SELECT status,phone,province,address,order_items,total_amount
                FROM orders WHERE id=? AND store_id=? AND sender_id=?''',
                                         (row[0], scope.store, scope.sender)).fetchone()
            if not order or order[0] != 'new':
                raise Conflict('order_changed_before_fulfillment')
            if (tuple(order[1:4]) != tuple(booking[f] for f in ('phone','province','address'))
                    or json.loads(order[4]) != booking['items'] or order[5] != booking['total_amount']):
                raise Conflict('order_changed_before_fulfillment')
            menger.enqueue(self.store.db, row[0], booking, list(self.load_catalog(scope.store).products.values()))
            self.store.db.execute("UPDATE sales_v2_order_tasks SET status='prepared' WHERE id=?", (task_id,))
            return dict(order_id=row[0], replayed=False)
