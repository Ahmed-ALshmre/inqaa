"""Explicit integration boundary for normalized Chatwoot/Zernio events.

Channel adapters determine actor/account/store from trusted routing metadata,
never from customer text. Existing webhook processing is not enabled by import.
"""
from .cart import Catalog
from .backup import BackupSuperseded
from .orders import Orders, migrate_tasks, CONFIRMATION
from .runtime import Runtime
from .store import Store, Scope, Conflict
from .contracts import DecisionRejected, DecisionUnavailable,ServiceRequired


class Entrypoint:
    def __init__(self, db, load_products, delivery_fee, decide, confirmation=CONFIRMATION,
                 media=None, writer=None,policy_loader=None,backup=None):
        self.store = Store(db)
        from .catalog_answers import enrich
        self.load_catalog = lambda sid: Catalog(sid, enrich(db,sid,load_products(sid)))
        from .policies import view
        self.policy_loader=lambda sid:view(sid,confirmation,delivery_fee,policy_loader(sid) if policy_loader else None)
        from .gateway import BACKUP_MODEL, BackupGateway, DecisionGateway
        if backup is None and isinstance(decide, DecisionGateway) and decide.model != BACKUP_MODEL:
            backup = BackupGateway(decide.api_key, decide.post)
        self.runtime = Runtime(self.store, self.load_catalog,
                               Orders(self.store, self.load_catalog, delivery_fee, confirmation),
                               decide,self.policy_loader,backup=backup)
        self.media = media
        from .response import Responses
        self.responses = Responses(self.store,self.load_catalog,writer,self.policy_loader,backup=backup) if writer else None

    def migrate(self):
        self.store.migrate()
        migrate_tasks(self.store.db)
        from .. import product_questions
        product_questions.init(self.store.db)
        self.store.db.commit()

    def receive(self, **kwargs):
        try:
            return self._receive(**kwargs)
        except BackupSuperseded:
            return dict(action='superseded', reply='')
        except ServiceRequired as exc:
            scope=Scope(kwargs['store_id'],kwargs['channel'],kwargs['account_id'],kwargs['sender_id'])
            event=self.store.db.execute('SELECT id FROM sales_v2_events WHERE scope=? AND external_id=?',(scope.key,kwargs['message_id'])).fetchone()
            return self.handoff_service(scope,event[0],exc.kind)
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
        if media_kind not in {'text', 'image', 'sticker', 'video', 'audio', 'file'}:
            raise ValueError('unsupported_media')
        scope = Scope(store_id, channel, account_id, sender_id)
        event = self.store.append(scope, message_id, kind='message', actor=actor,
                                  payload=dict(text=text, ad=ad or {}, media_kind=media_kind, media_ref=media_ref))
        if event['duplicate']:
            latest = self.store.db.execute("SELECT MAX(id) FROM sales_v2_events WHERE scope=? AND kind<>'receipt'",
                                           (scope.key,)).fetchone()[0]
            if event['id'] != latest:
                return dict(action='superseded', event_id=event['id'])
        if actor == 'staff':
            self.store.set_control(scope, f"staff:{event['id']}", self.store.snapshot(scope)['version'],
                                   event['id'], 'human')
            return dict(action='human', event_id=event['id'])
        if actor != 'customer' or not ai_enabled:
            return dict(action='skipped', event_id=event['id'])
        if self.store.snapshot(scope)['data']['control_mode'] != 'active':
            return dict(action='human', event_id=event['id'])
        from .. import sales_playbook,checkout
        import re
        clean=checkout.normalized(text)
        identity_typo=(re.search(r'ذكاء\s*(?:ا?صطناع\w*|صناعي)',clean) and
                       re.search(r'يرد|مخلي|انت|انتي|بوت',clean) and not re.search(r'صور|اعلان|فيديو',clean))
        if sales_playbook.human_requested(text) or identity_typo:
            return self.handoff_service(scope,event['id'],'customer_requested_human')
        if re.search(r'عندي\s+طلب|وين\s+(?:صار|طلبي)|طلع\s+طلب|متابع[ةه]\s+طلب',clean):
            return self.handoff_service(scope,event['id'],'order_tracking')
        # A sticker is social context, never a request for product recognition.
        # Image/video interpretation must be supplied by an explicit media adapter.
        if media_kind in {'image', 'video', 'audio', 'file'}:
            if self.media is None:
                return dict(action='needs_media_adapter', event_id=event['id'])
            saved = self.store.snapshot(scope)
            observed = saved['data'].get('media_observations', {}).get(str(event['id']))
            if observed is None:
                catalog = self.load_catalog(store_id)
                observed = self.media(scope, media_kind, media_ref, list(catalog.products.values()))
                if not isinstance(observed, dict) or observed.get('status') not in {'matched','ambiguous','unknown','sticker','unsupported'}:
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
                observed = clean
            if observed['status'] == 'sticker':
                return dict(action='skipped', event_id=event['id'])
            if observed['status'] == 'unsupported':
                return self.handoff_service(scope, event['id'], 'media_type_requires_staff')
            if observed['status'] != 'matched':
                return self.clarify_image(scope, event['id'])
            products_in_cart = {line['product_id'] for line in saved['data']['cart']}
            if products_in_cart and observed['product_ids'][0] not in products_in_cart:
                return self.handoff_service(scope, event['id'], 'image_changes_existing_cart')
        outcome = self.runtime.turn(scope, event['id'])
        if any(c.get('kind')=='delivery_deadline' for c in self.store.snapshot(scope)['data']['conditions']):
            return self.handoff_service(scope,event['id'],'delivery_deadline')
        if self.responses:
            reply=self.responses.prepare(scope,event['id'],outcome)
            if reply['action']=='missing_fact':
                return self.handoff_fact(scope,event['id'],outcome['state_version'],reply)
            return dict(outcome,reply=reply)
        return outcome

    def clarify_image(self, scope, event_id):
        """Keep an unresolved picture out of the checkout tools."""
        if not self.responses:
            return dict(action='media_unresolved', event_id=event_id)
        version = self.store.snapshot(scope)['version']
        self.responses.prepare(scope, event_id, dict(action='media_unresolved', state_version=version))
        return dict(action='media_unresolved', event_id=event_id)

    def handoff_fact(self, scope, event_id, version, missing):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from ..product_questions import question_key
        def handle(state):
            question=self.store.evidence(scope,event_id)['payload'].get('text','')
            product=(dict(product_id='@store',product_name='سياسة المتجر') if missing['product_id']=='@store'
                     else self.load_catalog(scope.store).get(missing['product_id']))
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

    def handoff_service(self,scope,event_id,kind):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        def handle(state):
            review=self.store.db.execute('''INSERT INTO human_reviews(sender_id,store_id,message_text,reason,status,created_at)
                VALUES(?,?,?,?,'pending',?)''',(scope.sender,scope.store,self.store.evidence(scope,event_id)['payload'].get('text',''),
                'خدمة مطلوبة: '+kind,datetime.now(ZoneInfo('Asia/Baghdad')).isoformat())).lastrowid
            state.update(control_mode='human',handoff_reason=kind)
            return dict(action='human',review_id=review,reason=kind,reply='')
        return self.store.apply(scope,f'service:{event_id}',self.store.snapshot(scope)['version'],dict(kind=kind),handle)['result']

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
