"""One turn coordinator, usable with recorded or remote structured decisions.

No Flask import, sending, fulfillment, or implicit model choice. Each operation is
replayable and version-checked; a new message supersedes an in-flight decision.
"""
import json
import re
import time
from copy import deepcopy

from .. import checkout
from .cart import CartService
from .references import References
from .store import Conflict, Store, digest
from .contracts import clean_plan, validate_plan, catalog_fact, DecisionRejected


class TurnTools(Store):
    """Reuse the cart validators inside one outer transaction and one state copy.

    This deliberately has no nested commits or per-tool operation journals. The
    enclosing Store.apply journals all successful tools as one atomic proposal.
    """
    def __init__(self, store, scope, state, version):
        super().__init__(store.db)
        self.scope,self.state,self.version=scope,state,version

    def apply(self, scope, key, expected_version, command, handler):
        if scope!=self.scope or expected_version!=self.version or not self.db.in_transaction:
            raise Conflict('invalid_turn_tool_transaction')
        return dict(result=handler(self.state),replayed=False)


def resolve_changes(plan, state, scope, event_id):
    """Models refer to existing lines; only the server assigns new identities."""
    commands=[]
    lines=deepcopy(state['cart'])
    for index,change in enumerate(plan.get('changes',[])):
        values=change['values']
        ref=change.get('line_ref')
        legacy=change.get('line_id')
        if ref is None and isinstance(legacy,str) and any(row['line_id']==legacy for row in lines):
            ref=legacy
        if ref:
            selected=[row for row in lines if row['line_id']==ref]
            if len(selected)!=1:raise Conflict('line_ref_not_in_current_cart')
        elif change.get('new_line'):
            selected=[]
        else:
            selected=[row for row in lines if (not values.get('product_id') or row['product_id']==values['product_id'])
                      and (not values.get('wearer_ref') or row.get('wearer_ref')==values['wearer_ref'])]
            if len(selected)>1:raise Conflict('ambiguous_line_reference: copy line_ref or explicitly add new_line')
        if selected:
            line_id=selected[0]['line_id']
        else:
            if change.get('remove'):raise Conflict('cannot_remove_unresolved_line')
            if not values.get('product_id'):raise Conflict('new_line_requires_catalog_product_id')
            line_id='line_'+digest([scope.key,event_id,index])[:20]
        commands.append(dict(line_id=line_id,values=values,remove=change.get('remove',False)))
        if change.get('remove'):
            lines=[row for row in lines if row['line_id']!=line_id]
        elif selected:
            lines[lines.index(selected[0])]=dict(selected[0],**values)
        else:
            lines.append(dict(line_id=line_id,**values))
    return commands


class Runtime:
    def __init__(self, store, catalog_loader, orders, decide):
        self.store, self.catalog_loader, self.orders, self.decide = store, catalog_loader, orders, decide

    def decision_call(self,scope,event_id,context,previous=None,error=None):
        # A duplicate webhook or worker restart cannot reset the model budget.
        with self.store.transaction():
            count=self.store.db.execute('SELECT COUNT(*) FROM sales_v2_model_attempts WHERE scope=? AND event_id=?',
                                       (scope.key,event_id)).fetchone()[0]
            if count>=2:raise DecisionRejected('decision_budget_exhausted')
            attempt=self.store.db.execute('''INSERT INTO sales_v2_model_attempts
                (scope,event_id,stage,status,created_at) VALUES(?,?,?,'running',?)''',
                (scope.key,event_id,'repair' if error else 'decision',time.time())).lastrowid
        try:
            result=self.decide(context) if error is None else self.decide.repair(context,previous,error)
        except Exception as exc:
            with self.store.transaction():
                self.store.db.execute("UPDATE sales_v2_model_attempts SET status='failed',error_code=? WHERE id=?",
                                      (type(exc).__name__,attempt))
            raise
        with self.store.transaction():
            self.store.db.execute("UPDATE sales_v2_model_attempts SET status='returned' WHERE id=?",(attempt,))
        return result

    def accept_checkout_prompt(self, scope, key, version, outbox_id):
        def handle(state):
            row = self.store.db.execute('SELECT status,payload FROM sales_v2_outbox WHERE id=? AND scope=?',
                                       (outbox_id, scope.key)).fetchone()
            payload = json.loads(row[1]) if row else {}
            if not row or row[0] != 'accepted' or payload.get('purpose') != 'checkout_contacts':
                raise Conflict('checkout_prompt_not_accepted')
            state['checkout_prompt'] = dict(outbox_id=outbox_id)
            return dict(checkout_prompt=outbox_id)
        return self.store.apply(scope, key, version, dict(prompt=outbox_id), handle)

    def turn(self, scope, event_id):
        key = f'turn-v2:{event_id}'
        done = self.store.db.execute('SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                    (scope.key, key + ':done')).fetchone()
        if done:
            return json.loads(done[0])['outcome']
        # All cart/contact/intent changes and the accepted decision share one
        # transaction. Failed proposals leave no partial state or evidence.
        prior = self.store.db.execute('SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                     (scope.key, key)).fetchone()
        catalog = self.catalog_loader(scope.store)
        cart = CartService(self.store, catalog)
        evidence = self.store.evidence(scope, event_id)
        if evidence['actor'] != 'customer':
            raise Conflict('customer_turn_required')
        if prior:
            saved = json.loads(prior[0])
            plan, version = saved['plan'], saved['state_version']
        else:
            snap = self.store.snapshot(scope)
            if snap['data']['control_mode'] != 'active':
                raise Conflict('human_control_active')
            latest = self.store.db.execute("SELECT MAX(id) FROM sales_v2_events WHERE scope=? AND kind<>'receipt'",
                                           (scope.key,)).fetchone()[0]
            if latest != event_id:
                raise Conflict('turn_superseded')
            reference = References(self.store, catalog).resolve(scope, ad=evidence['payload'].get('ad'))
            context = dict(state=snap['data'], event=evidence['payload'], reference=reference,
                           products=list(catalog.products.values()), readiness=cart.readiness(scope))
            recent=self.store.db.execute("SELECT actor,payload FROM sales_v2_events WHERE scope=? AND kind='message' ORDER BY id DESC LIMIT 30",
                                         (scope.key,)).fetchall()
            context['recent_messages']=[dict(actor=row[0],**json.loads(row[1])) for row in reversed(recent)]
            replies=self.store.db.execute("SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted' ORDER BY id DESC LIMIT 10",
                                          (scope.key,)).fetchall()
            context['recent_replies']=[json.loads(row[0]) for row in reversed(replies)]
            raw=self.decision_call(scope,event_id,context)
            for attempt in range(2):
                try:
                    plan=clean_plan(raw)
                    def apply_proposal(state):
                        tools=TurnTools(self.store,scope,state,snap['version'])
                        session_cart=CartService(tools,catalog)
                        commands=resolve_changes(plan,state,scope,event_id)
                        invalid_fields=[]
                        for index,command in enumerate(commands):
                            session_cart.change(scope,str(index),snap['version'],event_id,**command)
                        if plan.get('contact'):
                            contact_result=session_cart.contact(scope,'contact',snap['version'],event_id,**plan['contact'])
                            invalid_fields=contact_result['result']['invalid_fields']
                        for fact in plan.get('answer',{}).get('facts',[]):
                            catalog.get(fact['product_id'])
                        self.apply_intent(state,plan,event_id,evidence)
                        if reference.get('status')=='known':
                            state['focus_product_id']=reference['product_id']
                        return dict(plan=plan,invalid_fields=invalid_fields)
                    saved=self.store.apply(scope,key,snap['version'],dict(event_id=event_id),apply_proposal)['result']
                    break
                except ValueError as exc:
                    if str(exc) in {'state_version_changed','human_control_active'}:
                        raise
                    repair=getattr(type(self.decide),'repair',None)
                    if repair is None:raise
                    if attempt:raise DecisionRejected('decision_tools_invalid_after_repair') from None
                    # No SQL transaction is held across a model call.
                    current=self.store.snapshot(scope)
                    if current['version']!=snap['version']:
                        raise Conflict('state_version_changed') from None
                    raw=self.decision_call(scope,event_id,context,raw,str(exc))
            version = saved['state_version']
        ready = cart.readiness(scope)
        missing_fact = any(catalog_fact(catalog.get(fact['product_id']),fact['field']) is None
                           for fact in plan.get('answer',{}).get('facts',[]))
        if ready['ready'] and not missing_fact:
            booking = self.orders.commit(scope, f'{key}:booking', version)
            outcome = dict(action='booked', **booking['result'])
            version = booking['result']['state_version']
        else:
            outcome = dict(action='continue', readiness=ready, answer_plan=plan.get('answer', {}),
                           invalid_fields=saved.get('invalid_fields',[]),state_version=version)
        outcome['state_version'] = version + 1
        self.store.apply(scope, key + ':done', version, dict(event_id=event_id), lambda state: dict(outcome=outcome))
        return outcome

    @staticmethod
    def apply_intent(state,plan,event_id,evidence):
        if state['control_mode']!='active':raise Conflict('human_control_active')
        text=checkout.normalized(evidence['payload'].get('text',''))
        mode=plan['intent']
        if (re.search(r'(?<!\w)(?:اذا|بشرط)(?!\w)',text) or
                re.match(r'^\s*لو\s+(?:يوصل|توصل|يصير|تسوين|تخلين|تنطين)',text)):
            mode='conditional'
        elif re.search(r'ما\s*اريد|ماريد|لا\s*(?:تحجز|تثبت)|الغ[يى]',text):mode='withdraw'
        if mode in {'withdraw','conditional'}:
            state['purchase_event']=None
            state.pop('checkout_prompt',None)
            state['conditions']=[dict(event_id=event_id,text=text)] if mode=='conditional' else []
        elif not state['conditions']:
            fields=plan.get('contact') or {}
            contextual=(state.get('checkout_prompt') and bool(state['cart']) and
                        any(state['contact'].get(f) and f in fields for f in ('phone','address')))
            if checkout.explicit_purchase(text) or contextual:state['purchase_event']=event_id

    validate_plan = staticmethod(validate_plan)
