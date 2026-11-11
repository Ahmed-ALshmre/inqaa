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
from .contracts import clean_plan, validate_plan, DecisionRejected, BusinessChoice,ServiceRequired,message_context
from .facts import resolve as resolve_fact,PRODUCT_FACT_FIELDS,POLICY_FIELDS,requested_fields
from .intent import apply_intent, purchase_selected
from .orders import only_color_missing
from .recovery import grounded_plan


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

    def snapshot(self,scope):
        if scope!=self.scope:raise Conflict('invalid_turn_tool_scope')
        return dict(version=self.version,data=deepcopy(self.state))


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
    def __init__(self, store, catalog_loader, orders, decide,policy_loader=None):
        self.store, self.catalog_loader, self.orders, self.decide = store, catalog_loader, orders, decide
        self.policy_loader=policy_loader or (lambda sid:dict(inspection_policy=orders.confirmation.split('. ',1)[-1]))

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
        key = f'turn-v3:{event_id}'
        done = self.store.db.execute('SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                    (scope.key, key + ':done')).fetchone()
        if done:
            return json.loads(done[0])['outcome']
        # All cart/contact/intent changes and the accepted decision share one
        # transaction. Failed proposals leave no partial state or evidence.
        prior = self.store.db.execute('SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                     (scope.key, key)).fetchone()
        catalog = self.catalog_loader(scope.store)
        policy=self.policy_loader(scope.store)
        cart = CartService(self.store, catalog)
        evidence = self.store.evidence(scope, event_id)
        if evidence['actor'] != 'customer':
            raise Conflict('customer_turn_required')
        snap_now=self.store.snapshot(scope)
        text_now=checkout.normalized(evidence['payload'].get('text',''))
        if snap_now['data'].get('order_id') and re.search(r'(?:الغي|الغاء|لغي|الغ[يه]لي)\s+(?:الطلب|الحجز|طلبي)',text_now):
            result=self.orders.cancel(scope,key+':cancel',snap_now['version'],event_id)['result']
            outcome=dict(result,action='cancelled',state_version=result['state_version']+1)
            self.store.apply(scope,key+':done',result['state_version'],dict(event_id=event_id),lambda state:dict(outcome=outcome))
            return outcome
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
            media = snap['data'].get('media_observations', {}).get(str(event_id))
            reference = References(self.store, catalog).resolve(scope, ad=evidence['payload'].get('ad'), media=media)
            try:initial_readiness=cart.readiness(scope)
            except Conflict as exc:
                if isinstance(exc,BusinessChoice) or str(exc)=='product_not_available_in_store':
                    raise ServiceRequired('cart_catalog_changed') from None
                raise
            active_products=[p for p in catalog.products.values() if p.get('status','active')=='active']
            image_fields = {'image_url', 'image_urls', 'images', 'photos', 'attachments'}
            text_products = [{key: value for key, value in product.items() if key not in image_fields} for product in active_products]
            context = dict(state=snap['data'], event=message_context(evidence['payload']), reference=reference,policies=policy,
                           products=text_products, readiness=initial_readiness,
                           available_fact_fields={p['product_id']:[f for f in PRODUCT_FACT_FIELDS if resolve_fact(catalog,p['product_id'],f,policy)] for p in active_products})
            recent=self.store.db.execute("SELECT actor,payload FROM sales_v2_events WHERE scope=? AND kind='message' ORDER BY id DESC LIMIT 30",
                                         (scope.key,)).fetchall()
            context['recent_messages']=[dict(actor=row[0],**message_context(json.loads(row[1]))) for row in reversed(recent)]
            replies=self.store.db.execute("SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted' ORDER BY id DESC LIMIT 10",
                                          (scope.key,)).fetchall()
            context['recent_replies']=[json.loads(row[0]) for row in reversed(replies)]
            recovered=False;business_feedback=None
            try:raw=self.decision_call(scope,event_id,context)
            except DecisionRejected:
                if not getattr(type(self.decide),'fallback_allowed',False):raise
                raw=grounded_plan(context);recovered=True
            for attempt in range(45):  # Bounded local stripping of unavailable choices, never 45 model calls.
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
                            if fact['product_id']!='@store':catalog.get(fact['product_id'])
                        price=None
                        from .negotiation import destination,settle
                        province_hint=destination(evidence['payload'].get('text',''))
                        if province_hint:state['quote_province']=province_hint
                        recent_replies = [json.loads(row[0]) for row in self.store.db.execute(
                            """SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted'
                               ORDER BY id DESC LIMIT 24""", (scope.key,))]
                        last_reply = recent_replies[0] if recent_replies else {}
                        if state['cart']:
                            try:
                                province=state['contact'].get('province') or state.get('quote_province')
                                price=session_cart.quote(scope,self.orders.delivery_fee(scope.store,province) if province else None)
                            except ValueError:pass  # A partial package needs quantity clarification.
                        withdrawn=apply_intent(state,plan,event_id,evidence,price,last_reply)
                        if state['cart']:
                            try:
                                province=state['contact'].get('province') or state.get('quote_province')
                                price=session_cart.quote(scope,self.orders.delivery_fee(scope.store,province) if province else None)
                                settle(state,price,recent_replies,evidence['payload'].get('text',''),event_id)
                            except ValueError:pass
                        if business_feedback and state['cart']:
                            state['conditions']=[c for c in state['conditions'] if c.get('kind')!='choice']+[dict(kind='choice',status='pending',event_id=event_id,**business_feedback)]
                        elif plan.get('changes'):
                            changed_fields={f for c in plan['changes'] for f in c.get('values',{})}
                            if changed_fields & {'age_months','weight_kg'}:changed_fields.add('size')
                            state['conditions']=[c for c in state['conditions'] if c.get('kind')!='choice' or c.get('field') not in changed_fields]
                        if reference.get('status')=='known':
                            state['focus_product_id']=reference['product_id']
                        return dict(plan=plan,invalid_fields=invalid_fields,business_feedback=business_feedback,
                                    quote=price,recovered=recovered,withdrawn=withdrawn)
                    saved=self.store.apply(scope,key,snap['version'],dict(event_id=event_id),apply_proposal)['result']
                    break
                except BusinessChoice as exc:
                    business_feedback=exc.feedback
                    repaired=clean_plan(raw)
                    for change in repaired.get('changes',[]):
                        if change['values'].get('product_id') in (None,exc.feedback['product_id']):
                            field=exc.feedback['field']
                            change['values'].pop(field,None)
                            if field=='size':
                                change['values'].pop('age_months',None);change['values'].pop('weight_kg',None)
                    raw=repaired
                    if attempt==44:raise
                except ValueError as exc:
                    if isinstance(exc,ServiceRequired):raise
                    if str(exc) in {'state_version_changed','human_control_active'}:
                        raise
                    repair=getattr(type(self.decide),'repair',None)
                    if repair is None:raise
                    if attempt:
                        if not getattr(type(self.decide),'fallback_allowed',False) or recovered:
                            raise DecisionRejected('decision_tools_invalid_after_repair') from None
                        raw=grounded_plan(context);recovered=True
                        continue
                    # No SQL transaction is held across a model call.
                    current=self.store.snapshot(scope)
                    if current['version']!=snap['version']:
                        raise Conflict('state_version_changed') from None
                    try:raw=self.decision_call(scope,event_id,context,raw,str(exc))
                    except DecisionRejected:
                        if not getattr(type(self.decide),'fallback_allowed',False):raise
                        raw=grounded_plan(context);recovered=True
            version = saved['state_version']
        try:ready = cart.readiness(scope)
        except BusinessChoice as exc:
            ready=dict(ready=False,missing_fields=[f'product.{exc.feedback["product_id"]}.{exc.feedback["field"]}'],conditions=[],version=version)
            saved['business_feedback']=exc.feedback
        # Only specific requested facts block booking; an optional empty description is not a gap.
        requested=set(requested_fields(evidence['payload'].get('text','')))
        missing_fact = any(fact['field'] in requested and resolve_fact(catalog,fact['product_id'],fact['field'],policy) is None
                           for fact in plan.get('answer',{}).get('facts',[]))
        state=self.store.snapshot(scope)['data']
        focus=state.get('focus_product_id') or (state['cart'][0]['product_id'] if len(state['cart'])==1 else None)
        for field in requested_fields(evidence['payload'].get('text','')):
            pid='@store' if field in POLICY_FIELDS else focus
            if pid and resolve_fact(catalog,pid,field,policy) is None:missing_fact=True
        image_only = evidence['payload'].get('media_kind') == 'image' and not evidence['payload'].get('text', '').strip()
        new_contact = event_id in state.get('contact_events', {}).values()
        direct_purchase = purchase_selected(evidence['payload'].get('text', ''))
        hold_color = only_color_missing(ready) and (new_contact or direct_purchase)
        if (ready['ready'] or hold_color) and not image_only and not missing_fact and not state.get('order_id'):
            booking = self.orders.commit(scope, f'{key}:booking', version, allow_color_hold=hold_color)
            outcome = dict(action='booked', **booking['result'])
            version = booking['result']['state_version']
        else:
            outcome = dict(action='continue', readiness=ready, answer_plan=plan.get('answer', {}),
                           invalid_fields=saved.get('invalid_fields',[]),business_feedback=saved.get('business_feedback'),
                           quote=saved.get('quote'),recovered=saved.get('recovered',False),
                           withdrawn=saved.get('withdrawn',False),state_version=version)
        outcome['state_version'] = version + 1
        self.store.apply(scope, key + ':done', version, dict(event_id=event_id), lambda state: dict(outcome=outcome))
        return outcome

    apply_intent=staticmethod(apply_intent)
    validate_plan = staticmethod(validate_plan)
