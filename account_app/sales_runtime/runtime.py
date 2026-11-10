"""One turn coordinator, usable with recorded or remote structured decisions.

No Flask import, sending, fulfillment, or implicit model choice. Each operation is
replayable and version-checked; a new message supersedes an in-flight decision.
"""
import json
import re

from .. import checkout
from .cart import CartService
from .references import References
from .store import Conflict


class Runtime:
    def __init__(self, store, catalog_loader, orders, decide):
        self.store, self.catalog_loader, self.orders, self.decide = store, catalog_loader, orders, decide

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
        key = f'turn:{event_id}'
        done = self.store.db.execute('SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                    (scope.key, key + ':done')).fetchone()
        if done:
            return json.loads(done[0])['outcome']
        # Persist the decision before using tools so retries cannot generate a
        # different operation sequence after a partial successful turn.
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
            plan = self.decide(context)
            self.validate_plan(plan)
            saved = self.store.apply(scope, key, snap['version'], dict(event_id=event_id),
                                     lambda state: dict(plan=plan))['result']
            version = saved['state_version']
        for index, change in enumerate(plan.get('changes', [])):
            result = cart.change(scope, f'{key}:cart:{index}', version, event_id, **change)['result']
            version = result['state_version']
        if plan.get('contact'):
            result = cart.contact(scope, f'{key}:contact', version, event_id, **plan['contact'])['result']
            version = result['state_version']
        def intent(state):
            if state['control_mode'] != 'active':
                raise Conflict('human_control_active')
            text = checkout.normalized(evidence['payload'].get('text', ''))
            mode = plan['intent']
            conditional = re.search(r'(?<!\w)(?:اذا|لو|بشرط)(?!\w)', text)
            negated = re.search(r'ما\s*اريد|ماريد|لا\s*(?:تحجز|تثبت)|الغ[يى]', text)
            if conditional:
                mode = 'conditional'
            elif negated:
                mode = 'withdraw'
            if mode in {'withdraw', 'conditional'}:
                state['purchase_event'] = None
                state.pop('checkout_prompt', None)
                state['conditions'] = [dict(event_id=event_id, text=text)] if mode == 'conditional' else []
            elif not state['conditions']:
                fields = plan.get('contact') or {}
                contextual = (state.get('checkout_prompt') and bool(state['cart']) and
                              any(state['contact'].get(f) and f in fields for f in ('phone', 'address')))
                if not conditional and not negated and (checkout.explicit_purchase(text) or contextual):
                    state['purchase_event'] = event_id
            return dict(purchase_event=state['purchase_event'])
        result = self.store.apply(scope, f'{key}:intent', version,
                                  dict(event_id=event_id, intent=plan['intent']), intent)['result']
        version = result['state_version']
        ready = cart.readiness(scope)
        missing_fact = any(not catalog.get(fact['product_id']).get(fact['field'])
                           for fact in plan.get('answer',{}).get('facts',[]))
        if ready['ready'] and not missing_fact:
            booking = self.orders.commit(scope, f'{key}:booking', version)
            outcome = dict(action='booked', **booking['result'])
            version = booking['result']['state_version']
        else:
            outcome = dict(action='continue', readiness=ready, answer_plan=plan.get('answer', {}), state_version=version)
        outcome['state_version'] = version + 1
        self.store.apply(scope, key + ':done', version, dict(event_id=event_id), lambda state: dict(outcome=outcome))
        return outcome

    @staticmethod
    def validate_plan(plan):
        if not isinstance(plan, dict) or set(plan) - {'changes', 'contact', 'intent', 'answer'}:
            raise ValueError('invalid_decision_fields')
        if plan.get('intent') not in {'continue', 'buy', 'withdraw', 'conditional'}:
            raise ValueError('invalid_intent')
        if not isinstance(plan.get('changes', []), list) or len(plan.get('changes', [])) > 20:
            raise ValueError('invalid_changes')
        for change in plan.get('changes', []):
            if not isinstance(change, dict) or set(change) - {'line_id', 'values', 'remove'} or not change.get('line_id'):
                raise ValueError('invalid_cart_command')
        if (not isinstance(plan.get('contact', {}), dict) or
                set(plan.get('contact', {})) - {'phone', 'address'}):
            raise ValueError('invalid_contact_command')
        if not isinstance(plan.get('answer', {}), dict):
            raise ValueError('invalid_answer_plan')
        facts=plan.get('answer',{}).get('facts',[])
        if not isinstance(facts,list) or len(facts)>20:
            raise ValueError('invalid_fact_references')
        for fact in facts:
            if (not isinstance(fact,dict) or set(fact)!={'product_id','field'}
                    or fact['field'] not in {'fabric','description','notes','colors','sizes','price','offer','product_type'}):
                raise ValueError('invalid_fact_reference')
