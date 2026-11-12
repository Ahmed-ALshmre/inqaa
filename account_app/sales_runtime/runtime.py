"""One turn coordinator, usable with recorded or remote structured decisions.

No Flask import, sending, fulfillment, or implicit model choice. Each operation is
replayable and version-checked; a new message supersedes an in-flight decision.
"""
import json
import re
import time
from copy import deepcopy

from .. import checkout, product_variants
from .backup import BackupFailed, consult
from .cart import CartService
from .contact import capture as capture_contact, contact_only, declined_phone
from .references import References
from .store import Conflict, Store, digest
from .contracts import clean_plan, validate_plan, DecisionRejected, DecisionUnavailable, BusinessChoice,ServiceRequired,message_context
from .facts import resolve as resolve_fact,PRODUCT_FACT_FIELDS,POLICY_FIELDS,requested_fields
from .intent import (apply_intent, color_availability, color_correction, direct_clothing_size, direct_color,
                     explicit_cancellation, purchase_declined, purchase_selected, referred_photo_color)
from .orders import only_color_missing
from .recovery import catalog_photo_request, clothing_size_label, grounded_plan


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
    def __init__(self, store, catalog_loader, orders, decide,policy_loader=None, *, backup=None, text_reference=None):
        self.store, self.catalog_loader, self.orders, self.decide = store, catalog_loader, orders, decide
        self.policy_loader=policy_loader or (lambda sid:dict(inspection_policy=orders.confirmation.split('. ',1)[-1]))
        self.backup = backup
        self.text_reference = text_reference

    def photo_choice(self, scope, event_id, text, media, reference, state, catalog):
        # Only the current photo or the immediately adjacent customer turn can
        # explain a different name. An old picture cannot override a new choice.
        preceding = self.store.db.execute('''SELECT e.id,e.payload FROM sales_v2_events AS e
            JOIN sales_v2_events AS current ON current.scope=e.scope AND current.id=?
            WHERE e.scope=? AND e.id<? AND e.kind='message' AND e.actor='customer'
            AND current.received_at-e.received_at<=1800 ORDER BY e.id DESC LIMIT 1''',
            (event_id, scope.key, event_id)).fetchone()
        image_ref = reference if media else None
        referring_text = text
        image_event = event_id
        if media and not text.strip() and preceding:
            referring_text = json.loads(preceding[1]).get('text', '')
        elif not media and preceding:
            previous_media = state.get('media_observations', {}).get(str(preceding[0]))
            if previous_media and previous_media['status'] == 'matched':
                image_ref = References(self.store, catalog).resolve(scope, media=previous_media)
                image_event = preceding[0]
                if reference.get('product_id') not in (None, image_ref['product_id']):
                    return None
                named = direct_color(text) or color_availability(text)
                stock = product_variants.colors(catalog.get(image_ref['product_id']).get('colors'))
                if (named in stock and named != image_ref.get('available_photo_color')
                        and not re.search(r'صور[ةه]|^(?:هذا|هاي|هذي|نفس)\s+', text)):
                    return None
        if not image_ref:
            return None
        color = referred_photo_color(referring_text, image_ref.get('available_photo_color'))
        if color:
            return dict(product_id=image_ref['product_id'], color=color, image_event=image_event,
                        availability_question=bool(color_availability(referring_text)))
        return None

    def decision_call(self,scope,event_id,context,previous=None,error=None):
        # A duplicate webhook or worker restart cannot reset the model budget.
        with self.store.transaction():
            count=self.store.db.execute("""SELECT COUNT(*) FROM sales_v2_model_attempts WHERE scope=? AND event_id=?
                                           AND stage IN ('decision','repair')""",
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

    def recover_decision(self, scope, event_id, version, context, failure, previous=None, reason=None):
        alternate = consult(self.store, scope, event_id, version, 'decision',
                            self.backup.decide if self.backup else None, context, reason or str(failure), previous)
        if isinstance(alternate, dict):
            # The backup sees which contact fields exist. Exact values come from the scoped customer event.
            text = context['event'].get('text') or ''
            local = checkout.contact_fields(text, context['state']['contact'])
            if not local.get('address'):
                # The whole turn can also name the chosen size. Look at the
                # actual delivery line, or the declaration after the phone.
                address_candidates = list(reversed(text.splitlines()))
                phone_match = checkout.PHONE.search(text.translate(checkout.DIGITS))
                if phone_match:
                    address_candidates.insert(0, text[phone_match.end():].strip(' ,،؛.\n'))
                for candidate in address_candidates:
                    address = checkout.contact_fields(candidate).get('address')
                    if address:
                        local['address'] = address
                        break
            contacts = {field: value for field, value in local.items()
                        if field in {'phone', 'address'} and (field == 'phone'
                            or checkout.normalized(value) in checkout.normalized(text))}
            return dict(alternate, contact=contacts), True
        if self.backup:
            raise BackupFailed('backup_plan_invalid')
        if isinstance(failure, DecisionRejected) and getattr(type(self.decide), 'fallback_allowed', False):
            return grounded_plan(context), False
        raise failure

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

    @staticmethod
    def pending_photo(state):
        observations = state.get('media_observations', {})
        last = max((int(key) for key, observation in observations.items()
                    if observation['status'] not in {'sticker', 'unsupported'}), default=0)
        if last > state.get('resolved_photo_event', 0) and observations[str(last)]['status'] != 'matched':
            return last, observations[str(last)]
        return None, None

    def reference_for(self, scope, catalog, state, payload):
        text = payload.get('text', '')
        pending_id, pending = self.pending_photo(state)
        refs = References(self.store, catalog)
        ad = payload.get('ad')
        if not pending:
            reference = refs.resolve(scope, ad=ad, media=payload.get('observation'), text=text)
        else:
            reference = refs.resolve(scope, media=payload.get('observation'), text=text)
        if reference['status'] == 'known':
            return reference
        if pending and reference['status'] == 'none':
            ad = ad or self.store.evidence(scope, pending_id)['payload'].get('ad')
            if ad:
                reference = refs.resolve(scope, ad=ad)

        old_context = state.get('focus_product_id')
        if not old_context and len({line['product_id'] for line in state['cart']}) == 1:
            old_context = state['cart'][0]['product_id']
        if state.get('order_id'):
            return dict(status='none') if pending else reference
        if not pending and (old_context or state['cart'] or payload.get('media_kind') != 'text'):
            return reference
        if (reference.get('source') == 'product_link' and reference['status'] != 'known'
                or pending and state['cart'] and not old_context):
            return reference

        hint = (self.text_reference(scope, text, catalog, preferred=old_context if pending else None)
                if self.text_reference else None)
        if hint and hint['source'] == 'text':
            catalog.get(hint['product_id'])
            return dict(hint, status='known')
        if not pending:
            if hint and (reference['status'] == 'none' or reference == {'status': 'unknown', 'source': 'ad'}):
                catalog.get(hint['product_id'])
                return dict(hint, status='known')
            return reference
        candidate = reference if reference['status'] == 'known' else hint
        if not candidate or pending['status'] == 'ambiguous' or reference.get('status') == 'ambiguous':
            return dict(status='none')

        product_id = candidate['product_id']
        catalog.get(product_id)
        if checkout.is_confirmation(text):
            last_reply = self.store.db.execute(
                "SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted' ORDER BY id DESC LIMIT 1",
                (scope.key,)).fetchone()
            confirmation = json.loads(last_reply[0]).get('confirm_photo_product') if last_reply else None
            if confirmation == {'event': pending_id, 'product_id': product_id}:
                return dict(status='known', source='confirmed_photo_context', product_id=product_id)
        # An unresolved photo is never evidence for a cart change. The candidate
        # can still answer questions about the campaign in the meantime.
        return dict(candidate, status='provisional', photo_event=pending_id)

    def turn(self, scope, event_id):
        key = f'turn-v3:{event_id}'
        done = self.store.db.execute('SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                    (scope.key, key + ':done')).fetchone()
        if done:
            return json.loads(done[0])['outcome']
        booking = self.store.db.execute(
            'SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
            (scope.key, key + ':booking')).fetchone()
        if booking:
            # The order and its pending tasks already committed. A restarted
            # worker may have lost only the final turn marker.
            return dict(action='booked', **json.loads(booking[0]))
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
        photo_request = catalog_photo_request(text_now)
        if snap_now['data'].get('order_id') and explicit_cancellation(text_now):
            result=self.orders.cancel(scope,key+':cancel',snap_now['version'],event_id)['result']
            outcome=dict(result,action='cancelled',state_version=result['state_version']+1)
            self.store.apply(scope,key+':done',result['state_version'],dict(event_id=event_id),lambda state:dict(outcome=outcome))
            return outcome
        if prior:
            saved = json.loads(prior[0])
            plan, version = saved['plan'], saved['state_version']
        else:
            captured = capture_contact(self.store, scope, event_id, self.orders.allow_write)
            snap = self.store.snapshot(scope)
            if snap['data']['control_mode'] != 'active':
                raise Conflict('human_control_active')
            latest = self.store.db.execute("SELECT MAX(id) FROM sales_v2_events WHERE scope=? AND kind<>'receipt'",
                                           (scope.key,)).fetchone()[0]
            if latest != event_id:
                raise Conflict('turn_superseded')
            state = snap['data']
            media = state.get('media_observations', {}).get(str(event_id))
            pending_id, pending = self.pending_photo(state)
            reference = self.reference_for(scope, catalog, state, dict(evidence['payload'], observation=media))
            decline = bool(pending and purchase_declined(text_now))
            if pending and reference['status'] != 'known' and not decline:
                return dict(action='continue', state_version=snap['version'], photo_pending=pending_id,
                            provisional_product=reference.get('product_id') if reference['status']=='provisional' else None,
                            active_cart=bool(state['cart']))
            if pending and reference['status']=='known' and state['cart'] and reference['product_id'] not in {
                    line['product_id'] for line in state['cart']}:
                raise ServiceRequired('image_changes_existing_cart')
            photo_choice = self.photo_choice(scope, event_id, text_now, media, reference, state, catalog)
            if photo_choice:
                reference = dict(status='known', source='image', product_id=photo_choice['product_id'],
                                 available_photo_color=photo_choice['color'], photo_event=photo_choice['image_event'])
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
                           contact_received=captured.get('fields', []),
                           available_fact_fields={p['product_id']:[f for f in PRODUCT_FACT_FIELDS if resolve_fact(catalog,p['product_id'],f,policy)] for p in active_products})
            recent=self.store.db.execute("SELECT actor,payload FROM sales_v2_events WHERE scope=? AND kind='message' ORDER BY id DESC LIMIT 30",
                                         (scope.key,)).fetchall()
            context['recent_messages']=(snap['data'].get('prior_conversation', []) +
                [dict(actor=row[0], **message_context(json.loads(row[1]))) for row in reversed(recent)])[-30:]
            replies=self.store.db.execute("SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted' ORDER BY id DESC LIMIT 10",
                                          (scope.key,)).fetchall()
            context['recent_replies']=[json.loads(row[0]) for row in reversed(replies)]
            recovered=False;used_backup=False;business_feedback=None
            contact_turn = bool(captured.get('fields') and state['cart'] and not state.get('order_id')
                                and (state.get('purchase_event') or state.get('checkout_prompt'))
                                and contact_only(evidence['payload'].get('text', '')))
            focus = reference.get('product_id') or state.get('focus_product_id') or (
                state['cart'][0]['product_id'] if len({line['product_id'] for line in state['cart']}) == 1 else None)
            explicit_photo = photo_request and bool(focus)
            points_to_item = re.search(r'(?<!\w)(?:هذا|هاي|هذي)\s+(?:ال)?(?:موديل|قطع[هة]|اعلان)(?!\w)', text_now)
            if decline:
                raw = dict(intent='withdraw')
            elif contact_turn:
                raw = dict(intent='continue')
            elif (reference['status'] != 'known' and (reference['status'] != 'none' or points_to_item)
                    and not snap['data'].get('focus_product_id') and not snap['data']['cart']):
                # An unresolved referral or "this item" supplies no evidence for a catalog choice.
                raw=grounded_plan(context);recovered=True
            else:
                try:raw=self.decision_call(scope,event_id,context)
                except (DecisionRejected, DecisionUnavailable) as exc:
                    if explicit_photo:
                        # A clear request for a known product can still be served during an outage.
                        raw = dict(intent='continue')
                    else:
                        raw,used_backup=self.recover_decision(scope,event_id,snap['version'],context,exc)
                    recovered=True
            correction = color_correction(text_now)
            label = clothing_size_label(text_now)
            size_mode = (catalog.products.get(focus) or {}).get('selection_mode')
            uses_weight = size_mode == 'weight'
            size_label_for = focus if label and uses_weight else None
            measured = product_variants.weight_answer(text_now) if uses_weight else None
            weight = (measured or {}).get('value')
            same_product = not state['cart'] or (len(state['cart']) == 1 and state['cart'][0]['product_id'] == focus)
            explicit_size = direct_clothing_size(text_now) if size_mode in {'clothing', 'shoe'} else ''
            if not explicit_size and size_mode in {'clothing', 'shoe'} and photo_choice and direct_color(text_now):
                sizes = product_variants.requested(text_now)['sizes']
                if len(sizes) == 1:
                    explicit_size = sizes[0]
            for attempt in range(45):  # Bounded local stripping of unavailable choices, never 45 model calls.
                try:
                    plan=clean_plan(raw)
                    if explicit_photo:
                        # A photo-only turn cannot change the basket or the checkout fields.
                        quote = plan.get('answer', {}).get('catalog_photos')
                        plan = dict(intent='continue', contact={}, answer=dict(catalog_photos=quote))
                    answer = plan.get('answer', {})
                    photo_evidence = checkout.normalized(answer.get('catalog_photos'))
                    if photo_evidence and not (
                            focus and not decline and plan['intent']=='continue'
                            and evidence['payload'].get('media_kind','text')=='text'
                            and photo_evidence in text_now and not plan.get('changes') and not plan['contact']
                            and not checkout.is_confirmation(text_now)
                            and not requested_fields(text_now)
                            and not re.search(r'(?:ما|لا)\s*(?:اريد|تدز|ترسل)|\bمو\s+(?:ال)?صور', text_now)):
                        answer.pop('catalog_photos', None)
                    if photo_choice and same_product and not state.get('order_id'):
                        if plan['intent'] == 'withdraw':
                            plan['intent'] = 'continue'
                        changes = plan.setdefault('changes', [])
                        kept_color = next((row.get('color') for row in state['cart'] if row.get('color')), None)
                        # Asking whether another photographed color is in stock
                        # doesn't replace a color the customer already chose.
                        choice_color = kept_color if photo_choice['availability_question'] and kept_color else photo_choice['color']
                        if not changes and not business_feedback and choice_color != kept_color:
                            changes.append(dict(values=dict(product_id=focus, color=choice_color)))
                        elif (len(changes) == 1 and not changes[0].get('remove')
                              and changes[0]['values'].get('product_id') in (None, focus)
                              and not (business_feedback or {}).get('field') == 'color'):
                            changes[0]['values']['color'] = choice_color
                    if explicit_size and same_product and not state.get('order_id') and (business_feedback or {}).get('field') != 'size':
                        changes = plan.setdefault('changes', [])
                        if not changes:
                            changes.append(dict(values=dict(product_id=focus, size=explicit_size)))
                        elif (len(changes) == 1 and not changes[0].get('remove')
                              and changes[0]['values'].get('product_id') in (None, focus)):
                            values = changes[0]['values']
                            values.pop('weight_kg', None)
                            values.pop('age_months', None)
                            values['size'] = explicit_size
                    if size_label_for and same_product:
                        for change in plan.get('changes', []):
                            values = change['values']
                            if values.get('product_id') in (None, size_label_for):
                                values.pop('weight_kg', None)
                                values.pop('size', None)
                    if weight and same_product and not business_feedback and not state.get('order_id'):
                        changes = plan.setdefault('changes', [])
                        if not changes:
                            changes.append(dict(values=dict(product_id=focus, weight_kg=float(weight))))
                        elif (len(changes) == 1 and not changes[0].get('remove')
                              and changes[0]['values'].get('product_id') in (None, focus)):
                            changes[0]['values'].pop('size', None)
                            changes[0]['values']['weight_kg'] = float(weight)
                    if correction and not business_feedback and not state.get('order_id') and len(state['cart']) == 1:
                        line = state['cart'][0]
                        if product_variants.single_color(line.get('color')) == correction[1]:
                            color = photo_choice['color'] if photo_choice and same_product else correction[0]
                            plan['changes'] = [dict(line_ref=line['line_id'], values=dict(color=color))]
                    def apply_proposal(state):
                        if self.orders.allow_write and not self.orders.allow_write(scope):
                            raise Conflict('turn_superseded')
                        tools=TurnTools(self.store,scope,state,snap['version'])
                        session_cart=CartService(tools,catalog)
                        commands=resolve_changes(plan,state,scope,event_id)
                        invalid_fields=[]
                        for index,command in enumerate(commands):
                            session_cart.change(scope,str(index),snap['version'],event_id,**command)
                        hinted_contacts = {field: value for field, value in plan.get('contact', {}).items()
                                           if field not in captured.get('fields', ()) and not state['contact'].get(field)
                                           and not (field == 'phone' and declined_phone(text_now))}
                        if hinted_contacts:
                            contact_result=session_cart.contact(scope,'contact',snap['version'],event_id,**hinted_contacts)
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
                            if pending_id:
                                state['resolved_photo_event'] = pending_id
                        if size_label_for:
                            state['stated_clothing_size'] = dict(product_id=size_label_for, label=label)
                        return dict(plan=plan,invalid_fields=invalid_fields,business_feedback=business_feedback,
                                    quote=price,recovered=recovered,withdrawn=withdrawn,contact_turn=contact_turn,
                                    photo_choice=photo_choice)
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
                    if str(exc) in {'state_version_changed','human_control_active','turn_superseded'}:
                        raise
                    repair=getattr(type(self.decide),'repair',None)
                    if recovered:
                        if used_backup:
                            raise BackupFailed('backup_plan_invalid') from None
                        raise DecisionRejected('decision_tools_invalid_after_repair') from None
                    if repair is None and not self.backup:
                        raise
                    if attempt or repair is None:
                        raw,used_backup=self.recover_decision(scope,event_id,snap['version'],context,
                            DecisionRejected('decision_tools_invalid_after_repair'),raw,str(exc))
                        recovered=True
                        continue
                    # No SQL transaction is held across a model call.
                    current=self.store.snapshot(scope)
                    if current['version']!=snap['version']:
                        raise Conflict('state_version_changed') from None
                    try:raw=self.decision_call(scope,event_id,context,raw,str(exc))
                    except (DecisionRejected, DecisionUnavailable) as failure:
                        raw,used_backup=self.recover_decision(scope,event_id,snap['version'],context,failure,raw,str(exc))
                        recovered=True
            version = saved['state_version']
        try:ready = cart.readiness(scope)
        except BusinessChoice as exc:
            ready=dict(ready=False,missing_fields=[f'product.{exc.feedback["product_id"]}.{exc.feedback["field"]}'],conditions=[],version=version)
            saved['business_feedback']=exc.feedback
        photo_request = photo_request or bool(plan.get('answer', {}).get('catalog_photos'))
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
        if ((ready['ready'] or hold_color) and not image_only and not photo_request
                and not (saved.get('photo_choice') or {}).get('availability_question')
                and not missing_fact and not state.get('order_id')):
            booking = self.orders.commit(scope, f'{key}:booking', version, allow_color_hold=hold_color)
            outcome = dict(action='booked', **booking['result'])
            version = booking['result']['state_version']
        else:
            outcome = dict(action='continue', readiness=ready, answer_plan=plan.get('answer', {}),
                           invalid_fields=saved.get('invalid_fields',[]),business_feedback=saved.get('business_feedback'),
                           quote=saved.get('quote'),recovered=saved.get('recovered',False),
                           withdrawn=saved.get('withdrawn',False),contact_turn=saved.get('contact_turn',False),
                           photo_choice=saved.get('photo_choice'),state_version=version)
        outcome['state_version'] = version + 1
        self.store.apply(scope, key + ':done', version, dict(event_id=event_id), lambda state: dict(outcome=outcome))
        return outcome

    apply_intent=staticmethod(apply_intent)
    validate_plan = staticmethod(validate_plan)
