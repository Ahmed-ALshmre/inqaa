"""Ground the writer in the committed state and authoritative product facts."""
import json
import re

from .. import checkout, pricing, product_variants, sales_playbook
from .cart import CartService
from .store import Conflict, digest
from .contracts import DecisionRejected, FACT_FIELDS, message_context
from .copy_guard import validate_copy
from .facts import resolve as resolve_fact,requested_fields,POLICY_FIELDS,alternatives,choice_alternatives
from .intent import purchase_selected
from .negotiation import destination, offer_signature, price_disclosure, pushback, quantity_pushback, request as price_request
from .recovery import weight_fit
from .wording import mentions_free_size, next_options, quote_amounts, render_amounts, validate_next_step


class Responses:
    def __init__(self, store, catalog_loader, writer,policy_loader=None):
        self.store, self.catalog_loader, self.writer = store, catalog_loader, writer
        self.policy_loader=policy_loader or (lambda sid:{})

    def prepare(self, scope, turn_id, outcome):
        mode = 'withdrawn' if outcome.get('withdrawn') else outcome['action']
        if mode not in {'continue', 'withdrawn', 'cancelled', 'media_unresolved'}:
            # Order confirmations use durable order tasks, never model rewriting.
            return dict(action=outcome['action'])
        reply_key = {
            'continue': f'answer:{turn_id}',
            'withdrawn': f'answer:{turn_id}',
            'cancelled': f'cancellation:{turn_id}',
            'media_unresolved': f'clarify-image:{turn_id}',
        }[mode]
        key = reply_key if mode == 'media_unresolved' else f'writer-v3:{turn_id}'
        saved = self.store.db.execute('SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                     (scope.key,key)).fetchone()
        if saved:
            record=json.loads(saved[0])
            self.store.enqueue_reply(scope,reply_key,record['state_version'],record['parts'])
            return dict(action='queued',parts=len(record['parts']))
        if mode == 'cancelled' and self.store.db.execute(
            'SELECT 1 FROM sales_v2_outbox WHERE scope=? AND reply_key=?', (scope.key, reply_key),
        ).fetchone():
            return dict(action='queued',parts=1)
        snap = self.store.snapshot(scope)
        if snap['version'] != outcome['state_version']:
            raise Conflict('response_superseded')
        catalog = self.catalog_loader(scope.store)
        policy=self.policy_loader(scope.store)
        recent=self.store.db.execute("SELECT actor,payload FROM sales_v2_events WHERE scope=? AND kind='message' ORDER BY id DESC LIMIT 15",
                                     (scope.key,)).fetchall()
        messages=[dict(actor=row[0],**message_context(json.loads(row[1]))) for row in reversed(recent)]
        if mode != 'continue':
            question = 'product' if mode == 'media_unresolved' else None
            context = dict(mode=mode, question_field=question, next_step=dict(field=question, options=[]),
                           recent_messages=messages, amounts={}, action_results=dict(order_changed=mode == 'cancelled'))
            parts = self.write(context, catalog)
            if question:
                parts[-1].update(question_field=question, question_key=question)
            return self.save(scope, key, reply_key, turn_id, snap['version'], parts)
        readiness = CartService(self.store,catalog).readiness(scope)
        plan = outcome.get('answer_plan', {})
        question = plan.get('question_field')
        # A writer cannot request a field that is already known.
        if question and question not in readiness['missing_fields']:
            question = None
        if question and question.startswith('contact.') and (not snap['data'].get('purchase_event') or
                readiness['conditions'] or any(f.startswith('cart') for f in readiness['missing_fields'])):
            question=None
        if question=='purchase_intent' and readiness['conditions']:question=None
        current=messages[-1].get('text','') if messages else ''
        state = snap['data']
        lines = state['cart']
        focus=state.get('focus_product_id')
        if not focus and len({r['product_id'] for r in lines})==1:focus=lines[0]['product_id']
        product = catalog.get(focus) if focus else None
        requested=requested_fields(current)
        clean = checkout.normalized(current).strip(' .،!؟?')
        age_words = r'عمر|شهر|سنوات|سنه|سنة'
        previous_text = messages[-2].get('text', '') if len(messages) > 1 else ''
        if product and product.get('selection_mode') == 'age' and not requested and (
                re.search(age_words, clean) or clean == 'يصير' and re.search(age_words, previous_text)):
            requested = ['sizes']
        quote = outcome.get('quote')
        missing = readiness['missing_fields']
        cart_missing = [field for field in missing if field.startswith(('cart.', 'catalog.'))]
        received = {field for field, source in state.get('contact_events', {}).items() if source == turn_id}
        if state['contact'].get('phone') and checkout.phone_number(current) == state['contact']['phone']:
            received.add('phone')
        contact_cue = bool(lines and state.get('purchase_event') and clean in {'الرقم', 'رقمي', 'العنوان', 'عنواني'})
        contact_turn = not requested and bool(received or contact_cue)
        purchase_turn = bool(lines and state.get('purchase_event') == turn_id and purchase_selected(current))
        confirmation = checkout.is_confirmation(current)
        acknowledgment = (confirmation or bool(lines and (
            clean in {'الو', 'الو عيني'} or re.fullmatch(
                r'تمام\s+(?:(?:عيني|عيوني|عمري|حبيبتي)\s+)?(?:هذا|هاي)\s+(?:ال)?(?:تراك|اتراك|طقم|موديل|قطع[ةه])', clean)
        ))) and not purchase_turn and not requested
        mentioned = product_variants.requested(current)['colors'] if lines else []
        selected = bool(mentioned and all(color in {row.get('color') for row in lines} for color in mentioned)
                        and not re.search(r'متوفر|موجود|[؟?]|(?<!\w)(?:لو|او)(?!\w)', current))
        repeat_selection = bool(lines and state.get('purchase_event') and clean in
                                {'حسب قياسي','على قياسي','نفس قياسي','على نفس قياسي'})
        one_piece = (sum(row['quantity'] for row in lines) == 1 and
                     bool(re.fullmatch(r'(?:لا\s+)?(?:(?:عيني|عيوني|عمري|حبيبتي)\s+)?(?:بس|فقط)\s+(?:واحد[هة]?|قطعه|قطعة)', clean)))
        fit = weight_fit(product, current) if set(requested) <= {'sizes'} and not purchase_turn else None
        price_pending = any(condition.get('kind') == 'price' for condition in readiness['conditions'])
        requested_offer = price_request(current)
        quantity_objection = bool(state.get('price_request') and quantity_pushback(current))
        bargaining = bool(requested_offer or state.get('price_request') and pushback(current) or
                          price_pending and (contact_turn or purchase_turn or confirmation or selected or
                                             'price' in requested or destination(current)))
        quantity = sales_playbook.requested_quantity(current) if quote else None
        price_question = 'price' in requested and set(requested) <= {'price', 'delivery_fee'}
        different_quantity = bool(quote and quantity and quantity != sum(line['quantity'] for line in lines))
        per_piece = bool(quote and sum(line['quantity'] for line in lines) > 1 and
                         re.search(r'سعر\s*(?:القطع[ةه]|الوحد[ةه])|للقطع[ةه]|للواحد[ةه]', clean))
        if different_quantity or per_piece:
            price_question = False  # A hypothetical quantity is not the current cart price.
        quote_turn = bool(quote and not state.get('order_id') and (bargaining or price_question))
        references=list(plan.get('facts',[]))
        if contact_turn or acknowledgment:
            references = []
        if requested:
            relevant = set(requested) | ({'offer'} if 'price' in requested else set())
            references = [fact for fact in references if fact['field'] in relevant]
        for field in requested:
            pid='@store' if field in POLICY_FIELDS else focus
            if pid and not any(f['field']==field and f['product_id']==pid for f in references):
                references.append(dict(product_id=pid,field=field))
        allowed_facts = []
        for fact in references:
            if not isinstance(fact,dict) or set(fact)-{'product_id','field'}:
                raise ValueError('invalid_fact_reference')
            field=fact['field']
            if field not in FACT_FIELDS:
                raise ValueError('unsupported_fact_field')
            if quote_turn and (field == 'price' or field == 'delivery_fee' and quote.get('delivery_fee') is not None):
                continue  # The payable quote is authoritative for these amounts.
            fact_value=resolve_fact(catalog,fact['product_id'],field,policy)
            if fact_value is None:
                # An empty general-purpose field is not evidence that a specific answer is missing.
                if field in {'description','notes'} or field not in requested:continue
                return dict(action='missing_fact',product_id=fact['product_id'],field=field)
            if not any(f['field']==fact_value['field'] and f['product_id']==fact['product_id'] for f in allowed_facts):
                allowed_facts.append(dict(product_id=fact['product_id'],**fact_value))
        replies=self.store.db.execute("SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted' ORDER BY id DESC LIMIT 10",
                                      (scope.key,)).fetchall()
        accepted=[json.loads(row[0]) for row in reversed(replies)]
        size_explanation_requested = bool(not purchase_turn and (
            mentions_free_size(current) or 'sizes' in requested and re.search(
                r'[؟?]|شكد|شنو|شلون|يرهم|يناسب|قياساته|مقاساته', checkout.normalized(current))))
        free_size = product_variants.is_free_size_product(product)
        omit_free_size_explanation = bool(free_size and not size_explanation_requested and (
            any(part.get('size_explained_for') == focus for part in accepted)
            or self.store.db.execute(
                """SELECT 1 FROM sales_v2_outbox WHERE scope=? AND status='accepted'
                   AND json_extract(payload, '$.size_explained_for')=? LIMIT 1""", (scope.key, focus)
            ).fetchone()
        ))
        question_records=[json.loads(row[0]) for row in self.store.db.execute(
            "SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted' AND instr(payload,'question_field')>0 ORDER BY id DESC",(scope.key,)).fetchall()]
        # A known product can have a sales next-step without already being in a cart.
        if not focus and not snap['data']['cart']:
            attempts=sum(p.get('question_field')=='product' for p in accepted)
            if attempts>=1:return dict(action='product_unresolved')
            question='product'
        if question is None and not readiness['conditions']:
            question=next((f for f in readiness['missing_fields'] if f.startswith('cart.') and f.endswith(('.color','.size','.age_months','.weight_kg'))),None)
            if product and product.get('selection_mode') == 'weight' and not fit:
                question = next((f for f in cart_missing if f.endswith(('.size', '.weight_kg'))), question)
        if question is None and not snap['data']['cart'] and focus:
            product=catalog.get(focus)
            if product.get('sizes') and product.get('size_system')!='none':
                question=f'product.{focus}.size'
            elif product.get('colors'):
                question=f'product.{focus}.color'
        if (not readiness['conditions'] and snap['data']['cart'] and snap['data'].get('purchase_event')
                and not any(f.startswith('cart.') for f in readiness['missing_fields'])):
            if {'contact.phone','contact.address'}.issubset(readiness['missing_fields']):question='contact.details'
            elif question is None:
                question=next((f for f in readiness['missing_fields'] if f.startswith('contact.')),None)
        feedback = outcome.get('business_feedback')
        if not feedback and set(requested).intersection({'sizes', 'colors'}) and not contact_turn:
            feedback = next((c for c in readiness['conditions'] if c.get('kind') == 'choice'), None)
        recommendations=choice_alternatives(catalog,feedback) if feedback else alternatives(catalog,focus,current)
        if feedback:
            question=('alternative.'+feedback['product_id']) if recommendations['matches'] else None
        stage = ''
        if not state.get('order_id') and not feedback and (contact_turn or purchase_turn or selected or fit or one_piece):
            stage = 'contact' if contact_turn else 'selection' if purchase_turn or selected else 'fit'
            if contact_cue and not received:
                stage = 'contact-cue'
            elif one_piece:
                stage = 'quantity'
            if not contact_turn and purchase_turn and checkout.explicit_purchase(current):
                stage = 'checkout'
            if contact_turn and lines and state.get('purchase_event'):
                question = next((field for field in ('contact.phone', 'contact.address', 'contact.province')
                                 if field in missing), question)
                if not any(field.startswith('contact.') for field in missing) and cart_missing:
                    question = cart_missing[0]
            elif one_piece:
                question = next((field for field in cart_missing if field.endswith('.color')), question)
            elif fit and fit['within_catalog_range']:
                question = next((field for field in cart_missing if field.endswith('.color')), question)
            elif selected and not cart_missing and not readiness['conditions'] and not state.get('purchase_event'):
                question = 'purchase_intent'
        if acknowledgment and not quote_turn:
            # An acknowledgment alone did not answer a pending color or size question.
            # A new purchase confirmation can move directly to the contact step.
            if state.get('purchase_event') == turn_id and not cart_missing:
                stage = 'selection'
            elif question and not question.startswith('contact.'):
                question = None
        if quote_turn and price_pending:
            if quote.get('total') is not None:
                question = 'price.accept'
                stage = 'price-selecting' if cart_missing else 'price-ready'
                ask = state.get('price_request') or {}
                stage += f':{ask.get("basis")}:{ask.get("amount")}'
                if requested_offer and requested_offer.get('discretionary'):
                    stage += ':discount-followup'
            elif not (contact_turn and question in {'contact.address', 'contact.province'}):
                question = 'quote.province'
                if quantity_objection:
                    stage = 'price-quantity'
        if fit and not fit['within_catalog_range'] and not feedback:
            question = None
        def question_key(field):
            if field and field.startswith('cart.'):
                row=next((r for r in snap['data']['cart'] if r['line_id']==field.split('.')[1]),None)
                if row:return 'product.'+row['product_id']+'.'+field.rsplit('.',1)[-1]
            return field
        offer_key = offer_signature(quote) if quote_turn else None
        if question == 'price.accept' and accepted and not requested_offer:
            previous_reply = accepted[-1]
            if previous_reply.get('question_field') == 'price.accept' and previous_reply.get('quote_key') == offer_key:
                question = None  # Choosing a color does not require quoting the same total in consecutive replies.
        if question and question not in outcome.get('invalid_fields',[]):
            previous = [p for p in question_records
                        if (p.get('question_key') or question_key(p.get('question_field'))) == question_key(question)]
            if stage:
                previous = [p for p in previous if p.get('question_stage') == stage]
            if question == 'price.accept':
                previous = [p for p in previous if p.get('quote_key') == offer_key]
            if previous:
                question = None
        recap = bool(not feedback and (purchase_turn or selected or repeat_selection)
                     and set(requested) <= {'colors', 'sizes'})
        available_fields = set(requested) if requested or contact_turn or acknowledgment else set(FACT_FIELDS)
        omit_color_list = bool((contact_turn or acknowledgment or requested) and 'colors' not in requested
                               and not mentioned and not feedback
                               and not repeat_selection and not purchase_turn
                               and not recommendations and not (question or '').endswith('.color'))
        amounts = {}
        if quote_turn:
            shown = any(part.get('quote_key') == offer_key for part in accepted)
            if not shown or price_question or question == 'price.accept':
                amounts = quote_amounts(quote, compact=shown and not price_question)
            elif re.match(r'^(?:مع|ويا)\s+التوصيل', clean) and quote.get('total') is not None:
                amounts = quote_amounts(quote, compact=True)
        elif 'delivery_fee' in requested and not different_quantity:
            fee = (quote or {}).get('delivery_fee')
            fees = policy.get('delivery_fee')
            if fee is None and isinstance(fees, dict):
                province = destination(current) or state['contact'].get('province') or state.get('quote_province')
                fee = fees.get(province)
                if fee is None and len(set(fees.values())) == 1:
                    fee = next(iter(fees.values()))
            if type(fee) is int:
                amounts['delivery_fee'] = fee
        price_units = None
        if product and 'price' in requested and not quote_turn and not different_quantity:
            try:
                price_units, price, _ = pricing.terms(product)
                amounts['unit_price'] = price
            except ValueError:
                pass
        product_facts = {}
        value_facts = {}
        if product:
            for field in FACT_FIELDS:
                if field in POLICY_FIELDS:
                    continue
                if field in available_fields or field in {'fabric', 'fabric_feel', 'season', 'cut'}:
                    fact = resolve_fact(catalog, focus, field, policy)
                    if fact and field in available_fields:
                        product_facts[field] = fact['value']
                    if fact and field in {'fabric', 'fabric_feel', 'season', 'cut'}:
                        value_facts[field] = fact['value']
        step_product = product
        if question and question.startswith(('cart.', 'product.')):
            step_product = catalog.get(question_key(question).split('.', 1)[1].rsplit('.', 1)[0])
        context = dict(
            mode=mode, state=state, facts=allowed_facts, product_facts=product_facts,
            question_field=question, goal=plan.get('goal',''), policies=policy,
            quote=quote if quote_turn else None, amounts=amounts, business_feedback=feedback,
            recommendations=recommendations, value_facts=value_facts,
            next_step=dict(field=question, options=next_options(step_product, question)),
            progress=dict(stage=stage, received=sorted(received), acknowledgment=acknowledgment,
                          contact_turn=contact_turn, recap_selection=recap,
                          free_size=free_size,
                          quantity=sum(row['quantity'] for row in lines), fit=fit,
                          quantity_confirmed=one_piece or quantity_objection,
                          price_unit_quantity=price_units, negotiation=bargaining,
                          keep_selection_open=bool(cart_missing and not price_pending and contact_turn)),
            action_results=dict(order_saved=bool(state.get('order_id')) and state.get('order_status')!='cancelled',
                                carrier_contacted=False, delivery_time=policy.get('delivery_time'),
                                omit_color_list=omit_color_list, omit_free_size_explanation=omit_free_size_explanation,
                                discount_allowed=bool(quote_turn and quote.get('discount'))),
            recent_messages=messages, recent_replies=accepted,
        )
        if focus:
            from ..product_questions import guidance_for
            context['reply_guidance'] = guidance_for(product,current)
        parts = self.write(context, catalog)
        if question in {'contact.phone','contact.address','contact.province','contact.details'}:
            text_parts=[p for p in parts if p['type']=='text']
            if text_parts:
                text_parts[-1]['purpose']='checkout_contacts'
        if question:
            text_parts=[p for p in parts if p['type']=='text']
            if text_parts:text_parts[-1].update(question_field=question,question_key=question_key(question),question_stage=stage)
        if question == 'purchase_intent' and stage == 'selection' and recap:
            text_parts[-1]['cart_key'] = digest(lines)
        if quote_turn:
            parts[-1]['quote_key'] = offer_key
        if question=='price.accept':text_parts[-1]['quote_total']=quote['total']
        disclosure = price_disclosure(quote if quote_turn else None, amounts)
        if disclosure:
            parts[-1]['price_disclosure'] = disclosure
        if free_size:
            for part in parts:
                if part['type'] == 'text' and mentions_free_size(part['text']):
                    part['size_explained_for'] = focus
        return self.save(scope, key, reply_key, turn_id, snap['version'], parts)

    def write(self, context, catalog):
        from ..customer_copy import public_product_wording
        question = context['question_field']
        strict = getattr(type(self.writer), 'structured_sales', False)
        parts = None
        for attempt in range(2):
            try:
                parts = self.writer.repair(context, parts, error) if attempt else self.writer(context)
                # Validate structure before formatting or reading any model-authored fields.
                validate_copy(parts, question, context['action_results'])
                parts = render_amounts(parts, context['amounts'])
                for part in parts:
                    if part['type'] == 'text':
                        part['text'] = public_product_wording(dict(reply=part['text']), list(catalog.products.values()))['reply']
                    elif part.get('url') != catalog.get(part.get('product_id')).get('image_url') or not part.get('url'):
                        raise ValueError('image_not_in_catalog')
                validate_copy(parts, question, context['action_results'])
                if strict:
                    validate_next_step(parts, context['next_step'])
                return parts
            except ValueError as exc:
                if attempt or getattr(type(self.writer), 'repair', None) is None:
                    raise DecisionRejected('writer_invalid_after_validation') from exc
                error = str(exc)

    def save(self, scope, key, reply_key, turn_id, version, parts):
        record = self.store.apply(scope, key, version, dict(turn_id=turn_id), lambda state: dict(parts=parts))['result']
        self.store.enqueue_reply(scope, reply_key, record['state_version'], record['parts'])
        return dict(action='queued',parts=len(parts))
