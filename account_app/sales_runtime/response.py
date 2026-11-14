"""Ground the writer in the committed state and authoritative product facts."""
import json
import os
import re

from .. import catalog_profile, checkout, customer_copy, owner_sales, pricing, product_albums, product_variants, reply_layout, sales_answers, sales_intelligence, sales_playbook, sales_strategy, sales_voice, variant_inventory
from .backup import BackupFailed, consult
from .cart import CartService
from .store import Conflict, digest
from .contracts import DecisionRejected, DecisionUnavailable, FACT_FIELDS, message_context
from .copy_guard import ROOMY_FIT, validate_copy, validate_material
from .facts import resolve as resolve_fact,requested_fields,POLICY_FIELDS,alternatives,choice_alternatives,only_same_day_question,weight_limit_question
from .intent import constraint, purchase_selected
from .negotiation import destination, offer_signature, price_disclosure, pushback, quantity_pushback, request as price_request
from .recovery import catalog_photo_request, clothing_size_label, skirt_length_request, weight_fit
from .wording import checkout_question, mentions_free_size, natural_weight_wording, next_options, quote_amounts, render_amounts, validate_next_step


class Responses:
    def __init__(self, store, catalog_loader, writer,policy_loader=None, *, backup=None, product_photos=None,
                 history_loader=None):
        self.store, self.catalog_loader, self.writer = store, catalog_loader, writer
        self.policy_loader=policy_loader or (lambda sid:{})
        self.backup = backup
        self.product_photos = product_photos
        self.history_loader = history_loader

    def prepare(self, scope, turn_id, outcome, *, retry_revision=None):
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
        if retry_revision is not None:
            suffix = f':resume:{retry_revision}'
            key, reply_key = key + suffix, reply_key + suffix
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
        messages=(snap['data'].get('prior_conversation', []) +
                  [dict(actor=row[0], **message_context(json.loads(row[1]))) for row in reversed(recent)])[-15:]
        if self.history_loader and messages:
            messages = [*self.history_loader(scope, turn_id), messages[-1]][-15:]
        replies=self.store.db.execute("SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted' ORDER BY id DESC LIMIT 10",
                                      (scope.key,)).fetchall()
        accepted=[json.loads(row[0]) for row in reversed(replies)]
        current=messages[-1].get('text','') if messages else ''
        clean = checkout.normalized(current).strip(' .،!؟?')
        new_salam = bool(re.match(r'(?:ال)?سلام\s+عليكم(?!\w)|(?:و\s*)?عليكم\s+السلام(?!\w)', clean))
        new_welcome = bool(re.match(r'(?:هلو|هلا|اهلا|مرحبا|صباح الخير|مساء الخير)(?!\w)', clean))
        greeting = dict(
            omit_return_salam=not new_salam,
            omit_welcome=bool(accepted or snap['data'].get('order_id') or
                              any(message['actor'] in {'staff', 'assistant'} for message in messages))
                         and not (new_salam or new_welcome),
        )
        if mode == 'media_unresolved':
            parts = self.ask_about_photo(scope, turn_id, snap['version'], catalog, current, messages, accepted,
                                         greeting, 'product-detail', multiple=bool(outcome.get('multiple_images')))
            return self.save(scope, key, reply_key, turn_id, snap['version'], parts)
        if outcome.get('browsing_photos') and self.product_photos:
            parts = self.browsing_photos(catalog, outcome['browsing_photos'])
            return self.save(scope, key, reply_key, turn_id, snap['version'], parts)
        if outcome.get('photo_pending'):
            result = self.answer_pending_photo(scope, turn_id, outcome, catalog, policy, current, messages,
                                               accepted, greeting, snap['data']['contact'])
            if isinstance(result, dict):
                return result
            return self.save(scope, key, reply_key, turn_id, snap['version'], result)
        if outcome.get('catalog_option_photo') and self.product_photos:
            option = outcome['catalog_option_photo']
            source_event = option['source_event']
            source = self.store.evidence(scope, source_event)['payload'].get('text', '')
            history = self.history_loader(scope, source_event) if self.history_loader else []
            market = sales_intelligence.relevant(self.store.db, catalog, snap['data'], option['focus'], source,
                order_confirmed=sales_intelligence.confirmation_accepted(self.store.db, scope, snap['data']), history=history)
            match = next((item for item in market['matches'] if item['product_id'] == option['product_id']), None)
            photos = self.product_photos(catalog.get(match['product_id']), match['colors'] or [])[:4] if match else []
            if photos:
                return self.save(scope, key, reply_key, turn_id, snap['version'], photos)
            context = dict(mode='continue', question_field=None, next_step=dict(field=None, options=[]),
                customer_message=current, recent_messages=messages, amounts={}, greeting=greeting, action_results={},
                missing_catalog_photos=dict(colors=[], product_unavailable=match is None), copy_budget=145)
            parts = self.write(context, catalog, scope, turn_id, snap['version'])
            return self.save(scope, key, reply_key, turn_id, snap['version'], parts)
        if mode != 'continue':
            question = None
            context = dict(mode=mode, question_field=question, next_step=dict(field=question, options=[]),
                           recent_messages=messages, amounts={}, greeting=greeting,
                           customer_message=current, copy_budget=230,
                           action_results=dict(order_changed=mode == 'cancelled'))
            parts = self.write(context, catalog, scope, turn_id, snap['version'])
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
        state = snap['data']
        lines = state['cart']
        focus=state.get('focus_product_id')
        if not focus and len({r['product_id'] for r in lines})==1:focus=lines[0]['product_id']
        product = catalog.get(focus) if focus else None
        market = sales_intelligence.relevant(self.store.db, catalog, state, focus, current,
            order_confirmed=sales_intelligence.confirmation_accepted(self.store.db, scope, state),
            requested=bool(outcome.get('alternatives_requested')), history=messages[:-1],
            already_suggested=sales_intelligence.already_suggested(self.store.db, scope, state.get('order_id')))
        matched_photo_only = bool(product and state.get('focus_source') == 'image' and not current.strip()
                                  and messages[-1].get('media_kind') == 'image')
        if outcome.get('alternatives_requested') and product and self.product_photos:
            matches = (alternatives(catalog, focus, current, requested=True)['matches']
                       if not market['matches'] and accepted and accepted[-1].get('photo_offer_product') == focus
                       else market['matches'])
            photos = []
            for match in matches:
                photos.extend(self.product_photos(catalog.get(match['product_id']), [])[:1])
            parts = photos[:5] or [self.no_photo([], 'الموديلات البديلة المتوفرة')]
            return self.save(scope, key, reply_key, turn_id, snap['version'], parts)
        photo_choice = outcome.get('photo_choice')
        if photo_choice and photo_choice['product_id'] != focus:
            photo_choice = None
        product_unresolved = not focus and not lines
        if self.product_photos and product and (plan.get('catalog_photos') or catalog_photo_request(current)):
            colors = product_variants.requested(current)['colors']
            photos = self.product_photos(product, colors)
            color_key = f'product.{focus}.color'
            if not photos:
                photos = [self.no_photo(colors)]
            elif not colors and product.get('colors') and not any(line.get('color') for line in lines) and not any(
                    part.get('question_key') == color_key for part in accepted):
                history = [dict(role='user', text=m.get('text')) for m in messages if m['actor'] == 'customer']
                gender = owner_sales.address_gender('', text=current, history=history, store_id=scope.store)
                prompt = 'أي لون عجبج؟' if gender == 'female' else 'يا لون نختار؟'
                photos = photos[:4] + [dict(type='text', text=prompt, question_field=color_key,
                                             question_key=color_key, question_stage='photos')]
            return self.save(scope, key, reply_key, turn_id, snap['version'], photos[:5])
        requested=requested_fields(current)
        if matched_photo_only and not lines:
            requested.append('price')
        offer_question = bool(re.fullmatch(r'(?:شنو|شني|ما هو|ماهو)\s+(?:ال)?عرض(?:كم|ه)?', clean))
        if offer_question and 'price' not in requested:
            requested.append('price')
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
        previous_reply = accepted[-1] if accepted else {}
        weight_question = (previous_reply.get('text', '')
                           if previous_reply.get('question_key') == f'product.{focus}.weight_kg' else '')
        saved_weights = {line.get('weight_kg') or line.get('size') for line in lines
                         if line['product_id'] == focus and (line.get('weight_kg') or line.get('size_source_kind') == 'weight')}
        labels = product_variants.garment_labels(product)
        asked_labels = set(product_variants.requested(current)['sizes']).intersection(labels)
        if not asked_labels and product_variants.requested(current)['colors']:
            asked_labels = {line['size'] for line in lines if line['product_id'] == focus and line.get('size') in labels}
        saved_weight = next(iter(saved_weights)) if len(saved_weights) == 1 and (asked_labels or sales_answers.fit_question(current)) else None
        fit = weight_fit(product, current, weight_question, saved_weight=saved_weight) if not purchase_turn else None
        fit_stock = product_albums.stock_state(product) if fit else None
        tracked = bool(product and product.get('variant_stock'))
        inventory_feedback = outcome.get('variant_stock_feedback')
        requested_color, requested_size = variant_inventory.target(current) if tracked else ('', '')
        selected_line = next((line for line in lines if line['product_id'] == focus), {})
        chosen_color = variant_inventory.color_key(requested_color or selected_line.get('color'))
        chosen_size = (variant_inventory.size_key(product, requested_size) if requested_size
                       else variant_inventory.line_key(product, selected_line)[1]) if tracked else ''
        stock_rows = (variant_inventory.choices(product) if tracked
                      and product_albums.stock_state(product) == 'available' else [])
        if tracked and fit and stock_rows and (chosen_color or chosen_size) and not variant_inventory.choices(
                product, color=chosen_color, size=chosen_size):
            fit_stock = 'out'
        wanted_colors = product_variants.requested(current)['colors']
        label_availability = None
        if asked_labels and product_albums.stock_state(product) == 'available':
            offered = product_variants.colors(product.get('colors'))
            checked_colors = wanted_colors or ([selected_line['color']] if selected_line.get('color') else [])
            available = (all(variant_inventory.choices(product, color=color, size=label)
                             for color in checked_colors or [''] for label in sorted(asked_labels)) if tracked
                         else all(color in offered for color in checked_colors))
            label_availability = dict(labels=sorted(asked_labels), available=bool(available),
                                      weight_compatible=fit['within_catalog_range'] if fit else None,
                                      weight_was_asked=bool(product_variants.weight_answer(current) or sales_answers.fit_question(current)))
        needs_weight = bool(product and product.get('selection_mode') == 'weight'
                            and not product_variants.is_free_size_product(product) and clothing_size_label(current)
                            and state.get('stated_clothing_size', {}).get('product_id') == focus
                            and not any(line.get('product_id') == focus and (
                                line.get('weight_kg') or line.get('size_source_kind') == 'weight') for line in lines))
        price_pending = any(condition.get('kind') == 'price' for condition in readiness['conditions'])
        requested_offer = price_request(current)
        quantity_objection = bool(state.get('price_request') and quantity_pushback(current))
        bargaining = bool(requested_offer or state.get('price_request') and pushback(current) or
                          price_pending and (contact_turn or purchase_turn or confirmation or selected or
                                             'price' in requested or destination(current)))
        quantity = sales_playbook.requested_quantity(current)
        if not quantity and 'delivery_fee' in requested and re.search(r'قطعه?\s+وحد[هة]|القطع[ةه]\b', clean):
            quantity = 1
        price_question = 'price' in requested and set(requested) <= {'price', 'delivery_fee'}
        different_quantity = bool(quote and quantity and quantity != sum(line['quantity'] for line in lines))
        per_piece = bool(quote and sum(line['quantity'] for line in lines) > 1 and
                         re.search(r'سعر\s*(?:القطع[ةه]|الوحد[ةه])|للقطع[ةه]|للواحد[ةه]', clean))
        if different_quantity or per_piece:
            price_question = False  # A hypothetical quantity is not the current cart price.
        quote_turn = bool(quote and not state.get('order_id') and (bargaining or price_question))
        if market['opportunity'] == 'price_hesitation' and market['matches'] and not bargaining:
            quote_turn = False  # A cheaper option gets its own price, separate from the current cart quote.
        price_units = retail_price = None
        promotion = None
        product_free_delivery = False
        product_fee = None
        free_quantity = None
        if product and all(line['product_id'] == focus for line in lines) and not state.get('order_id'):
            free_quantity, product_fee = pricing.delivery_terms(product)
        if product and ('price' in requested or 'delivery_fee' in requested) and not state.get('order_id'):
            try:
                price_units, retail_price, _ = pricing.terms(product)
                shipping_count = quantity or sum(line['quantity'] for line in lines) or price_units
                product_free_delivery = bool(free_quantity and shipping_count >= free_quantity)
                if price_question and not bargaining and not state.get('purchase_event') and not (quantity and quantity > 1) and (
                        not quote_turn or sum(line['quantity'] for line in lines) == 1):
                    promotion = next(iter(pricing.offer_tiers(product)), None)
            except ValueError:
                pass
        references = [fact for fact in plan.get('facts', []) if fact['product_id'] in (focus, '@store')]
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
                missing = dict(action='missing_fact',product_id=fact['product_id'],field=field)
                if self.backup:
                    relevant_ids = {pid for pid in (focus, fact['product_id']) if pid and pid != '@store'}
                    relevant_ids.update(line['product_id'] for line in state['cart'])
                    review = dict(state=state, event=messages[-1] if messages else {},
                                  recent_messages=messages, recent_replies=accepted, policies=policy,
                                  products=[catalog.get(pid) for pid in sorted(relevant_ids)], missing_fact=missing)
                    try:
                        advice = consult(self.store, scope, turn_id, snap['version'], 'fact', self.backup,
                                         review, 'requested_catalog_fact_missing', plan,
                                         history_loader=self.history_loader)
                    except BackupFailed:
                        advice = None  # Preserve the existing factual review and question record.
                    if advice and focus and field not in POLICY_FIELDS:
                        fact_value = resolve_fact(catalog, focus, field, policy)
                        if fact_value is not None:
                            fact = dict(product_id=focus, field=field)
                            outcome = dict(outcome, consultant_guidance=advice)
                if fact_value is None:
                    return missing
            if not any(f['field']==fact_value['field'] and f['product_id']==fact['product_id'] for f in allowed_facts):
                allowed_facts.append(dict(product_id=fact['product_id'],**fact_value))
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
        photo_stage = ''
        if product_unresolved:
            # Policy-only follow-ups and acknowledgments can continue while a photo is pending.
            photo_questions = [p for p in question_records if p.get('question_field')=='product']
            needs_product = bool(set(requested).difference(POLICY_FIELDS)
                                 or re.search(r'اريد|اطلب|احجز|اخذ|اشتري|بدله|دشداش|هالموديل|هذا\s+الموديل', clean))
            already_requested_for_item = any(p.get('question_stage')=='product-detail' for p in photo_questions)
            policy_only = bool(requested and not needs_product)
            question = ('product' if not market['matches'] and not policy_only and
                        (not photo_questions or needs_product and not already_requested_for_item) else None)
            if question:
                photo_stage = 'product-detail' if needs_product else 'product-opening'
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
        if not feedback and (market['requested'] or market['opportunity']):
            recommendations = dict(matches=market['matches'], searched=True)
        if feedback:
            question=('alternative.'+feedback['product_id']) if recommendations['matches'] else None
        stage = photo_stage
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
                question = (None if fit_stock == 'out' else
                            next((field for field in cart_missing if field.endswith('.color')), question))
                if (fit_stock == 'available' and question is None and lines and not cart_missing and not readiness['conditions']
                        and not state.get('purchase_event')):
                    question = 'purchase_intent'
            elif selected and not cart_missing and not readiness['conditions'] and not state.get('purchase_event'):
                question = 'purchase_intent'
        if acknowledgment and not quote_turn:
            # An acknowledgment alone did not answer a pending color or size question.
            # A new purchase confirmation can move directly to the contact step.
            if state.get('purchase_event') == turn_id and not cart_missing:
                stage = 'checkout-confirmed' if question and question.startswith('contact.') else 'selection'
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
        if (product and product.get('selection_mode') == 'weight' and question
                and (question == f'product.{focus}.size'
                     or any(question == f"cart.{line['line_id']}.size" for line in lines if line['product_id'] == focus))):
            question = f'product.{focus}.weight_kg'
        if needs_weight and not feedback and not readiness['conditions']:
            question = f'product.{focus}.weight_kg'
        if promotion and not feedback and not readiness['conditions']:
            question = 'offer.quantity'
            stage = 'offer-quantity'
        seeking_other_colors = bool(product and 'colors' in requested and
                                    re.search(r'غير\s+(?:ال)?الوان|(?:ال)?الوان\s+(?:غير|ثاني|اخر)', clean))
        same_palette_confirmation = bool(product and re.fullmatch(
            r'(?:يعني\s+)?بس\s+(?:هاي|هذي|هذني)\s+(?:ال)?الوان', clean))
        if seeking_other_colors and not state.get('order_id') and not feedback and not readiness['conditions']:
            question = 'alternative.color'
            stage = 'other-colors'
        stock_conflict = bool(outcome.get('staff_stock_conflict'))
        if product and not state.get('order_id') and (stock_conflict or product_albums.stock_state(product) == 'out'):
            recommendations = alternatives(catalog, focus, current, requested=True)
            can_send = self.product_photos and any(
                self.product_photos(catalog.get(match['product_id']), []) for match in recommendations['matches'])
            question = 'alternatives.photos' if can_send else None
            stage = 'stock-alternatives'
        if only_same_day_question(current):
            question = None
        if inventory_feedback:
            alternatives_available = inventory_feedback['available']
            if alternatives_available and inventory_feedback['product_id'] == focus:
                question = 'inventory.alternative'
            else:
                question = None
        if outcome.get('separate_photo_product'):
            if question and not (question.startswith(f'product.{focus}.')
                                 or question in {'alternative.color', 'alternatives.photos'}):
                question = None
            if feedback and feedback.get('product_id') != focus:
                feedback, recommendations = None, None
        def question_key(field):
            if field and field.startswith('cart.'):
                row=next((r for r in snap['data']['cart'] if r['line_id']==field.split('.')[1]),None)
                if row:return 'product.'+row['product_id']+'.'+field.rsplit('.',1)[-1]
            return field
        # A question about the upper weight limit is a factual follow-up.
        # Leave an already asked color choice open across cart/offer stage changes.
        if weight_limit_question(clean) and question and question.endswith('.color'):
            question = None
        offer_key = offer_signature(quote) if quote_turn else None
        if question == 'price.accept' and accepted and not requested_offer:
            previous_reply = accepted[-1]
            if previous_reply.get('question_field') == 'price.accept' and previous_reply.get('quote_key') == offer_key:
                question = None  # Choosing a color does not require quoting the same total in consecutive replies.
        if stage == 'checkout-confirmed' and accepted and accepted[-1].get('question_field') == question:
            question = None
        if label_availability and question and question.startswith('product.') and question.endswith(('.size', '.weight_kg')):
            question = f'product.{focus}.color' if product.get('colors') and not wanted_colors and not chosen_color else None
        if label_availability and label_availability['weight_compatible'] is False and not quote_turn:
            # An alphabetical label cannot make an out-of-range garment fit.
            if question and (question.startswith(('contact.', 'cart.', 'product.')) or question == 'purchase_intent'):
                question = None
        if market['requested'] and market.get('requested_age_months') and not market['matches'] and not feedback and not quote_turn:
            question = None
        if (market['matches'] and not feedback and not quote_turn and not inventory_feedback
                and not (market['opportunity'] and (contact_turn or purchase_turn or (question or '').startswith('contact.')))):
            question = 'catalog.show_option' if self.product_photos else None
            photo_product = catalog.get(market['matches'][0]['product_id'])
            if question and not self.product_photos(photo_product, market['matches'][0]['colors'] or []):
                question = None
        if (question and question != 'product' and question not in outcome.get('invalid_fields', [])
                and not (outcome.get('waiting_for_color') and question.endswith('.color'))):
            previous = [p for p in question_records
                        if (p.get('question_key') or question_key(p.get('question_field'))) == question_key(question)]
            if question == 'purchase_intent':
                cart_key = digest(lines)
                previous = [p for p in previous if p.get('cart_key') == cart_key]
            elif stage:
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
        if seeking_other_colors or same_palette_confirmation:
            omit_color_list = True
        amounts = {}
        if quote_turn:
            shown = any(part.get('quote_key') == offer_key for part in accepted)
            if not shown or price_question or question == 'price.accept':
                amounts = quote_amounts(quote, compact=shown and not price_question)
            elif re.match(r'^(?:مع|ويا)\s+التوصيل', clean) and quote.get('total') is not None:
                amounts = quote_amounts(quote, compact=True)
        elif 'delivery_fee' in requested:
            fee = (quote or {}).get('delivery_fee') if not different_quantity else None
            if fee is None and product_free_delivery:
                fee = 0
            if fee is None:
                fee = product_fee
            fees = policy.get('delivery_fee')
            if fee is None and isinstance(fees, dict):
                province = destination(current) or state['contact'].get('province') or state.get('quote_province')
                fee = fees.get(province)
                if fee is None and len(set(fees.values())) == 1:
                    fee = next(iter(fees.values()))
            if type(fee) is int:
                amounts['delivery_fee'] = fee
        if product and 'price' in requested and not quote_turn:
            try:
                price_units, price, _ = pricing.terms(product)
                if quantity and quantity != price_units:
                    requested_quote = pricing.quote([dict(product_id=focus, quantity=quantity)], [product],
                                                    amounts.get('delivery_fee'))
                    amounts['subtotal'] = requested_quote['product_total']
                else:
                    amounts['unit_price'] = price
            except ValueError:
                pass
        if promotion:
            requested_fee = amounts.get('delivery_fee') if 'delivery_fee' in requested else None
            amounts = {'unit_price': retail_price, 'offer_total': promotion[1]}
            if requested_fee is not None:
                amounts['delivery_fee'] = requested_fee
        fees = policy.get('delivery_fee')
        free_delivery = product_free_delivery or (isinstance(fees, dict) and bool(fees) and set(fees.values()) == {0})
        if price_question and not bargaining and free_delivery and 'delivery_fee' not in amounts and not any(
                part.get('price_disclosure', {}).get('delivery_fee') == 0 for part in accepted):
            amounts['delivery_fee'] = 0
        if market['matches'] and 'price' in requested and not quote_turn and (not market['opportunity'] or question == 'catalog.show_option'):
            # The available alternative has its own price and delivery terms.
            # Never substitute the active model's price into a recommendation.
            top_match = market['matches'][0]
            amounts = {'alternative_price': top_match['price_iqd']}
            if 'delivery_fee' in requested:
                if top_match['free_delivery_at_pieces'] and top_match['unit_pieces'] >= top_match['free_delivery_at_pieces']:
                    amounts['delivery_fee'] = 0
                elif top_match['delivery_fee_iqd'] is not None:
                    amounts['delivery_fee'] = top_match['delivery_fee_iqd']
                else:
                    province = destination(current) or state['contact'].get('province')
                    fees = policy.get('delivery_fee')
                    if isinstance(fees, dict) and type(fees.get(province)) is int:
                        amounts['delivery_fee'] = fees[province]
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
            if stock_conflict:
                product_facts.pop('stock', None)
                allowed_facts = [fact for fact in allowed_facts if fact['field'] != 'stock']
        if fit and fit['within_catalog_range']:
            cue = sales_strategy.persuasion_cue(product)
            cut_notes = ' '.join((value_facts.get('cut', ''), customer_copy.fact(product.get('fit_notes'))))
            roomy = re.search(r'(?<!\w)(?:راهي|واسع[هة]?|فضفاض[هة]?)(?!\w)', checkout.normalized(cut_notes))
            negated = re.search(r'(?:مو|غير|ليس|ما يجي|ما تجي)\s+(?:راهي|واسع|فضفاض)', checkout.normalized(cut_notes))
            value_facts['fit_availability'] = dict(
                stock='unknown' if stock_conflict else fit_stock,
                limited_stock=bool(not stock_conflict and cue and cue['kind']=='limited_stock'),
                roomy_cut=bool(roomy and not negated))
        step_product = product
        if question and question.startswith(('cart.', 'product.')):
            step_product = catalog.get(question_key(question).split('.', 1)[1].rsplit('.', 1)[0])
        step_options = [] if question == 'alternative.color' else next_options(step_product, question)
        if stock_rows and question and question_key(question).startswith(f'product.{focus}.'):
            if question.endswith('.color'):
                step_options = list(dict.fromkeys(row['color'] for row in
                    variant_inventory.choices(product, size=chosen_size) if row['color']))[:2]
            elif question.endswith('.size'):
                matching_sizes = list(dict.fromkeys(row['size'] for row in
                    variant_inventory.choices(product, color=chosen_color) if row['size']))
                step_options = matching_sizes if len(matching_sizes) <= 2 else []
        context = dict(
            mode=mode, state=state, facts=allowed_facts, product_facts=product_facts,
            **({'consultant_guidance': outcome['consultant_guidance']} if outcome.get('consultant_guidance') else {}),
            question_field=question, goal=plan.get('goal',''), policies=policy,
            customer_message=current, requested_fields=requested,
            copy_budget=520 if len(requested) > 1 or quote_turn or recommendations else 230,
            quote=quote if quote_turn else None, amounts=amounts, business_feedback=feedback,
            recommendations=recommendations, value_facts=value_facts,
            catalog_memory=dict(market, matches=[], opportunity=None) if market['opportunity'] and question != 'catalog.show_option' else market,
            learned_sales_tactics=sales_intelligence.learned_tactics(
                self.store.db, scope.store, current, bool(market['matches'])),
            promotion_quantity=promotion[0] if promotion else None,
            free_delivery_min_quantity=free_quantity if free_quantity and free_quantity > 1 else None,
            stock_conflict=stock_conflict,
            weight_only=bool(product and product.get('selection_mode') == 'weight' and not free_size),
            documented_size_labels=labels,
            label_availability=label_availability,
            next_step=dict(field=question, options=step_options),
            progress=dict(stage=stage, received=sorted(received), acknowledgment=acknowledgment,
                          checkout_confirmed=stage=='checkout-confirmed',
                          contact_turn=contact_turn, recap_selection=recap,
                          product_unresolved=product_unresolved,
                          free_size=free_size,
                          quantity=sum(row['quantity'] for row in lines), fit=fit,
                          quantity_confirmed=one_piece or quantity_objection,
                          price_unit_quantity=price_units, negotiation=bargaining,
                          keep_selection_open=bool(cart_missing and not price_pending and contact_turn)),
            action_results=dict(order_saved=bool(state.get('order_id')) and state.get('order_status')!='cancelled',
                                carrier_contacted=False, delivery_time=policy.get('delivery_time'),
                                omit_color_list=omit_color_list, omit_free_size_explanation=omit_free_size_explanation,
                                discount_allowed=bool(quote_turn and quote.get('discount'))),
            recent_messages=messages, recent_replies=accepted, greeting=greeting,
        )
        if market['opportunity'] and question != 'catalog.show_option':
            context['recommendations'] = None
        if state.get('focus_source') == 'image' and focus:
            context['verified_photo_product_id'] = focus
        if tracked:
            matching = variant_inventory.choices(product, color=chosen_color, size=chosen_size) if stock_rows else []
            context['variant_stock'] = dict(available=stock_rows, color=chosen_color, size=chosen_size,
                selected_quantity=sum(row['quantity'] for row in matching),
                requested_quantity=(inventory_feedback or {}).get('requested', {}).get('quantity')
                                   or sales_playbook.requested_quantity(current))
        if inventory_feedback:
            context['variant_stock_feedback'] = inventory_feedback
            if context.get('variant_stock') and inventory_feedback['product_id'] == focus:
                context['variant_stock']['available'] = inventory_feedback['available']
        if (skirt_length_request(current) and not product_variants.weight_answer(current) and
                (catalog_profile.effective_family(product) == 'skirts' or any(
                    catalog_profile.query_type(message.get('text')) == 'skirts'
                    for message in messages if message['actor'] == 'customer'))):
            context['requested_skirt_length_cm'] = skirt_length_request(current)
        if focus:
            from ..product_questions import guidance_for
            context['reply_guidance'] = guidance_for(product,current)
        if photo_choice:
            context['verified_photo_choice'] = photo_choice
        if (outcome.get('waiting_for_color') and self.product_photos and product
                and question and question.endswith('.color') and not requested):
            photos = self.product_photos(product, [])[:4]
            parts = photos + [dict(type='text', text='باقي بس اللون اللي يعجبج حتى أثبتلج الحجز 🌷')]
        elif (needs_weight and question == f'product.{focus}.weight_kg' and set(requested) <= {'sizes'}
              and 'verified_photo_product_id' not in context):
            parts = [dict(type='text', text='هالموديل قياساته حسب الوزن عيني، شكد الوزن تقريباً؟')]
        elif outcome.get('contact_turn') and not requested and not quote_turn and not feedback and (
                getattr(type(self.writer), 'structured_sales', False)):
            if not question:
                return dict(action='skipped')
            if question.startswith('contact.'):
                history = [dict(role='user', text=m.get('text')) for m in messages if m['actor'] == 'customer']
                gender = owner_sales.address_gender('', text=current, history=history, store_id=scope.store)
                final_detail = set(missing) == {question} or question == 'contact.details'
                parts = [dict(type='text', text=checkout_question(question, gender, final_detail=final_detail))]
            else:
                try:
                    parts = self.write(context, catalog, scope, turn_id, snap['version'])
                except (DecisionRejected, DecisionUnavailable):
                    if not outcome.get('waiting_for_color'):
                        raise
                    parts = [dict(type='text', text='باقي بس اللون اللي يعجبج حتى أثبتلج الحجز 🌷')]
        else:
            parts = self.write(context, catalog, scope, turn_id, snap['version'])
        if question in {'contact.phone','contact.address','contact.province','contact.details'}:
            text_parts=[p for p in parts if p['type']=='text']
            if text_parts:
                text_parts[-1]['purpose']='checkout_contacts'
        if market['opportunity'] and market['matches'] and question == 'catalog.show_option':
            parts[-1]['proactive_offer'] = state.get('order_id') or 0
        if question:
            text_parts=[p for p in parts if p['type']=='text']
            if text_parts:text_parts[-1].update(question_field=question,question_key=question_key(question),question_stage=stage)
            if question == 'alternatives.photos':
                text_parts[-1]['photo_offer_product'] = focus
            if question == 'catalog.show_option' and text_parts:
                text_parts[-1]['catalog_photo_offer'] = dict(product_id=market['matches'][0]['product_id'],
                    focus=focus, source_event=turn_id)
        if question == 'purchase_intent':
            text_parts[-1]['cart_key'] = digest(lines)
        if quote_turn:
            parts[-1]['quote_key'] = offer_key
        if question=='price.accept':text_parts[-1]['quote_total']=quote['total']
        disclosure = {} if 'alternative_price' in amounts else price_disclosure(quote if quote_turn else None, amounts)
        if disclosure:
            parts[-1]['price_disclosure'] = disclosure
        if free_size:
            for part in parts:
                if part['type'] == 'text' and mentions_free_size(part['text']):
                    part['size_explained_for'] = focus
        return self.save(scope, key, reply_key, turn_id, snap['version'], parts)

    @staticmethod
    def no_photo(colors, label=''):
        what = f'لـ{label}' if label else 'لهاللون' if colors else 'لهالموديل'
        return dict(type='text', text=f'هسه ما عدنا صور {what} حتى أدزها.')

    def browsing_photos(self, catalog, request):
        def matches(product):
            if (catalog_profile.effective_family(product) != request['category']
                    or product_albums.stock_state(product) != 'available'):
                return False
            if request['length'] is None:
                return True
            length = sales_strategy.measurement_fact(product, 'length')
            return bool(re.search(rf'(?<!\d){request["length"]}(?![\d.,])', checkout.normalized(length)))

        candidates = sorted((product for product in catalog.products.values() if matches(product)),
                            key=lambda product: not bool(set(request['occasions']) & set(product.get('occasions') or [])))
        photos = []
        seen = set()
        for product in candidates:
            for color in request['colors'] or [None]:
                for part in self.product_photos(product, [color] if color else [])[:1]:
                    if part['url'] not in seen:
                        seen.add(part['url'])
                        photos.append(part)
                        if len(photos) == 5:
                            return photos
        return photos or [self.no_photo(request['colors'], 'بهالمواصفات')]

    def answer_pending_photo(self, scope, turn_id, outcome, catalog, policy, current, messages, accepted, greeting, contact):
        """Discuss a possible campaign product without applying it to the cart."""
        requested = requested_fields(current)
        wants_photos = catalog_photo_request(current)
        wants_purchase = not wants_photos and purchase_selected(current)
        product_id = outcome.get('provisional_product')
        product = catalog.get(product_id) if product_id else {}
        if 'availability' in sales_answers.topics(current):
            requested.append('stock')
            if 'colors' not in requested and product_variants.requested(current)['colors']:
                requested.append('colors')
            if 'sizes' not in requested and product_variants.requested(current)['sizes']:
                requested.append('sizes')
        if not product_id and not wants_purchase and set(requested) <= {'price', 'sizes', 'colors', 'stock'}:
            if requested and accepted and accepted[-1].get('question_stage') == 'product-detail':
                return self.ask_about_photo(scope, turn_id, outcome['state_version'], catalog, current, messages,
                                            accepted, greeting, 'image-choice', multiple=True)
            if accepted and accepted[-1].get('question_stage') == 'image-choice':
                return dict(action='skipped')
        if not product_id and (wants_photos or wants_purchase or any(field not in POLICY_FIELDS for field in requested)):
            return dict(action='photo_review')
        if wants_photos and product and self.product_photos:
            colors = product_variants.requested(current)['colors']
            photos = self.product_photos(product, colors)
            if photos:
                return photos[:5]
            context = dict(mode='continue', customer_message=current, recent_messages=messages,
                           recent_replies=accepted, question_field=None, next_step=dict(field=None, options=[]),
                           copy_budget=145, amounts={}, greeting=greeting, action_results={},
                           missing_catalog_photos=dict(colors=colors))
            return self.write(context, catalog, scope, turn_id, outcome['state_version'])
        if wants_purchase:
            if outcome.get('active_cart') or constraint(current):
                return dict(action='photo_review')
            target = {'event': outcome['photo_pending'], 'product_id': product_id}
            if accepted and accepted[-1].get('confirm_photo_product') == target:
                return dict(action='skipped')
            label = customer_copy.reply_category(product) or customer_copy.fact(product.get('product_name'))[:70].strip()
            parts = self.ask_about_photo(scope, turn_id, outcome['state_version'], catalog, current, messages,
                                         accepted, greeting, 'confirm-product', label=label)
            parts[-1]['confirm_photo_product'] = target
            return parts
        if product and not current.strip() and turn_id == outcome['photo_pending']:
            requested = ['price']
        if not requested and outcome.get('latest_verified_image') and weight_fit(product, current):
            requested = ['sizes']
        if not requested:
            return dict(action='skipped')

        facts = []
        for field in requested:
            pid = '@store' if field in POLICY_FIELDS else product_id
            fact = resolve_fact(catalog, pid, field, policy)
            if field == 'stock':
                stock = product_albums.stock_state(product)
                fact = dict(field='stock', value=stock) if stock != 'unknown' else None
            if fact is None:
                if pid and field == 'wholesale':
                    return dict(action='missing_fact', product_id=pid, field=field)
                return dict(action='photo_review')
            facts.append(dict(product_id=pid, **fact))
        amounts = {}
        price_units = None
        promotion = None
        if 'price' in requested:
            try:
                price_units, amounts['unit_price'], _ = pricing.terms(product)
                if outcome.get('latest_verified_image'):
                    promotion = next(iter(pricing.offer_tiers(product)), None)
                    if promotion:
                        amounts['offer_total'] = promotion[1]
            except ValueError:
                return dict(action='photo_review')
        if 'delivery_fee' in requested:
            free_minimum, product_fee = pricing.delivery_terms(product)
            fees = policy.get('delivery_fee')
            province = destination(current) or contact.get('province')
            fee = 0 if free_minimum == 1 else product_fee
            if fee is None and isinstance(fees, dict):
                fee = fees.get(province)
                if fee is None and fees and len(set(fees.values())) == 1:
                    fee = next(iter(fees.values()))
            if type(fee) is int:
                amounts['delivery_fee'] = fee
        version = outcome['state_version']
        context = dict(mode='continue', state={'contact': contact}, facts=facts, policies=policy, amounts=amounts,
                       product_facts={fact['field']: fact['value'] for fact in facts if fact['product_id']==product_id},
                       question_field=None, next_step=dict(field=None, options=[]), copy_budget=220,
                       customer_message=current, requested_fields=requested, recent_messages=messages,
                       recent_replies=accepted, greeting=greeting, action_results={},
                       provisional_photo=not outcome.get('latest_verified_image'),
                       promotion_quantity=promotion[0] if promotion else None)
        if outcome.get('latest_verified_image'):
            fit = weight_fit(product, current)
            context.update(verified_photo_product_id=product_id,
                           progress=dict(fit=fit, price_unit_quantity=price_units),
                           value_facts=dict(fit_availability=dict(stock=product_albums.stock_state(product))))
        elif product_id:
            context['possible_photo_product'] = customer_copy.reply_category(product) or customer_copy.fact(
                product.get('product_name'))[:70].strip()
        if product.get('variant_stock'):
            color, size = variant_inventory.target(current)
            context['variant_stock'] = dict(color=variant_inventory.color_key(color),
                size=variant_inventory.size_key(product, size) if size else '',
                available=variant_inventory.choices(product) if product_albums.stock_state(product) == 'available' else [])
        parts = self.write(context, catalog, scope, turn_id, version)
        if any(part['type'] != 'text' for part in parts):
            return dict(action='photo_review')
        return parts

    def ask_about_photo(self, scope, turn_id, version, catalog, current, messages, accepted, greeting,
                        stage, *, multiple=False, label=''):
        context = dict(mode='continue', customer_message=current, recent_messages=messages, recent_replies=accepted,
                       question_field='photo.reference', next_step=dict(field='photo.reference', options=[]),
                       copy_budget=145,
                       amounts={}, greeting=greeting, action_results={},
                       photo_clarification=dict(multiple=multiple, possible_product=label))
        parts = self.write(context, catalog, scope, turn_id, version)
        parts[-1].update(question_field='product', question_key='product', question_stage=stage)
        return parts

    def write(self, context, catalog, scope, turn_id, version):
        question = context['question_field']
        strict = getattr(type(self.writer), 'structured_sales', False)
        parts = None
        for attempt in range(2):
            try:
                parts = self.writer.repair(context, parts, error) if attempt else self.writer(context)
                return self.validate_parts(parts, context, catalog, strict)
            except (ValueError, DecisionUnavailable) as exc:
                failure = exc
                error = str(exc)
                if isinstance(exc, DecisionUnavailable) or getattr(type(self.writer), 'repair', None) is None:
                    break

        try:
            state = self.store.snapshot(scope)['data']
            product_ids = {line['product_id'] for line in state['cart']}
            if state.get('focus_product_id') in catalog.products:
                product_ids.add(state['focus_product_id'])
            product_ids.update(fact['product_id'] for fact in context.get('facts', []) if fact['product_id'] != '@store')
            consultation_context = dict(context, products=[catalog.products[pid] for pid in sorted(product_ids)
                                                           if pid in catalog.products])
            advice = consult(self.store, scope, turn_id, version, 'writer', self.backup,
                             consultation_context, error, parts, history_loader=self.history_loader)
            if advice is not None:
                try:
                    guided = dict(context, consultant_guidance=advice)
                    return self.validate_parts(self.writer(guided), guided, catalog, strict)
                except (ValueError, DecisionUnavailable) as exc:
                    raise BackupFailed('primary_reply_failed_after_consultation') from exc
        except BackupFailed:
            recovered = self.same_day_recovery(context) or self.greeting_recovery(context)
            if recovered:
                return recovered
            raise
        recovered = self.same_day_recovery(context)
        if recovered:
            return recovered
        if isinstance(failure, DecisionUnavailable):
            raise failure
        if question == 'product' or context.get('progress', {}).get('product_unresolved'):
            raise DecisionUnavailable('product_reply_retryable') from failure
        raise DecisionRejected('writer_invalid_after_validation') from failure

    @staticmethod
    def same_day_recovery(context):
        if only_same_day_question(context['customer_message']) and not context.get('action_results', {}).get('order_saved'):
            text = sales_answers.same_day_reply(context.get('policies', {}).get('delivery_time'))
            if text:
                return [dict(type='text', text=text)]
        return None

    @staticmethod
    def greeting_recovery(context):
        clean = checkout.normalized(context.get('customer_message', '')).strip(' .!؟?،🌷')
        if (context.get('progress', {}).get('product_unresolved') and context.get('mode') == 'continue'
                and re.fullmatch(r'(?:(?:السلام|سلام)\s+عليكم(?:\s+ورحمة\s+الله)?|مرحبا|هلا|هلو|صباح الخير|مساء الخير)', clean)):
            greeting = ('وعليكم السلام، يا هلا بيج 🌷' if clean.startswith(('السلام', 'سلام')) else
                        'صباح النور 🌷' if clean == 'صباح الخير' else
                        'مساء النور 🌷' if clean == 'مساء الخير' else 'يا هلا بيج 🌷')
            return [dict(type='text', text=greeting)]
        return None
    def validate_parts(self, parts, context, catalog, strict):
        from ..customer_copy import public_product_wording
        question = context['question_field']
        copy_guard_enabled = os.getenv('SALES_VALIDATE_COPY', '1') != '0'
        if copy_guard_enabled:
            validate_copy(parts, question, context['action_results'])
        advice = (context.get('consultant_guidance') or {}).get('guidance', '')
        if advice and len(advice.strip()) >= 24:
            text = ' '.join(part['text'] for part in parts if part['type'] == 'text')
            if ' '.join(advice.split()) in ' '.join(text.split()):
                raise ValueError('internal_consultation_copied')
        parts = [dict(part, text=natural_weight_wording(part['text'])) if part['type'] == 'text' else part
                 for part in parts]
        parts = self.remove_stale_greetings(parts, context['greeting'])
        parts = render_amounts(parts, context['amounts'])
        for part in parts:
            if part['type'] == 'text':
                part['text'] = public_product_wording(dict(reply=part['text']), list(catalog.products.values()))['reply']
            elif part.get('url') != catalog.get(part.get('product_id')).get('image_url') or not part.get('url'):
                raise ValueError('image_not_in_catalog')
        if strict:
            text = '\n\n'.join(part['text'] for part in parts if part['type'] == 'text')
            gaps = sales_answers.current_question_gaps(
                text, context['customer_message'], fit=context.get('progress', {}).get('fit'),
                delivery_window=context.get('policies', {}).get('delivery_time', ''))
            if gaps:
                raise ValueError('reply.current_question: ' + '، '.join(gaps))
            value_facts = context.get('value_facts', {})
            fabric = value_facts.get('fabric') or context.get('product_facts', {}).get('fabric')
            validate_material(text, fabric, 'fabric' in context.get('requested_fields', ()))
            clean_copy = checkout.normalized(text)
            label_availability = context.get('label_availability')
            if label_availability and not label_availability['weight_was_asked'] and re.search(
                    r'\d{2,3}\s*(?:كيلو|كغم)|شكد\s+وزن|مو\s+بنظام', clean_copy):
                raise ValueError('reply.size_label: answer the label and continue without a weight range')
            if label_availability and label_availability['weight_compatible'] is False and not re.search(
                    r'(?:ما|مو|غير)\s+(?:راح\s+)?(?:يرهم|ترهم|يناسب|تناسب)|ما\s+(?:انصح|ادري)|خارج\s+(?:مدى|مجال|نطاق)', clean_copy):
                raise ValueError('reply.size_label: acknowledge the known weight does not fit this product')
            if context.get('requested_skirt_length_cm') and re.search(r'وزن|كيلو|كغم', clean_copy):
                raise ValueError('reply.length: the customer described skirt length, not body weight')
            if context.get('weight_only'):
                reported_size = clothing_size_label(context['customer_message'])
                if reported_size and reported_size.isdigit() and not context['progress'].get('fit'):
                    # A catalog range can be quoted, but a size numeral from this turn
                    # cannot be attributed to the customer as a second body weight.
                    range_text = r'(?<!\d)\d{1,3}\s*(?:كيلو\s*)?(?:الى|الي|لحد|[-–])\s*\d{1,3}(?:\s*كيلو)?'
                    for clause in re.split(r'[.!،؛\n]', clean_copy):
                        if re.search(r'تقصد|اذا\s+قصد', clause):
                            continue
                        without_range = re.sub(range_text, '', clause)
                        if re.search(rf'(?:وزن\w*.{{0,24}}(?<!\d){reported_size}(?!\d)|'
                                     rf'(?<!\d){reported_size}\s*(?:كيلو|كغم))', without_range):
                            raise ValueError('reply.fit: the customer stated a size, not a second body weight')
                for clause in re.split(r'[.!،؛\n]', clean_copy):
                    labels = {match[0].upper() for match in product_variants.SIZE_RE.finditer(clause)}
                    positive = re.sub(r'(?:مو|غير|ليس|ما)\s+(?:متوفر|موجود|مناسب|يناسب)', '', clause)
                    if labels.difference(context['documented_size_labels']) and re.search(
                            r'متوفر|موجود|يناسب|مناسب|اعتمد|اختاري|خيار|يرهم', positive):
                        raise ValueError('reply.fit: a weight range cannot verify XL or XXL garment inventory')
                cut_notes = checkout.normalized(value_facts.get('cut', ''))
                if re.search(ROOMY_FIT, clean_copy) and not re.search(r'راهي|واسع|فضفاض', cut_notes):
                    raise ValueError('reply.fit: a roomy cut needs an explicit catalog fact')
            if context.get('stock_conflict') and re.search(
                    r'(?:هالموديل|هاللون|قياسج|نفس القطعه).{0,16}(?:متوفر|موجود)|خلصت\s+(?:كل|ال)?كمي',
                    clean_copy):
                raise ValueError('reply.stock: conflicting staff and catalog stock require checking')
            for sentence in re.split(r'[.!\n،؛]', checkout.normalized(text)):
                if re.search(r'توصيل.{0,12}(?:مو|ماكو|ليس)\s*(?:بلاش|مجاني)', sentence):
                    continue
                free_claim = re.search(r'توصيل.{0,12}(?:بلاش|مجاني|(?<!\d)0(?!\d))|(?:مجاني|بلاش)\s+(?:ال)?توصيل', sentence)
                if free_claim and context['amounts'].get('delivery_fee') != 0:
                    minimum = context.get('free_delivery_min_quantity')
                    quantity_word = r'قطعتين|قطعتان' if minimum == 2 else rf'{minimum}\s*قطع'
                    if not minimum or not re.search(quantity_word, sentence):
                        raise ValueError('reply.delivery_fee: free delivery must use the quoted fee or state the exact eligible quantity')
            availability = value_facts.get('fit_availability')
            if availability:
                clean = checkout.normalized(text)
                if not availability['roomy_cut'] and re.search(ROOMY_FIT, clean):
                    raise ValueError('reply.fit: a roomy cut needs an explicit catalog fact')
                if not availability['limited_stock'] and 'limited_stock' in sales_strategy._claim_kinds(text):
                    raise ValueError('reply.stock: a low-stock claim needs confirmed catalog evidence')
                positive = re.sub(r'(?<!\w)(?:مو|ما|غير)\s+(?:متوفر\w*|موجود\w*)', '', clean)
                if availability['stock'] != 'available' and re.search(
                        r'(?:قياس(?:ج|چ|ك)|مقاس(?:ج|چ|ك)).{0,12}(?:متوفر|موجود)', positive):
                    raise ValueError('reply.stock: distinguish a compatible weight from confirmed availability')
        if strict and all(part['type'] == 'text' for part in parts):
            clean = checkout.normalized(text)
            if variant_inventory.availability_error(text, context.get('variant_stock')):
                raise ValueError('reply.variant_stock: confirm only a color and size available together')
            contact = context.get('state', {}).get('contact', {})
            echoes_contact = (contact.get('phone') and checkout.phone_number(text) == contact['phone']
                               or contact.get('address') and checkout.normalized(contact['address']) in clean)
            if echoes_contact or re.search(
                    r'(?<!\w)(?:وصلن\w*|استلم\w*|سجلت\s+يمي|حفظت\s+يمي).{0,24}(?:ال)?(?:رقم|عنوان|بيانات)|'
                    r'(?:بياناتج|بياناتچ|بياناتك)\s+(?:كامل\w*|موجود\w*|محفوظ\w*)', clean):
                raise ValueError('reply.style: go directly to the answer or the one missing detail')
            photo = context.get('verified_photo_choice')
            color_only = photo and not re.search(r'قياس|مقاس|سايز|عمر|وزن|كيلو|كمية', context['customer_message'])
            if color_only and not context.get('variant_stock') and re.search(r'(?<!\w)(?:مو|ما|غير)\s*(?:متوفر\w*|موجود\w*)', clean
                                       ) and not context.get('business_feedback'):
                raise ValueError('reply.photo_color: this photographed catalog color is available')
            if len(reply_layout.compact(text)) > context.get('copy_budget', 520):
                raise ValueError('reply.length: shorten to the current answer and one relevant next step')
            sales_turn = context.get('mode') == 'continue' and not context['action_results'].get('order_saved')
            layout = reply_layout.plan_sales_bubbles(
                {'_human_sales_voice': True, '_short_sales_bubbles': sales_turn}, text)
            bubble_limit = 145 if sales_turn and not layout['exception'] else 270
            if layout['needs_rewrite'] or any(len(reply_layout.compact(part)) > bubble_limit for part in layout['reply_parts']):
                raise ValueError('reply.layout: remove repetition or break long prose into complete short ideas')
            parts = [dict(type='text', text=part) for part in layout['reply_parts']]
        if copy_guard_enabled:
            validate_copy(parts, question, context['action_results'])
        if strict:
            validate_next_step(parts, context['next_step'])
        return parts

    @staticmethod
    def remove_stale_greetings(parts, greeting):
        kept = []
        for part in parts:
            if part['type'] == 'text':
                text = sales_voice.trim_opening_greetings(part['text'], **greeting)
                if not text:
                    continue
                part = dict(part, text=text)
            kept.append(part)
        if not kept:
            raise ValueError('reply.relevance: answer the current message beyond repeating the greeting')
        return kept

    def save(self, scope, key, reply_key, turn_id, version, parts):
        record = self.store.apply(scope, key, version, dict(turn_id=turn_id), lambda state: dict(parts=parts))['result']
        self.store.enqueue_reply(scope, reply_key, record['state_version'], record['parts'])
        return dict(action='queued',parts=len(parts))
