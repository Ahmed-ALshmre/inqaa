"""Ground the writer in the committed state and authoritative product facts."""
from .cart import CartService
from .store import Conflict
from .contracts import FACT_FIELDS
from .contracts import DecisionRejected
from .copy_guard import validate_copy
from .facts import resolve as resolve_fact,requested_fields,POLICY_FIELDS,alternatives,choice_alternatives
from .recovery import safe_text
import json


class Responses:
    def __init__(self, store, catalog_loader, writer,policy_loader=None):
        self.store, self.catalog_loader, self.writer = store, catalog_loader, writer
        self.policy_loader=policy_loader or (lambda sid:{})

    def prepare(self, scope, turn_id, outcome):
        if outcome['action']=='cancelled':
            self.store.enqueue_reply(scope,f'cancellation:{turn_id}',outcome['state_version'],[dict(type='text',text='تم إلغاء الطلب.')])
            return dict(action='queued',parts=1)
        if outcome['action'] != 'continue':
            # Order confirmations use durable order tasks, never model rewriting.
            return dict(action=outcome['action'])
        key = f'writer-v3:{turn_id}'
        saved = self.store.db.execute('SELECT result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                     (scope.key,key)).fetchone()
        if saved:
            record=json.loads(saved[0])
            self.store.enqueue_reply(scope,f'answer:{turn_id}',record['state_version'],record['parts'])
            return dict(action='queued',parts=len(record['parts']))
        snap = self.store.snapshot(scope)
        if snap['version'] != outcome['state_version']:
            raise Conflict('response_superseded')
        catalog = self.catalog_loader(scope.store)
        policy=self.policy_loader(scope.store)
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
        recent=self.store.db.execute("SELECT actor,payload FROM sales_v2_events WHERE scope=? AND kind='message' ORDER BY id DESC LIMIT 15",
                                     (scope.key,)).fetchall()
        messages=[dict(actor=row[0],**json.loads(row[1])) for row in reversed(recent)]
        current=messages[-1].get('text','') if messages else ''
        focus=snap['data'].get('focus_product_id')
        if not focus and len({r['product_id'] for r in snap['data']['cart']})==1:focus=snap['data']['cart'][0]['product_id']
        requested=requested_fields(current)
        references=list(plan.get('facts',[]))
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
        question_records=[json.loads(row[0]) for row in self.store.db.execute(
            "SELECT payload FROM sales_v2_outbox WHERE scope=? AND status='accepted' AND instr(payload,'question_field')>0 ORDER BY id DESC",(scope.key,)).fetchall()]
        # A known product can have a sales next-step without already being in a cart.
        if not focus and not snap['data']['cart']:
            attempts=sum(p.get('question_field')=='product' for p in accepted)
            if attempts>=1:return dict(action='product_unresolved')
            question='product'
        if question is None and not readiness['conditions']:
            question=next((f for f in readiness['missing_fields'] if f.startswith('cart.') and f.endswith(('.color','.size','.age_months','.weight_kg'))),None)
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
        feedback=outcome.get('business_feedback') or next((c for c in readiness['conditions'] if c.get('kind')=='choice'),None)
        recommendations=choice_alternatives(catalog,feedback) if feedback else alternatives(catalog,focus,current)
        if feedback:
            question=('alternative.'+feedback['product_id']) if recommendations['matches'] else None
        if not feedback and any(c.get('kind')=='price' for c in readiness['conditions']):
            question='price.accept' if (outcome.get('quote') or {}).get('total') is not None else 'quote.province'
        def question_key(field):
            if field and field.startswith('cart.'):
                row=next((r for r in snap['data']['cart'] if r['line_id']==field.split('.')[1]),None)
                if row:return 'product.'+row['product_id']+'.'+field.rsplit('.',1)[-1]
            return field
        if question and question not in outcome.get('invalid_fields',[]):
            previous=next((p for p in question_records if (p.get('question_key') or question_key(p.get('question_field')))==question_key(question)),None)
            if previous and (question!='price.accept' or previous.get('quote_total')==(outcome.get('quote') or {}).get('total')):question=None
        context=dict(state=snap['data'], facts=allowed_facts,
                                  question_field=question, goal=plan.get('goal',''),
                                  policies=policy,quote=outcome.get('quote'),business_feedback=feedback,
                                  recommendations=recommendations,
                                  action_results=dict(order_saved=bool(snap['data'].get('order_id')) and snap['data'].get('order_status')!='cancelled',
                                                      carrier_contacted=False,delivery_time=policy.get('delivery_time')),
                                  product_facts=({f:resolve_fact(catalog,focus,f,policy)['value'] for f in FACT_FIELDS
                                                 if f not in POLICY_FIELDS and resolve_fact(catalog,focus,f,policy)} if focus else {}),
                                  recent_messages=messages,recent_replies=accepted,
                                  instructions='صوت بنت عراقية ودود ومختصر؛ أجيبي السؤال ثم خطوة مناسبة. لا ترحيب متكرر، لا ادعاء حجز، لا سؤال عن معلوم، ولا اختراع ندرة أو عرض أو حقيقة.')
        if feedback:context['instructions']+=' الاختيار المطلوب يبقى نفسه: لا تطلبين عمرًا أو قياسًا أصغر ولا تفترضين طفلًا ثانيًا؛ اعرضي فقط البدائل المتوافقة الموثقة في recommendations.'
        if focus:
            from ..product_questions import guidance_for
            context['reply_guidance']=guidance_for(catalog.get(focus),current)
        parts=None
        if question=='price.accept':
            total=outcome['quote']['total']
            parts=[dict(type='text',text=f'عيوني أگدر أحسبلج المجموع ويا التوصيل {total:,} دينار. يناسبج حتى أكمللج الحجز؟')]
        for attempt in range(2):
            try:
                if attempt:parts=self.writer.repair(context,parts,error)
                elif parts is None:parts=self.writer(context)
                from ..customer_copy import public_product_wording
                if isinstance(parts,list):
                    parts=[dict(p,text=public_product_wording(dict(reply=p['text']),list(catalog.products.values()))['reply'])
                           if isinstance(p,dict) and p.get('type')=='text' and isinstance(p.get('text'),str) else p for p in parts]
                validate_copy(parts,question,context['action_results'])
                break
            except ValueError as exc:
                if getattr(type(self.writer),'repair',None) is None:raise
                if attempt:
                    parts=safe_text(context)
                    validate_copy(parts,question,context['action_results'])
                    break
                error=str(exc)
        if not isinstance(parts,list) or not 1<=len(parts)<=5:
            raise ValueError('invalid_reply_parts')
        for part in parts:
            if not isinstance(part,dict) or part.get('type') not in {'text','image'}:
                raise ValueError('invalid_reply_part')
            if part['type']=='text':
                if not isinstance(part.get('text'),str) or not part['text'].strip():
                    raise ValueError('empty_reply_text')
            else:
                product = catalog.get(part.get('product_id'))
                urls = {product.get('image_url')}
                # More complex album schemas require the existing album adapter.
                if not part.get('url') or part['url'] not in urls:
                    raise Conflict('image_not_in_catalog')
        if question in {'contact.phone','contact.address','contact.province','contact.details'}:
            text_parts=[p for p in parts if p['type']=='text']
            if text_parts:
                text_parts[-1]['purpose']='checkout_contacts'
        if question:
            text_parts=[p for p in parts if p['type']=='text']
            if text_parts:text_parts[-1].update(question_field=question,question_key=question_key(question))
        if question=='price.accept':text_parts[-1]['quote_total']=outcome['quote']['total']
        record=self.store.apply(scope,key,snap['version'],dict(turn_id=turn_id),lambda state:dict(parts=parts))['result']
        self.store.enqueue_reply(scope,f'answer:{turn_id}',record['state_version'],parts)
        return dict(action='queued',parts=len(parts))
