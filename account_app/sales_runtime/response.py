"""Ground the writer in the committed state and authoritative product facts."""
from .cart import CartService
from .store import Conflict
import json


class Responses:
    def __init__(self, store, catalog_loader, writer):
        self.store, self.catalog_loader, self.writer = store, catalog_loader, writer

    def prepare(self, scope, turn_id, outcome):
        if outcome['action'] != 'continue':
            # Order confirmations use durable order tasks, never model rewriting.
            return dict(action=outcome['action'])
        key = f'writer:{turn_id}'
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
        readiness = CartService(self.store,catalog).readiness(scope)
        plan = outcome.get('answer_plan', {})
        question = plan.get('question_field')
        # A writer cannot request a field that is already known.
        if question and question not in readiness['missing_fields']:
            question = None
        allowed_facts = []
        for fact in plan.get('facts', []):
            if not isinstance(fact,dict) or set(fact)-{'product_id','field'}:
                raise ValueError('invalid_fact_reference')
            product=catalog.get(fact['product_id'])
            field=fact['field']
            if field not in {'fabric','description','notes','colors','sizes','price','offer','product_type'}:
                raise ValueError('unsupported_fact_field')
            if not product.get(field):
                return dict(action='missing_fact',product_id=fact['product_id'],field=field)
            allowed_facts.append(dict(product_id=fact['product_id'],field=field,value=product[field]))
        parts = self.writer(dict(state=snap['data'], facts=allowed_facts,
                                  question_field=question, goal=plan.get('goal',''),
                                  instructions='صوت بنت عراقية ودود ومختصر؛ أجيبي السؤال ثم خطوة مناسبة. لا ترحيب متكرر، لا ادعاء حجز، لا سؤال عن معلوم، ولا اختراع ندرة أو عرض أو حقيقة.'))
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
        if question in {'contact.phone','contact.address','contact.province'}:
            text_parts=[p for p in parts if p['type']=='text']
            if text_parts:
                text_parts[-1]['purpose']='checkout_contacts'
        record=self.store.apply(scope,key,snap['version'],dict(turn_id=turn_id),lambda state:dict(parts=parts))['result']
        self.store.enqueue_reply(scope,f'answer:{turn_id}',record['state_version'],parts)
        return dict(action='queued',parts=len(parts))
