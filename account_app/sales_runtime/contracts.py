"""Shared wire contract, local validation, and server-owned cart identities."""
from copy import deepcopy
import math
from .store import Conflict
from .facts import PRODUCT_FACT_FIELDS,POLICY_FIELDS,product_fact


class DecisionRejected(Conflict):
    """A bounded decision/wording attempt could not produce a usable proposal."""


class DecisionUnavailable(RuntimeError):
    """Provider auth, billing, transport or service availability failure."""


class BusinessChoice(Conflict):
    """Normal unavailable selection; must become a sales response, not an incident."""
    def __init__(self,message,*,product_id,field,available,requested=None):
        super().__init__(message)
        self.feedback=dict(product_id=product_id,field=field,available=available,reason=message)
        if requested is not None:self.feedback['requested']=requested


class ServiceRequired(Conflict):
    def __init__(self,kind):
        super().__init__(kind)
        self.kind=kind

FACT_FIELDS = PRODUCT_FACT_FIELDS+POLICY_FIELDS
TEXT_FIELDS = ('product_id','color','size','size_system','wearer_ref')
NUMBER_FIELDS = ('quantity','age_months','weight_kg')


def message_context(payload):
    """Expose conversational evidence to the text model without attachment URLs."""
    return {key: payload[key] for key in ('text', 'media_kind') if key in payload}


def obj(properties):
    return dict(type='object',properties=properties,required=list(properties),additionalProperties=False)


def nullable(kind):
    return dict(type=[kind,'null'])


DECISION_SCHEMA = obj({
    'intent':dict(type='string',enum=['continue','buy','withdraw','conditional']),
    'changes':dict(type='array',items=obj({
        'line_ref':nullable('string'), 'new_line':dict(type='boolean'),
        'remove':dict(type='boolean'),
        'values':obj({**{f:nullable('string') for f in TEXT_FIELDS},
                      'quantity':nullable('integer'),'age_months':nullable('integer'),
                      'weight_kg':nullable('number')})})),
    'contact':obj({'phone':nullable('string'),'address':nullable('string')}),
    'answer':obj({'facts':dict(type='array',items=obj({'product_id':dict(type='string'),
                  'field':dict(type='string',enum=list(FACT_FIELDS))})),
                  'question_field':nullable('string'),'goal':dict(type='string'),
                  'catalog_photos':nullable('string')})})
WRITER_SCHEMA=obj({'parts':dict(type='array',items=obj({'type':dict(type='string',enum=['text']),
                                                       'text':dict(type='string')}))})


def validate_plan(plan):
    """Accept sparse local commands and nullable native output; reject coercions."""
    if not isinstance(plan,dict) or set(plan)-{'intent','changes','contact','answer'}:
        raise ValueError('plan: expected intent,changes,contact,answer only')
    if plan.get('intent') not in {'continue','buy','withdraw','conditional'}:
        raise ValueError('intent: use continue,buy,withdraw,conditional')
    changes=plan.get('changes',[])
    if not isinstance(changes,list) or len(changes)>20:
        raise ValueError('changes: expected at most 20 commands')
    for index,change in enumerate(changes):
        path=f'changes[{index}]'
        # line_id is read only for compatibility with pre-upgrade recorded plans;
        # the native schema never asks a model to create one.
        if not isinstance(change,dict) or set(change)-{'line_ref','line_id','new_line','values','remove'}:
            raise ValueError(path+': unexpected command fields')
        if change.get('line_ref') is not None and not isinstance(change['line_ref'],str):
            raise ValueError(path+'.line_ref: copy an existing string reference or use null')
        for field in ('new_line','remove'):
            if field in change and type(change[field]) is not bool:
                raise ValueError(path+'.'+field+': expected boolean')
        values=change.get('values',{})
        if not isinstance(values,dict) or set(values)-set(TEXT_FIELDS+NUMBER_FIELDS):
            raise ValueError(path+'.values: only catalog selection and measurement fields are allowed')
        for field,value in values.items():
            if value is None:continue
            if field in TEXT_FIELDS and not isinstance(value,str):
                raise ValueError(path+'.values.'+field+': expected text, not a number')
            if field in NUMBER_FIELDS:
                if type(value) not in {int,float} or not math.isfinite(value):
                    raise ValueError(path+'.values.'+field+': expected finite number')
                if field in {'quantity','age_months'} and type(value) is not int:
                    raise ValueError(path+'.values.'+field+': expected integer')
                if field=='quantity' and not 1<=value<=20:
                    raise ValueError(path+'.values.quantity: expected 1..20')
                if field=='age_months' and value<0 or field=='weight_kg' and value<=0:
                    raise ValueError(path+'.values.'+field+': invalid measurement')
    contact=plan.get('contact',{})
    if not isinstance(contact,dict) or set(contact)-{'phone','address'}:
        raise ValueError('contact: expected phone,address only')
    if any(value is not None and not isinstance(value,str) for value in contact.values()):
        raise ValueError('contact: phone and address must be source text')
    answer=plan.get('answer',{})
    if not isinstance(answer,dict) or set(answer)-{'facts','question_field','goal','catalog_photos'}:
        raise ValueError('answer: unexpected answer field')
    if answer.get('question_field') is not None and not isinstance(answer['question_field'],str):
        raise ValueError('answer.question_field: expected text or null')
    if 'goal' in answer and not isinstance(answer['goal'],str):
        raise ValueError('answer.goal: expected text')
    photo_evidence = answer.get('catalog_photos')
    if photo_evidence is not None and (not isinstance(photo_evidence,str) or not 1<=len(photo_evidence.strip())<=240):
        raise ValueError('answer.catalog_photos: use a short exact quote from the current message or null')
    facts=answer.get('facts',[])
    if not isinstance(facts,list) or len(facts)>20:
        raise ValueError('answer.facts: expected at most 20 field references')
    for fact in facts:
        if (not isinstance(fact,dict) or set(fact)!={'product_id','field'} or
                not isinstance(fact['product_id'],str) or fact['field'] not in FACT_FIELDS):
            raise ValueError('answer.facts: use {product_id: text, field: catalog field}; never raw fact values')


def clean_plan(plan):
    validate_plan(plan)
    result=deepcopy(plan)
    result['contact']={k:v for k,v in result.get('contact',{}).items() if v is not None}
    for change in result.get('changes',[]):
        change['values']={k:v for k,v in change.get('values',{}).items() if v is not None}
    return result


def catalog_fact(product,field):
    return product_fact(product,field)
