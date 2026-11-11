"""Evidence-backed purchase constraints; questions are not purchase conditions."""
import re
from ..checkout import normalized,explicit_purchase,is_confirmation
from .store import digest


def explicit_cancellation(text):
    return bool(re.search(r'(?:الغي|الغاء|لغي|الغ[يه]لي)\s+(?:الطلب|الحجز|طلبي)',normalized(text)))


def constraint(text):
    value=normalized(text)
    # Politeness, fit questions and inspection questions do not suspend consent.
    if re.search(r'(?:اذا|لو)\s*(?:امكن|ممكن)|اذا\s+(?:انطي|وزن)|(?:اذا|لو)\s+(?:ما\s*)?(?:رهم|طلع|عجب)',value):
        return None
    if re.search(r'(?:اريد[هة]|لازم|بشرط|ضروري|اذا|لو).{0,30}(?:باجر|باچر|غدا|الصبح|الخميس|الجمعه|السبت)',value):
        return dict(kind='delivery_deadline',text=text,status='pending')
    if (re.search(r'(?:اذا|لو|بشرط|بس\s+ب).{0,35}(?:ب\s*\d|بعشرين|بثلاثين)',value)
            and re.search(r'احجز|ثبت|اشتري|اطلب|اريد',value)):
        return dict(kind='price',text=text,status='pending')
    if re.search(r'بشرط|ما\s*احجز\s*الا|ما\s*اشتري\s*الا',value):
        return dict(kind='other',text=text,status='pending')
    return None


def purchase_selected(text):
    value=normalized(text)
    if re.search(r'ما\s*اريد|ماريد|لا\s*(?:تحجز|تثبت)|الغ[يى]',value):return False
    return explicit_purchase(text) or bool(re.search(r'(?<!\w)اريد(?:ه|ها|هم|هن)?(?!\w)',value)) and not bool(
        re.search(r'اريد\s+(?:اعرف|استفسر|اسال|اشوف|خصم|تخفيض|تخيض|سعر)|شلون|بشكد|شكد\s*(?:سعر|واص)',value))


def apply_intent(state,plan,event_id,evidence,quote=None,last_reply=None):
    from .negotiation import pushback, request
    text=evidence['payload'].get('text','');value=normalized(text)
    bargaining = bool(state.get('price_request') and pushback(text))
    reply = last_reply or {}
    confirmed_purchase = bool(is_confirmation(text) and state['cart'] and (
        state.get('purchase_event') or
        reply.get('question_field') == 'purchase_intent' and reply.get('question_stage') == 'selection'
        and reply.get('cart_key') == digest(state['cart'])))
    if (plan['intent']=='withdraw' and not bargaining and not confirmed_purchase) or re.search(r'ما\s*اريد|ماريد|لا\s*(?:تحجز|تثبت)|الغ[يى]',value):
        state.update(purchase_event=None,conditions=[])
        state.pop('checkout_prompt',None)
        state.pop('price_request',None)
        return True
    # Remove legacy arbitrary "if" barriers. Real typed constraints remain.
    state['conditions']=[c for c in state['conditions'] if c.get('kind') and c.get('status')=='pending']
    pending=constraint(text)
    price_request=request(text)
    if price_request and (state['cart'] or state.get('purchase_event')):
        if not price_request.get('discretionary') or not state.get('price_request'):
            state['price_request']=price_request
            pending=dict(kind='price',text=text,status='pending')
    if pending:
        pending['event_id']=event_id
        state['conditions']=[c for c in state['conditions'] if c['kind']!=pending['kind']]+[pending]
    if re.search(r'مو\s*شرط|ما\s*يحتاج|عادي\s+اي\s+وقت',value):
        state['conditions']=[c for c in state['conditions'] if c['kind']!='delivery_deadline']
    fields=plan.get('contact') or {}
    contextual=state.get('checkout_prompt') and bool(state['cart']) and any(
        state['contact'].get(f) and f in fields for f in ('phone','address'))
    contacts=state.get('contact_events',{})
    # Two independent delivery declarations in the current product conversation
    # also express checkout; a single price enquiry with contacts does not.
    declared=(bool(state['cart']) and all(state['contact'].get(f) for f in ('phone','address','province'))
              and contacts.get('phone') and contacts.get('address') and contacts['phone']!=contacts['address'])
    if purchase_selected(text) or contextual or declared or confirmed_purchase:
        state['purchase_event']=event_id
    return False
