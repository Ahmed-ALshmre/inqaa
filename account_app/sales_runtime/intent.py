"""Evidence-backed purchase constraints; questions are not purchase conditions."""
import re
from .. import product_variants
from ..checkout import normalized,explicit_purchase,is_confirmation
from .store import digest


def direct_color(text):
    """A single unambiguous choice, possibly with an explicit clothing size."""
    value = normalized(text).strip(' .،!')
    value = re.sub(r'^(?:اريد(?:ها|ه)?|خلي(?:ها|ه|لي)?|اخذ|ثبتي(?:لي)?|احجزي(?:لي)?)\s+', '', value)
    value = re.sub(r'^(?:هذا|هاذا|هاي|هذي|نفس|(?:ال)?لون)\s+', '', value)
    value = re.sub(r'\s+(?:(?:اللي\s+)?بالصور[ةه]|اللي\s+دزيت(?:ها|ه))$', '', value)
    value = re.sub(r'\s+(?:قياس|مقاس|سايز)\s+[^؟?،,\n]+$', '', value)
    return product_variants.single_color(value)


def color_availability(text):
    """Keep a named-color stock question out of cart changes."""
    value = normalized(text).strip(' .،!؟?')
    match = re.fullmatch(r'(?:اكو|عندكم|متوفر|موجود)\s+(.+?)|(.+?)\s+(?:متوفر[هة]?|موجود[هة]?)', value)
    if match:
        return direct_color(next(part for part in match.groups() if part is not None))
    return product_variants.single_color(value) if text.rstrip().endswith(('؟', '?')) else ''


def referred_photo_color(text, available_color):
    """Treat a nearby name as a reference only when this turn has photo evidence."""
    if not available_color:
        return ''
    correction = color_correction(text)
    name = direct_color(text) or color_availability(text) or (correction and correction[0])
    if name and (name == available_color or product_variants.nearby_color(name, [available_color])):
        return available_color
    value = normalized(text).strip(' .،!')
    if re.fullmatch(r'(?:(?:اريد|اخذ|خلي|اقصد|ثبتيلي|احجزيلي)\s+)?(?:هذا|هاذا|هاي|هذي)(?:\s+(?:بالصور[ةه]|اللون))?', value):
        return available_color
    return ''


def direct_clothing_size(text):
    value = normalized(product_variants.normalize_size_words(text)).strip(' .،!')
    match = re.fullmatch(r'(?:(?:لا|اقصد|اني|انا|اريد|البس|خلي)\s+)?'
                         r'(?:قياس(?:ي)?|مقاس(?:ي)?|سايز)\s*[:=]?\s*(\d{2,3}(?:\.5)?|[2-6]xl|xl|xs|s|m|l|xxl)', value)
    return match[1].upper() if match else ''


def color_correction(text):
    value = normalized(text).strip(' .!،')
    if re.match(r'^(?:لا\s*[,،]?\s*)?(?:اريد|بدلي|خلي\w*)\s+', value):
        return product_variants.color_replacement(value)
    match = re.fullmatch(r'(?:لا[،,]\s*)?(?:ما\s*اريد|لا\s*اريد)\s+(.+?)[،,\s]+(?:اريد|خلي\w*|بدلي)\s+(.+)', value)
    if match:
        old, new = (product_variants.single_color(part.strip()) for part in match.groups())
        if old and new and old != new:
            return new, old
    return None


def explicit_cancellation(text):
    value = normalized(text)
    return bool(not re.search(r'(?:لا|ما|مو)\s+(?:الغي|تلغي|تلغين)|(?:اذا|لو).{0,45}(?:الغي|الغاء)', value)
                and re.search(r'(?:الغي|الغاء|لغي|الغ[يه]لي)\s+(?:الطلب|الحجز|طلبي|طلب)(?!\w)', value))


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
    if (re.search(r'لا\s*(?:تحجز|تثبت)|الغ[يى]', value)
            or re.search(r'ما\s*اريد|لا\s*اريد', value) and not color_correction(value)):
        return False
    return explicit_purchase(text) or bool(re.search(r'(?<!\w)اريد(?:ه|ها|هم|هن)?(?!\w)',value)) and not bool(
        re.search(r'اريد\s+(?:اعرف|استفسر|اسال|اشوف|خصم|تخفيض|تخيض|(?:ال)?(?:سعر|صور|فيديو|تفاصيل|قياسات|الوان))'
                  r'|شلون|بشكد|شكد\s*(?:سعر|واص)',value))


def purchase_declined(text):
    value = normalized(text)
    return bool(re.search(r'لا\s*(?:تحجز|تثبت)|الغ[يى]', value) or
                re.search(r'ما\s*اريد|لا\s*اريد', value) and not color_correction(value))


def apply_intent(state,plan,event_id,evidence,quote=None,last_reply=None):
    from .negotiation import pushback, request
    text=evidence['payload'].get('text','');value=normalized(text)
    bargaining = bool(state.get('price_request') and pushback(text))
    reply = last_reply or {}
    confirmed_purchase = bool(is_confirmation(text) and state['cart'] and (
        state.get('purchase_event') or
        reply.get('question_field') == 'purchase_intent'
        and reply.get('cart_key') == digest(state['cart'])))
    corrected = color_correction(value)
    if purchase_declined(value) or not corrected and plan['intent']=='withdraw' and not bargaining and not confirmed_purchase:
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
    contacts=state.get('contact_events',{})
    contextual=state.get('checkout_prompt') and bool(state['cart']) and any(
        state['contact'].get(f) and contacts.get(f) == event_id for f in ('phone','address'))
    # Two independent delivery declarations in the current product conversation
    # also express checkout; a single price enquiry with contacts does not.
    declared=(bool(state['cart']) and all(state['contact'].get(f) for f in ('phone','address','province'))
              and contacts.get('phone') and contacts.get('address') and contacts['phone']!=contacts['address']
              and event_id in (contacts['phone'],contacts['address'],contacts.get('province')))
    if purchase_selected(text) or contextual or declared or confirmed_purchase:
        state['purchase_event']=event_id
    return False
