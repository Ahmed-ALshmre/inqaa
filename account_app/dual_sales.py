"""Experimental sales-agent policy; no production routing or network side effects.

The caller owns durable cart/offer state and actual tool execution. Amounts here
are integer IQD. The discount cap is per order, never per turn or per item.
"""
import hashlib
import json
import re

from .pricing import quote
from .dual_sales_strategy import PLANNER_PROMPT, WRITER_PROMPT, MAX_REPLY_PARTS, MAX_PART_CHARACTERS

MAX_NEGOTIATED_DISCOUNT = 2000


def _dinars(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('Expected nonnegative integer dinars')
    return value


def negotiated_quote(items, catalog, delivery_fee, *, requested_discount=0,
                     previous_discount=0, customer_requested=False):
    """Price canonically, then apply a single authorized order-level reduction.

    A fresh offer has to be accepted by the customer before booking. Returning a
    quote never implies agreement, reservation, fulfillment, or persistence.
    Existing concessions survive variant edits while the purchase stays multi-
    item, and disappear when it becomes one item. Delivery is never discounted.
    """
    request = _dinars(requested_discount)
    previous = _dinars(previous_discount)
    fee = _dinars(delivery_fee)
    if not isinstance(customer_requested, bool):
        raise ValueError('customer_requested must be explicit')
    count = 0
    for item in items:
        quantity = item.get('quantity', 1)
        if isinstance(quantity, bool) or not isinstance(quantity, int) or not 1 <= quantity <= 20:
            raise ValueError('Invalid quantity')
        count += quantity
    base = quote(items, catalog, fee)
    eligible = count > 1
    proposed = max(previous, request if customer_requested else 0) if eligible else 0
    discount = min(proposed, MAX_NEGOTIATED_DISCOUNT, base['product_total'])
    shipping = base['delivery_fee']
    total = base['product_total'] - discount + shipping
    grouped = {}
    for row in items:
        identity = tuple(row.get(k) for k in ('product_id','color','size'))
        grouped[identity] = grouped.get(identity,0) + row.get('quantity',1)
    basis = {'items': sorted(
        (dict(product_id=key[0],color=key[1],size=key[2],quantity=count)
         for key,count in grouped.items()), key=lambda row: json.dumps(row, sort_keys=True)),
        'product_total': base['product_total'], 'delivery_fee': shipping,
        'negotiated_discount': discount, 'total_amount': total}
    fingerprint = hashlib.sha256(json.dumps(basis, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    quantities={}
    for row in items:
        quantities[row['product_id']]=quantities.get(row['product_id'],0)+row.get('quantity',1)
    money_basis={k:basis[k] for k in ('product_total','delivery_fee','negotiated_discount','total_amount')}
    money_basis['product_quantities']=quantities
    monetary_fingerprint=hashlib.sha256(json.dumps(money_basis,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    return dict(base, negotiated_discount=discount, total_amount=total,
                discount_eligible=eligible, quote_fingerprint=fingerprint,
                monetary_fingerprint=monetary_fingerprint)


def offer_matches(accepted_fingerprint, current_quote):
    """Variants, quantity, catalog price, delivery, and discount all matter."""
    return bool(accepted_fingerprint and accepted_fingerprint == current_quote['quote_fingerprint'])


def controlled_reply(actions, requested_silence=False):
    """Tool outcomes own receipts and silent handoff, not the language model."""
    for action in actions:
        result = action['result']
        if (action['name'] == 'commit_order' and result.get('ok')
                and result.get('action') == 'created' and result.get('confirmation_text')):
            return dict(mode='owner_confirmation_bypass',parts=[result['confirmation_text']])
    if requested_silence or any(a['name']=='handoff_to_staff' and a['result'].get('ok') for a in actions):
        return dict(mode='silent_handoff',parts=[])
    return dict(mode='writer',parts=None)






def validate_parts(parts, *, message_count=1, next_question=None, effects=(),
                   allowed_numbers=(), known_contact=(), previous_replies=()):
    """Reject a candidate before delivery; the caller may request one rewrite.

    This is a scoped commitment/numeric guard, not a proof of semantic truth or
    dialect fluency. Raw rejected candidates must remain available for audit.
    """
    problems = []
    if not isinstance(parts, list) or not parts or any(not isinstance(p, str) or not p.strip() for p in parts):
        return ['invalid_parts']
    if isinstance(message_count,bool) or not isinstance(message_count,int) or not 1<=message_count<=MAX_REPLY_PARTS or len(parts) != message_count:
        problems.append('message_count')
    if any(len(p)>MAX_PART_CHARACTERS for p in parts):
        problems.append('long_unsplit_part')
    text = '\n'.join(parts)
    clean = text.translate(str.maketrans('٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹', '01234567890123456789'))
    clean = clean.replace('أ', 'ا').replace('إ', 'ا').replace('ى', 'ي').replace('ة', 'ه')
    if re.search(r'صار\s+تدللين', clean):
        problems.append('unnatural_phrase')
    if re.search(r'(?<!\w)بيتد(?:ج|ك|كم|كن)?(?!\w)|ارشح\s+الج(?!\w)',clean):
        problems.append('observed_dialect_typo')
    if re.search(r'(?<!\w)(?:درهم|دراهم|ريال|ريالات)(?!\w)',clean):
        problems.append('foreign_currency')
    if re.search(r'submit_reply|commit_order|update_cart|negotiate_price|\bP\d{3}\b', text):
        problems.append('internal_details')
    claims = {
        'fulfilled': r'(?<!\w)(?:جاهز(?:ه|ات|ين)?|انشحن|شحنا|شحنته|بالطريق)(?!\w)|تم\s+(?:التجهيز|الشحن)|(?<!\w)(?:جهزت|غلفت|مجهز)\w*',
        'cancelled': r'تم\s+(?:الالغاء|الغاء)|لغيت(?:ه|ها|لج|لك)',
        'saved': r'تم\s+(?:تثبيت|حجز|تسجيل)\s*(?:الطلب|طلبج|طلبك|الحجز)|(?:طلبج|طلبك|الطلب)\s+(?:مثبت|تثبت|ثبت)|ثبت(?:ت|تلج|تلك)|حجزت(?:لج|لك)',
        'updated': r'عدل(?:ت|تلج|تلك)|بدلت(?:ه|ها|لج|لك)',
        'images': r'(?:دزيت|ارسلت|رسلت)(?:لج|لك|ها|هن|هم)?\s*(?:الصور|صور)|(?:هاي|هذن|هذي)\s+صور',
    }
    for effect, pattern in claims.items():
        matches = list(re.finditer(pattern, clean))
        asserted = any(not re.search(r'(?:ما|مو|لم)\s*$', clean[max(0,m.start()-12):m.start()]) for m in matches)
        if effect not in effects and asserted:
            problems.append('unsupported_' + effect)
    numbers = {x.replace(',','').replace('٬','') for x in re.findall(r'\d+(?:[.,٬]\d+)*', clean)}
    permitted = {str(value).replace(',','').replace('٬','') for value in allowed_numbers}
    if numbers - permitted:
        problems.append('unapproved_number')
    if next_question is None and re.search(r'[؟?]|(?<!\w)(?:دزيلي|دزلي|ارسلي|شكد|شنو|يالون)(?!\w)|(?:^|[.!]\s*)تحبين\s+(?:اثبت|احجز|نثبت|تشوفين|تاخذين|تختارين)', clean):
        problems.append('unplanned_question')
    if len(re.findall(r'[؟?]', clean)) > 1:
        problems.append('multiple_questions')
    if len(parts)>1 and any(re.search(r'[؟?]|(?<!\w)(?:دزيلي|ارسلي|يالون|شكد|شنو)(?!\w)',p) for p in parts[:-1]):
        problems.append('question_before_final_part')
    for field, pattern in (
        ('phone', r'(?:دزي|ارسلي|شنو|ممكن).{0,45}(?:رقم|موبايل)'),
        ('address', r'(?:دزي|ارسلي|شنو|ممكن).{0,45}(?:عنوان|محافظه)'),
    ):
        correction = bool(next_question and re.search(r'جديد|تصحيح|تغيير', next_question))
        if field in known_contact and not correction and re.search(pattern, clean):
            problems.append('repeated_' + field)
    if re.search(r'السعر\s+ثابت|ما\s*(?:نكدر|اكدر|اگدر|يرهم|يصير).{0,20}(?:ننزل|انزل|نخصم|اقل)|يحتاج\s+موافقه|ما\s+كدرت\s+اسجل.{0,10}خصم|الخصم\s+ما\s+كدرت', clean):
        problems.append('dry_negotiation')
    if re.search(r'يا\s*ريت\s*بيدي', clean):
        problems.append('defensive_negotiation')
    if previous_replies:
        prior = previous_replies[-1].strip()
        if prior and text.strip() == prior:
            problems.append('repeated_reply')
    return list(dict.fromkeys(problems))
