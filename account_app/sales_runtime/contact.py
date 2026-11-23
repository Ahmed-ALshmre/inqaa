"""Extract delivery details from customer evidence before calling a model."""
import re

from .. import checkout
from .. import product_variants
from .store import Conflict


INSPECTION_QUESTION = re.compile(
    r'(?:و?اذا|لو|في حال)\s+.{0,55}(?:ارجع\w*|يرجع\w*|ترجع\w*|استبدل\w*)')
LOCAL_MOBILE = re.compile(r'(?:(?:رقمي|رقم(?: الموبايل| الهاتف)?|موبايل)\s*[:،,-]?\s*)?(7(?:[ \t-]*\d){9})')
TOTAL_QUESTION = (r'(?:و\s*)?(?:شكد|كم)\s+(?:يطلع\s+)?(?:ال)?(?:حساب|مجموع)'
                  r'(?:\s+(?:مع|ويا|وي)\s+(?:ال)?توصيل)?')
PHONE_LABEL = re.compile(r'(?<!\w)(?:رقمي|رقم(?:\s+(?:الهاتف|الموبايل|التلفون))?|موبايل)\s*[:،,-]?\s*'
                         rf'(?={checkout.PHONE.pattern})')
NOTE = re.compile(
    r'(?:^|(?<=[\n،,؛]))\s*(?:رجاء[ااً]?\s+)?'
    r'(?:(?:اكتبوا|اكتبي|اكتب|سجلوا|سجلي|حطوا|حطي)\s+)?ملاحظ[هة]'
    r'(?:\s+(?:على|بال|في)\s*(?:ال)?(?:وصل|طلب))?\s*[:：،,-]?\s*(?P<note>[^\n]{1,400})')


def delivery_note(text):
    """Extract a courier preference without turning a purchase condition into a note."""
    match = NOTE.search(text)
    if not match:
        return text, ''
    note = match['note'].strip(' .،')
    clean = checkout.normalized(note)
    if (not re.search(r'توصيل|استلام|مندوب|اتصل|اتصال', clean)
            or re.search(r'(?<!\w)(?:بشرط|لازم|ضروري|اذا|لو|الا)(?!\w)|[؟?]', clean)
            or checkout.phone_number(note)):
        return text, ''
    return text[:match.start()].rstrip(' ،,؛') + text[match.end():], note


def checkout_inspection_question(text):
    """A question about inspecting the parcel before paying."""
    clean = checkout.normalized(text)
    return bool(INSPECTION_QUESTION.search(clean)
                and not re.search(r'استلمت|وصلتني|رجعته|الغ[يى]|بشرط|ثبتي|احجزي|اشتري|اطلب', clean))


def declined_phone(text):
    return bool(re.search(r'رقم(?:كم| المندوب| المتجر)|مو رقمي|ما اريد استخدم هالرقم', checkout.normalized(text)))


def stated_checkout_total(line):
    text = checkout.normalized(line).strip(' .،,:؟?')
    match = re.fullmatch(TOTAL_QUESTION + r'\s+(\d{1,7})(?:\s*(الف|دينار))?', text)
    if not match:
        return None
    amount = int(match[1])
    return amount * 1000 if match[2] == 'الف' or match[2] is None and amount < 1000 else amount


def checkout_followup(line, *, quoted_total=None):
    """A checkout question or acknowledgement that does not change the selection."""
    clean = checkout.normalized(line).strip(' .،,:؟?')
    stated = stated_checkout_total(clean)
    if stated is not None:
        return stated == quoted_total
    return bool(re.fullmatch(
        r'(?:(?:دزيت|ارسلت|بعثت)\s+(?:(?:ال)?رقم\s+و\s*(?:ال)?عنوان|(?:ال)?بيانات)'
        r'|' + TOTAL_QUESTION +
        r'|(?:و\s*)?(?:باجر|باچر|اليوم|غدا|شوكت|اشوكت|متى|كم\s+يوم|شكد\s+يوم)\s+'
        r'(?:يوصل(?:ني|نا)?|توصل|تجي|يجي)(?:\s+(?:الطلب|المندوب))?'
        r'|(?:يوصل|توصل)\s+(?:باجر|باچر|اليوم|غدا))', clean))


def _zone_unit(line, address):
    # Some mapped developments use a zone plus a separate house code, e.g. H611.
    return bool(re.search(r'(?<!\w)زون\s*\d+', checkout.normalized(address))
                and re.fullmatch(r'[a-z]\d{2,5}', checkout.normalized(line).strip()))


def shipping_selection(line):
    """Separate a declared color and size from inline delivery details."""
    clean = checkout.normalized(line)
    match = re.match(r'^\s*(?:اريد\s+)?(?:ال)?لون\s+(.{2,22}?)\s+'
                     r'(?:قياس|مقاس|سايز)\s*([a-z\d]{1,4})\s*[،,]?\s+(\S.*)$', clean)
    if match and (color := product_variants.single_color(match[1])):
        remainder = line[match.start(3):] if len(clean) == len(line) else clean[match.start(3):]
        return dict(color=color, size=product_variants.normalize_size_words(match[2]).upper()), remainder
    return {}, line


def delivery_line(line):
    """A trailing delivery-time question is independent of the address."""
    parts = re.split(r'\s+(?=(?:و?كم\s+يوم|و?شكد\s+يوم|و?شوكت|و?يمتى|و?متى)\s+)', line, maxsplit=1)
    return parts[0] if len(parts) == 2 and checkout_followup(parts[1]) else line


def declarations(text, previous):
    # Messaging apps wrap pasted phone numbers in bidi formatting characters.
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]', '', text)
    fields = {}
    address_text, _ = delivery_note(text)
    for line in address_text.splitlines():
        current = {**previous, **fields}
        line = delivery_line(shipping_selection(line)[1])
        parsed = checkout.contact_fields(PHONE_LABEL.sub('', line.translate(checkout.DIGITS)), current)
        # A standalone Iraqi mobile can arrive without its initial zero.
        local_mobile = LOCAL_MOBILE.fullmatch(checkout.normalized(line).strip(' .،,:'))
        if local_mobile:
            parsed['phone'] = '0' + re.sub(r'\D', '', local_mobile[1])
        if _zone_unit(line, fields.get('address')):
            parsed['address'] = f"{fields['address']}، {line.strip()}"
        if declined_phone(line):
            parsed.pop('phone', None)
        if parsed.get('phone') and not parsed.get('address'):
            # A selection and the shipping declaration can share one bubble.
            phone = checkout.PHONE.search(line.translate(checkout.DIGITS))
            if phone:
                parsed.update(checkout.contact_fields(line[phone.end():].strip(' ،,.'), current))
        if parsed.get('province') and current.get('province') not in {None, '', parsed['province']}:
            # A new province alone cannot silently reuse the old locality.
            if not parsed.get('address'):
                parsed['address'] = ''
        fields.update(parsed)
    return fields


def contact_only(text, *, quoted_total=None):
    """Allow local checkout for plain phone and address declarations."""
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]', '', text)
    address = ''
    contact_found = False
    address_text, _ = delivery_note(text)
    raw_lines = [line for line in address_text.splitlines() if line.strip()]
    lines = []
    idx = 0
    while idx < len(raw_lines):
        if idx + 1 < len(raw_lines):
            a = checkout.normalized(raw_lines[idx]).strip(' .،,:')
            b = checkout.normalized(raw_lines[idx + 1]).strip(' .،,:')
            if len(a) <= 4 and len(b) <= 4 and checkout.is_confirmation(a + b):
                idx += 2
                continue
        lines.append(raw_lines[idx])
        idx += 1
    for line in lines:
        clean = checkout.normalized(delivery_line(shipping_selection(line)[1])).strip(' .،,:')
        if not clean:
            continue
        if checkout_inspection_question(clean) and not checkout.phone_number(clean):
            continue
        if (checkout_followup(clean, quoted_total=quoted_total)
                or checkout.is_confirmation(clean)
                or re.fullmatch(
                    r'(?:راح\s+)?(?:ادز|ارسل|انطي(?:ج|ك)?|ابعت)(?:لج|لك)?\s+(?:ال)?(?:عنوان|رقم|موبايل|تفاصيل|بيانات)(?:\s+و(?:ال)?(?:رقم|عنوان))?|'
                    r'(?:هذا|هاي)\s+(?:ال)?(?:عنوان|رقمي|عنواني)|(?:ال)?عنوان\s+و(?:ال)?رقم',
                    clean,
                )):
            continue
        if LOCAL_MOBILE.fullmatch(clean):
            contact_found = True
            continue
        remainder = checkout.PHONE.sub('', PHONE_LABEL.sub('', clean)).strip(' .،,:')
        remainder = re.sub(r'^(?:(?:هذا|هاي)\s+)?(?:رقمي|رقم(?: الموبايل| الهاتف)?|موبايل)\s*[:،,]?\s*',
                           '', remainder).strip(' .،,:')
        if not remainder and checkout.phone_number(clean):
            contact_found = True
            continue
        if _zone_unit(remainder, address):
            continue
        if re.search(r'[؟?]|اريد|احجز|ثبتي|لون|قياس|وزن|سعر|خلاص|بشرط|توصيل|اخر|غير|تاخر|اذا', remainder):
            return False
        parsed = checkout.contact_fields(remainder)
        if not parsed.get('address') and not parsed.get('province'):
            return False
        contact_found = True
        address = parsed.get('address') or address
    return contact_found


def capture(store, scope, event_id, allow_write):
    evidence = store.evidence(scope, event_id)
    if evidence['actor'] != 'customer':
        return {}
    snapshot = store.snapshot(scope)
    text = evidence['payload'].get('text', '')
    fields = declarations(text, snapshot['data']['contact'])
    _, note = delivery_note(text)
    if not fields and not note:
        return {}

    def save(state):
        latest = store.db.execute("SELECT MAX(id) FROM sales_v2_events WHERE scope=? AND kind<>'receipt'",
                                  (scope.key,)).fetchone()[0]
        if latest != event_id or state['control_mode'] != 'active' or allow_write and not allow_write(scope):
            raise Conflict('turn_superseded')
        if state.get('order_id'):
            return dict(fields=[])
        if note:
            state['delivery_note'] = note
        events = state.setdefault('contact_events', {})
        for field, value in fields.items():
            if value:
                state['contact'][field] = value
                events[field] = event_id
            else:
                state['contact'].pop(field, None)
                events.pop(field, None)
        return dict(fields=list(fields))

    return store.apply(scope, f'contact:{event_id}', snapshot['version'],
                       dict(event_id=event_id), save)['result']
