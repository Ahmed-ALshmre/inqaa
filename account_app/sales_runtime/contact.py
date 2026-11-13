"""Extract delivery details from customer evidence before calling a model."""
import re

from .. import checkout
from .store import Conflict


INSPECTION_QUESTION = re.compile(
    r'(?:و?اذا|لو|في حال)\s+.{0,55}(?:ارجع\w*|يرجع\w*|ترجع\w*|استبدل\w*)')
LOCAL_MOBILE = re.compile(r'(?:(?:رقمي|رقم(?: الموبايل| الهاتف)?|موبايل)\s*[:،,-]?\s*)?(7(?:[ \t-]*\d){9})')


def checkout_inspection_question(text):
    """A question about inspecting the parcel before paying."""
    clean = checkout.normalized(text)
    return bool(INSPECTION_QUESTION.search(clean)
                and not re.search(r'استلمت|وصلتني|رجعته|الغ[يى]|بشرط|ثبتي|احجزي|اشتري|اطلب', clean))


def declined_phone(text):
    return bool(re.search(r'رقم(?:كم| المندوب| المتجر)|مو رقمي|ما اريد استخدم هالرقم', checkout.normalized(text)))


def _zone_unit(line, address):
    # Some mapped developments use a zone plus a separate house code, e.g. H611.
    return bool(re.search(r'(?<!\w)زون\s*\d+', checkout.normalized(address))
                and re.fullmatch(r'[a-z]\d{2,5}', checkout.normalized(line).strip()))


def declarations(text, previous):
    fields = {}
    for line in text.splitlines():
        current = {**previous, **fields}
        parsed = checkout.contact_fields(line, current)
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


def contact_only(text):
    """Allow local checkout for plain phone and address declarations."""
    address = ''
    contact_found = False
    for line in text.splitlines():
        clean = checkout.normalized(line).strip(' .،,:')
        if not clean:
            continue
        if checkout_inspection_question(clean) and not checkout.phone_number(clean):
            continue
        if LOCAL_MOBILE.fullmatch(clean):
            contact_found = True
            continue
        remainder = checkout.PHONE.sub('', clean).strip(' .،,:')
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
    fields = declarations(evidence['payload'].get('text', ''), snapshot['data']['contact'])
    if not fields:
        return {}

    def save(state):
        latest = store.db.execute("SELECT MAX(id) FROM sales_v2_events WHERE scope=? AND kind<>'receipt'",
                                  (scope.key,)).fetchone()[0]
        if latest != event_id or state['control_mode'] != 'active' or allow_write and not allow_write(scope):
            raise Conflict('turn_superseded')
        if state.get('order_id'):
            return dict(fields=[])
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
