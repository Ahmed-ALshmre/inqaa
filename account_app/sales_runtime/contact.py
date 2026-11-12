"""Extract delivery details from customer evidence before calling a model."""
import re

from .. import checkout
from .store import Conflict


def declined_phone(text):
    return bool(re.search(r'رقم(?:كم| المندوب| المتجر)|مو رقمي|ما اريد استخدم هالرقم', checkout.normalized(text)))


def declarations(text, previous):
    fields = {}
    for line in text.splitlines():
        current = {**previous, **fields}
        parsed = checkout.contact_fields(line, current)
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
    for line in text.splitlines():
        clean = checkout.normalized(line).strip(' .،,:')
        if not clean:
            continue
        remainder = checkout.PHONE.sub('', clean).strip(' .،,:')
        remainder = re.sub(r'^(?:(?:هذا|هاي)\s+)?(?:رقمي|رقم(?: الموبايل| الهاتف)?|موبايل)\s*[:،,]?\s*',
                           '', remainder).strip(' .،,:')
        if not remainder and checkout.phone_number(clean):
            continue
        if re.search(r'[؟?]|اريد|احجز|ثبتي|لون|قياس|وزن|سعر|خلاص|بشرط|توصيل|اخر|غير|تاخر|اذا', remainder):
            return False
        parsed = checkout.contact_fields(remainder)
        if not parsed.get('address') and not parsed.get('province'):
            return False
    return bool(text.strip())


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
