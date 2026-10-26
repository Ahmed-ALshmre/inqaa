"""Identify explicit corrections to a garment type without selecting a catalog model."""
import re
try:
    from .customer_copy import normalized
except ImportError:
    from customer_copy import normalized

KINDS = {'dress': r'نفنوف|نفانيف|فستان|فساتين',
         'set': r'طقم|اطقم|سوت|قاط|قطعتين'}


def kind(product):
    text = normalized(' '.join(str((product or {}).get(key) or '')
                               for key in ('product_name', 'category')))
    found = [name for name, pattern in KINDS.items() if re.search(pattern, text)]
    return found[0] if len(found) == 1 else ''


def correction_kind(text):
    """A sent set 'مو نفنوف' rejects that suggestion; 'هذا نفنوف مو قطعتين' asserts dress."""
    text = normalized(text)
    for wanted, pattern in KINDS.items():
        other = KINDS['set' if wanted == 'dress' else 'dress']
        if (re.search(r'(?:دزيتي|دزيتلي|رسلتي|ارسلتي).{0,25}(?:' + other +
                      r').{0,12}مو\s+(?:' + pattern + ')', text)
                or re.search(r'(?:هذا|هاذا|هاي|اقصد|اريد).{0,12}(?:' + pattern +
                             r').{0,65}مو\s+(?:' + other + ')', text)
                or re.search(r'مو\s+(?:' + other + r').{0,20}(?:اريد|اقصد)\s+(?:' + pattern + ')', text)):
            return wanted
    return ''
