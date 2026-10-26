"""Compose delivery facts once without removing distinct policy conditions."""
import re
try:
    from . import customer_copy
except ImportError:
    import customer_copy

_COVERAGE = r'(?:التوصيل|الشحن)\s+متوفر\s+(?:لجميع|لكل)\s+(?:المحافظات|محافظات العراق)\s+مع\s+(?:إمكانية|امكانية)\s+'
_QUALIFIER = r'\s*(?:بشرط|عدا|إلا|الا|فقط|إذا|اذا|لو|مع|بحضور|قبل|لهذا|لهال|للموديل)(?!\w)'
_AMOUNT = r'\d+(?:\s*(?:ألف|آلاف|الف|الاف)(?:\s+و\d+)?)'
_COUNTRY = r'(?:لكل المحافظات|لجميع المحافظات|لجميع محافظات العراق)'
_ATOM = re.compile(r'(?<!\w)(?:و)?(?:'
    r'(?P<fee>(?:التوصيل|الشحن)\s*[:：]?\s*' + _AMOUNT + r'\s+' + _COUNTRY + r')|'
    r'(?P<payment>الدفع\s+(?:(?:يصير|يكون)\s+)?عند الاستلام)|'
    r'(?P<inspection>الفحص\s+عند الاستلام))')


def inspection_only(value):
    """Remove the redundant coverage lead-in, keeping the inspection conditions."""
    text = customer_copy.fact(value).strip()
    return re.sub(r'^' + _COVERAGE, '', text, count=1).strip(' .،')


def inspection_reply(value):
    """Use the owner's approved wording for the plain policy, retaining exceptions."""
    text = inspection_only(value)
    if text in {'الفحص عند الاستلام', 'إمكانية الفحص عند الاستلام',
                'امكانية الفحص عند الاستلام'}:
        return 'إلكم حق الفحص بوجود المندوب قبل الدفع'
    return text


def join_policy(parts):
    """Connect the standard inspection right directly to the quoted delivery fee."""
    text = ' '.join(parts)
    return re.sub(r'(?<=\S)\.\s+إلكم حق الفحص', '، وإلكم حق الفحص', text)


def carrier_quote(value):
    """Keep the actual quoted fees and explain who receives them once."""
    return str(value).rstrip(' .') + '، هاي أجور التوصيل تابعة لشركة التوصيل مو إلنا عيني.'


def compact_repetition(value):
    """Deduplicate only recognized plain policy facts; preserve qualified clauses."""
    original = str(value or '')
    text = customer_copy.chat_money(value)
    def inspection(match):
        if re.match(_QUALIFIER, text[match.end():]):
            return match[0]
        return 'الفحص عند الاستلام. '
    before_coverage = text
    text = re.sub(_COVERAGE + r'الفحص عند الاستلام\s*[.]?', inspection, text)
    coverage_changed = text != before_coverage
    seen = set()
    def unique(match):
        before = text[max(0, match.start()-15):match.start()]
        if (re.search(r'(?:مو|غير|لا(?:\s+(?:يمكن|يتوفر|يسمح))?|بدون|ما|ممنوع)\s*$', before)
                or re.match(_QUALIFIER, text[match.end():])):
            return match[0]
        kind = match.lastgroup
        key = (kind, customer_copy.normalized(match[kind])) if kind == 'fee' else (kind, '')
        if kind == 'fee':
            key = (kind, re.sub(r'[:：]|\s+', '', key[1]).replace('لجميعمحافظاتالعراق', 'لكلالمحافظات').replace('لجميعالمحافظات', 'لكلالمحافظات'))
        if key in seen:
            return ''
        seen.add(key)
        return match[0]
    cleaned = _ATOM.sub(unique, text)
    if cleaned == text:
        return text.strip() if coverage_changed else original
    cleaned = re.sub(r'([.؛،])(?:\s*[.؛،])+', r'\1', cleaned)
    cleaned = re.sub(r'[ \t]+([.؛،])', r'\1', cleaned)
    cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
    return cleaned.strip(' ،؛')

