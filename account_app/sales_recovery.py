"""Narrow recovery for policy questions and explicit photo requests."""
import re

try:
    from .checkout import PROVINCES, normalized
except ImportError:
    from checkout import PROVINCES, normalized


def policy_question(text):
    clean = normalized(text).strip(' ؟?!.،\n')
    clean = re.sub(r'^(?:السلام عليكم|سلام عليكم|هلو|مرحبا)\s*', '', clean)
    clean = re.sub(r'^و(?=توصيل|التوصيل)', '', clean)
    names = {normalized(name) for aliases in PROVINCES.values() for name in aliases}
    names.update('لل' + name[2:] for name in list(names) if name.startswith('ال'))
    location = '|'.join(sorted({re.escape(name) for name in names}, key=len, reverse=True))
    clean = re.sub(r'(?:^|\s)(?:ل|الى |لـ)?(?:' + location + r')(?=\s|$)', ' ', clean)
    clean = ' '.join(clean.split())
    if re.fullmatch(r'(?:متى|يمتى)\s+(?:(?:يتم|يصير)\s+)?(?:التوصيل|توصيل|يوصل الطلب)', clean):
        return 'delivery_time'
    if re.fullmatch(r'(?:ما هي طريقة|ماهي طريقة|كيفية|شلون|كيف|شنو طريقة)\s*(?:الدفع|ادفع)(?:\s*(?:و|او)\s*(?:الشحن|التوصيل))?(?: المتاحة)?', clean):
        return 'payment'
    if re.fullmatch(r'(?:اكو|عدكم|عندكم|هل يوجد)\s+(?:توصيل|شحن)|(?:ما هي طريقة|ماهي طريقة)\s+التوصيل', clean):
        return 'delivery'
    if re.fullmatch(r'(?:شكد|اشكد|كم|بشكد)(?: تبلغ)?\s+(?:(?:تكلفة|اجور|سعر)\s+)?(?:التوصيل|توصيل|الشحن|شحن)|(?:التوصيل|توصيل|الشحن|شحن)\s+(?:شكد|اشكد|كم|بشكد)', clean):
        return 'delivery'
    return ''


def explicit_photo_followup(text):
    clean = normalized(text).strip(' ؟?!.،\n')
    return bool(re.fullmatch(r'(?:دزو|دزوا|دزيهن|دزهن|دزهم|ارسلهم|ارسليهن|شوفينياهن|اشوفهن|اشوفها|اشوف صورته|اشوف صورتها)', clean))


def simple_photo_request(text):
    clean = normalized(text).strip(' ؟?!.،\n')
    clean = re.sub(r'(?:\s+(?:حبيبتي|عيني|حبي|حياتي|بلا زحمة|لو سمحتي))+$', '', clean)
    if explicit_photo_followup(clean):
        return True
    return bool(re.fullmatch(r'(?:(?:ممكن|اريد)\s+)?(?:اشوف|دزيلي|دزي|دزلي|ارسلي|شوفيني|وريني)\s+(?:الصور|صور|صوره|صورة|صورته|صورتها|صورهم|صورهن)', clean))


def claims_sent_models(text):
    clean = normalized(text)
    if re.search(r'(?:ما|لم|مو)\s+(?:دزيت|ارسلت|رسلت|بعثت)', clean):
        return False
    return bool(re.search(r'(?:دزيت\w*|ارسلت\w*|رسلت\w*|بعثت\w*)\s+(?:الموديلات|البدائل|الخيارات|الصور)|شوفي\s+الخاص\s+(?:دزيت|ارسلت)', clean))
