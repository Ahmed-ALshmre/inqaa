"""Conservative checks for staff drafts: preserve facts and reject new commitments."""
import re


def tokens(text):
    return set(re.findall(r'[\w]+', text.translate(str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789'))))


def valid_rewrite(draft, reply):
    source, target = tokens(draft), tokens(reply)
    if {t for t in source if any(c.isdigit() for c in t)} != {t for t in target if any(c.isdigit() for c in t)}:
        return False
    colors = {'وردي', 'أحمر', 'احمر', 'أزرق', 'ازرق', 'أسود', 'اسود', 'أبيض', 'ابيض',
              'زيتي', 'نيلي', 'رصاصي', 'بنفسجي', 'بيج', 'أصفر', 'اصفر', 'اخضر', 'أخضر'}
    if source & colors != target & colors:
        return False
    negative = r'\b(?:لا|مو|غير|ليس|للأسف|للاسف|نفذ|خلص|ماكو)\b'
    if bool(re.search(negative, draft)) != bool(re.search(negative, reply)):
        return False
    commitments = (r'تم\s+(?:تثبيت|حجز|إلغاء|الغاء|تعديل|إرسال|ارسال)', r'مجاني|مجاناً|مجانا',
                   r'ضمان|مضمون', r'خصم|تخفيض')
    if any(re.search(pattern, reply) and not re.search(pattern, draft) for pattern in commitments):
        return False
    return len(target) <= max(25, len(source) * 2)
