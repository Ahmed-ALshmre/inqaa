"""Owner-entered catalog classification used by the editor and recommendations."""
import math
import re

AUDIENCES = {'boys': 'ولادي', 'girls': 'بناتي', 'children': 'أطفال للجنسين',
             'women': 'نسائي', 'men': 'رجالي', 'adults': 'بالغين للجنسين'}
TYPES = {'dresses': 'فستان', 'sets': 'طقم / سوت', 'abaya': 'عباية', 'skirts': 'تنورة',
         'trousers': 'بنطلون', 'tops': 'قميص / بلوزة', 'outerwear': 'جاكيت / معطف',
         'shoes': 'أحذية', 'accessories': 'إكسسوارات', 'other': 'أخرى'}
SIZE_SYSTEMS = {'legacy': 'المقاسات المحفوظة', 'age': 'عمر الطفل', 'clothing': 'قياس الملابس',
                'weight': 'نطاق وزن تقريبي', 'none': 'لا يحتاج قياساً'}
OCCASIONS = {'everyday': 'يومي', 'birthday': 'عيد ميلاد', 'eid': 'العيد',
             'occasion': 'مناسبات', 'school': 'مدرسة', 'winter': 'شتوي', 'summer': 'صيفي'}
FIELDS = ('audience', 'product_type', 'size_system', 'age_min_months', 'age_max_months',
          'weight_min_kg', 'weight_max_kg', 'occasions', 'recommendation_groups',
          'recommendation_enabled', 'recommendation_priority', 'recommendation_notes')


def normalize(product, strict=False):
    result = dict(product)
    def invalid(message):
        if strict:
            raise ValueError(message)
    for field, allowed, default in (('audience', AUDIENCES, ''), ('product_type', TYPES, ''),
                                    ('size_system', SIZE_SYSTEMS, 'legacy')):
        value = result.get(field) or default
        if not isinstance(value, str) or (value and value not in allowed):
            invalid('قيمة غير صحيحة لحقل ' + field)
            value = default
        result[field] = value
    for field in ('occasions', 'recommendation_groups'):
        raw = result.get(field)
        if raw in (None, ''):
            raw = []
        if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
            invalid('القوائم يجب أن تحتوي نصوصاً فقط: ' + field)
            raw = []
        values = list(dict.fromkeys(item.strip() for item in raw if item.strip()))
        if len(values) > 20 or any(len(item) > 60 for item in values):
            invalid('الحد 20 مجموعة، و60 حرفاً لكل اسم.')
            values = []
        if field == 'occasions' and any(item not in OCCASIONS for item in values):
            invalid('اختاري مناسبة من القائمة المعتمدة.')
            values = []
        result[field] = values
    enabled = result.get('recommendation_enabled', True)
    if enabled in (None, ''):
        enabled = True
    if type(enabled) is not bool:
        invalid('خيار السماح بالاقتراح يجب أن يكون نعم أو لا.')
        enabled = True
    result['recommendation_enabled'] = enabled
    priority = result.get('recommendation_priority')
    if priority in (None, ''):
        priority = 3
    if type(priority) is not int or not 1 <= priority <= 5:
        invalid('أولوية الاقتراح يجب أن تكون بين 1 و5.')
        priority = 3
    result['recommendation_priority'] = priority
    result['recommendation_notes'] = str(result.get('recommendation_notes') or '').strip()[:1000]
    for field, maximum in (('age_min_months', 216), ('age_max_months', 216),
                            ('weight_min_kg', 400), ('weight_max_kg', 400)):
        value = result.get(field)
        if value in (None, ''):
            result[field] = None
            continue
        try:
            number = float(str(value).translate(str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')))
            if type(value) is bool or not math.isfinite(number) or not 0 <= number <= maximum:
                raise ValueError
            if not number.is_integer():
                raise ValueError
            result[field] = int(number) if number.is_integer() else number
        except (ValueError, TypeError, OverflowError):
            invalid('العمر بالأشهر والوزن بالكيلو يحتاجان أرقاماً صحيحة ضمن النطاق.')
            result[field] = None
    for mode, low, high, unit in (('age', 'age_min_months', 'age_max_months', 'أشهر'),
                                  ('weight', 'weight_min_kg', 'weight_max_kg', 'كيلو')):
        if result['size_system'] != mode:
            continue
        a, b = result[low], result[high]
        if a is None or b is None or a > b or (mode == 'weight' and a <= 0):
            invalid('حددي بداية ونهاية صحيحتين للعمر أو الوزن؛ البداية لا تتجاوز النهاية.')
            continue
        if mode == 'age' and result['audience'] in {'women', 'men', 'adults'}:
            invalid('عمر الطفل لا يستخدم مع جمهور البالغين.')
        if mode == 'weight' and result['audience'] in {'boys', 'girls', 'children'}:
            invalid('استخدمي عمر الطفل أو قياس الملابس للأطفال، وليس الوزن.')
        result['sizes'] = f'{a} إلى {b} {unit}' if mode == 'age' else f'وزن {a} إلى {b} {unit}'
    if result['size_system'] == 'none':
        result['sizes'] = ''
    return result


def compatible_audience(candidate, requested):
    if not requested:
        return not candidate
    return candidate == requested or (requested in {'boys', 'girls'} and candidate == 'children') or (
        requested in {'women', 'men'} and candidate == 'adults')


def query_audience(text):
    text = str(text or '').replace('أ', 'ا').replace('إ', 'ا')
    groups = {'boys': r'ولادي|اولاد|ابني|ولدي|للولد', 'girls': r'بناتي|بنات|بنتي|بنيتي|للبنت',
              'women': r'نسائي|نساي|نسوان|حريمي', 'men': r'رجالي|للرجال'}
    hits = [group for group, pattern in groups.items() if re.search(pattern, text)]
    return hits[0] if len(hits) == 1 else ''


def query_occasions(text):
    text = str(text or '').replace('أ', 'ا').replace('إ', 'ا')
    patterns = {'birthday': r'عيد ميلاد|ميلاد', 'eid': r'(?<!ميلاد )\bالعيد\b',
                'school': r'مدرس[هة]|دوام', 'winter': r'شتوي|شتاء', 'summer': r'صيفي|صيف',
                'occasion': r'مناسب[هة]|عرس|حفله|حفلة', 'everyday': r'يومي|للبيت'}
    return [key for key, pattern in patterns.items() if re.search(pattern, text)]


def discovery_candidates(text, products):
    """Filter suggestions by explicit facts; the direct selected item is untouched."""
    try:
        from . import product_variants, product_albums
    except ImportError:
        import product_variants, product_albums
    audience = query_audience(text)
    ages = product_variants.child_ages(text)
    sizes = product_variants.requested(text)['sizes']
    occasions = query_occasions(text)
    result = []
    for product in products:
        if product.get('recommendation_enabled') is False or product_albums.stock_state(product) != 'available':
            continue
        if audience:
            try:
                from .product_links import _audience
            except ImportError:
                from product_links import _audience
            if not compatible_audience(_audience(product), audience):
                continue
        if ages:
            ranges = product_variants.size_data(str(product.get('sizes') or ''))['years']
            if not ranges or not all(any(lo <= age <= hi for lo, hi in ranges) for age in ages):
                continue
        if sizes:
            checks = product_variants.assess(product, text)
            if not checks or any(check['supported'] is not True for check in checks):
                continue
        if occasions and not set(occasions) & set(product.get('occasions') or []):
            continue
        result.append(product)
    return sorted(result, key=lambda p: -normalize(p)['recommendation_priority'])
