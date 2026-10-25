"""Owner-selected product links, with current facts checked before suggesting.

Product CRUD keeps owner-selected links reciprocal. Eligibility reads the
source product's links; they are never customer consent or a cart. This module
does not write catalog records, messages, orders, or automatic follow-ups.
"""
import math
import re

try:
    from . import customer_copy, product_albums, product_variants
    from .pricing import terms
except ImportError:
    import customer_copy
    import product_albums
    import product_variants
    from pricing import terms


def normalize_related_ids(value):
    """Read the canonical array safely; malformed legacy values create no links."""
    if not isinstance(value, list):
        return []
    return list(dict.fromkeys(item.strip() for item in value
                              if isinstance(item, str) and item.strip()))


def validate_related_ids(value, product_id, store_id, products):
    """Validate an admin write against the complete catalog, then normalize it."""
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError('المنتجات المرتبطة يجب أن تكون قائمة أكواد منتجات.')
    ids = normalize_related_ids(value)
    source = str(product_id or '').strip()
    store = str(store_id or 'default').strip() or 'default'
    catalog = {str(item.get('product_id') or '').strip() for item in products or []
               if isinstance(item, dict) and _store(item) == store}
    if source in ids:
        raise ValueError('لا يمكن ربط المنتج بنفسه؛ اختاري موديلًا مختلفًا.')
    if any(item not in catalog for item in ids):
        raise ValueError('أحد المنتجات المرتبطة غير موجود في هذا المتجر.')
    return ids


def _store(product):
    return str(product.get('store_id') or 'default').strip() or 'default'


def _facts(product, field):
    return customer_copy.normalized(customer_copy.fact(product.get(field), field))


def _audience(product):
    text = ' '.join(_facts(product, field) for field in ('category', 'product_name'))
    groups = {
        'boys': r'ولادي|ولاد|اولادي|اولاد|صبيان|\bboys?\b',
        'girls': r'بناتي|بنات|بنوت|\bgirls?\b',
        'women': r'نسائي|نسوان|نسويه|نسوية|حريمي|\bwomens?\b',
        'men': r'رجالي|رجال|\bmens?\b',
    }
    found = [name for name, pattern in groups.items() if re.search(pattern, text, re.I)]
    return found[0] if len(found) == 1 else ''


def _category(product):
    text = _facts(product, 'category')
    groups = (
        ('dresses', r'فستان|فساتين|dress'),
        ('abaya', r'عباي|عباء|عبايات|abaya'),
        ('sets', r'سوت|طقم|اطقم|set'),
        ('skirts', r'تنور|تنانير|skirt'),
        ('trousers', r'بنطل|بنطر|شروال|trouser|pants'),
        ('tops', r'بلوز|قميص|قمصان|shirt|blouse'),
    )
    return next((name for name, pattern in groups if re.search(pattern, text)), text)


def _size_data(product):
    return product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))


def _overlap(left, right):
    return any(max(a, c) <= min(b, d) for a, b in left for c, d in right)


def _shared_sizes(left, right):
    a, b = _size_data(left), _size_data(right)
    if a['years']:
        return bool(b['years'] and _overlap(a['years'], b['years']))
    if a['known']:
        ar = a['ranges'] + [(number, number) for number in a['numbers']]
        br = b['ranges'] + [(number, number) for number in b['numbers']]
        return bool(_overlap(ar, br) or set(a['labels']) & set(b['labels']))
    # Weight-only ranges do not prove that two garments have the same size.
    return not a['weights']


def eligible_related_products(product, products, *, size='', age_years=None,
                              audience='', limit=2):
    """Resolve source links to different, compatible, currently offered models.

    Explicit customer size/age takes precedence over a broad overlap of catalog
    ranges.  Missing evidence never becomes a confident fit recommendation.
    Returned dicts are catalog records, not selected order lines.
    """
    if not isinstance(product, dict):
        return []
    category = _category(product)
    if not category:
        return []
    try:
        maximum = max(0, min(int(limit), 2))
        age = float(age_years) if age_years is not None else None
    except (ValueError, TypeError, OverflowError):
        return []
    if maximum == 0 or (age is not None and (not math.isfinite(age) or age < 0)):
        return []
    source = str(product.get('product_id') or '').strip()
    store = _store(product)
    target_audience = str(audience or _audience(product)).strip().lower()
    if target_audience and target_audience not in {'boys', 'girls', 'women', 'men'}:
        return []
    catalog = {str(item.get('product_id') or '').strip(): item for item in products or []
               if isinstance(item, dict) and _store(item) == store}
    result = []
    for pid in normalize_related_ids(product.get('related_product_ids')):
        candidate = catalog.get(pid)
        if not candidate or pid == source or product_albums.stock_state(candidate) != 'available':
            continue
        quantity = candidate.get('stock_quantity')
        if quantity not in (None, ''):
            try:
                remaining = float(str(quantity).translate(str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')))
                if not math.isfinite(remaining) or remaining <= 0:
                    continue
            except (ValueError, TypeError):
                continue
        if _category(candidate) != category:
            continue
        if target_audience and _audience(candidate) != target_audience:
            continue
        if not target_audience and _audience(candidate):
            continue  # An ambiguous source does not prove a gender/age audience.
        if age is not None:
            if not any(low <= age <= high for low, high in _size_data(candidate)['years']):
                continue
        elif not size and not _shared_sizes(product, candidate):
            continue
        if size:
            checks = product_variants.assess(candidate, 'قياس ' + str(size))
            if not checks or any(check['supported'] is not True for check in checks):
                continue
        try:
            terms(candidate)
        except (ValueError, TypeError, KeyError):
            continue
        result.append(dict(candidate))
        if len(result) >= maximum:
            break
    return result


def eligible_age_alternatives(product, products, age_years):
    """The owner allows an opt-in age alternative from the current catalog."""
    source = dict(product or {})
    source['related_product_ids'] = [str(item.get('product_id') or '') for item in products or []
                                    if isinstance(item, dict) and item.get('image_url')]
    return eligible_related_products(source, products, age_years=age_years, limit=2)
