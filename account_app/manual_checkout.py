"""Staff-authored order lines preserve what the employee actually entered."""
from decimal import Decimal, InvalidOperation

try:
    from .pricing import quote
except ImportError:
    from pricing import quote


def prepare(raw_items, catalog, delivery_fee):
    products = {str(p['product_id']): p for p in catalog}
    items = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            raise ValueError('تفاصيل القطعة غير صحيحة')
        product = products.get(str(raw.get('product_id') or ''), {})
        item = {key: str(raw.get(key) or '').strip() for key in
                ('product_id', 'product_name', 'color', 'size', 'size_type', 'weight', 'notes')}
        item['product_name'] = item['product_name'] or product.get('product_name') or item['product_id']
        if not item['product_name']:
            raise ValueError('اكتب اسم القطعة أو اخترها من القائمة')
        try:
            quantity = Decimal(str(raw.get('quantity', 1)))
            if not quantity.is_finite() or quantity < 1 or quantity != quantity.to_integral_value():
                raise ValueError()
            item['quantity'] = int(quantity)
        except (ValueError, InvalidOperation):
            raise ValueError('اكتب عدداً صحيحاً موجباً للقطع') from None
        item['size_type'] = item['size_type'] or ('weight' if item['weight'] and not item['size'] else 'size')
        if item['size_type'] == 'weight' and not item['size']:
            item['size'] = item['weight']
        if raw.get('unit_price') not in ('', None):
            try:
                price = Decimal(str(raw['unit_price']))
                if not price.is_finite() or price < 0 or price != price.to_integral_value():
                    raise ValueError()
                item['unit_price'] = int(price)
            except (ValueError, InvalidOperation):
                raise ValueError('اكتب السعر بالدينار كرقم موجب أو صفر') from None
        items.append(item)
    if not items:
        raise ValueError('أضف القطعة المطلوبة')
    # Keep catalogue offers when no staff price overrides them. Inventory and
    # variant checks belong to AI checkout, not the employee's manual entry.
    if all('unit_price' not in item for item in items):
        try:
            return quote(items, catalog, delivery_fee)
        except (ValueError, KeyError, TypeError):
            pass
    for item in items:
        if 'unit_price' not in item:
            try:
                item['unit_price'] = quote([dict(item, quantity=1)], catalog, 0)['product_total']
            except (ValueError, KeyError, TypeError):
                item.update(unit_price=0, manual_price_pending=True)
    return dict(items=items, product_total=sum(item['unit_price'] * item['quantity'] for item in items),
                delivery_fee=delivery_fee)
