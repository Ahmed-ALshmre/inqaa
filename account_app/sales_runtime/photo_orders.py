"""Evidence for purchase selections that point to the customer's photos."""
import json
import re

from .. import checkout, product_albums, product_variants, variant_inventory
from .intent import purchase_selected


def caption_size(text):
    """A short selection can label the adjacent photo. A question cannot."""
    clean = checkout.normalized(product_variants.normalize_size_words(text)).strip(' .،!')
    match = re.fullmatch(
        r'(?:(?:و\s*)?(?:هذا|هاي[هة]?|هذي|هذه)\s+)?(?:(?:قياس|مقاس|سايز)\s*)?'
        r'([2-6]xl|[2-6]x|xxl|xl|xs|s|m|l|\d{2})', clean)
    return match[1].upper() if match else ''


def refers_to_sent_choices(text):
    clean = checkout.normalized(text)
    return bool(re.search(r'(?:مو\s+)?(?:دزيت|ارسلت|رسلت).{0,22}صور.{0,30}'
                          r'(?:قياس|مقاس|الوان|لون)', clean))


def requests_two_photos(text):
    clean = checkout.normalized(text)
    return purchase_selected(clean) and bool(re.search(
        r'(?<!\w)(?:هذني|هذين|هذوله|هذول|ذني|ذوله|ذول|هاي[هة]?|هذي|هذه|هذن)\s*'
        r'(?:ال)?(?:قطعتين|موديلين|ثنين|اثنين|تنين)(?:هن|هم)?(?!\w)', clean))


def recent_choices(store, scope, event_id, state, catalog):
    """Keep each recognized attachment and its adjacent size in arrival order."""
    rows = store.db.execute("""SELECT e.id,e.payload FROM sales_v2_events AS e
        JOIN sales_v2_events AS current ON current.scope=e.scope AND current.id=?
        WHERE e.scope=? AND e.id<=current.id AND e.actor='customer' AND e.kind='message'
        AND current.received_at-e.received_at<=28800 ORDER BY e.id DESC LIMIT 24""",
        (event_id, scope.key)).fetchall()
    observations = state.get('media_observations', {})
    choices, adjacent = [], None
    for source, encoded in reversed(rows):
        payload = json.loads(encoded)
        observed = observations.get(str(source))
        if observed and observed['status'] not in {'sticker', 'unsupported'}:
            positions = observed.get('positions')
            if positions is None:
                refs = payload.get('media_ref')
                matched = observed['status'] == 'matched' and (isinstance(refs, str) or len(refs or []) == 1)
                positions = [dict(product_id=observed['product_ids'][0], color=observed.get('observed_color'))
                             if matched else {}]
            labels = payload.get('photo_sizes') or []
            batch = []
            for index, position in enumerate(positions):
                if position.get('sticker'):
                    continue
                product = catalog.products.get(position.get('product_id'), {})
                color = position.get('color')
                if not product or color not in product_variants.colors(product.get('colors')):
                    color = None
                label = labels[index] if index < len(labels) else ''
                choice = dict(event=source, position=index + 1, product_id=product.get('product_id'),
                              color=color, size=label, fingerprint=position.get('fingerprint'),
                              evidence=observed.get('positions') is not None or bool(color or label))
                batch.append(choice)
            if len(batch) == 1 and not batch[0]['size']:
                batch[0]['size'] = caption_size(payload.get('text', ''))
            choices.extend(batch)
            adjacent = batch[0] if len(batch) == 1 else None
        else:
            if adjacent and caption_size(payload.get('text', '')):
                adjacent['size'] = caption_size(payload['text'])
            adjacent = None
    distinct = {}
    for choice in choices:
        fingerprint = choice['fingerprint'] or (choice['event'], choice['position'])
        sizes = {size for image, size in distinct if image == fingerprint and size}
        if not choice['size'] and len(sizes) == 1:
            choice['size'] = next(iter(sizes))
        key = fingerprint, choice['size']
        if choice['size']:
            distinct.pop((fingerprint, ''), None)
        distinct.pop(key, None)
        distinct[key] = choice
    return list(distinct.values())[-8:]


def checkout_lines(choices, catalog):
    """Build a draft only when each requested entry has supported stock evidence."""
    lines, gaps = [], []
    for index, choice in enumerate(choices, 1):
        product = catalog.products.get(choice['product_id'])
        if not product or (product.get('colors') and not choice['color']):
            gaps.append(dict(number=index, size=choice['size'], field='photo'))
            continue
        if product_albums.stock_state(product) != 'available':
            gaps.append(dict(number=index, color=choice['color'], size=choice['size'], field='stock'))
            continue
        size = choice['size']
        system = product.get('size_system') or 'legacy'
        if system != 'none':
            proof = product_variants.assess(product, 'قياس ' + size) if size else []
            if system in {'age', 'weight'} or not any(
                       item['field'] == 'size' and item['supported'] is True and item['value'] == size
                       for item in proof) or (product.get('variant_stock') and not variant_inventory.choices(
                           product, color=choice['color'], size=size)):
                gaps.append(dict(number=index, color=choice['color'], size=size, field='size'))
                continue
        values = dict(product_id=choice['product_id'], color=choice['color'] or '', quantity=1)
        if size and product.get('size_system') != 'none':
            values['size'] = size
        lines.append(values)
    return lines, gaps


def followup(pending, photos, event_id, text, marker, cart_key):
    gap = pending['gap']
    if marker != gap:
        return [], False
    choices = [dict(choice) for choice in pending['choices']]
    current = choices[gap['number'] - 1]
    if gap['field'] == 'size' and (label := caption_size(text)):
        current['size'] = label
        return choices, False
    if gap['field'] == 'photo':
        incoming = [photo for photo in photos if photo['event'] == event_id]
        if len(incoming) == 1 and incoming[0]['product_id']:
            current.update(incoming[0], size=incoming[0]['size'] or current['size'])
            return choices, False
    if gap['field'] == 'cart' and pending['cart_key'] == cart_key and checkout.is_confirmation(text):
        return choices, True
    return [], False


def cart_changes(lines, cart, *, replace=False):
    """Reuse booked-intent draft lines and avoid silently replacing distinct choices."""
    unused = list(cart)
    changes = []
    for values in lines:
        matches = [line for line in unused if line['product_id'] == values['product_id']
                   and all(not line.get(field) or line[field] == values.get(field) for field in ('size', 'color'))
                   and line.get('quantity') == 1 and not line.get('weight_kg') and not line.get('age_months')]
        exact = next((line for line in matches if all(line.get(field, '') == values.get(field, '')
                     for field in ('size', 'color'))), None)
        match = exact or (matches[0] if len(matches) == 1 else None)
        if match:
            unused.remove(match)
        changes.append(dict(line_ref=match['line_id'], values=values) if match
                       else dict(new_line=True, values=values))
    if unused and not replace:
        return None
    removals = [dict(line_ref=line['line_id'], remove=True, values={}) for line in unused]
    return removals + changes


def stated_offer(store, scope, photos, event_id):
    rows = store.db.execute("""SELECT payload FROM sales_v2_events WHERE scope=? AND kind='message'
        AND actor='customer' AND id BETWEEN ? AND ? ORDER BY id DESC LIMIT 24""",
        (scope.key, min(photo['event'] for photo in photos), event_id)).fetchall()
    for (encoded,) in rows:
        text = checkout.normalized(json.loads(encoded).get('text', ''))
        match = re.search(r'(?:فلوسهن|الحساب|المجموع)(?:\s+(?:الكلي|يطلع|يصير))?\s*(\d{2,6})\s*(الف|دينار)?', text)
        expected_terms = match or re.search(r'حسب\s+كلام|كلت[مور]?[ي]?|قلت[مور]?[ي]?|اتفقنا', text)
        free = bool(expected_terms and re.search(r'التوصيل\s+(?:مجانا|مجاني|بلاش)', text))
        if match or free:
            amount = int(match[1]) if match else None
            if amount and (match[2] == 'الف' or not match[2] and amount < 1000):
                amount *= 1000
            return dict(total=amount, free=free)
    return None
