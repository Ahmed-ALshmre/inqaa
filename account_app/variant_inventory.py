"""Store-scoped stock for individual color and size combinations."""
from collections import Counter, defaultdict
from contextlib import contextmanager
import hashlib
import json
import re

try:
    from . import product_variants as variants
except ImportError:
    import product_variants as variants


CANCELLED = {'cancelled', 'canceled', 'ملغي'}
GENERAL = 'general'
DETAILED = 'detailed'
FREE_SIZE = 'فري سايز'
WEIGHT_STOCK = 'حسب الوزن'
STOPPED = re.compile(r'نفذ|نافذ|نافد|خلص|غير متوفر|مو متوفر|not available|\bout\b', re.I)


class StockError(ValueError):
    def __init__(self, product_id, rows, key=None, quantity=0):
        self.product_id = product_id
        self.available = [row for row in rows if row['quantity'] > 0]
        self.requested = dict(color=key[0], size=key[1], quantity=quantity) if key else {}
        label = ' / '.join(value for value in (key or ()) if value)
        message = f'الكمية المطلوبة غير متوفرة ({label}).' if label else 'الكمية المطلوبة غير متوفرة.'
        super().__init__(message)

    def feedback(self):
        return dict(product_id=self.product_id, requested=self.requested, available=self.available)


def init(db):
    db.execute("""CREATE TABLE IF NOT EXISTS general_product_stock (
        store_id TEXT NOT NULL, product_id TEXT NOT NULL,
        PRIMARY KEY(store_id, product_id))""")
    db.execute("""CREATE TABLE IF NOT EXISTS product_variant_stock (
        store_id TEXT NOT NULL, product_id TEXT NOT NULL, color TEXT NOT NULL, size TEXT NOT NULL,
        quantity INTEGER NOT NULL CHECK(quantity >= 0),
        PRIMARY KEY(store_id, product_id, color, size))""")
    db.execute("""CREATE TABLE IF NOT EXISTS order_variant_stock (
        order_id INTEGER NOT NULL, store_id TEXT NOT NULL, product_id TEXT NOT NULL,
        color TEXT NOT NULL, size TEXT NOT NULL, quantity INTEGER NOT NULL CHECK(quantity > 0),
        PRIMARY KEY(order_id, store_id, product_id, color, size))""")


@contextmanager
def transaction(db):
    """Keep order, quantity and reservation changes in the same SQLite transaction."""
    nested = db.in_transaction
    db.execute('SAVEPOINT variant_stock_write' if nested else 'BEGIN IMMEDIATE')
    try:
        yield
    except BaseException:
        if nested:
            db.execute('ROLLBACK TO variant_stock_write')
            db.execute('RELEASE variant_stock_write')
        else:
            db.rollback()
        raise
    else:
        if nested:
            db.execute('RELEASE variant_stock_write')
        else:
            db.commit()


def rows_by_product(db, store, *, active_only=False):
    if not db or not db.execute("SELECT 1 FROM sqlite_master WHERE name='product_variant_stock'").fetchone():
        return {}
    grouped = defaultdict(list)
    filter_mode = ('AND NOT EXISTS (SELECT 1 FROM general_product_stock g WHERE g.store_id=s.store_id '
                   'AND g.product_id=s.product_id)' if active_only else '')
    for product, color, size, count in db.execute(f"""SELECT s.product_id,s.color,s.size,s.quantity
        FROM product_variant_stock s WHERE s.store_id=? {filter_mode} ORDER BY s.product_id,s.color,s.size""", (store,)):
        grouped[product].append(dict(color=color, size=size, quantity=count))
    return grouped


def with_stock(db, store, products):
    stock = rows_by_product(db, store, active_only=True)
    return [dict(p, variant_stock=stock[p['product_id']])
            if p.get('store_id', store) == store and p['product_id'] in stock else p for p in products]


def general_products(db, store):
    return {row[0] for row in db.execute('SELECT product_id FROM general_product_stock WHERE store_id=?', (store,))}


def mode_for(product_id, rows, general):
    return DETAILED if rows and product_id not in general else GENERAL


def set_mode(db, store, product_id, mode):
    if mode == GENERAL:
        db.execute('INSERT OR IGNORE INTO general_product_stock VALUES(?,?)', (store, product_id))
    else:
        db.execute('DELETE FROM general_product_stock WHERE store_id=? AND product_id=?', (store, product_id))


def revision(rows, mode):
    return hashlib.sha256(json.dumps([mode, rows], sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def color_key(value):
    return variants.single_color(value) or variants.normalized(value).strip()


def size_key(product, value='', age_months=None):
    mode = product.get('size_system')
    if mode == 'none':
        return ''
    data = variants.size_data(product.get('sizes'))
    if variants.is_free_size_product(product) and not data['known']:
        return FREE_SIZE
    if mode == 'weight' or (mode in (None, '', 'legacy') and variants.weight_only_ranges(product)):
        return WEIGHT_STOCK
    clean = variants.normalized(variants.normalize_size_words(value)).strip()
    if variants.FREE_SIZE_RE.fullmatch(clean):
        return FREE_SIZE
    if mode == 'age' or data['years'] and not data['known']:
        if age_months is None:
            months = re.fullmatch(r'(\d{1,3})\s*(?:اشهر|شهر|شهور)', clean)
            if months:
                return f'{int(months[1])} شهر'
            age = variants.child_age(clean)
            if age is None:
                age = variants.year_size(clean)
            age_months = round(age * 12) if age is not None else None
        return f'{age_months} شهر' if type(age_months) is int else ''
    return clean.upper()


def validated_rows(product, supplied):
    if not isinstance(supplied, list) or len(supplied) > 500:
        raise ValueError('أدخل حتى 500 تركيبة لون وقياس لهذا المنتج.')
    normalized = {}
    listed_colors = {color_key(label) for label in re.split(r'[،,؛;\n]+', str(product.get('colors') or ''))}
    listed_colors.update(variants.colors(product.get('colors')))
    listed_colors.discard('')
    for row in supplied:
        if not isinstance(row, dict) or any(not isinstance(row.get(field), str) or len(row[field]) > 80
                                            for field in ('color', 'size')):
            raise ValueError('بيانات اللون والقياس غير صحيحة.')
        count = row.get('quantity')
        if type(count) is not int or not 0 <= count <= 1000000:
            raise ValueError('العدد المتبقي يجب أن يكون عدداً صحيحاً من صفر إلى مليون.')
        color, size = color_key(row['color']), size_key(product, row['size'])
        if len(variants.colors(row['color'])) > 1 or (product.get('colors') and not color):
            raise ValueError('حدد لوناً واحداً لكل سطر مخزون.')
        if color and color not in listed_colors:
            raise ValueError('اختر لوناً من ألوان المنتج، أو أضف اللون إلى البطاقة أولاً.')
        if not size and (product.get('sizes') or product.get('size_system') in {'age', 'clothing', 'shoe'}):
            raise ValueError('حدد قياساً واحداً لكل سطر مخزون.')
        if size == FREE_SIZE and not variants.is_free_size_product(product):
            raise ValueError('الفري سايز لازم يكون مسجلاً ضمن قياسات المنتج.')
        if size and size not in {FREE_SIZE, WEIGHT_STOCK} and (
                not (variants.SIZE_RE.fullmatch(size) or re.fullmatch(r'\d{1,3}(?:\.5)?|\d+ شهر', size)) or
                ('شهر' not in size and variants.order_choice_error(product, 'size', row['size']))):
            raise ValueError('استخدم قياساً مفرداً من مقاسات المنتج، أو عمر القياس للأطفال بالأشهر.')
        if 'شهر' in size:
            months = int(size.split()[0])
            low, high = product.get('age_min_months'), product.get('age_max_months')
            if (not 0 <= months <= 216 or low is not None and months < low or high is not None and months > high
                    or product.get('size_system') != 'age' and
                    not any(a * 12 <= months <= b * 12 for a, b in variants.size_data(product.get('sizes'))['years'])):
                raise ValueError('العمر المدخل خارج قياسات هذا المنتج.')
        if (color, size) in normalized:
            raise ValueError('هذا اللون والقياس مكرران في المخزون.')
        normalized[color, size] = count
    return [dict(color=color, size=size, quantity=count) for (color, size), count in sorted(normalized.items())]


def replace(db, store, product_id, rows):
    keys = {(row['color'], row['size']) for row in rows}
    allocated = db.execute("""SELECT DISTINCT color,size FROM order_variant_stock
        WHERE store_id=? AND product_id=?""", (store, product_id))
    if any(tuple(row) not in keys for row in allocated):
        raise ValueError('توجد طلبات مرتبطة بهذي التركيبة. خلّ العدد صفراً حتى توقف بيعها.')
    db.execute('DELETE FROM product_variant_stock WHERE store_id=? AND product_id=?', (store, product_id))
    db.executemany('INSERT INTO product_variant_stock VALUES(?,?,?,?,?)', [
        (store, product_id, row['color'], row['size'], row['quantity']) for row in rows])


def line_key(product, item):
    color = color_key(item.get('color'))
    size = size_key(product, item.get('size'), item.get('age_months'))
    if size == WEIGHT_STOCK and not any(item.get(field) for field in ('size', 'weight', 'weight_kg')):
        size = ''
    return color, size


def choices(product, *, color='', size=''):
    color = color_key(color)
    size = size_key(product, size) if size else ''
    return [row for row in product.get('variant_stock', []) if row['quantity'] > 0
            and (not color or row['color'] == color) and (not size or row['size'] == size)]


def target(text):
    """Read explicit variant labels from the current question."""
    wanted = variants.requested(text)
    size = wanted['sizes'][0] if len(wanted['sizes']) == 1 else ''
    color = wanted['colors'][0] if len(wanted['colors']) == 1 else ''
    return color, size


def availability_error(text, context):
    if not context:
        return False
    available = context['available']
    for clause in re.split(r'[.،؛\n]+', variants.normalized(text)):
        if clause.rstrip().endswith(('؟', '?')):
            continue
        if not re.search(r'متوفر|موجود|اكدر احجز|اكو', clause) or re.search(
                r'(?<!\w)(?:مو|ما|غير|لا|ماكو|نفذ|خلص)(?!\w)', clause):
            continue
        named = variants.requested(clause)
        colors = named['colors'] or ([context['color']] if context['color'] else [])
        sizes = named['sizes'] or ([context['size']] if context['size'] else [])
        # A color by itself can be confirmed while a size is still open.
        explicit = bool(named['colors'] or named['sizes'])
        quantity = context.get('requested_quantity') or 1
        matching = [row for row in available if (not colors or row['color'] in colors)
                    and (not sizes or row['size'] in sizes)]
        same_choice = (not explicit or (colors == [context['color']] and sizes == [context['size']]))
        if not matching or (same_choice and colors and sizes and sum(row['quantity'] for row in matching) < quantity):
            return True
    return False


def reservation_changes(db, order_id, store, items, products):
    old = {}
    if order_id is not None and db.execute("SELECT 1 FROM sqlite_master WHERE name='order_variant_stock'").fetchone():
        old = {(pid, color, size): units for pid, color, size, units in db.execute(
            'SELECT product_id,color,size,quantity FROM order_variant_stock WHERE order_id=? AND store_id=?',
            (order_id, store))}
    wanted = Counter()
    stock = rows_by_product(db, store, active_only=True)
    paused = {pid for pid, _, _ in old if pid not in stock}
    catalog = {p['product_id']: p for p in products}
    for item in items:
        pid = item.get('product_id')
        if pid not in stock and pid not in paused:
            continue
        if pid not in catalog:
            if pid in paused:
                continue
            raise StockError(pid, stock[pid])
        key = line_key(catalog[pid], item)
        count = item.get('quantity', 1)
        if type(count) is not int or count <= 0:
            raise StockError(pid, stock.get(pid, []), key)
        wanted[(pid, *key)] += count
    # During general mode, existing detailed bookings still own the pieces
    # they took. Cancelled/changed lines release them; new sales take none.
    for key in list(wanted):
        if key[0] in paused:
            if key in old:
                wanted[key] = min(wanted[key], old[key])
            else:
                del wanted[key]
    changes = {key: wanted[key] - old.get(key, 0) for key in wanted.keys() | old.keys()}
    # Validate every line before changing any counts.
    for (pid, color, size), units in changes.items():
        if units <= 0:
            continue
        count = next((row['quantity'] for row in stock[pid] if (row['color'], row['size']) == (color, size)), 0)
        stopped = (catalog[pid].get('status', 'active') != 'active'
                   or STOPPED.search(str(catalog[pid].get('stock') or '')))
        if stopped or count < units:
            raise StockError(pid, [] if stopped else stock[pid], (color, size), wanted[pid, color, size])
    return wanted, changes


def allocate(db, order_id, store, items, products):
    """Apply the difference between desired and existing reservations once."""
    wanted, changes = reservation_changes(db, order_id, store, items, products)
    if not changes:
        return
    for (pid, color, size), units in changes.items():
        if units:
            db.execute("""UPDATE product_variant_stock SET quantity=quantity-?
                WHERE store_id=? AND product_id=? AND color=? AND size=?""", (units, store, pid, color, size))
    db.execute('DELETE FROM order_variant_stock WHERE order_id=? AND store_id=?', (order_id, store))
    db.executemany('INSERT INTO order_variant_stock VALUES(?,?,?,?,?,?)', [
        (order_id, store, *key, units) for key, units in wanted.items()])


def sales_view(product):
    rows = product.get('variant_stock')
    if not rows:
        return product
    available = [row for row in rows if row['quantity'] > 0]
    colors = dict.fromkeys(row['color'] for row in available if row['color'])
    sizes = dict.fromkeys(row['size'] for row in available if row['size'])
    result = dict(product, colors='، '.join(colors),
                  stock_quantity=sum(row['quantity'] for row in available))
    if not result.get('stock') and available:
        result['stock'] = 'متوفر'
    if not available:
        result['stock'] = 'نفذ'
    if sizes and WEIGHT_STOCK not in sizes and not (
            FREE_SIZE in sizes and variants.is_free_size_product(product)) and product.get('size_system') != 'age':
        result['sizes'] = '، '.join(sizes)
    return result
