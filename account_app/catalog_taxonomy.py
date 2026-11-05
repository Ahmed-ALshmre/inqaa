"""Store-owned category and recommendation-group vocabulary."""
import hashlib
import json
import re
import uuid
try:
    from . import catalog_profile
except ImportError:
    import catalog_profile


def init_db(db):
    db.execute('''CREATE TABLE IF NOT EXISTS catalog_classifications (
        store_id TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL,
        name TEXT NOT NULL, family TEXT NOT NULL DEFAULT '',
        size_system TEXT NOT NULL DEFAULT 'legacy', active INTEGER NOT NULL DEFAULT 1,
        aliases TEXT NOT NULL DEFAULT '[]', PRIMARY KEY(store_id,kind,id))''')


def name_key(value):
    return re.sub(r'\s+', ' ', str(value or '')).strip().casefold().translate(str.maketrans('أإآة', 'اااه'))


def listing(db, store_id, products=()):
    init_db(db)
    categories={key:dict(id=key,name=name,family=key,size_system=(
        'shoe' if key=='shoes' else 'none' if key in {'bags','accessories'} else 'legacy'),active=True,aliases=[])
        for key,name in catalog_profile.TYPES.items()}
    groups={}
    for product in products:
        if product.get('store_id','default')!=store_id:continue
        key=product.get('product_type') or ''
        if catalog_profile.custom_type(key) and product.get('product_type_label'):
            categories.setdefault(key,dict(id=key,name=product['product_type_label'],
                family=catalog_profile.family(product),size_system=product.get('size_system') or 'legacy',active=True,aliases=[]))
        for name in product.get('recommendation_groups') or []:
            key='group_'+hashlib.sha256(name_key(name).encode()).hexdigest()[:24]
            groups.setdefault(key,dict(id=key,name=name,family='',size_system='legacy',active=True,aliases=[]))
    records={'category':categories,'group':groups}
    rows=db.execute('SELECT * FROM catalog_classifications WHERE store_id=?',(store_id,)).fetchall()
    for row in rows:
        record=dict(row);kind=record.pop('kind');record.pop('store_id')
        record.update(active=bool(record['active']),aliases=json.loads(record['aliases']))
        if kind in records:records[kind][record['id']]=record
    # A renamed group keeps its identity and its old names continue to resolve.
    persisted={row['id'] for row in rows if row['kind']=='group'}
    alias_keys={name_key(alias):r['id'] for r in groups.values() if r['id'] in persisted for alias in [r['name'],*r['aliases']]}
    groups={key:r for key,r in groups.items() if name_key(r['name']) not in alias_keys or alias_keys[name_key(r['name'])]==key}
    for kind,items in (('category',categories),('group',groups)):
        for record in items.values():
            names={name_key(record['name']),*(name_key(x) for x in record['aliases'])}
            record['product_count']=sum(1 for p in products if p.get('store_id','default')==store_id and (
                p.get('product_type')==record['id'] if kind=='category' else
                any(name_key(n) in names for n in p.get('recommendation_groups') or [])))
    return dict(categories=sorted(categories.values(),key=lambda r:r['name']),
                groups=sorted(groups.values(),key=lambda r:r['name']))


def hydrate(products, registry):
    categories={r['id']:r for r in registry['categories']}
    names={name_key(name):r['name'] for r in registry['groups'] for name in [r['name'],*r['aliases']]}
    result=[]
    for product in products:
        record=categories.get(product.get('product_type'))
        item=dict(product)
        if record:item.update(product_type_label=record['name'],product_family=record['family'])
        item['recommendation_groups']=list(dict.fromkeys(names.get(name_key(n),n) for n in item.get('recommendation_groups') or []))
        result.append(item)
    return result


def validate_product(product, registry, existing=None):
    key=product.get('product_type') or ''
    if key:
        record=next((r for r in registry['categories'] if r['id']==key),None)
        if not record or (not record['active'] and key!=(existing or {}).get('product_type')):
            raise ValueError('اختَر فئة مفعّلة من صفحة الفئات والمجموعات.')
        product=dict(product,product_type_label=record['name'],product_family=record['family'])
    return product if key else dict(product,product_type_label='',product_family='')


def save(db,store_id,kind,data,products,identity=None):
    if kind not in {'category','group'}:raise ValueError('نوع التصنيف غير صحيح.')
    registry=listing(db,store_id,products)
    items=registry['categories' if kind=='category' else 'groups']
    previous=next((r for r in items if r['id']==identity),None)
    if identity and not previous:raise LookupError('التصنيف غير موجود في هذا المتجر.')
    data=dict(previous or {},**data)
    name=data.get('name')
    if not isinstance(name,str) or not name.strip() or len(name.strip())>60 or re.search(r'[\n\r\x00-\x1f]',name):
        raise ValueError('اكتب اسم الفئة أو المجموعة في سطر واحد، حتى 60 حرفاً.')
    name=' '.join(name.split())
    if any(r['id']!=identity and name_key(name) in {name_key(x) for x in [r['name'],*r['aliases']]} for r in items):
        raise ValueError('هذا الاسم موجود مسبقاً في المتجر.')
    if any(key in data and data[key] is not None and not isinstance(data[key],str) for key in ('family','size_system')):
        raise ValueError('نوع المنتج أو طريقة المقاس غير صحيحة.')
    family=data.get('family') or 'other'
    mode=data.get('size_system') or 'legacy'
    active=data.get('active',True)
    if type(active) is not bool:raise ValueError('حالة الظهور يجب أن تكون نعم أو لا.')
    if kind=='category':
        if not isinstance(family,str) or not isinstance(mode,str) or family not in catalog_profile.TYPES or mode not in catalog_profile.SIZE_SYSTEMS:
            raise ValueError('نوع المنتج أو طريقة المقاس غير صحيحة.')
        if identity in catalog_profile.TYPES and family!=identity:
            raise ValueError('يمكن تعديل اسم الفئة الأساسية أو إنشاء فئة جديدة من النوع المطلوب.')
        if previous and previous['product_count'] and family!=previous['family']:
            raise ValueError('لا يمكن تغيير نوع فئة مرتبطة بمنتجات؛ أنشئ فئة جديدة وانقل المنتجات المطلوبة.')
        if family=='shoes' and mode not in {'shoe','legacy'}:
            raise ValueError('الأحذية تحتاج قياس الحذاء، وليس العمر أو الوزن.')
    else:family,mode='','legacy'
    identity=identity or ('cat_' if kind=='category' else 'group_')+uuid.uuid4().hex
    aliases=list((previous or {}).get('aliases') or [])
    if previous and name_key(previous['name'])!=name_key(name):aliases.append(previous['name'])
    with db:
        db.execute('''INSERT INTO catalog_classifications VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(store_id,kind,id) DO UPDATE SET name=excluded.name,family=excluded.family,
            size_system=excluded.size_system,active=excluded.active,aliases=excluded.aliases''',
            (store_id,kind,identity,name,family,mode,int(active),json.dumps(list(dict.fromkeys(aliases)),ensure_ascii=False)))
    return identity


def remove(db,store_id,kind,identity,products):
    registry=listing(db,store_id,products)
    record=next((r for r in registry['categories' if kind=='category' else 'groups'] if r['id']==identity),None)
    if not record:raise LookupError('التصنيف غير موجود في هذا المتجر.')
    if record['product_count']:raise ValueError('التصنيف مرتبط بمنتجات؛ يمكنك إخفاؤه من الاختيارات الجديدة بدلاً من حذفه.')
    # Tombstones also hide built-in and imported labels without changing products.
    save(db,store_id,kind,dict(record,active=False),products,identity)
