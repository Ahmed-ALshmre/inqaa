"""Partial, store-scoped classification updates for an owner's selected products."""
import hashlib
import json
try:
    from . import catalog_profile,catalog_taxonomy,product_links
except ImportError:
    import catalog_profile,catalog_taxonomy,product_links

FIELDS={'audience','product_type','occasions','recommendation_groups','recommendation_enabled'}


def revision(products,store):
    rows=sorted((p for p in products if p.get('store_id','default')==store),key=lambda p:p['product_id'])
    return hashlib.sha256(json.dumps(rows,sort_keys=True,ensure_ascii=False,default=str).encode()).hexdigest()


def classify(products,store,data,registry):
    if not isinstance(data,dict):raise ValueError('بيانات التصنيف غير صحيحة.')
    ids=data.get('product_ids');changes=data.get('changes')
    if not isinstance(ids,list) or not ids or len(ids)>200 or any(not isinstance(i,str) or not i for i in ids):
        raise ValueError('اختر من منتج واحد إلى 200 منتج من المتجر.')
    if len(set(ids))!=len(ids):raise ValueError('قائمة المنتجات تحتوي أكواداً مكررة.')
    if not isinstance(changes,dict) or not changes or set(changes)-FIELDS:
        raise ValueError('حدد حقول التصنيف أو المجموعة المراد تعديلها فقط.')
    selected={p['product_id']:p for p in products if p.get('store_id','default')==store and p['product_id'] in ids}
    if set(selected)!=set(ids):raise ValueError('بعض المنتجات غير موجودة في هذا المتجر؛ حدّث القائمة.')
    result=[]
    for original in products:
        if original.get('store_id','default')!=store or original['product_id'] not in selected:
            result.append(original);continue
        updated=dict(original)
        for key,value in changes.items():
            if key in {'occasions','recommendation_groups'}:
                if not isinstance(value,list) or any(not isinstance(v,str) for v in value):raise ValueError('اختر الاستخدامات والمجموعات كنصوص.')
                if any(not v.strip() or len(v)>60 or any(ord(c)<32 for c in v) for v in value):raise ValueError('اكتب أسماء قصيرة في سطر واحد.')
                value=[' '.join(v.split()) for v in value]
                if key=='recommendation_groups':
                    aliases={catalog_taxonomy.name_key(n):r['name'] for r in registry['groups'] for n in [r['name'],*r['aliases']]}
                    value=[aliases.get(catalog_taxonomy.name_key(v),v) for v in value]
                mode=data.get(key+'_mode','add')
                if not isinstance(mode,str) or mode not in {'add','replace','remove'}:raise ValueError('طريقة تعديل القائمة غير صحيحة.')
                old=list(updated.get(key) or [])
                updated[key]=list(dict.fromkeys(old+value)) if mode=='add' else value if mode=='replace' else [v for v in old if v not in value]
            else:
                if key=='recommendation_enabled' and type(value) is not bool:raise ValueError('حدد حالة الاقتراح بنعم أو لا.')
                if key!='recommendation_enabled' and not isinstance(value,str):raise ValueError('اختر الجمهور والنوع من القائمة.')
                updated[key]=value
        updated=catalog_taxonomy.validate_product(updated,registry,original)
        normalized=catalog_profile.normalize(updated,strict=True)
        # Only classification fields change. Normalization must not rewrite an
        # unrelated size range, price, image, ad identifier or store metadata.
        for key in FIELDS|{'product_type_label','product_family'}:updated[key]=normalized.get(key)
        result.append(updated)
    return result


def preview(source,products,data):
    audience=data.get('audience') or source.get('audience') or ''
    if not isinstance(audience,str) or audience and audience not in catalog_profile.AUDIENCES:raise ValueError('اختر جمهوراً صحيحاً.')
    months=data.get('age_months');age=None
    if months not in (None,''):
        if type(months) is not int or not 0<=months<=216:raise ValueError('العمر بالأشهر من 0 إلى 216.')
        age=months/12
    size=data.get('size') or ''
    if not isinstance(size,str) or len(size)>30:raise ValueError('اكتب القياس المطلوب فقط.')
    if age is not None and size.strip():raise ValueError('حدد عمر الطفل أو قياس الملابس، وليس الاثنين معاً.')
    occasion=data.get('occasion') or ''
    if not isinstance(occasion,str) or occasion and occasion not in catalog_profile.OCCASIONS:raise ValueError('اختر استخداماً صحيحاً.')
    return product_links.eligible_related_products(source,products,age_years=age,size=size.strip(),audience=audience,
        occasions=[occasion] if occasion else [])
