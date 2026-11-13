"""Single factual view over product fields, measurements and store policies."""
import re
try:
    from .. import sales_answers,sales_strategy,customer_copy
except ImportError:
    import sales_answers,sales_strategy,customer_copy

PRODUCT_FACT_FIELDS=('fabric','description','notes','colors','sizes','price','offer','product_type','size_system','stock',
                     'length','width','stretch','lining','closure','cut','care','season','fabric_feel','origin','composition','faq')
POLICY_FIELDS=('inspection_policy','delivery_time','delivery_fee','payment','shop_address')


def product_fact(product,field):
    if field=='sizes' and product.get('size_system')=='none':return dict(field='size_system',value='none')
    if field=='fabric':value=customer_copy.fact(product.get('fabric'))
    elif field in {'length','width'}:value=sales_strategy.measurement_fact(product,field)
    elif field=='season':value=sales_answers.season_fact(product)
    elif field=='fabric_feel':value=sales_answers.fabric_feel_fact(product)
    elif field=='care':value=sales_answers.care_fact(product)
    elif field=='composition':value=customer_copy.fact(product.get('bundle_contents'),'bundle_contents')
    elif field=='origin':
        value=product.get('country_of_origin') or product.get('origin')
        if not value:
            match=re.search(r'(?:صناع[ةه]|منش[اأ]|بلد\s*الصنع)\s*[:：]?\s*[^.،\n]{2,55}',str(product.get('notes') or '')+'\n'+str(product.get('description') or ''))
            value=match[0] if match else None
    else:value=sales_answers.field_fact(product,field)
    if not value and field in {'fabric_feel','season'}:
        recorded='\n'.join(customer_copy.fact(product.get(k)) for k in ('fabric','fit_notes','notes','description'))
        pattern=r'ثخين|ثقيل|سميك|خفيف|بارد|ناعم' if field=='fabric_feel' else r'شتوي|صيفي|بهاري|خريفي'
        value='، '.join(line for line in re.split(r'[.\n،]',recorded) if len(line)<=220 and re.search(pattern,line))
    return dict(field=field,value=value) if value not in (None,'') else None


def resolve(catalog,product_id,field,policy):
    if product_id=='@store':
        value=policy.get(field) if field in POLICY_FIELDS else None
        return dict(field=field,value=value) if value not in (None,'') else None
    return product_fact(catalog.get(product_id),field)


def requested_fields(text):
    mapping={'price':'price','colors':'colors','sizes':'sizes','fabric':'fabric','length':'length','width':'width','stretch':'stretch','lining':'lining',
             'closure':'closure','cut':'cut','care':'care','season':'season','fabric_feel':'fabric_feel',
             'composition':'composition','inspection':'inspection_policy','payment':'payment',
             'delivery_time':'delivery_time','delivery':'delivery_fee','location':'shop_address'}
    fields=[mapping[t] for t in sales_answers.topics(text) if t in mapping]
    clean=customer_copy.normalized(text)
    if weight_limit_question(clean):
        fields.append('sizes')
    from .contact import checkout_inspection_question
    if any(checkout_inspection_question(line) for line in clean.splitlines()):
        fields.append('inspection_policy')
    if re.search(r'(?:ال)?توصيل\s+(?:بلاش|مجاني)|(?:بلاش|مجاني)\s+(?:ال)?توصيل', clean):
        fields.append('delivery_fee')
    if re.search(r'صناعته|صناعتها|منشا|صيني\s*لو\s*تركي|تركي\s*لو\s*صيني',clean):fields.append('origin')
    if re.search(r'بس\s*هو.{0,20}(?:جوار|جنط)|تجي.{0,20}(?:وياه|هديه)',clean):fields.append('composition')
    return list(dict.fromkeys(fields))


def weight_limit_question(text):
    return bool(re.search(r'(?:اعلى|اقصى|اكثر|اكبر)\s+وزن|(?:لحد|حدود)\s+(?:كم|شكد)\s+(?:وزن|كيلو)', text))


def only_same_day_question(text):
    from .intent import constraint, purchase_selected
    from ..sales_playbook import human_requested
    return (sales_answers.same_day_delivery(text) and not human_requested(text)
            and not purchase_selected(text) and not constraint(text)
            and set(requested_fields(text)) <= {'delivery_time'})


def alternatives(catalog,focus,text, *, requested=False):
    clean=customer_copy.normalized(text)
    if not requested and not re.search(r'بديل|موديل\s*ثاني|موديلات\s*اخر|غير\s*موديل',clean):return None
    from ..product_albums import stock_state
    from ..product_links import _category
    source=catalog.get(focus) if focus else {}
    category = _category(source) if source else None
    wanted='صيف' if 'صيف' in clean else 'شتو' if 'شتو' in clean else None
    length=product_fact(source,'length') if 'الطول' in clean else None
    matches=[]
    for product in catalog.products.values():
        if product['product_id']==focus or product.get('status','active')!='active' or stock_state(product)!='available':continue
        if category and _category(product) != category:continue
        if source.get('audience') and product.get('audience') not in (None, '', source['audience'], 'all', 'unisex'):continue
        season=product_fact(product,'season')
        if wanted and (not season or wanted not in season['value']):continue
        candidate_length=product_fact(product,'length')
        if length and (not candidate_length or candidate_length['value']!=length['value']):continue
        matches.append({k:product.get(k) for k in ('product_id','reply_category','product_type_label','colors','sizes','price','offer')})
    return dict(matches=matches[:5],searched=True,requested_season=wanted,same_length=bool(length))


def choice_alternatives(catalog,feedback):
    """Offer only catalog alternatives that accept the rejected selection."""
    from ..product_albums import stock_state
    source=catalog.get(feedback['product_id']);matches=[]
    requested=feedback.get('requested')
    if requested is None:return dict(matches=[],searched=False)
    for product in catalog.products.values():
        if product['product_id']==source['product_id'] or product.get('status','active')!='active':continue
        if stock_state(product)!='available' or product.get('selection_mode')!=source.get('selection_mode'):continue
        if source.get('product_type') and product.get('product_type')!=source['product_type']:continue
        if source.get('audience') and product.get('audience') not in (source['audience'],'all','unisex'):continue
        line=dict(product_id=product['product_id'],size_system=product.get('size_system') or 'legacy',quantity=1,color='',size='')
        line[feedback['field']]=requested
        try:catalog.validate(line)
        except ValueError:continue
        matches.append(dict(product_id=product['product_id'],label=customer_copy.reply_category(product) or 'موديل ثاني',
                            sizes=product.get('sizes'),colors=product.get('colors'),price=product.get('price')))
    return dict(matches=matches[:5],searched=True,requested=requested,never_substitute_without_consent=True)
