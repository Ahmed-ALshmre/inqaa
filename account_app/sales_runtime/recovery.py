"""Deterministic grounded recovery when a model proposal cannot be executed."""
import re
from .. import catalog_profile,checkout,product_variants,sales_playbook
from ..sales_recovery import simple_photo_request as catalog_photo_request
from .facts import requested_fields,POLICY_FIELDS
from .intent import constraint, direct_color, direct_clothing_size, purchase_selected


def grounded_plan(context):
    state=context['state'];text=context['event'].get('text','')
    reference=context.get('reference',{})
    product_id=(reference.get('product_id') if reference.get('status')=='known' else None) or state.get('focus_product_id')
    if not product_id and len({r['product_id'] for r in state['cart']})==1:product_id=state['cart'][0]['product_id']
    product=next((p for p in context['products'] if p['product_id']==product_id),None)
    facts=[];changes=[]
    if product:
        wanted=product_variants.requested(text)
        values=dict(product_id=product_id)
        if len(wanted['colors'])==1:values['color']=wanted['colors'][0]
        if len(wanted['sizes'])==1:values['size']=wanted['sizes'][0]
        age=product_variants.child_age(text) if product.get('selection_mode')=='age' else None
        if age is not None:values.update(age_months=round(age*12));values.pop('size',None)
        weight=product_variants.weight_answer(text) if product.get('selection_mode')=='weight' else None
        if weight and weight.get('value') is not None:values['weight_kg']=float(weight['value'])
        quantity=sales_playbook.requested_quantity(text)
        if quantity:values['quantity']=quantity
        if len(values)>1:changes=[dict(values=values)]
        for field in requested_fields(text):
            facts.append(dict(product_id='@store' if field in POLICY_FIELDS else product_id,field=field))
    for field in requested_fields(text):
        if field in POLICY_FIELDS and not any(f['field']==field for f in facts):facts.append(dict(product_id='@store',field=field))
    fields=checkout.contact_fields(text,state['contact'])
    contact={k:v for k,v in fields.items() if k in {'phone','address'}}
    return dict(intent='buy' if purchase_selected(text) else 'continue',changes=changes,contact=contact,
                answer=dict(facts=facts,question_field=None,goal='answer_and_advance'))


def recoverable_turn(context):
    """Continue with server tools when the provider cannot interpret a simple turn."""
    event = context['event']
    text = event.get('text', '')
    reference = context.get('reference', {})
    focus = reference.get('product_id') if reference.get('status') == 'known' else None
    state = context['state']
    focus = focus or state.get('focus_product_id')
    fields = set(requested_fields(text))
    wanted = product_variants.requested(text)
    if (focus and not state.get('order_id') and not constraint(text) and purchase_selected(text)
            and fields <= {'colors', 'sizes'}
            and not re.search(r'[؟?]|(?<!\w)(?:لو|موجود|متوفر|خصم|تخفيض|بدلي|غيري)(?!\w)', text)
            and len(wanted['colors']) == len(wanted['sizes']) == 1
            and len(state['cart']) <= 1 and all(line['product_id'] == focus for line in state['cart'])):
        plan = grounded_plan(context)
        if plan['changes']:
            return plan
    if fields and not purchase_selected(text):
        plan = grounded_plan(context)
        # A question about stock or price is not a request to change an order.
        plan['changes'] = []
        return plan
    if focus and 'price' in fields and re.search(r'اذا|لو|خصم|تخفيض', checkout.normalized(text)):
        return dict(intent='continue')
    if (focus and not state.get('order_id') and len(state['cart']) <= 1
            and all(line['product_id'] == focus for line in state['cart'])
            and (direct_color(text) or direct_clothing_size(text))):
        return grounded_plan(context)
    product = next((product for product in context['products'] if product['product_id'] == focus), None)
    if not purchase_selected(text) and weight_fit(product, text):
        return grounded_plan(context)
    if not text.strip() and event.get('media_kind') in {'text', 'file'}:
        return dict(intent='continue')
    return None


def clothing_size_label(text):
    clean = checkout.normalized(product_variants.normalize_size_words(text))
    if product_variants.weight_answer(clean):
        return None
    match = re.search(r'(?<!\w)(?:قياس(?:ي)?|مقاس(?:ي)?|سايز(?:ي)?)\s*[:=]?\s*(\d{2,3})(?!\d|\s*(?:كيلو|كغم))', clean)
    labels = list(dict.fromkeys(m[0] for m in product_variants.SIZE_RE.finditer(clean)))
    if len(labels) == 1:
        return labels[0].upper()
    return match[1] if match else None


def weight_fit(product, text, question='', *, saved_weight=None):
    if not product or product.get('selection_mode') != 'weight':
        return None
    ranges = product_variants.weight_only_ranges(product)
    weight = product_variants.weight_answer(text, question)
    value = weight.get('value') if weight else saved_weight
    if not value or not ranges:
        return None
    fits = any(low <= float(value) <= high for low, high in ranges)
    return dict(weight_kg=float(value), within_catalog_range=fits)


def browsing_photo_request(current, recent):
    """Resolve a request for several catalog options from the customer’s recent turns."""
    clean = checkout.normalized(current)
    if re.search(r'(?:ما|لا)\s*(?:اريد|تدز|ترسل)|احجز|ثبت|اشتري', clean):
        return None
    clauses = clean.splitlines()
    plural = r'صور(?:ت)?(?:هن|هم)(?!\w)|صور\s+(?:التنانير|الموديلات)'
    if not any(catalog_photo_request(line) and re.search(plural, line) for line in clauses):
        return None
    customer_texts = [message.get('text', '') for message in recent if message['actor'] == 'customer']
    category = catalog_profile.query_type(current)
    request = []
    for text in reversed(customer_texts[-8:]):
        previous_type = catalog_profile.query_type(text)
        if previous_type and category and previous_type != category:
            break
        category = category or previous_type
        request.insert(0, text)
    if not category:
        return None
    text = '\n'.join(request)
    length = skirt_length_request(text) if category == 'skirts' else None
    return dict(category=category, length=length, occasions=catalog_profile.query_occasions(text),
                colors=next((product_variants.colors(line) for line in reversed(request)
                             if product_variants.colors(line)), []))


def skirt_length_request(text):
    """The Iraqi garment term «تسعيني» describes length in skirt shopping."""
    clean = checkout.normalized(text)
    matches = list(re.finditer(r'(?<!\w)تسعين[يهة](?!\w)|(?:طولها|طوله|طول|الطول)\s*(\d{2,3})(?!\d)', clean))
    return int(matches[-1][1] or 90) if matches else None
