"""Deterministic grounded recovery when a model proposal cannot be executed."""
import re
from .. import checkout,product_variants,sales_playbook,sales_recovery
from .facts import requested_fields,POLICY_FIELDS
from .intent import purchase_selected


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


def catalog_photo_request(text):
    clean = checkout.normalized(text).strip(' .،!؟?')
    return sales_recovery.simple_photo_request(text) or bool(re.fullmatch(
        r'(?:اريد|وين|ممكن|دزيلي|ادزي|ارسلي|ما\s*وصل(?:ت|ني)?)\s+'
        r'(?:ال)?صور(?:ه|ها|هن|هم)?(?:\s+(?:الالوان|الموديل))?', clean))


def clothing_size_label(text):
    clean = checkout.normalized(text)
    match = re.search(r'(?<!\w)(?:قياسي|مقاسي|سايزي|البس\s+قياس)\s*[:=]?\s*(\d{2,3})(?!\d|\s*(?:كيلو|كغم))', clean)
    return match[1] if match and not product_variants.weight_answer(clean) else None


def weight_fit(product, text):
    if not product or product.get('selection_mode') != 'weight':
        return None
    ranges = product_variants.weight_only_ranges(product)
    weight = product_variants.weight_answer(text)
    value = weight and weight.get('value')
    if not weight and not clothing_size_label(text):
        match = re.search(r'(?:قياس|مقاس)\s*(\d{1,3}(?:\.\d+)?)(?![\d.])', checkout.normalized(text))
        value = match[1] if match else None
    if not value or not ranges:
        return None
    fits = any(low <= float(value) <= high for low, high in ranges)
    return dict(weight_kg=float(value), within_catalog_range=fits)
