"""Deterministic grounded recovery when a model proposal cannot be executed."""
import re
from .. import checkout,product_variants,sales_playbook
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


def safe_text(context):
    """Last-resort wording from verified facts only; never an action assertion."""
    if context.get('business_feedback'):
        available=context['business_feedback'].get('available')
        body='حبيبتي هالاختيار مو متوفر، المتوفر '+str(available)
    elif context.get('facts'):
        def words(fact):
            if fact['field']=='price' and re.fullmatch(r'\d+',str(fact['value'])):
                from ..customer_copy import chat_amount
                return 'السعر '+chat_amount(int(fact['value']))+' دينار'
            if fact['field']=='size_system' and fact['value']=='none':return 'ما تحتاج لا وزن ولا قياس'
            if fact['field']=='delivery_fee' and isinstance(fact['value'],dict):
                amounts=set(fact['value'].values())
                return 'التوصيل '+str(next(iter(amounts)))+' دينار' if len(amounts)==1 else 'أجور التوصيل تختلف حسب المحافظة'
            return str(fact['value']).replace('؟','').replace('?','')
        body='إي حبيبتي، '+'، '.join(words(f) for f in context['facts'])
    else:body='صار عيوني 🌷'
    questions={'contact.phone':'باقي بس رقم موبايلج حتى أثبتلج الحجز.',
               'contact.address':'باقي بس عنوانج كامل حتى أثبتلج الحجز.',
               'contact.province':'شنو المحافظة حبيبتي؟',
               'contact.details':'دزيلي رقمج وعنوانج حتى أثبتلج الحجز.',
               'quote.province':'التوصيل لأي محافظة حبيبتي؟',
               'product':'أي قطعة تقصدين حبيبتي؟ دزيلي صورتها أو اسمها.'}
    question=context.get('question_field')
    if question in questions:body+=' '+questions[question]
    elif question and question.startswith('alternative.'):body+=' تحبين تشوفين البديل حبيبتي؟'
    elif question and question.endswith('.color'):body+=' يا لون عجبج حبيبتي؟'
    elif question and question.endswith(('.size','.age_months','.weight_kg')):body+=' شنو القياس اللي تختارينه حبيبتي؟'
    return [dict(type='text',text=body)]
