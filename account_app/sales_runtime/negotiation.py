"""Requested-only discounts within the owner's 2,000 IQD multi-item authority."""
import re
from ..checkout import normalized,is_confirmation,PROVINCES


def request(text):
    value=normalized(text)
    if not re.search(r'سوي|احسب|خلي|نزل|خصم|تخفيض|بشرط|مايصير|احجز|ثبت',value):return None
    if re.search(r'وزن|كيلو|عمر|سنوات|قياس',value) and not re.search(r'سعر|دينار|الف|خصم|تخفيض',value):return None
    discount=re.search(r'(?:خصم|نزل\w*|تخفيض)\s*(?:لنا|الي|النا|لي)?\s*(الفين|الف|\d+(?:[.,]\d+)?)',value)
    if discount:
        amount={'الفين':2000,'الف':1000}.get(discount[1])
        if amount is None:
            amount=float(discount[1].replace(',','.'))
            if amount<1000:amount*=1000
        if amount>0 and amount==int(amount):return dict(amount=int(amount),basis='discount',text=text)
    match=re.search(r'ب\s*(\d+(?:[.,]\d+)?)',value)
    if not match:return None
    if not re.search(r'سعر|دينار|الف|خصم|تخفيض|سوي|احسب|نزل',value):
        if not re.search(r'(?:احجز\w*|ثبت\w*|خلي\w*)\s*(?:الطلب|الحجز|هذني|هذوله|هن|ها)?\s*ب\s*\d',value):return None
    amount=float(match[1].replace(',','.'))
    if amount<1000:amount*=1000
    if amount<=0 or amount!=int(amount):return None
    return dict(amount=int(amount),basis='unit' if re.search(r'الوحد[هة]|للقطع[هة]',value) else 'total',text=text)


def adjust(state,quote):
    ask=state.get('price_request')
    if not ask:return quote
    count=sum(i['quantity'] for i in quote['items'])
    base=quote['product_total'] if ask['basis'] in {'unit','discount'} else quote.get('total')
    target=(ask['amount']*count if ask['basis']=='unit' else base-ask['amount'] if ask['basis']=='discount' else ask['amount'])
    if base is None:return dict(quote,request_satisfied=False,requested_total=target)
    wanted=max(0,base-target)
    discount=min(wanted,2000,quote['product_total']) if count>1 else 0
    quote=dict(quote,base_product_total=quote['product_total'],discount=discount,
               requested_total=target,request_satisfied=wanted<=discount)
    if discount:
        # Proportional allocation keeps a cheap line nonnegative in a mixed cart.
        pieces=[]
        for item in quote['items']:
            for _ in range(item['quantity']):
                reduction=discount*item['unit_price']//quote['product_total']
                pieces.append(dict(item,quantity=1,unit_price=item['unit_price']-reduction,price_basis='negotiated_discount'))
        remainder=discount-(quote['product_total']-sum(i['unit_price'] for i in pieces))
        for item in pieces:
            if remainder and item['unit_price']>0:item['unit_price']-=1;remainder-=1
        if remainder:raise ValueError('discount_allocation_failed')
        priced={}
        for item in pieces:
            key=(item['line_id'],item['unit_price'])
            if key in priced:priced[key]['quantity']+=1
            else:priced[key]=item
        quote['items']=list(priced.values())
        quote['product_total']-=discount
        if quote.get('total') is not None:quote['total']-=discount
    return quote


def settle(state,quote,last_reply,text):
    if not state.get('price_request') or not quote:return
    pending=[c for c in state['conditions'] if c.get('kind')=='price']
    if not pending and not quote.get('request_satisfied'):
        state['conditions'].append(dict(kind='price',status='pending',text=state['price_request'].get('text','')))
        pending=True
    if not pending:return
    if (not quote.get('request_satisfied') and last_reply.get('question_field')=='price.accept'
            and last_reply.get('quote_total')==quote.get('total') and is_confirmation(text)):
        state['price_request']=dict(amount=quote['total'],basis='total',text=text)
        quote['request_satisfied']=True
    if quote.get('request_satisfied'):
        state['conditions']=[c for c in state['conditions'] if c.get('kind')!='price']


def destination(text):
    value=normalized(text)
    candidates=[province for province,aliases in PROVINCES.items() if any(
        re.search(r'(?<!\w)(?:ل|ب|بال)?'+re.escape(normalized(alias))+r'(?!\w)',value) for alias in aliases)]
    return candidates[0] if len(candidates)==1 else None
