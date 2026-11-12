"""Verified inputs and output checks for generated sales wording."""
import re

from .. import checkout, customer_copy, product_variants


SLOT = re.compile(r'\{\{([a-z_]+)\}\}')
RAW_MONEY = re.compile(
    r'\d[\d,.٬٫]*\s*(?:الف|الاف|دينار)|(?<!\w)(?:الفين|الفان)(?!\w)|'
    r'(?<!\w)(?:عشر(?:ين|ون|[هة])|[ثت]لاثين|اربعين|خمسين|ستين|سبعين|ثمانين|تسعين|'
    r'واحد|ثلاث|اربع|خمس|ست|سبع|ثمان|تسع|مي[هة]|مائ[هة])\s+(?:الف|الاف|دينار)(?!\w)'
)
FREE_SIZE_WORDS = re.compile(r'فري\s*سايز|free\s*size|(?:ال)?(?:قياس|مقاس)\s+(?:ال)?موحد')


def mentions_free_size(text):
    return bool(FREE_SIZE_WORDS.search(checkout.normalized(text)))


def quote_amounts(quote, compact=False):
    discount = quote.get('discount', 0)
    total = quote.get('total')
    if compact and total is not None:
        return {'total': total, **({'discount': discount} if discount else {})}
    amounts = {'subtotal': quote.get('base_product_total', quote['product_total'])}
    if quote.get('delivery_fee') is not None:
        amounts['delivery_fee'] = quote['delivery_fee']
    if discount:
        amounts['discount'] = discount
    if total is not None:
        amounts['total'] = total
    elif discount:
        amounts['discounted_subtotal'] = quote['product_total']
    return amounts


def render_amounts(parts, amounts):
    """Substitute the server's figures into model-authored prose."""
    seen = []
    rendered = []
    for part in parts:
        if part.get('type') != 'text':
            rendered.append(part)
            continue
        text = part['text']
        if amounts and RAW_MONEY.search(checkout.normalized(text)):
            raise ValueError('reply.money: use the approved amount tokens instead of writing numbers')

        def replace(match):
            name = match[1]
            if name not in amounts or name in seen:
                raise ValueError('reply.money: unknown or repeated amount token')
            seen.append(name)
            amount = amounts[name]
            if name == 'delivery_fee' and amount == 0:
                return 'مجاناً'
            return 'ألفين' if amount == 2000 else customer_copy.chat_amount(amount)

        text = SLOT.sub(replace, text)
        if '{{' in text or '}}' in text:
            raise ValueError('reply.money: malformed amount token')
        rendered.append(dict(part, text=text))
    if set(seen) != set(amounts):
        raise ValueError('reply.money: include every required amount token exactly once')
    return rendered


def next_options(product, question):
    if not product or not question:
        return []
    if question.endswith('.color'):
        return product_variants.colors(product.get('colors'))[:2]
    if question.endswith('.size') and product.get('selection_mode') not in {'age', 'weight'}:
        sizes = product_variants.size_data(product.get('sizes'))
        return (sizes['labels'] + [str(value) for value in sizes['numbers']])[:2]
    return []


def validate_next_step(parts, step):
    """Check the planned request in a question or a natural Iraqi imperative."""
    question = step['field']
    if not question:
        return
    texts = [part['text'] for part in parts if part.get('type') == 'text']
    marks = sum(len(re.findall(r'[؟?]+', text)) for text in texts)
    if not texts or marks > 1 or marks and not re.search(r'[؟?]+[^\w؟?]*$', texts[-1]):
        raise ValueError('reply.next_step: end with one request about the planned field')
    last = checkout.normalized(re.split(r'[؟?]', texts[-1])[0]).strip().rstrip('.!')
    last = re.split(r'[.!\n]', last)[-1]
    if not marks and not re.search(
            r'دز|ارسل|نط|عط|قولي|[گك]ولي|خبر|باقي|اختار|تختار|حدد|تحدد|شنو|شكد|وين|'
            r'يا\s*لون|اي\s*(?:لون|قياس|محافظ)|تحب|تريد|يناسب|نمشي|نكمل|موافق', last):
        raise ValueError('reply.next_step: make the next request clear')
    required = {
        'contact.phone': (r'رقم|موبايل|تلفون|هاتف',),
        'contact.address': (r'عنوان|منطق|وين.*(?:اوصل|نوصل|توصيل)',),
        'contact.details': (r'رقم|موبايل|تلفون|هاتف', r'عنوان|منطق'),
        'contact.province': (r'محافظ|مدين',),
        'quote.province': (r'محافظ|مدين',),
        'price.accept': (r'سعر|حساب|مجموع|عرض|يناسب|موافق|نكم[لّ]|نمشي|اوك[ي]?',),
        'purchase_intent': (r'ثبت|حجز|نسجل|اكمل|نكمل|طلب|رتب',),
        'product': (r'صور|سكرين|لقط',),
    }
    field = question.rsplit('.', 1)[-1]
    if field == 'color' and question not in required:
        if not re.search(r'لون', last) and not set(step['options']).intersection(product_variants.colors(last)):
            raise ValueError('reply.next_step: ask about the color or its verified options')
    if field == 'size' and not re.search(r'قياس|مقاس|سايز', last):
        if not any(re.search(r'(?<!\w)' + re.escape(checkout.normalized(option)) + r'(?!\w)', last)
                   for option in step['options']):
            raise ValueError('reply.next_step: ask about the size or its verified options')
    if not all(re.search(pattern, last) for pattern in required.get(question, ())):
        raise ValueError('reply.next_step: ask for ' + question + ' in the final question')


def checkout_question(field, gender, *, final_detail):
    welcome = 'حبيبتي' if gender == 'female' else 'عيني'
    labels = {'contact.phone': 'رقم الموبايل', 'contact.address': 'العنوان',
              'contact.province': 'المحافظة', 'contact.details': 'رقم الموبايل والعنوان'}
    request = 'باقي بس' if final_detail else 'دزّيلي' if gender == 'female' else 'دزلي'
    return f'{welcome}، {request} {labels[field]} حتى أثبت الحجز.'
