"""Parse catalog choices independently of prose used to guide staff."""
import re
try:
    from .customer_copy import normalized
except ImportError:
    from customer_copy import normalized

COLORS = {
    'أسود': ('اسود', 'اسودا', 'black'), 'أبيض': ('ابيض', 'white'),
    'أحمر': ('احمر', 'red'), 'أزرق ملكي': ('ازرق ملكي', 'ملكي'),
    'أزرق': ('ازرق', 'blue'), 'سماوي': ('سماوي', 'سماي'),
    'وردي': ('وردي', 'pink'), 'بيج': ('بيج', 'بيجي', 'beige'),
    'زيتي': ('زيتي', 'زيتوني'), 'بنفسجي': ('بنفسجي', 'purple'),
    'أخضر': ('اخضر', 'اخظر', 'green'), 'جوزي': ('جوزي', 'بني', 'brown'),
    'رصاصي': ('رصاصي', 'رمادي', 'gray', 'grey'), 'نيلي': ('نيلي', 'كحلي', 'navy'),
    'كريمي': ('كريمي', 'كريم', 'cream'), 'ماروني': ('ماروني', 'maroon'),
    'تركوازي': ('تركوازي', 'تركواز', 'turquoise'), 'أصفر': ('اصفر', 'yellow'),
    'برتقالي': ('برتقالي', 'orange'), 'ذهبي': ('ذهبي', 'gold'),
    'فضي': ('فضي', 'silver'), 'خمري': ('خمري', 'عنابي'),
}
ALIASES = {alias: color for color, aliases in COLORS.items() for alias in aliases}
COLOR_RE = re.compile(r'(?<!\w)(?:و?ال|و?لل|و?ل|و)?(' + '|'.join(re.escape(x) for x in sorted(ALIASES,key=len,reverse=True)) + r')(?!\w)', re.I)
SIZE_RE = re.compile(r'(?<!\w)(?:[2-6]xl|xxxl|xxl|xl|xs|s|m|l)(?!\w)',re.I)
RANGE_RE = re.compile(r'(\d{1,3})\s*(?:كيلو\s*)?(?:الي|[-–—]|لحد)\s*(\d{1,3})')


def colors(value):
    return list(dict.fromkeys(ALIASES[m[1]] for m in COLOR_RE.finditer(normalized(value))))


def size_data(value):
    text=normalized(value)
    ranges, years, weights = [], [], []
    spans=[]
    for match in RANGE_RE.finditer(text):
        lo,hi=int(match[1]),int(match[2])
        if lo>hi:
            continue
        before=text[max(0,match.start()-60):match.start()]
        inside=text[match.start():match.end()]
        after=text[match.end():match.end()+15]
        explicit=bool(re.search(r'(?:قياس(?:ات)?|مقاس(?:ات)?)\s*(?:هذا المنتج\s*)?(?:من\s*)?$',before))
        if re.match(r'\s*(?:سن[ةه]|سنوات|اعوام|عام)',after):
            years.append((lo,hi))
        elif not explicit and ('كيلو' in inside or re.search(r'وزن\s*(?:من\s*)?$',before)
                               or re.match(r'\s*(?:كيلو|كغم|كغ)',after)):
            weights.append((lo,hi))
        elif explicit or (lo>=10 and hi<=80 and not re.search(r'وزن|كيلو|سنوات',text)):
            ranges.append((lo,hi))
        spans.append(match.span())
    labels=list(dict.fromkeys(m[0].upper() for m in SIZE_RE.finditer(text)))
    numbers=[]
    if re.fullmatch(r'[\d\s،,./–—\-]+',text):
        numbers=[int(n) for n in re.findall(r'\d+',text)
                 if 10<=int(n)<=80]
    return {'ranges':ranges,'numbers':numbers,'labels':labels,'years':years,'weights':weights,
            'known':bool(ranges or numbers or labels)}


def display(value,field):
    if field=='colors':
        return '، '.join(colors(value))
    data=size_data(value)
    result=[f'{lo} إلى {hi}' for lo,hi in data['ranges']]
    result += [str(n) for n in data['numbers'] if not any(lo<=n<=hi for lo,hi in data['ranges'])]
    result += data['labels']
    if result:
        return '، '.join(dict.fromkeys(result))
    if data['years']:
        return '، '.join(f'{lo} إلى {hi} سنوات' for lo,hi in data['years'])
    if data['weights']:
        return 'يناسب تقريباً وزن ' + '، '.join(f'{lo} إلى {hi} كيلو' for lo,hi in data['weights'])
    return ''


def requested(question):
    text=normalized(question)
    sizes=list(dict.fromkeys(m[1].upper() for m in re.finditer(
        r'(?:قياس(?:ه|ها)?|مقاس(?:ه|ها)?|سايز)\s*(\d{1,3}|[2-6]xl|xxxl|xxl|xl|xs|s|m|l)(?!\w)',text)))
    choices=[]
    for match in COLOR_RE.finditer(text):
        before=text[max(0,match.start()-22):match.start()]
        if re.search(r'(?:مو|لا|ما)\s*(?:اريد\s*)?(?:اللون\s*)?$',before):
            continue
        # "غير لون الأسود متوفر؟" asks for another color. The imperative
        # "غيري اللون للأسود" still selects black and must not be excluded.
        if re.search(r'(?<!\w)(?:ب?غير|عدا|باستثناء|سوى)\s*(?:(?:ال)?لون\s*)?$', before):
            continue
        after = text[match.end():match.end()+24]
        if re.match(r'\s+(?:ما\s*اريد(?:ه|ها)?|مو\s+مرغوب)(?:\s|[،,.!?؟]|$)', after):
            continue
        color=ALIASES[match[1]]
        if color not in choices:
            choices.append(color)
    return {'sizes':sizes,'colors':choices}


def assess(product,question):
    request=requested(question)
    # Strip staff guidance before parsing to avoid learning facts from examples.
    try:
        from . import customer_copy
    except ImportError:
        import customer_copy
    data=size_data(customer_copy.fact(product.get('sizes'),'sizes'))
    offered=colors(customer_copy.fact(product.get('colors'),'colors'))
    result=[]
    for size in request['sizes']:
        supported=None
        if data['known']:
            supported=(int(size) in data['numbers'] or any(lo<=int(size)<=hi for lo,hi in data['ranges'])) if size.isdigit() else size in data['labels']
        result.append({'field':'size','value':size,'supported':supported})
    for color in request['colors']:
        supported=color in offered if offered else None
        if color=='أزرق' and 'أزرق ملكي' in offered:
            supported=True
        result.append({'field':'color','value':color,'supported':supported})
    return result


def is_lookup(question):
    text=normalized(question)
    return (any(requested(question).values())
            and bool(re.search(r'متوفر|موجود|اكو|عندكم|عندج|عندك|[؟?]',text))
            and not re.search(r'احجز|ثبت|طلبية|توصيل|فيديو|قماش|خامة|مناسب|وزن|عمر|طول|عرض|غالي|ارخص|ميزاني|خصم|تخفيض',text))


def reply_error(reply,product,question):
    """Reject explicit positive claims about a missing requested variant."""
    text=normalized(reply)
    for check in assess(product,question):
        if check['supported'] is not False:
            continue
        target=(r'(?:قياس|مقاس)\s*'+re.escape(check['value'].lower()) if check['field']=='size'
                else r'(?:اللون\s*)?(?:ال)?'+re.escape(normalized(check['value'])))
        for match in re.finditer(target+r'[^.؟?\n،؛]{0,35}(?:متوفر|موجود|عدنا)|(?:متوفر|موجود|عدنا)\s*(?:ب)?'+target,text):
            before=text[max(0,match.start()-10):match.start()]
            if not re.search(r'\b(?:مو|ما|غير|ليس|لا)\b',before+' '+match[0]):
                return 'الخيار المطلوب غير موجود في بيانات هذا الموديل؛ أجب عن القياس أو اللون المطلوب دون تأكيد توفره.'
    return ''


def order_choice_error(product,field,value):
    """Reject only choices that contradict explicit, current catalog options."""
    try:
        from . import customer_copy
    except ImportError:
        import customer_copy
    text=normalized(value)
    if field=='color':
        offered=colors(customer_copy.fact(product.get('colors'),'colors'))
        choices=colors(text)
        if offered and (not choices or any(c not in offered and not (c=='أزرق' and 'أزرق ملكي' in offered) for c in choices)):
            return 'اللون '+str(value)+' مو موجود بهالموديل، المتوفر '+ '، '.join(offered)
    else:
        data=size_data(customer_copy.fact(product.get('sizes'),'sizes'))
        if not data['known']:
            return ''  # Weight/age guidance is not a garment size inventory.
        numeric=re.fullmatch(r'\d{1,3}',text)
        if numeric:
            valid=int(text) in data['numbers'] or any(lo<=int(text)<=hi for lo,hi in data['ranges'])
        else:
            valid=text.upper() in data['labels']
        if not valid:
            return 'قياس '+str(value)+' مو موجود بهالموديل، قياساته '+customer_copy.fact(product.get('sizes'),'sizes')
    return ''
