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


ADULT_LABELS = ('S', 'M', 'L', 'XL', 'XXL', '3XL', '4XL', '5XL')


def normalize_size_words(value):
    text = str(value or '')
    for pattern, label in (
        (r'(?<!\w)(?:اكسين(?:\s+لارج)?|اكس\s+اكس(?:\s+لارج)?|(?:دبل|دوبل|دابل|تو)\s*اكس(?:ات)?(?:\s+لارج)?)(?!\w)', 'XXL'),
        (r'(?<!\w)ثلاث[ةه]?\s*اكس(?:ات)?(?:\s+لارج)?(?!\w)', '3XL'),
        (r'(?<!\w)اربع[ةه]?\s*اكس(?:ات)?(?:\s+لارج)?(?!\w)', '4XL'),
        (r'(?<!\w)خمس[ةه]?\s*اكس(?:ات)?(?:\s+لارج)?(?!\w)', '5XL'),
        (r'(?<!\w)(?:اكس\s*لارج|اكسترا لارج)(?!\w)', 'XL')):
        text = re.sub(pattern, label, text, flags=re.I)
    return text


def owner_adult_labels(product):
    """Store-owner stock declaration; never a universal weight conversion."""
    if (product or {}).get('audience') in {'boys', 'girls', 'children'} or (product or {}).get('size_system') == 'none':
        return ()
    text = normalized(' '.join(str((product or {}).get(k) or '') for k in ('product_name', 'category', 'sizes')))
    if re.search(r'اطفال|طفل|بناتي|ولادي|سنوات|سنين|سن[ةه]|شهر', text):
        return ()
    sizes = size_data((product or {}).get('sizes'))
    if sizes['labels']:
        return ()  # An explicit variant inventory takes precedence over store defaults.
    adult_chart = bool(sizes['weights'] or any(lo >= 30 for lo, hi in sizes['ranges']))
    return ADULT_LABELS if 'نسائي' in text or adult_chart else ()


def colors(value):
    return list(dict.fromkeys(ALIASES[m[1]] for m in COLOR_RE.finditer(normalized(value))))


def single_color(value):
    """Accept one explicit color label, never free-form recognition prose."""
    match = COLOR_RE.fullmatch(normalized(value).strip())
    return ALIASES[match[1]] if match else ''


def chosen_colors(value):
    """A positive color list is a choice; alternatives and availability questions are not."""
    text = normalized(value)
    if re.search(r'[؟?]|(?<!\w)(?:لو|او|بين)(?!\w)|/|متوفر|موجود|اكو|شنو|يا لون', text):
        return []
    return requested(text)['colors']


def color_replacement(value):
    """Read an explicit replacement or one specific replacement question, never infer one."""
    text = re.split(r'[.\n]', normalized(value))[-1].strip(' .!؟?')
    hits = list(COLOR_RE.finditer(text))
    if len(hits) != 2 or not re.fullmatch(r'\s*بدل\s*', text[hits[0].end():hits[1].start()]):
        return None
    if not re.fullmatch(r'(?:تحبين|تحب|تريدين|تريد|خلي|خليلي|بدلي|اريد)\s*', text[:hits[0].start()]):
        return None
    if text[hits[1].end():].strip():
        return None
    return ALIASES[hits[0][1]], ALIASES[hits[1][1]]


def child_age(value):
    """Read the whole age expression; years and months belong to the same child."""
    text = normalized(value)
    if re.search(r'\d+\s*(?:سنوات|سنه|سنة|سنين|اشهر|شهر|شهور)?\s*(?:الي|لحد|[-–—])\s*\d+', text):
        return None
    if re.search(r'سن[هة]\s*(?:الي|لحد)\s*سنتين', text):
        return None
    years = re.compile(r'(?<!\w)(?:(\d+(?:\.\d+)?)\s*(?:سنوات|سن[هة]|سنين|اعوام|عام)|'
                       r'(سنتين|سنتان|عامين|سن[هة]))(?=\W|$|و)')
    months = re.compile(r'(?<!\w)(?:(\d+)\s*(?:اشهر|شهر|شهور)|(شهرين|شهر))(?!\w)')

    def read(fragment, explicit=False):
        match = years.match(fragment) if explicit else years.search(fragment)
        if match:
            age = float(match[1]) if match[1] else (2 if match[2] in {'سنتين', 'سنتان', 'عامين'} else 1)
            extra = re.match(r'\s*(?:و\s*(?:(\d+)\s*(?:اشهر|شهر|شهور)|(شهرين|شهر))|(?:و|او)\s*(نصف|نص|ربع))(?!\w)',
                             fragment[match.end():])
            if extra:
                age += (int(extra[1]) / 12 if extra[1] else
                        (2 if extra[2] == 'شهرين' else 1) / 12 if extra[2] else
                        .25 if extra[3] == 'ربع' else .5)
            return age
        match = months.match(fragment) if explicit else months.search(fragment)
        if match:
            return (int(match[1]) if match[1] else 2 if match[2] == 'شهرين' else 1) / 12
        match = re.match(r'\d+(?:\.\d+)?(?![\d\w])', fragment) if explicit else None
        return float(match[0]) if match else None

    explicit = re.search(r'عمر(?:ه|ها)?\s*', text)
    if explicit:
        age = read(text[explicit.end():].lstrip(), explicit=True)
        if age is not None:
            return age
    return read(text)


def age_text(value):
    """Customer-facing ages use years and months, never fractional-year decimals."""
    try:
        total_months = round(float(value) * 12)
    except (ValueError, TypeError, OverflowError):
        return ''
    if total_months < 1:
        return 'أقل من شهر'
    years, months = divmod(total_months, 12)
    parts = []
    if years:
        parts.append('سنة' if years == 1 else 'سنتين' if years == 2 else
                     f'{years} سنوات' if years <= 10 else f'{years} سنة')
    if months:
        parts.append('شهر' if months == 1 else 'شهرين' if months == 2 else
                     f'{months} أشهر' if months <= 10 else f'{months} شهر')
    return ' و'.join(parts)


def child_ages(value):
    """Collect distinct requested ages while keeping a year-and-month age intact."""
    text = normalized(value)
    pieces = re.split(r'[\n،؛;]+|\s*و(?=عمر|سنتين|سنتان|سن[هة](?:\W|$)|\d+\s*(?:سنوات|سن[هة]|سنين))', text)
    ages = []
    for piece in pieces:
        age = child_age(piece)
        if age is not None and age not in ages:
            ages.append(age)
    return ages


def age_wording(value):
    """Normalize fractional year labels without touching prices or kilogram weights."""
    text = re.sub(r'(?<![\w.])(\d+\.\d+)\s*(?:سنوات|سنة|سنه|سنين)(?!\w)',
                  lambda match: age_text(match[1]), str(value or ''))
    return re.sub(r'((?:عمر(?:ه|ها)?|بعمر|قياس(?:ه|ها)?)\s+)([12])\s+سنوات(?!\w)',
                  lambda match: match[1] + age_text(match[2]), text)


def year_size(value):
    """A selected year label; an age in months is never garment size N years."""
    text = normalized(value).strip()
    if re.fullmatch(r'(?:قياس|مقاس|عمر)?\s*\d+(?:\s*(?:سنوات|سنه|سنة|سنين|عام|اعوام))?', text):
        return int(re.search(r'\d+', text)[0])
    return 2 if text in {'سنتين', 'سنتان'} else None


def requested_age(product, question):
    data = _product_sizes(product)
    if not data['years'] or data['known']:
        return None
    age = child_age(question)
    if age is not None:
        return age
    sizes = requested(question)['sizes']
    return year_size(sizes[0]) if len(sizes) == 1 else None


def child_build(text):
    """Use the customer's body description, never fabric softness or a name."""
    text = normalized(text)
    if re.search(r'قماش|خامه|خامة|ملمس|فستان|موديل|ملابس|حبيب|عيني', text) and not re.search(r'جسم|بنيتي|بنتي|ابني|ولدي|طفل|عمر', text):
        return ''
    hits = []
    for kind, pattern in [('large', r'خشن[هة]?|ممتل[يئ][هة]?|متين[هة]?|سمين[هة]?'),
                          ('slim', r'ناعم[هة]?|نحيف[هة]?|ضعيف[هة]?')]:
        for match in re.finditer(r'(?<!\w)(?:و)?(?:' + pattern + r')(?!\w)', text):
            before = text[max(0, match.start()-30):match.start()]
            if re.search(r'قماش|خام[هة]|ملمس|فستان|موديل|ملابس', before) and not re.search(r'جسم|بنتي|ابني|ولدي|طفل', before):
                continue
            if not re.search(r'(?:مو|ما|غير|ليس|لا)\s*(?:كلش\s*)?$', text[max(0, match.start()-16):match.start()]):
                hits.append(kind)
    return hits[0] if len(set(hits)) == 1 else ''


def boundary_fit(product, age):
    """Owner-approved nearby fits for the 3-year and 7-year endpoints only."""
    years = _product_sizes(product)['years']
    if not years or age is None or any(lo <= age <= hi for lo, hi in years):
        return None
    low, high = min(lo for lo, hi in years), max(hi for lo, hi in years)
    if low == 3 and age == 2:
        return {'size': 3, 'build': 'large', 'condition': 'جسمه خشن شوي'}
    if high == 7 and age == 8:
        return {'size': 7, 'build': 'slim', 'condition': 'جسمه ناعم'}
    return None


def _product_sizes(product):
    try:
        from . import customer_copy
    except ImportError:
        import customer_copy
    return size_data(customer_copy.fact(product.get('sizes'), 'sizes'))


def size_data(value):
    text=normalized(value)
    # Written year endpoints carry an age unit even when the other end is numeric.
    text=re.sub(r'(?<!\w)(سن[هة]|سنتين|سنتان)\s*(?:الي|لحد|[-–—])\s*'
                r'(\d{1,2}|سنتين|سنتان)(?:\s*(?:سنوات|سن[هة]|سنين|اعوام|عام))?(?!\w)'
                r'(?!\s*(?:اشهر|شهر|شهور|كيلو|كغم|كغ))',
                lambda m: ('2' if m[1] in {'سنتين','سنتان'} else '1')+' الي '+
                          ('2' if m[2] in {'سنتين','سنتان'} else m[2])+' سنوات',text)
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
        elif re.match(r'\s*(?:اشهر|شهر|شهور)', after):
            years.append((lo / 12, hi / 12))
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
        # This canonical form is also used by fact sanitization and parsed again.
        # Keep numbers explicit here; natural year/month labels belong in replies.
        return '، '.join(f'{int(lo)} إلى {int(hi)} سنوات' if float(lo).is_integer() and float(hi).is_integer()
                         else f'{round(lo * 12)} إلى {round(hi * 12)} أشهر' for lo,hi in data['years'])
    if data['weights']:
        return 'يناسب تقريباً وزن ' + '، '.join(f'{lo} إلى {hi} كيلو' for lo,hi in data['weights'])
    return ''


def weight_only_ranges(product):
    data=_product_sizes(product or {})
    return data['weights'] if not any(data[k] for k in ('ranges','numbers','years','labels')) else []


def weight_answer(value, question=''):
    """Return a declared kilogram value, or an explicit unresolved answer."""
    text=normalized(value).strip()
    explicit=bool(re.search(r'وزن(?:ي|ه|ها)?\s*[:=]?\s*\d|\d\s*(?:كيلو|كغم|كغ)',text))
    bare=bool(re.fullmatch(r'\d{1,3}(?:[.,]\d{1,2})?',text) and
              re.search(r'وزن|كيلو|كغم',normalized(question)) and
              not re.search(r'عمر|عدد|كمية|قطعة',normalized(question)))
    if not explicit and not bare:return None
    if re.search(r'\d\s*(?:لو|او|/|الي|لحد|[-–—])\s*\d',text):
        return {'value':None,'ambiguous':True}
    hits=(re.findall(r'وزن(?:ي|ه|ها)?\s*[:=]?\s*(\d{1,3}(?:[.,]\d{1,2})?)(?!\d)',text)
          or re.findall(r'(?<!\d)(\d{1,3}(?:[.,]\d{1,2})?)\s*(?:كيلو|كغم|كغ)',text)
          or ([text] if bare else []))
    values=list(dict.fromkeys(hit.replace(',','.') for hit in hits))
    if len(values)!=1 or not 1<=float(values[0])<=300:return {'value':None,'ambiguous':True}
    return {'value':values[0],'ambiguous':False}


def quantity_number(value):
    text=normalized(value).strip(' .،!')
    words={'قطعة':1,'قطعه':1,'واحدة':1,'واحده':1,'واحد':1,'وحدة':1,'وحده':1,
           'قطعتين':2,'اثنين':2,'ثنين':2,'ثنتين':2,'ثلاث':3,'ثلاثة':3}
    if text in words:return words[text]
    hit=re.fullmatch(r'(?:عدد\s*)?(\d{1,2})(?:\s*قطع[ةه]?)?',text)
    return int(hit[1]) if hit and 1<=int(hit[1])<=20 else None


def color_quantities(value):
    text=re.sub(r'(?<=[\u0600-\u06ff])(?=\d)|(?<=\d)(?=[\u0600-\u06ff])',' ',normalized(value))
    hits=list(COLOR_RE.finditer(text));result={}
    requested_colors=requested(text)['colors']
    quantity=r'قطعتين|اثنين|ثنين|ثنتين|قطع[ةه]\s*واحد[ةه]?|قطع[ةه]|واحد[ةه]?|وحد[ةه]|\d{1,2}'
    for index,hit in enumerate(hits):
        color=ALIASES[hit[1]]
        if color not in requested_colors:continue
        tail=text[hit.end():hits[index+1].start() if index+1<len(hits) else len(text)]
        after=re.match(r'\s*(?:عدد\s*)?('+quantity+r')(?!\w)',tail)
        before=text[hits[index-1].end() if index else 0:hit.start()]
        preceding=re.search(r'(قطعتين|ثنتين|اثنين|ثنين|قطع[ةه]\s*واحد[ةه]?|قطع[ةه])\s*$',before)
        token=after[1] if after else preceding[1] if preceding else ''
        if token.startswith(('قطعة واحد','قطعه واحد')):token='واحدة'
        if not token:continue
        unresolved=bool(re.search(r'[؟?]|(?<!\w)(?:لو|او)(?!\w)|ما\s*اريد|ماريد',text))
        result[color]=None if unresolved else quantity_number(token)
    return result


def requested(question):
    text=normalized(normalize_size_words(question))
    sizes=list(dict.fromkeys(m[1].upper() for m in re.finditer(
        r'(?:قياس(?:ه|ها)?|مقاس(?:ه|ها)?|سايز)\s*(\d{1,3}|[2-6]xl|xxxl|xxl|xl|xs|s|m|l)(?!\w)',text)))
    sizes = list(dict.fromkeys(sizes + [m[0].upper() for m in SIZE_RE.finditer(text)]))
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
    data['labels'] = list(dict.fromkeys(data['labels'] + list(owner_adult_labels(product))))
    data['known'] = data['known'] or bool(data['labels'])
    offered=colors(customer_copy.fact(product.get('colors'),'colors'))
    result=[]
    sizes = request['sizes']
    age = requested_age(product, question)
    ages = child_ages(question) if data['years'] and not data['known'] else []
    if age is not None and (not sizes or re.search(
            r'(?:قياس|مقاس|سايز)\s*\d+\s*(?:اشهر|شهر|شهور)', normalized(question))):
        sizes = [str(int(item)) if float(item).is_integer() else str(item) for item in (ages or [age])]
    for size in sizes:
        supported=None
        age_based = bool(data['years'] and not data['known'])
        numeric_known = bool(data['ranges'] or data['numbers'])
        if (numeric_known if size.isdigit() else bool(data['labels'])):
            supported=(int(size) in data['numbers'] or any(lo<=int(size)<=hi for lo,hi in data['ranges'])) if size.isdigit() else size in data['labels']
        elif age_based:
            value = float(size) if re.fullmatch(r'\d+(?:\.\d+)?', size) else None
            supported = bool(value is not None and any(lo <= value <= hi for lo, hi in data['years']))
        check = {'field':'size','value':size,'supported':supported}
        if age_based:
            check['age_based'] = True
        result.append(check)
    for color in request['colors']:
        supported=color in offered if offered else None
        if color=='أزرق' and 'أزرق ملكي' in offered:
            supported=True
        result.append({'field':'color','value':color,'supported':supported})
    return result


def is_lookup(question):
    text=normalized(normalize_size_words(question))
    return (any(requested(question).values())
            and bool(re.search(r'متوفر|موجود|اكو|عندكم|عندج|عندك|[؟?]',text))
            and not re.search(r'احجز|ثبت|طلبية|توصيل|فيديو|قماش|خامة|مناسب|وزن|عمر|طول|عرض|غالي|ارخص|ميزاني|خصم|تخفيض',text))


def reply_error(reply,product,question):
    """Reject explicit positive claims about a missing requested variant."""
    text=normalized(reply)
    for check in assess(product,question):
        if check['supported'] is not False:
            continue
        target=((r'(?:قياس|مقاس|عمر)' if check.get('age_based') else r'(?:قياس|مقاس)')+r'\s*'+re.escape(check['value'].lower()) if check['field']=='size'
                else r'(?:اللون\s*)?(?:ال)?'+re.escape(normalized(check['value'])))
        if check.get('age_based') and age_text(check['value']):
            target = '(?:' + target + r'|(?:قياس|مقاس|عمر)\s*' + re.escape(normalized(age_text(check['value']))) + ')'
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
        if data['years'] and not data['known']:
            age = child_age(value)
            if age is None:
                age = year_size(value)
            if age is None or not any(lo <= age <= hi for lo, hi in data['years']):
                return 'قياس '+str(value)+' مو موجود بهالموديل، قياساته '+customer_copy.fact(product.get('sizes'),'sizes')
            return ''
        data['labels'] = list(dict.fromkeys(data['labels'] + list(owner_adult_labels(product))))
        data['known'] = data['known'] or bool(data['labels'])
        requested_range = re.fullmatch(r'(\d{2})\s*[-،,/]\s*(\d{2})', text)
        if requested_range:
            lo, hi = map(int, requested_range.groups())
            valid = lo <= hi and all(n in data['numbers'] or any(a <= n <= b for a, b in data['ranges']) for n in (lo, hi))
            return '' if valid else 'نطاق القياس المطلوب مو موجود بهالموديل'
        if not data['known'] or (text.isdigit() and not (data['ranges'] or data['numbers'])):
            return ''  # Weight/age guidance is not a garment size inventory.
        numeric=re.fullmatch(r'\d{1,3}',text)
        if numeric:
            valid=int(text) in data['numbers'] or any(lo<=int(text)<=hi for lo,hi in data['ranges'])
        else:
            valid=text.upper() in data['labels']
        if not valid:
            return 'قياس '+str(value)+' مو موجود بهالموديل، قياساته '+customer_copy.fact(product.get('sizes'),'sizes')
    return ''
