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
    sizes = request['sizes']
    age = requested_age(product, question)
    ages = child_ages(question) if data['years'] and not data['known'] else []
    if age is not None and (not sizes or re.search(
            r'(?:قياس|مقاس|سايز)\s*\d+\s*(?:اشهر|شهر|شهور)', normalized(question))):
        sizes = [str(int(item)) if float(item).is_integer() else str(item) for item in (ages or [age])]
    for size in sizes:
        supported=None
        age_based = bool(data['years'] and not data['known'])
        if data['known']:
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
