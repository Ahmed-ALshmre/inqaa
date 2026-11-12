"""Parse catalog choices independently of prose used to guide staff."""
import re
try:
    from .customer_copy import normalized
except ImportError:
    from customer_copy import normalized

COLORS = {
    'أسود': ('اسود', 'اسودا', 'black'), 'أبيض': ('ابيض', 'white'),
    'أحمر': ('احمر', 'red'), 'أزرق ملكي': ('ازرق ملكي', 'ملكي'),
    'أزرق': ('ازرق', 'blue'), 'سماوي': ('سماوي', 'سماي', 'سمائي'),
    'وردي': ('وردي', 'pink'), 'بيج': ('بيج', 'بيجي', 'beige'),
    'زيتي': ('زيتي', 'زيتوني'), 'بنفسجي': ('بنفسجي', 'purple'),
    'أخضر': ('اخضر', 'اخظر', 'green'), 'جوزي': ('جوزي', 'بني', 'brown'),
    'رصاصي': ('رصاصي', 'رمادي', 'gray', 'grey'), 'نيلي': ('نيلي', 'كحلي', 'navy'),
    'كريمي': ('كريمي', 'كريم', 'cream'), 'ماروني': ('ماروني', 'maroon'),
    'تركوازي': ('تركوازي', 'تركواز', 'turquoise'), 'أصفر': ('اصفر', 'yellow'),
    'خردلي': ('خردلي', 'mustard'),
    'برتقالي': ('برتقالي', 'orange'), 'ذهبي': ('ذهبي', 'gold'),
    'فضي': ('فضي', 'silver'), 'خمري': ('خمري', 'عنابي'),
}
ALIASES = {alias: color for color, aliases in COLORS.items() for alias in aliases}
COLOR_RE = re.compile(r'(?<!\w)(?:و?ال|و?لل|و?ل|و)?(' + '|'.join(re.escape(x) for x in sorted(ALIASES,key=len,reverse=True)) + r')(?!\w)', re.I)
SIZE_RE = re.compile(r'(?<!\w)(?:[2-6]xl|xxxl|xxl|xl|xs|s|m|l)(?!\w)',re.I)
FREE_SIZE_RE = re.compile(r'(?<!\w)(?:فري\s*سايز|free\s*size|قياس\s*موحد|مقاس\s*موحد)(?!\w)', re.I)
RANGE_RE = re.compile(r'(\d{1,3})\s*(?:كيلو\s*)?(?:الي|[-–—]|لحد)\s*(\d{1,3})')
WIDTH_REQUEST_PATTERN = (r'(?<!\w)(?:و?ال)?عرض(?:ه|ها)(?!\w)|'
                        r'(?<!\w)و?العرض\s+(?:شكد|كم|اشكد)(?!\w)|'
                        r'عرض\s+(?:القطعة|الصدر|الورك|الخصر|الكتف)|'
                        r'عرض.{0,25}(?<!\w)(?:سم|سانتي|سنتيمتر)(?!\w)|تحت الابط|محيط الصدر')


ADULT_LABELS = ('S', 'M', 'L', 'XL', 'XXL', '3XL', '4XL', '5XL')


def normalize_size_words(value):
    text = str(value or '').translate(str.maketrans('٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹', '01234567890123456789'))
    text = re.sub(r'(?<!\w)2\s*xl(?!\w)', 'XXL', text, flags=re.I)
    text = re.sub(r'(?<!\w)xxxl(?!\w)', '3XL', text, flags=re.I)
    for pattern, label in (
        (r'(?<!\w)(?:[2٢]\s*اكس(?:ات)?(?:\s+لارج)?|اكسين(?:\s+لارج)?|اكس\s+اكس(?:\s+لارج)?|(?:دبل|دوبل|دابل|تو)\s*اكس(?:ات)?(?:\s+لارج)?)(?!\w)', 'XXL'),
        (r'(?<!\w)(?:[3٣]|ثلاث[ةه]?)\s*اكس(?:ات)?(?:\s+لارج)?(?!\w)', '3XL'),
        (r'(?<!\w)(?:[4٤]|اربع[ةه]?)\s*اكس(?:ات)?(?:\s+لارج)?(?!\w)', '4XL'),
        (r'(?<!\w)(?:[5٥]|خمس[ةه]?)\s*اكس(?:ات)?(?:\s+لارج)?(?!\w)', '5XL'),
        (r'(?<!\w)(?:[6٦]|ست[ةه]?)\s*اكس(?:ات)?(?:\s+لارج)?(?!\w)', '6XL'),
        (r'(?<!\w)(?:[1١]\s*اكس(?:\s+لارج)?|اكس\s*لا(?:ر)?ج|لكس\s*لا(?:ر)?ج|اكسترا لارج)(?!\w)', 'XL')):
        pattern = pattern.replace(r'(?<!\w)', r'(?<!\w)(?:و)?(?:(?:بال|ال)\s*)?', 1)
        pattern = pattern.replace('اكس', '[اأإ]كس')
        text = re.sub(pattern, label, text, flags=re.I)
    return re.sub(r'(?<!\w)(?:بال|ال)?(?:لا?رج|large)(?!\w)', 'L', text, flags=re.I)


def size_choice_text(value):
    """Keep the new label in an explicit correction, without guessing alternatives."""
    text = normalized(normalize_size_words(value))
    accepted = re.fullmatch(r'(\d{2})\s+(?:هاي|هذه)\s*(?:هي|هيا)', text.strip())
    if accepted:
        return 'قياس ' + accepted[1]
    if re.search(r'[؟?]|(?<!\w)(?:اذا|لو|او)(?!\w)', text):
        return text
    atom = r'(?:[2-6]xl|xxl|xl|xs|[sml]|\d{2})(?!\w)'
    label = r'(?:(?:ال)?(?:قياس|مقاس|سايز)\s*)?'
    # "46 بدل 44" and "قياس 46 مو 44" explicitly reject the second label.
    pattern = r'(?<!\w)' + label + '(' + atom + r')\s+(?:بدل|مو|وليس)\s+' + label + atom
    matches = list(re.finditer(pattern, text, re.I))
    if len(matches) == 1:
        hit = matches[0]
        return text[:hit.start()] + 'قياس ' + hit[1] + text[hit.end():]
    # "غيري قياس 44 إلى 46" explicitly chooses the second label.
    pattern = r'(?:غيري|بدلي|عدلي)\s+' + label + atom + r'\s+(?:الي|لـ?|الى)\s*(' + atom + ')'
    matches = list(re.finditer(pattern, text, re.I))
    if len(matches) == 1:
        hit = matches[0]
        return text[:hit.start()] + 'قياس ' + hit[1] + text[hit.end():]
    return text


def normalize_age_words(value):
    """Written numbers need an age unit; prices and unrelated counts stay intact."""
    text = normalized(value)
    # Iraqi declarations also put the unit first: "سنه 3، سنه 5".
    # Do not let the standalone word "سنة" become a separate one-year child.
    def unit_first(hit):
        if not hit[1] and re.search(r'\d[ \t]*\Z', text[:hit.start()]):
            return hit[0]  # In "3 سنوات 5 سنوات", the unit belongs to the 3.
        return hit[1] + hit[3] + ' ' + hit[2]
    text = re.sub(r'(?<!\w)(و?)(سن[هة]|سنوات|سنين|اشهر|شهور|شهر)\s*(\d{1,2}(?:\.\d+)?)(?![\d.])',
                  unit_first, text)
    text = re.sub(r'([لب]?عمر(?:ه|ها)?\s*\d{1,2}(?:\.\d+)?)(?![\d.])\s*و\s*(نص|نصف|ربع)(?!\w)',
                  r'\1 سنوات و\2', text)
    # "عندي طفلين، الأول 3 ونص والثاني 5" and "واحد 5 ونص
    # والثاني 6 سنوات" refer to ages, not an unlabeled clothing size or price.
    age_context = bool(re.search(r'طفلين|طفلتين|اولادي|بناتي|عمر|سنوات|سنين|سن[هة]', text))
    if age_context and not re.search(r'الف|دينار|سعر|مجموع|اشهر|شهور|شهر', text):
        def ordinal_age(hit):
            tail = text[hit.end():]
            if re.match(r'\s*(?:سنوات|سنين|سن[هة]|اشهر|شهور|شهر|الف|دينار)', tail):
                return hit[0]
            return hit[1] + ' عمر ' + hit[2] + ' سنوات' + ((' و' + hit[3]) if hit[3] else '')
        text = re.sub(r'(?<!\w)(و?(?:الاول[ىي]?|الثاني[هة]?|واحد[هة]?|وحد[هة]))\s+(\d{1,2}(?:\.\d+)?)(?![\d.])'
                      r'(?:\s*و\s*(نص|نصف))?', ordinal_age, text)
    words = {'ثلاث': 3, 'ثلاثه': 3, 'اربع': 4, 'اربعه': 4, 'خمس': 5, 'خمسه': 5,
             'ست': 6, 'سته': 6, 'سبع': 7, 'سبعه': 7, 'ثمان': 8, 'ثماني': 8,
             'ثمانيه': 8, 'تسع': 9, 'تسعه': 9, 'عشر': 10, 'عشره': 10}
    words.update({word[:-1] + 'ة': number for word, number in list(words.items()) if word.endswith('ه')})
    text = re.sub(r'((?:قياس|عمر(?:ه|ها)?)\s+)ال(سنتين|سنتان|سن[هة]|شهرين|شهر)(?!\w)',r'\1\2',text)
    text = re.sub(r'(?<!\w)(و?)(?:ال)?(' + '|'.join(sorted(words, key=len, reverse=True)) +
                  r')(?=\s*(?:سنوات|سنين|سن[هة]|اشهر|شهور|شهر)(?!\w))',
                  lambda hit: hit[1] + str(words[hit[2]]), text)
    # In a correction the rejected value often omits its repeated unit:
    # "أربع سنوات مو ثلاث، والثانية ست سنوات". Resolve only that local
    # rejection, never arbitrary unlabelled numbers elsewhere in the message.
    old_number=r'(?:\d{1,2}|'+'|'.join(sorted(words,key=len,reverse=True))+')'
    text=re.sub(r'(\d{1,2}\s+(سنوات|سنين|سن[هة]|اشهر|شهور|شهر)\s+(?:مو|بدل|وليس)\s+)('
                +old_number+r')(?=\s*(?:[،,.!?؟]|$|و(?=\w)|الثاني|الثانية))',
                lambda h:h[1]+str(words.get(h[3],h[3]))+' '+h[2],text)
    return text


def age_choice_text(value):
    """Remove only a rejected age, retaining every other child's declaration."""
    text = normalize_age_words(value)
    future = re.search(r'(?:بعدل[هة]|باقيل[هة]|بعد)\s*(شهرين|شهر|\d{1,2}\s*(?:اشهر|شهور|شهر))'
                       r'\s*(?:يلا\s*)?يصير\s*((?:\d{1,2}\s*(?:سنوات|سن[هة]|سنين)|سن[هة]|سنتين)'
                       r'(?:\s*و\s*(?:نص|نصف|ربع|\d{1,2}\s*(?:اشهر|شهر)|شهرين))?)', text)
    if future:
        duration, target = child_age(future[1]), child_age(future[2])
        if duration is not None and target is not None and 0 < target - duration < 18:
            text = text[:future.start()] + 'عمر ' + normalized(age_text(target - duration)) + text[future.end():]
    if re.search(r'[؟?]|(?<!\w)(?:اذا|لو|او)(?!\w)', text):
        return text
    atom = r'(?:\d{1,2}(?:\.\d+)?\s*(?:سنوات|سنين|سن[هة]|اشهر|شهور|شهر)|سنتين|سنتان|سن[هة])'
    return re.sub(r'(?<!\w)(?:عمر(?:ه|ها)?\s*)?(' + atom + r')\s+(?:بدل|مو|وليس)\s*(?:عمر(?:ه|ها)?\s*)?' + atom + r'(?!\w)',
                  lambda hit: 'عمر ' + hit[1], text)


def child_age_correction(value):
    text = normalize_age_words(value)
    if age_choice_text(text) == text:
        return None
    atom = r'(?:\d{1,2}(?:\.\d+)?\s*(?:سنوات|سنين|سن[هة]|اشهر|شهور|شهر)|سنتين|سنتان|سن[هة])'
    hit = re.search('(' + atom + r')\s+(?:بدل|مو|وليس)\s*(?:عمر(?:ه|ها)?\s*)?(' + atom + ')', text)
    return (child_age(hit[1]), child_age(hit[2])) if hit else None


def chosen_child_size(value):
    text = normalized(value)
    if not re.search(r'سن[هة]|سنتين|سنوات|سنين|شهر|اشهر|اعوام', text):
        return ''
    if not re.search(r'قياس|(?:صور\w*\s+)?مال\s*(?:ال)?(?:سنتين|سن[هة]|\d)', text):
        return ''
    text = normalize_age_words(value)
    text = re.sub(r'مال\s*(?:ال)?(?=سنتين|سن[هة]|\d)', 'قياس ', text)
    ages = child_ages(text)
    return age_text(ages[0]) if len(ages) == 1 else ''


def is_free_size_product(product):
    return bool(FREE_SIZE_RE.search(normalized((product or {}).get('sizes'))))


def owner_adult_labels(product):
    """Store-owner stock declaration; never a universal weight conversion."""
    try:
        from . import catalog_profile
    except ImportError:
        import catalog_profile
    if catalog_profile.shoe(product) or (product or {}).get('audience') in {'boys', 'girls', 'children'} or (product or {}).get('size_system') == 'none':
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


def nearby_color(wanted, available):
    """Suggest an explicitly named alternative; never normalize it into a choice."""
    groups = (('خمري','ماروني','أحمر'),('سماوي','تركوازي','أزرق'),('زيتي','أخضر'),
              ('كريمي','بيج','أبيض'),('رصاصي','فضي'))
    if wanted in available:
        return ''
    return next((color for group in groups if wanted in group for color in group if color in available),'')


def chosen_colors(value):
    """A positive color list is a choice; alternatives and availability questions are not."""
    text = normalized(value)
    if re.search(r'[؟?]|(?<!\w)(?:لو|او|بين)(?!\w)|/|متوفر|موجود|اكو|شنو|يا لون', text):
        return []
    return requested(text)['colors']


def color_replacement(value):
    """Read an explicit replacement or one specific replacement question, never infer one."""
    text = normalized(value).strip(' .!؟?')
    text = re.sub(r'[.،,\n]\s*(?:(?:ثبتي|احجزي) (?:وحدة|وحده|قطعة)|الثانية (?:تبقى|تبقي) نفس ما اتفقنا)[.! ]*$', '', text)
    text = re.split(r'[.\n]', text)[-1].strip(' .!؟?')
    text = re.sub(r'^لا لحظة[،, ]+|^لا لحظه[،, ]+', '', text)
    text = re.sub(r'\s+بالقطعة (?:الاولى|الاولي) بس$', '', text)
    hits = list(COLOR_RE.finditer(text))
    if len(hits) != 2 or not re.fullmatch(r'\s*بدل\s*', text[hits[0].end():hits[1].start()]):
        return None
    if not re.fullmatch(r'(?:تحبين|تحبون|تحب|تريدين|تريدون|تريد|خلي|خليها|خليلي|بدلي|اريد)\s*', text[:hits[0].start()]):
        return None
    if text[hits[1].end():].strip():
        return None
    return ALIASES[hits[0][1]], ALIASES[hits[1][1]]


def child_age(value):
    """Read the whole age expression; years and months belong to the same child."""
    text = age_choice_text(value)
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


def child_age_mentions(value):
    """Keep year-and-month expressions together, including in multi-child turns."""
    text = age_choice_text(value)
    unit = r'(?:سنوات|سنين|سن[هة]|اعوام|عام|اشهر|شهور|شهر)'
    text = re.sub(r'\d+\s*(?:' + unit + r')?\s*(?:الي|لحد|[-–—])\s*\d+\s*' + unit, '', text)
    text = re.sub(r'سن[هة]\s*(?:الي|لحد)\s*سنتين', '', text)
    amount = r'(?:\d{1,2}(?:\.\d+)?\s*' + unit + r'|سنتين|سنتان|عامين|سن[هة]|شهرين|شهر)'
    extra = r'(?:\s*و\s*(?:\d+\s*(?:اشهر|شهور|شهر)|شهرين|شهر|نصف|نص|ربع))?'
    expression = r'(?:(?:[لب]?عمر(?:ه|ها)?\s*)?' + amount + extra + r'|[لب]?عمر(?:ه|ها)?\s*\d{1,2}(?:\.\d+)?)'
    ages = []
    for hit in re.finditer(r'(?<!\w)(?:و)?' + expression + r'(?=\W|$|و)', text):
        age = child_age(hit[0][1:] if hit[0].startswith('و') else hit[0])
        if age is not None:
            ages.append(age)
    return ages


def child_ages(value):
    """Distinct requested ages; quantity-bearing callers can retain mentions."""
    return list(dict.fromkeys(child_age_mentions(value)))


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
    text=normalized(normalize_size_words(value))
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
    labels=list(dict.fromkeys([m[0].upper() for m in SIZE_RE.finditer(text)]+
        re.findall(r'(?<!\d)\d{2}\.5(?!\d)',text)))
    numbers=[]
    if re.fullmatch(r'[\d\s،,./–—\-]+',text):
        numbers=[int(n) for n in re.findall(r'\d+',re.sub(r'\d{2}\.5','',text))
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


def canonical_weight_size(product, size, weight):
    """A catalog weight band is guidance; the declared kg value is the order unit."""
    bounds = weight_only_ranges(product)
    value = normalized(weight).strip()
    if not bounds or not re.fullmatch(r'\d{1,3}(?:\.\d{1,2})?', value):
        return size
    if not any(lo <= float(value) <= hi for lo, hi in bounds):
        return size
    supplied = size_data(size)
    if supplied['weights'] and supplied['weights'] == bounds:
        return value
    return size


_WEIGHT_TENS={'خمسين':50,'ستين':60,'سبعين':70,'ثمانين':80,'تسعين':90}
_WEIGHT_UNITS={'واحد':1,'واحده':1,'واحدة':1,'اثنين':2,'اثنان':2,'ثنين':2,
               'ثلاث':3,'ثلاثه':3,'ثلاثة':3,'اربع':4,'اربعه':4,'اربعة':4,
               'خمس':5,'خمسه':5,'خمسة':5,'ست':6,'سته':6,'ستة':6,
               'سبع':7,'سبعه':7,'سبعة':7,'ثمان':8,'ثمانيه':8,'ثمانية':8,
               'تسع':9,'تسعه':9,'تسعة':9}


def _written_weight_values(text):
    """Parse common Iraqi written kilogram values only inside weight answers."""
    tens='|'.join(_WEIGHT_TENS)
    units='|'.join(_WEIGHT_UNITS)
    values=[]
    occupied=[]
    for match in re.finditer(rf'(?<!\w)({units})\s*و\s*({tens})(?!\w)',text):
        values.append(_WEIGHT_UNITS[match[1]]+_WEIGHT_TENS[match[2]])
        occupied.append(match.span())
    for match in re.finditer(rf'(?<!\w)({tens})(?:\s*و\s*({units}))?(?!\w)',text):
        if any(start<=match.start()<end for start,end in occupied):continue
        values.append(_WEIGHT_TENS[match[1]]+(_WEIGHT_UNITS.get(match[2],0) if match[2] else 0))
    for match in re.finditer(r'(?<!\w)(ميه|مية|مئه|مئة)(?:\s*و\s*(عشره|عشرة|عشرون|عشرين))?(?!\w)',text):
        values.append(100+(20 if match[2] in {'عشرون','عشرين'} else 10 if match[2] else 0))
    return list(dict.fromkeys(values))


def weight_answer(value, question=''):
    """Return a declared kilogram value, or an explicit unresolved answer."""
    text=normalized(value).strip()
    written=_written_weight_values(text)
    explicit=bool(re.search(r'وزن(?:ي|ه|ها)?\s*[:=]?\s*\d|\d\s*(?:كيلو|كغم|كغ)',text)
                  or written and re.search(r'وزن|كيلو|كغم|كغ',text))
    bare=bool(re.fullmatch(r'\d{1,3}(?:[.,]\d{1,2})?',text) and
              re.search(r'وزن|كيلو|كغم',normalized(question)) and
              not re.search(r'عمر|عدد|كمية|قطعة',normalized(question)))
    if not explicit and not bare:return None
    if re.search(r'\d\s*(?:لو|او|/|الي|لحد|[-–—])\s*\d',text):
        return {'value':None,'ambiguous':True}
    if written:
        if len(written)!=1 or re.search(r'(?<!\w)(?:لو|او)(?!\w)',text):
            return {'value':None,'ambiguous':True}
        return {'value':str(written[0]),'ambiguous':False}
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
    text=size_choice_text(question)
    sizes=list(dict.fromkeys(m[1].upper() for m in re.finditer(
        r'(?:قياس(?:ي|ه|ها)?|مقاس(?:ي|ه|ها)?|سايز)\s*(?:هو\s+|هي\s+)?(\d{1,3}(?:\.5)?|[2-6]xl|xxxl|xxl|xl|xs|s|m|l)(?![\w.])',text)))
    # A narrow stock question supplies a preferred garment size, never consent.
    # Do not read an amount, weight, age or size range as a clothing size.
    stock_size=re.fullmatch(r'(?:متوفر[هة]?|موجود[هة]?|اكو)\s*(\d{2}(?:\.5)?)\s*[؟?!.،]*|'
                           r'(\d{2}(?:\.5)?)\s*(?:متوفر[هة]?|موجود[هة]?|اكو)\s*[؟?!.،]*',text.strip())
    if stock_size:
        sizes=list(dict.fromkeys(sizes+[stock_size[1] or stock_size[2]]))
    sizes = list(dict.fromkeys(sizes + [m[0].upper() for m in SIZE_RE.finditer(text)]))
    if FREE_SIZE_RE.search(text) and 'فري سايز' not in sizes:
        sizes.append('فري سايز')
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
        if size == 'فري سايز' and is_free_size_product(product):
            supported = True
        elif (numeric_known if size.isdigit() else bool(data['labels'])):
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
        from . import customer_copy,catalog_profile
    except ImportError:
        import customer_copy,catalog_profile
    text=normalized(value)
    if field=='color':
        offered=colors(customer_copy.fact(product.get('colors'),'colors'))
        choices=colors(text)
        if offered and (not choices or any(c not in offered and not (c=='أزرق' and 'أزرق ملكي' in offered) for c in choices)):
            return 'اللون '+str(value)+' مو موجود بهالموديل، المتوفر '+ '، '.join(offered)
    else:
        data=size_data(customer_copy.fact(product.get('sizes'),'sizes'))
        if FREE_SIZE_RE.fullmatch(text) and is_free_size_product(product):
            return ''
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
        if not data['known'] or (text.isdigit() and not (data['ranges'] or data['numbers']) and not catalog_profile.shoe(product)):
            return ''  # Weight/age guidance is not a garment size inventory.
        numeric=re.fullmatch(r'\d{1,3}',text)
        if numeric:
            valid=int(text) in data['numbers'] or any(lo<=int(text)<=hi for lo,hi in data['ranges'])
        else:
            valid=text.upper() in data['labels']
        if not valid:
            return 'قياس '+str(value)+' مو موجود بهالموديل، قياساته '+customer_copy.fact(product.get('sizes'),'sizes')
    return ''
