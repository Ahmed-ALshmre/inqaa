"""Keep catalog facts separate from instructions meant for staff or models."""
import re
import unicodedata


INTERNAL = re.compile(
    r'اعتمد(?:ي)?\s+(?:الجدول|هذه|هذا|التعليمات)|عند\s+الرد\s+عل[ىي]\s+الزبون[ةه]?|'
    r'(?<!\w)(?:قولي|قول|قل|اخبر(?:ي)?|خبري|ابلغ(?:ي)?|اسال(?:ي)?|اطلب(?:ي)?|'
    r'اذكر(?:ي)?|اكتب(?:ي)?|استخدم(?:ي)?|اعرض(?:ي)?|قدم(?:ي)?|اقترح(?:ي)?|وضح(?:ي)?|'
    r'تجنب(?:ي)?|تاكد(?:ي)?|اجب|اجيبي|جاوب(?:ي)?)(?!\w).{0,100}?'
    r'(?:(?:ال|لل?)زبون[ةه]?|(?:ال|لل?)عميل[ةه]?|الزباين|العملاء|المستخدم)|'
    r'عل[ىي]\s+(?:المساعد|الموظف|البوت|النظام)|'
    r'(?:يجب\s+عل[ىي]|لازم)\s+(?:المساعد|الموديل|الموظف|البوت|النظام)|'
    r'عل[ىي]\s+الموديل\s+(?:ان|طلب|يكتب|يرد|الرد|الرئيسي)|'
    r'(?<!\w)لا\s*(?:تذكر(?:ي)?|تطلب(?:ي)?|تخبر(?:ي)?|ترسل(?:ي)?|تقول(?:ي)?|توضح(?:ي)?)'
    r'\s*(?:ها|هم|هن|لها|له|لل?زبون[ةه]?|التعليمات)|'
    r'تعليمات\s+(?:داخلية|داخليه|النظام|للموديل|للمساعد|للموظف)|'
    r'\b(?:fix_instruction|create_order|reply_parts|image_index)\b|'
    r'\b(?:system\s+prompt|developer\s+instructions|ignore\s+(?:all\s+)?(?:previous|prior)\s+instructions)\b|'
    r'\b(?:tell|ask|instruct|inform)\s+(?:the\s+)?(?:customer|client|user|assistant)\b', re.I | re.S)

DIGITS = str.maketrans('٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹أإآى', '01234567890123456789اااي')
FABRIC_WORDS = set(('قماش القماش خام خامة الخامه الخامة قطن قطني قطنية تركي تركية '
                   'فرسان درجة اولى ثانية لينن باربي فور وي ستن ساتان ايطالي ايطالية '
                   'كريب حرير حريري مخمل قديفة قطيفة دانتيل شيفون جينز دنيم صوف بوليستر فسكوز '
                   'ناعم ناعمة مريح مريحة عملي عملية خفيف خفيفة ثقيل ثقيلة شتوي شتوية '
                   'صيفي صيفية بهاري بهارية خريفي خريفية دبل دابل مطاط مطاطي مطاطية ليكرا مع من و').split())


def normalized(value):
    return _normalized_positions(value)[0]


def _normalized_positions(value):
    chars, positions = [], []
    for index, original in enumerate(str(value or '')):
        for char in unicodedata.normalize('NFKC', original).translate(DIGITS):
            if unicodedata.category(char) in {'Mn', 'Cf'} or char == 'ـ':
                continue
            chars.append(char.lower())
            positions.append(index)
    return ''.join(chars), positions


FABRIC_WORDS = {normalized(word) for word in FABRIC_WORDS}


def chat_amount(amount):
    """Exact Iraqi chat notation, without currency abbreviations."""
    amount = int(amount)
    thousands, remainder = divmod(amount, 1000)
    if not thousands:
        return str(amount)
    unit = 'آلاف' if 3 <= thousands <= 10 else 'ألف'
    return f'{thousands} {unit}' + (f' و{remainder}' if remainder else '')


def chat_money(text):
    def replace(match):
        number = match[1].translate(DIGITS).replace(',', '').replace('٬', '')
        return chat_amount(int(number))
    return re.sub(r'(?<![\d,٬])([0-9٠-٩۰-۹]+(?:[,٬][0-9٠-٩۰-۹]{3})*)\s*'
                  r'(?:د\.\s*ع\.?|دينار)(?!\w)', replace, str(text or ''))


def chat_wording(text):
    return chat_money(text).replace('الهاتف', 'الموبايل')


def reply_category(product):
    return fact((product or {}).get('reply_category')).strip()


def public_product_wording(result, products):
    """Render catalog titles only in outgoing prose, after image/order resolution."""
    titles = {}
    protected = set()
    seen_products = set()
    for product in products:
        if not product:
            continue
        pid = product.get('product_id')
        if pid and pid in seen_products:
            continue
        seen_products.add(pid)
        label = reply_category(product)
        fabric = fact(product.get('fabric'), 'fabric')
        # A short catalog title can also be an actual fact (e.g. "دانتيل").
        # Keep facts and owner-entered labels intact when a different model's
        # internal title happens to share their wording.
        for value in (label, fabric,
                      fact(product.get('colors'), 'colors'), fact(product.get('sizes'), 'sizes')):
            protected.update(normalized(part).strip() for part in re.split(r'[،,\n]+', value)
                             if part.strip())
        protected.update(normalized(word) for word in fabric.split())
        name = normalized(fact(product.get('product_name'))).strip()
        if len(name) >= 2 and any(c.isalpha() for c in name):
            if name in titles and titles[name] != label:
                label = ''
            titles[name] = label
    if not titles:
        return result
    patterns = []
    names = sorted(titles, key=len, reverse=True)
    for name in names:
        words = [re.escape(word.removeprefix('ال')) if word.startswith('ال') and len(word) > 3
                 else re.escape(word) for word in name.split()]
        patterns.append(r'(?:هال|ال)?' + r'\s+(?:ال)?'.join(words))
    pattern = re.compile(r'(?<!\w)([وفب]?)(?:' + '|'.join(
        f'(?P<n{i}>{part})' for i, part in enumerate(patterns)) + r')(?!\w)')
    labels = [titles[name] for name in names]
    protected_pattern = re.compile(r'(?<!\w)[وفب]?(?:' + '|'.join(
        r'(?:ال)?' + re.escape(value.removeprefix('ال'))
        for value in sorted(protected, key=len, reverse=True)) + r')(?!\w)') if protected else None

    def render(text):
        clean, pos = _normalized_positions(text)
        protected_spans = [m.span() for m in protected_pattern.finditer(clean)] if protected_pattern else []
        pieces, end = [], 0
        for m in pattern.finditer(clean):
            if any(start <= m.start() and m.end() <= stop for start, stop in protected_spans):
                continue
            start, stop = pos[m.start()], pos[m.end()-1] + 1
            tail = text[stop:]
            prefix = m.group(1)
            before = clean[:m.start()].rstrip()
            label = labels[int(m.lastgroup[1:])] or ('الموديل' if before.endswith('هذا') else 'هالموديل')
            # A price or fabric answer already identifies its subject.
            redundant = re.match(r'^[ \t،,:]*(?=(?:قماش(?:ه|ها)|سعر(?:ه|ها)|ألوانه|قياساته)\b)', tail)
            if not prefix and redundant and not before.endswith(('هذا', 'هاي', 'هذه', 'عن')):
                label = ''
                stop += redundant.end()
            pieces.extend((text[end:start], prefix + label))
            end = stop
        return ''.join(pieces) + text[end:]

    updated = dict(result, reply=render(result.get('reply') or ''))
    if isinstance(result.get('reply_parts'), list):
        updated['reply_parts'] = [render(part) for part in result['reply_parts'] if isinstance(part, str)]
    if result.get('_post_image_question'):
        updated['_post_image_question'] = render(result['_post_image_question'])
    return updated


def _fabric_word(word):
    candidates=(word,word[1:] if word.startswith('و') else word)
    return any(value in FABRIC_WORDS or (value.endswith('ه') and value[:-1]+'ة' in FABRIC_WORDS)
               for value in candidates)


def has_instructions(text):
    return bool(INTERNAL.search(normalized(text)))


def fact(value, field=''):
    """Use only the fact preceding internal guidance; never copy its directives."""
    text = str(value or '').strip()
    normalized_text, positions = _normalized_positions(text)
    instruction = INTERNAL.search(normalized_text)
    if instruction:
        text = text[:positions[instruction.start()]].strip(' \n،,؛:.')
    if field in {'sizes', 'colors'}:
        # Render structured values only; never copy arbitrary prose from these
        # fields, even if an instruction uses a new spelling.
        try:
            from . import product_variants
        except ImportError:
            import product_variants
        return product_variants.display(text, field)
    if field == 'fabric':
        # A fabric fact is a short material description, not free-form staff
        # prose. New/unrecognized descriptions need review instead of copying.
        first=re.split(r'[.!؟?\n]',text,1)[0].strip()
        words=re.findall(r'[^\W\d_]+', normalized(first))
        if not words or len(words)>16 or any(not _fabric_word(w) for w in words):
            return ''
        return first
    return text
