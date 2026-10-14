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
                   'كريب حرير حريري مخمل دانتيل شيفون جينز دنيم صوف بوليستر فسكوز '
                   'ناعم ناعمة مريح مريحة عملي عملية خفيف خفيفة ثقيل ثقيلة شتوي شتوية '
                   'صيفي صيفية مطاط مطاطي مطاطية ليكرا مع من و').split())


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
