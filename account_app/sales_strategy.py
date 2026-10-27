"""Context-specific sales coaching, with factual checks and no invented offers."""
import json
import re
try:
    from .checkout import normalized
    from . import customer_copy
except ImportError:
    from checkout import normalized
    import customer_copy


SALES_CUES = {
    'limited_stock': 'الكمية الباقية قليلة عيني.',
    'high_demand': 'عليه طلب ما يوكف هالفترة.',
    'frequent_bookings': 'هاي من القطع اللي تنحجز هواية عندنا.',
    'many_purchases': 'هالموديل اشتروه هواية من زبائننا.',
}
_CLAIMS = {
    'limited_stock': r'اخر\s*(?:ال)?(?:\d+\s*)?قطع|اخر\s+الكمي[هة]|قطع[هة]\s*اخير[هة]|الكمي[هة]\s*الباقي[هة]\s*قليل[هة]|باقي(?:\s+منه|\s+منها)?\s*(?:بس\s*)?(?:\d+|ثلاث|اربع|خمس|ست|سبع|ثمان|تسع|عشر|قطعتين|قطع|قطع[هة]|حبتين)|(?:الكمي[هة]|كمياته|كميتها)\s*(?:صارت\s*)?(?:محدود[هة]|قليل[هة])|راح\s*يخلص|قبل\s*(?:لا\s*|ما\s*)?يخلص',
    'high_demand': r'عليه\s*طلب|عليها\s*طلب|الطلب.{0,14}(?:عالي|كبير|قوي|هواية|ما يوكف)|مطلوب\s*(?:هواية|كلش)|اكثر\s*(?:موديل\s*)?طلب',
    'frequent_bookings': r'حجوزات.{0,14}(?:هواية|كثير|قوي)|(?:ينحجز|تنحجز|انحجز|حجزوه|حاجزينه).{0,12}(?:هواية|كثير)|هواية.{0,15}(?:حجزوه|حجزوا|حاجزين)|بعد\s*الحجوزات',
    'many_purchases': r'(?:اشتروه|اشتروها|اشتروا منه|اشتروا منها|انباع|انبايع).{0,25}(?:هواية|كثير)|(?:هواية|كثير).{0,25}(?:اشتروه|اشتروا|شارينه)|زبائن.{0,18}(?:اشتروا|شارين)|اكثر.{0,10}مبيع',
    'repeat_purchases': r'اغلب.{0,25}(?:مرتين|يطلبنها|يرجع)|يرجعن.{0,20}يطلبن|تنباع.{0,35}يرجع',
    'daily_bookings': r'كل\s*يوم.{0,20}حجوزات|حجوزات.{0,20}كل\s*يوم',
    'offer': r'عليه\s*عرض|عليها\s*عرض|عرض\s*(?:خاص|حصري|اليوم|لفتر[هة])|(?<!\w)(?:خصم|تخفيض)(?!\w)',
}


def _stock_number(product):
    value = (product or {}).get('stock_quantity')
    if isinstance(value, bool):
        return None
    text = customer_copy.normalized(value).strip()
    return int(text) if re.fullmatch(r'\d{1,7}', text) else None


def _sales_available(product):
    try:
        from .product_albums import stock_state
    except ImportError:
        from product_albums import stock_state
    count = _stock_number(product)
    return bool(product and stock_state(product) != 'out' and count != 0
                and (stock_state(product) == 'available' or (count is not None and count > 0)))


def approved_sales_cues(product):
    """Only the product editor's explicit approvals are marketing evidence.

    Bookings, completed purchases and demand remain separate claims. Free-form
    model/staff instructions and conversation examples cannot approve a cue.
    """
    values = (product or {}).get('sales_cues')
    return [key for key in SALES_CUES if isinstance(values, list) and key in values]


def _offer_fact(product):
    value = customer_copy.fact((product or {}).get('offer')).strip()
    if re.search(r'لا يوجد|ماكو|ما اكو|بدون عرض|منتهي|انتهى|ملغي', normalized(value)):
        return ''
    return value


def _claim_kinds(text):
    result = set()
    for clean in re.split(r'[\n،؛.!؟?]+', normalized(text)):
        # Only an explicit denial of a sales claim is exempt. "ما عندي فيديو"
        # must not conceal an unsupported claim later in the same response.
        if re.match(r'\s*(?:مو|ليس|ماكو|ما اكو|لا يوجد)\s*(?:اخر|باقي|عرض|خصم|تخفيض|عليه طلب|طلب|حجوزات|كمي[هة])', clean):
            continue
        result.update(key for key, pattern in _CLAIMS.items() if re.search(pattern, clean))
    return result


def _assistant_texts(history):
    for item in history or []:
        if not isinstance(item, dict):
            continue
        if item.get('role') != 'assistant' and item.get('direction') != 'outgoing':
            continue
        metadata = item.get('raw_payload') or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (ValueError, TypeError):
                metadata = {}
        if isinstance(metadata, dict) and metadata.get('_delivery_status', 'accepted') != 'accepted':
            continue
        yield str(item.get('content') or item.get('text') or '')


def persuasion_used(history):
    """Caller supplies accepted conversation history, never model drafts."""
    return any(_claim_kinds(clause) for text in _assistant_texts(history)
               for clause in re.split(r'[\n،؛.!؟?]+', text))


def used_sales_claims(history):
    return set().union(*(_claim_kinds(text) for text in _assistant_texts(history)))


def separate_scarcity_available(product, history):
    return (used_sales_claims(history) == {'offer'}
            and 'limited_stock' in _claim_kinds(_offer_fact(product)))


def persuasion_cue(product, history=()):
    """One nonnumeric approved reason to act; availability still takes priority."""
    if not _sales_available(product):
        return None
    if persuasion_used(history):
        if separate_scarcity_available(product, history):
            return {'kind': 'limited_stock', 'text': SALES_CUES['limited_stock']}
        return None
    available = approved_sales_cues(product)
    count = _stock_number(product)
    stock = customer_copy.fact((product or {}).get('stock'))
    if ((count is not None and 1 <= count <= 5)
            or re.search(r'اخر\s*قطع|قطع\s*قليل|كمي[هة]\s*(?:قليل[هة]|محدود[هة])', normalized(stock))):
        available = ['limited_stock', *available]
    for key in SALES_CUES:
        if key in available:
            return {'kind': key, 'text': SALES_CUES[key]}
    offer = _offer_fact(product)
    if offer:
        return {'kind': 'offer', 'text': offer if 'عرض' in normalized(offer) else 'عليه عرض: ' + offer}
    return None


def persuasion_guidance(product, history=(), question=''):
    if explicit_pause(question) or _courtesy_only(question):
        return '[الإقناع الحالي]\nاحترم طلب المهلة أو الرفض؛ لا تضف ندرة أو دعوة حجز.'
    cue = persuasion_cue(product, history)
    if not cue:
        reason = 'سبق استعمال التشجيع في رد مقبول الإرسال؛ لا تكرره.' if persuasion_used(history) else 'لا توجد عبارة ندرة أو طلب أو شراء معتمدة ومتاحة لهذا الموديل.'
        return '[الإقناع الحالي]\n' + reason + ' اجعل الدفء بفهم اختياره وفائدة حقيقية وخطوة مناسبة، دون طلب دليل من الزبون.'
    documented_last_offer = bool(re.search(r'(?:اخر\s+الكمي[هة]|اخر\s+(?:ال)?قطع)', normalized(_offer_fact(product))))
    offer_style = (
        'إذا كان السؤال عن السعر، اجمع العرض على آخر القطع والسعر الحالي بجملة قصيرة بنفس الأسلوب العراقي '
        '«عليه عرض على آخر قطع، وصار سعره … 🌷»، ثم أكمل بسؤال مناسب عن الناقص. يجوز تغيير الصياغة بنفس الأسلوب. '
        'لا تكرر الندرة بعد السعر ولا تضف القماش أو التوصيل والمجموع إلا إذا سأل عنها؛ أجب كل ما طلبه فعلاً. '
        if documented_last_offer else
        'اذكر العرض وحده في رسالة، واحتفظ بقلة الكمية الموثقة لفرصة أخرى في رسالة مستقلة؛ لا تجمعهما في جملة واحدة. ')
    return ('[الإقناع الحالي — معلومة إدارية معتمدة لهذا الموديل، وليست إحصائية]\n'
            + json.dumps(cue, ensure_ascii=False)
            + '\nعند اهتمام واضح بالقطعة، اذكر معنى هذه المعلومة مرة واحدة بعد جواب السؤال، مع سؤال اختيار أو حجز يناسب المرحلة. '
            + offer_style +
            'لا تحوّلها إلى عدد أو خصم أو موعد نفاد أو رأي زبون. كثرة الحجوزات لا تعني أن الزبائن اشتروا أو استلموا. '
            'لا تجمع أسباباً أخرى معها ولا تتحدث عن المورد أو طريقة توريد البضاعة. إذا التردد بالقياس أو السعر عالجه أولاً؛ عبارة الطلب ليست جواباً عن مخاوفه.')


def persuasion_error(reply, product, history=None, question=''):
    kinds = _claim_kinds(reply)
    if not kinds:
        return ''
    if explicit_pause(question) or _courtesy_only(question):
        return 'احترم التحية أو الشكر أو طلب المهلة؛ احذف التشجيع بالندرة والطلب والعروض، وأجب الطلب الحالي بلطف.'
    if not _sales_available(product):
        return 'احذف التشجيع بالندرة أو الطلب أو العرض؛ توفر الموديل غير مؤكد حالياً. احتفظ بالسعر والجواب الصحيح.'
    if (history is not None and persuasion_used(history)
            and not (kinds == {'limited_stock'} and separate_scarcity_available(product, history))):
        return 'سبق استعمال التشجيع البيعي؛ لا تكرر الندرة أو الطلب أو العرض. احتفظ بالجواب وخطوة القرار المناسبة.'
    allowed = set(approved_sales_cues(product))
    count = _stock_number(product)
    stock = normalized(customer_copy.fact((product or {}).get('stock')))
    if (count is not None and 1 <= count <= 5) or re.search(r'اخر\s*قطع|قطع\s*قليل|كمي[هة]\s*(?:قليل[هة]|محدود[هة])', stock):
        allowed.add('limited_stock')
    offer = _offer_fact(product)
    offer_has_limited_stock = 'limited_stock' in _claim_kinds(offer)
    if offer:
        allowed.add('offer')
        if offer_has_limited_stock:
            allowed.add('limited_stock')
    if kinds - allowed:
        return 'احذف الندرة أو الطلب أو الشراء أو العرض غير المعتمد لهذا المنتج؛ لا تحول الحجوزات إلى مشتريات. احتفظ بالسعر والفائدة الصحيحة.'
    if len(kinds) > 1 and not (kinds == {'offer', 'limited_stock'} and offer_has_limited_stock):
        return 'استخدم سبب إقناع واحداً فقط، لا تجمع الندرة وكثرة الطلب والحجوزات والمشتريات والعرض.'
    clean = normalized(reply)
    prefix = r'باقي(?:\s+منه|\s+منها)?\s*(?:بس\s*)?'
    claimed_count = re.search(r'(?:' + prefix + r'|اخر\s*)(\d+)\s*(?:قطع|حب)', clean)
    expected = int(claimed_count[1]) if claimed_count else (2 if re.search(r'(?:' + prefix + r'|اخر\s*)(?:قطعتين|حبتين)', clean)
                else 1 if re.search(r'اخر\s*قطع[هة](?!\w)|قطع[هة]\s*اخير[هة]|' + prefix + r'قطع[هة](?:\s+وحد[هة])?(?!\w)', clean) else None)
    for word, number in [('ثلاث', 3), ('اربع', 4), ('خمس', 5), ('ست', 6), ('سبع', 7), ('ثمان', 8), ('تسع', 9), ('عشر', 10)]:
        if re.search(prefix + word + r'[هة]?\s*قطع', clean):
            expected = number
    if expected is not None and count != expected:
        return 'لا تخترع العدد المتبقي أو آخر قطعة؛ استخدم عبارة الكمية القليلة المعتمدة دون رقم، واحتفظ ببقية الجواب.'
    if ('limited_stock' in kinds and re.search(r'(?:قياس\s*\d+|اللون\s*\S+).{0,15}(?:باقي|اخر)|(?:باقي|اخر).{0,25}(?:قياس\s*\d+|اللون\s*\S+)', clean)):
        return 'الكمية تخص الموديل كله؛ لا تنسب الندرة لقياس أو لون محدد دون مخزون منفصل معتمد.'
    if kinds & {'high_demand', 'frequent_bookings', 'many_purchases'} and re.search(r'\d+\s*(?:زبون|زبائن|حجز|طلب|مبيع)|كل\s*الزبائن|اكثر.{0,15}(?:مبيع|طلب)', clean):
        return 'استخدم تشجيع الطلب المعتمد بصياغة طبيعية؛ لا تضف أرقام مبيعات أو ادعاء الجميع أو الأكثر مبيعاً.'
    if 'offer' in kinds and re.search(r'خصم|تخفيض', clean) and not re.search(r'خصم|تخفيض', normalized(offer)):
        return 'العرض المعتمد لا يثبت خصماً؛ اذكر شروط العرض نفسها دون اختراع تخفيض.'
    for match in re.finditer(r'اليوم\s*فقط|اخر\s*يوم|(?:ينتهي|ينفد|راح يخلص)\s+[^.،؟?\n]{1,25}', clean):
        if match[0] not in normalized(offer):
            return 'لا تخترع موعد نفاد أو نهاية عرض؛ احتفظ بالعبارة المعتمدة دون موعد وبالجواب الصحيح.'
    return ''


def repair_persuasion_reply(reply, product, history=None, question=''):
    """Drop only unsupported marketing clauses, preserving factual answers.

    No substitute facts are generated here. An empty return asks the caller to
    use its catalog-backed recovery, rather than escalate a copy defect.
    """
    parts = re.split(r'(?<=[.!؟?؛])\s+|\n+|،|(?<!\d),(?!\d)|\s+(?=و(?:آخر|اخر|باقي|عليه طلب|عليها طلب|الكمية))', str(reply or ''))
    kept = []
    used = bool(explicit_pause(question) or _courtesy_only(question))
    for part in parts:
        part = part.strip(' ،؛')
        if not part:
            continue
        kinds = _claim_kinds(part.lstrip('و'))
        if kinds:
            if used or persuasion_error(part, product, history, question):
                continue
            used = True
        kept.append(part)
    return ' '.join(kept).strip()


def inspection_claim_error(reply, policy):
    """Detect return or try-on promises beyond the current inspection policy."""
    text = normalized(reply)
    allowed = normalized(policy)
    if re.search(r'تجرب(?:ين|ي|ون)?|تلبس(?:ين|ي|ون)?\s+(?:القطعه|الفستان)', text) and not re.search(r'تجرب|تلبس', allowed):
        return 'سياسة الفحص لا تذكر تجربة القطعة؛ احذف وعد التجربة.'
    if re.search(r'يرجع|ترجع|نرجع|ارجاع|بدون\s+(?:ما\s+)?تدفع|بدون\s+دفع|مجانا', text) and not re.search(r'يرجع|ترجع|ارجاع|بدون\s+دفع|مجانا', allowed):
        return 'سياسة الفحص الحالية لا تتضمن وعد الإرجاع أو عدم الدفع؛ اذكر الفحص المعتمد فقط.'
    return ''


def repair_inspection_reply(reply, policy):
    """Remove an unsupported return/try-on clause, preserving other facts."""
    if not inspection_claim_error(reply, policy):
        return str(reply or '')
    allowed = normalized(policy)
    parts = re.split(r'(?<=[.!؟?؛])\s+|\n+|،', str(reply or ''))
    kept = []
    for part in parts:
        clean = normalized(part)
        if not part.strip():
            continue
        if (not re.search(r'تجرب|تلبس', allowed)
                and re.search(r'تجرب(?:ين|ي|ون)?|تلبس(?:ين|ي|ون)?\s+(?:القطعه|الفستان)', clean)):
            continue
        if (not re.search(r'يرجع|ترجع|ارجاع|بدون\s+دفع|مجانا', allowed)
                and re.search(r'يرجع|ترجع|نرجع|ارجاع|بدون\s+(?:ما\s+)?تدفع|بدون\s+دفع|مجانا', clean)):
            continue
        kept.append(part.strip())
    return '، '.join(kept).strip(' ،')


def explicit_pause(question):
    """Immediate dialogue interpretation; does not change follow-up eligibility."""
    text = normalized(question)
    return bool(_opted_out(text) or _declined(text) or re.search(
        r'خل(?:ي|يني).{0,20}(?:افكر|استلم الراتب)|بعدين اطلب|مو هس[هة]|بعد الشهر|'
        r'(?:افكر|اشاور|اسال).{0,18}(?:ارجع|اردل|بعدين)|ذكرني|ذكريني|'
        r'(?:لا|مو|ما)\s*(?:تثبت\w*|ثبتي|تحجز\w*|احجزي)|بدون.{0,12}(?:تثبيت|تثبت|حجز)|'
        r'(?:لا|ما)\s*(?:اريد\s*)?(?:اطلب|احجز|اشتري)\s*(?:هسه|هسة)', text))


def _courtesy_only(question):
    text = re.sub(r'[^\w\s]', '', normalized(question)).strip()
    return bool(re.fullmatch(r'(?:السلام عليكم|وعليكم السلام|مرحبا|هلو|هلا|شكرا|تسلمين|تسلم|مشكور[هة]?|ممنون[هة]?)(?:\s+(?:عيني|حبيبتي|حبي|شكرا|جزيلا))*', text))


def soft_indecision(question):
    text = normalized(question)
    return bool(not explicit_pause(text) and not _explicit_purchase(text)
                and re.search(r'افكر|محتار|متردد|ما قررت|مو متاكد|مو متاكده', text))


METHODS = {
    'price': 'اعتراض السعر: ابدأ بتفهم لطيف مثل «أفهمج، يهمج يكون السعر مناسب». اربط ميزة موثقة واحدة بحاجة ذكرها الزبون ووضّح الكلفة الكلية والتوصيل. لا تجادله ولا تقل «رخيص» ولا تستخدم الفحص كبديل لجواب السعر. إذا يريد الأرخص اقترح بديلاً مناسباً مرتبطاً بالموديل بسعره؛ لا تسأل عن الميزانية إذا ذكرها. إذا لا يوجد بديل والميزانية مجهولة اسأل عن سقفها مرة واحدة. إذا وافق على السعر وطلب الحجز أكمل الناقص ولا تعد للإقناع.',
    'trust': 'بناء الثقة: جاوب مصدر القلق تحديداً. عند الشك بالصورة اعرض تصويراً حقيقياً فقط إذا مرفق ومثبت، وإلا صرّح بأن الفيديو غير متوفر واطلب المعلومة من الموظف إذا لازمة للقرار. اشرح سياسة الفحص كما هي دون ضمان التطابق التام أو أن الفحص يسمح بالتجربة إلا إذا مصرح بذلك. لا تطلب العنوان قبل حسم السؤال.',
    'fit': 'ملاءمة القياس: فرّق بين وزن الزبون ومقاسه وبين أبعاد القطعة. اعتمد المقاس الصريح الذي اختاره ولا تعده بمقاس مضمون من الوزن. إن المقاس مقترح سابقاً اسأل عن اعتماده بصيغة محددة بدل إعادة السؤال. لا تستخدم طول جسم الزبون لإجابة طول القميص. عند غياب قياس بالسنتيمتر قل ما ينقص بالضبط.',
    'comparison': 'المقارنة: قارن خيارين موجودين فقط بمعايير طلبها الزبون: اللون أو القياس أو السعر أو القماش. اذكر الفرق العملي الموثق لكل خيار، ثم سؤال اختيار واحد. لا تعرض جميع الموديلات ولا تذم المنافس أو تدعي أن الأغلى أفضل دون دليل.',
    'delivery': 'الشحن والتوقيت: بيّن رسوم التوصيل والمجموع حين يمكن حسابهما، والمدة الموثقة. عند مناسبة قريبة وضح أن الموعد غير مضمون إذا لا يوجد التزام مؤكد؛ لا تخترع موعداً لإغلاق البيع. لا تحول سؤال التوصيل إلى طلب عنوان قبل الإجابة.',
    'bundle': 'الكمية والعرض: استخدم العرض المعتمد فقط وبيّن عدد القطع والمجموع والتوصيل. سجّل كل لون وقياس وكمية مستقلة. لا تضف قطعة لرفع السلة دون اختيار العميل، ولا تستنتج سعر مفرد من قسمة سعر البكج. إن طلب قطعة واحدة أجب عن إمكانية المفرد قبل اقتراح البكج.',
    'quantity_uncertain': 'العدد غير محسوم: اسأل عن العدد أولاً بسؤال واحد يميز المجموع عن عدد لون محدد؛ مثال «المطلوب ثلاث قطع بالمجموع لو ثلاث من الكريمي؟». لا تفترض أن ثلاث تخص لوناً معيناً ولا تضف سؤال القياس قبل حسم العدد ولا تنشئ الطلب.',
    'ready': 'إغلاق البيع: عند اختيار واضح وموافقة شراء انتقل مباشرة لإكمال الناقص فقط. اجمع الهاتف والمحافظة والعنوان في طلب قصير إذا جميعها ناقصة؛ لا تطلب الاسم إجبارياً. إذا اكتمل كل شيء ضع create_order=true وorder.items كاملة، ولا تقل تم التثبيت قبل نجاح النظام. لا تعاود عرض بدائل أو البيع الإضافي في هذه المرحلة.',
    'deferred': 'تأجيل القرار: احترم التوقيت ولا تستغل الراتب أو الضيق المالي للضغط. إذا طلب تذكيراً صريحاً بمدة محددة فلا تعد بأنه جُدول ما لم ينجح النظام؛ التأجيل وحده ليس موافقة على الملاحقة. بعد الشكر اختم بلا سؤال حجز جديد.',
    'undecided': 'حيرة وليست طلب مهلة: اعترف بها بود مثل «أكيد عيني، أهم شي تكونين مرتاحة باختيارج». إذا السبب ظاهر عالجه مباشرة؛ وإلا اسأل مرة واحدة «التردد من القياس لو من السعر؟». لا تكرر السؤال إذا سبق توضيحه، ولا تعتبر الحيرة موافقة حجز. عند طلب مهلة صريح توقف عن الإقناع.',
    'refusal': 'رفض الشراء أو إيقاف التواصل: اختم بلطف بلا أسئلة بيع وبلا عروض إضافية وبلا إنشاء طلب. رفض لون أو موديل واحد ليس رفض التواصل كله؛ اعرض البديل فقط عندما يطلبه أو يوضح حاجته.',
    'discovery': 'اكتشاف الحاجة: أجب المعلومات المتاحة أولاً، واسأل فقط عن العامل الناقص الذي يحدد الاختيار. سؤال واحد سهل أفضل من استجواب. لا تسأل عن سبب الشراء أو المناسبة إذا لا يفيد القرار الحالي. لا تعرض فائدة لا ترتبط بحاجة صرح بها الزبون.',
}


def upgrade_legacy_rules(text):
    """Replace only known obsolete built-in advice; retain custom store facts."""
    replacements = {
        "الرد يجب أن يكون قصيراً جداً (جملة إلى جملتين، لا يتجاوز 30 كلمة).": "اكتب رداً عراقياً دافئاً ومختصراً يجيب جميع الأسئلة، دون سقف كلمات يحذف معلومة مفيدة.",
        "لا تنهي رسالتك بنقطة أبداً، اختميها دائماً بسؤال (قياس، محافظة، أو حجز) لتبقى الزبونة متفاعلة.": "اختم بخطوة واحدة مفيدة فقط عند الحاجة، وبعد الرفض أو التأجيل أو الشكر اختم بلطف دون سؤال بيع.",
        "طول الرد إلزامي: من جملة إلى جملتين قصيرتين فقط (≤ 25 كلمة). ممنوع الإطالة.": "اجعل الرد مختصراً، لكن أجب جميع الأسئلة المهمة دون حد كلمات يحذف جواباً أو سعراً.",
        "إذا اكتملت بيانات الحجز (موبايل واضح + عنوان + منتج متوفر) اجعل create_order=true.": "إذا وافق الزبون صراحة واكتملت القطع وخياراتها والتكلفة وبيانات التوصيل اجعل create_order=true.",
        "لا تختم بسؤال عام مثل 'شنو تحبين تعرفين عنه؟' — اختم بسؤال بيع واضح يجلب الخطوة التالية مثل 'أحجزه إلج؟' أو 'دزّيلي العنوان والموبايل وأحجزه؟'.": "اختر خطوة تناسب قرار الزبون: أجب الاعتراض أولاً، ثم سؤال اختيار أو معلومة ناقصة. لا تطلب الحجز قبل إشارة شراء ولا بعد الرفض أو الشكر.",
        "كل رد لازم يدفع المحادثة للأمام نحو الحجز: إما يطلب معلومة ناقصة (موبايل/عنوان) أو يستفز الرغبة (مثل 'الموديل قاعد ينتظرج، أحجزه؟'). تجنب الردود الميتة التي لا تجلب رد من الزبون.": "تقدم المحادثة بإزالة الغموض أو حل التردد؛ ليس مطلوباً سؤال حجز في كل رد. بعد موافقة الشراء اجمع الناقص فقط.",
        "لا تختم بسؤال عام مثل: شنو تحبين تعرفين عنه؟ اختم بسؤال بيع واضح مثل: أحجزه إلج؟": "اجعل الخطوة التالية مناسبة للسياق؛ لا تكرر سؤال الحجز قبل معالجة التردد.",
        "لم يسأل عن أي تفاصيل؟ → لا تعرض أي تفاصيل، فقط استمر بالحوار وحفّزه للحجز.": "لم يسأل عن تفاصيل؟ → افهم طلبه الحالي؛ التحية والشكر لا يحتاجان دعوة حجز.",
        "هدفك الوحيد في كل رد: تقريب الزبون خطوة واحدة من الحجز.": "هدفك مساعدة الزبون على قرار مناسب بمعلومة صحيحة وخطوة تناسب اختياره؛ الحجز بعد موافقته فقط.",
        "إذا المنتج stock فارغ أو 'نفذ' — قل صراحة 'خلص حالياً' ولا تقل متوفر.": "إذا التوفر مجهول قل إنه يحتاج تأكيداً؛ فراغ stock لا يثبت النفاد. اعتمد حالة المنتج الحالية ولا تختلق توفراً.",
        "عند سؤال عن الجودة/الفحص/الثقة — أكد أن الفحص عند الاستلام، وإذا غير مطابق يرجع مجاناً.": "أجب عن مصدر القلق نفسه، واذكر الفحص أو الإرجاع وشروطهما من سياسة المتجر الحالية فقط؛ لا تخترع تجربة أو إرجاعاً مجانياً.",
        "سأل عن التوصيل؟ → رد بأجور التوصيل فقط.": "سأل عن التوصيل؟ → أجب عن الرسوم أو المدة أو مناطق الخدمة بحسب سؤاله، مع الشروط المعروفة ودون موعد مختلق.",
        "إذا كلمات مثل 'هذا/هاي/الموديل/سعره/متوفر/قياس/ارجعه/احجز' وردت — اعتبرها تخص آخر منتج محفوظ للزبون.": "فسر هذا/هاي من آخر مرجع واضح في المحادثة أو ترتيب الصور، مع تقديم تصحيح الزبون الأحدث على المنتج القديم؛ عند احتمال أكثر من قطعة استوضح المرجع فقط.",
    }
    for old,new in replacements.items():
        text=str(text or '').replace(old,new)
    return text


def _opted_out(text):
    return bool(re.search(r'لا تراسل|وقف الرسائل|لا اريد رسائل|ما اريد رسائل|'
                         r'لا\s+تدز(?:لي|ين|ون)?(?:\s+(?:بعد|رسائل|شي)(?:\s|$)|\s*[.!؟?]*$)', text))


def _declined(text):
    """Separate an opt-out or declined purchase from changing one item."""
    if _opted_out(text):
        return True
    # Polite endings and emoji must not send a declined customer back through
    # the product classifier. An actual color/model before "ما أريد" is not
    # small talk and remains a product correction.
    plain = re.sub(r'[^\w\s]', ' ', text)
    negative = re.search(r'(?<!\w)(?:ما\s*ا+ريد|ماريد)(?!\w)', plain)
    polite = r'(?:(?:لا|عيني|عيوني|حبي|حبيبتي|حياتي|اختي|خلاص|بعد|والله|كولشي|كلشي|كل|شي|شكرا|جزيلا|مشكور(?:ه|ة|ين)?|ممنون(?:ه|ة|ين)?)\s*)*'
    if negative and re.fullmatch(polite, plain[:negative.start()].strip()) and re.fullmatch(polite, plain[negative.end():].strip()):
        return True
    if re.search(r'(?:بس|لكن).{0,35}(?:شكد|شنو|سعر|صور|متوفر|اريد)', text):
        tail = re.split(r'(?:بس|لكن)', text)[-1]
        # "شكراً بس ما أريد أحجز" ends the purchase; the negated
        # "أريد" is not a new positive request. A real question still wins.
        if (not re.search(r'(?:ما\s*ا+ريد|ماريد)\s*(?:اطلب|احجز|اشتري)', tail)
                or re.search(r'شكد|شنو|سعر|صور|متوفر', tail)):
            return False
    if re.search(r'ما\s*ا+ريد\s+(?:اطلب|احجز|اشتري)', text):
        return True
    if re.search(r'لا\s+شكرا(?:\s|[،.!؟?]|$)', text) and not re.search(r'اريد\s+(?:ال|بديل|غير)|خلي(?:ها|ه)?\s', text):
        return True
    if re.search(r'ما\s*اريد\s+(?:اي\s+)?شي(?:ء)?(?:\s|$)', text) and not re.search(r'ما\s*اريد\s+(?:اي\s+)?شي(?:ء)?\s+(?:غير|الا)', text):
        return True
    if re.search(r'(?:شكرا|مشكور|ممنون)[،\s]+ما\s*اريد\s*[،.!؟?]*$', text):
        return True
    # "ما اريد شي غير الأسود" and "بطلت الكريمي، خلي الأسود" retain intent.
    if re.search(r'مو مهتم(?:[ةه])?(?:\s|$)', text):
        tail = re.split(r'مو مهتم(?:[ةه])?', text, maxsplit=1)[-1].strip(' ،.!؟?')
        if not tail or re.match(r'(?:بالشراء|بالحجز|اطلب|احجز)(?:\s|$)', tail):
            return True
    if re.search(r'(?<!\w)بطلت(?:\s|$)', text):
        tail = text.split('بطلت', 1)[-1].strip(' ،.!؟?')
        if not tail or re.match(r'(?:اطلب|احجز|الحجز|الطلب|الشراء)(?:\s|$)', tail):
            return True
    return False


def stop_kind(question):
    """Respect a purchase pause without treating an item correction as opt-out."""
    text = normalized(question)
    if _opted_out(text):
        return 'opt_out'
    if re.search(r'(?:ما\s*اريد|ماريد|مارد)\s*(?:احجز|اطلب|اشتري)\s*(?:هسه|هسة|الان)', text):
        return 'deferred'
    if _declined(text):
        return 'declined'
    if re.search(r'خل(?:ي|يني).{0,20}(?:افكر|استلم الراتب)|بعدين اطلب|بعدني محتار|ما قررت|مو هس[هة]', text):
        return 'deferred'
    return ''


def signals(text):
    text = normalized(text)
    result = []
    patterns = {
        'deferred': r'بعدين|افكر|راتب|مو هسه|مو هسة|بعد الشهر|ذكرني|ذكريني',
        'price': r'غالي|غاليه|غالية|خصم|تخفيض|ارخص|ميزاني|باقل|حسبي|تحسبي|سعر|بكم|بشكد|^\s*شكد\s*[؟?]?$' ,
        'trust': r'حقيقي|حقيقيه|حقيقية|فيديو|اصطناعي|نفس الصور|يختلف|اخاف|فحص|ارجع|يرجع|وهمي|ثقه',
        'fit': r'قياس|مقاس|وزن|كيلو|طول|سم\b|سنتيمتر|يلبس|واسع|ضيق|عمر',
        'comparison': r'فرق|افضل|احسن|غير موديل|غير مديل|بديل|قارن|غير الوان|موديلات',
        'delivery': r'توصيل|شحن|يوصل|شوكت|مندوب|مناسبه|مناسبة|باجر',
        'bundle': r'بكج|عرض|ثلاث|٣ قطع|3 قطع|ثنين|قطعتين|مفرد|قطعه وحده|قطعة وحدة',
        'ready': r'ثبتي|ثبتلي|ثبتوا|احجزي|احجزلي|اريد اطلب|اريد هذا|اريد هذ|رايده هذ',
    }
    for key, pattern in patterns.items():
        if re.search(pattern,text):
            result.append(key)
    if soft_indecision(text):
        result = [key for key in result if key != 'deferred']
        result.append('undecided')
    if re.search(r'ثنين|اثنين|ثلاث|اربع|\d+\s*قطع',text) and re.search(r'\b(?:لو|او)\b|محتار',text):
        result.append('quantity_uncertain')
    if _declined(text):
        return ['refusal']
    # Mentioning a salary already received does not postpone a current order.
    if ('deferred' in result and _explicit_purchase(text)
            and not purchase_block_reason(text)
            and not re.search(r'ذكرني|ذكريني', text)):
        result.remove('deferred')
    # Negation must not be interpreted as purchase intent.
    if re.search(r'(?:لا|مو|ما)\s*(?:تثبت|ثبتي|احجز|اريد)',text) and 'ready' in result:
        result.remove('ready')
    return result or ['discovery']


def guidance(ev, customer, history):
    if ev.get('_post_order'):
        return ''
    selected = signals(ev.get('text'))
    missing = [label for key,label in [('phone','رقم الهاتف'),('province','المحافظة'),('address','العنوان')] if not (customer or {}).get(key)]
    return ('[خطة الرد البيعي لهذه الرسالة — توجيه داخلي لا يعرض للزبون؛ تتقدم على قوالب الحجز العامة القديمة عند التعارض]\n'
            'اجعل القرار أسهل بالمعلومة الصحيحة: جواب مباشر، فائدة مرتبطة بالحاجة إن لزمت، ثم خطوة واحدة فقط. '
            'هذه الأدوات تختار حسب الحالة، وليست قائمة تنفذ كلها في كل رد. لا تذكر أسماء تقنيات البيع. '
            'الأولوية لسؤال الزبون الحالي؛ لا تعتبر السكوت اعتراضاً ولا تعتبر السؤال موافقة. '
            'راجع الرسائل المتتالية كوحدة واحدة وأجب السعر والخامة والقياس والفحص والتوصيل المطلوبة كلها؛ نقص تفصيل واحد لا يحذف بقية الأجوبة المعروفة. '
            'عند غياب معلومة الخطاب استخدم صياغة عراقية محايدة مثل «شنو القياس المطلوب؟»؛ لا تستنتج جنس المشتري من المنتج أو الطفل. '
            'اعتمد المعلومات الموثقة في الكتالوج أولاً، ولا تحول استفسار السعر أو اللون أو القياس أو طلب الصور إلى موظف. '
            'إذا المنتج غير متوفر وكان البديل مناسباً لحاجته، اعرض خياراً متوفراً يلائم العمر والقياس والميزانية مع سعره؛ لا تعرض عمر سنة لمن طلب سبع سنوات ولا تكرر لوناً رفضه. لا تضف البديل للسلة دون اختياره. '
            'العمر أو الوزن نطاق تقريبي فقط، وجدول المنتج المحدد هو المرجع؛ إذا ذكر قياس ملابسه أو اختار قياساً صراحة لا تعاود طلبه بسبب ذكر الوزن. '
            'سؤال التوصيل قد يقصد المدة أو الرسم أو المنطقة: أجب المقصود أولاً. إذا الرسوم ثابتة لكل المحافظات فلا تطلب المحافظة لحساب رسم معروف. '
            'عند وجود اختيار سابق لا تعيد سؤال الموديل؛ عند الموافقة أكمل الحقول الناقصة وثبت الطلب بعد التحقق. '
            'لا تكرر طلب الحجز بعد كل جملة؛ اجعل سؤال الخطوة التالية مفيداً بعد جواب مكتمل. الحيرة تسمح بمساعدة واحدة، أما الرفض أو طلب المهلة الصريح أو الشكر فلا يحتاج سؤال بيع. لا تستخدم ندرة أو تقييمات أو ضمانات أو هدايا غير معتمدة. '
            'عند شرط شراء لم يحسم مثل وصول باجر أو عرض بالسنتيمتر، احسمه أو وضح حاجته للتأكيد قبل طلب البيانات؛ رقم الهاتف وحده لا يلغي الشرط.\n'
            + '\n'.join(METHODS[key] for key in selected)
            + '\nحقول التواصل الناقصة من الملف (قد تكون وصلت بالرسالة الحالية؛ اقرأها قبل السؤال): '
            + json.dumps(missing,ensure_ascii=False)
            + '\nاحتفظ بخيارات السلة في order.items أثناء الإجابة عن استفسارات جانبية، لكن لا تضف مجرد البدائل المعروضة إليها.')


def reply_error(reply, question, product=None):
    text = normalized(reply)
    if re.search(r'(?:انا|اني|انيه|وياج|وياك)\s+(?:(?:موظف|موظفه)(?:\s+المبيعات)?|بشر|انسان)|(?:انا|اني)\s+(?:مو|لست|ما)\s*(?:ذكاء|بوت|روبوت)', text):
        return 'لا تدع أنك موظف بشري؛ أنت المساعد الآلي للمتجر. إذا سُئلت عن هويتك أجب بصدق مع متابعة الإجابة عن السؤال.'
    evidence = normalized(' '.join(str((product or {}).get(k) or '') for k in ('stock','stock_quantity','offer','notes','description','real_photo_notes')))
    marketing_error = persuasion_error(reply, product, question=question)
    if marketing_error:
        return marketing_error
    if re.search(r'نفس الصور[ةه] بالضبط|مضمون.{0,12}(?:قياس|يلبس)|يناسب.{0,10}100%',text):
        return 'لا تضمن تطابق الصورة أو ملاءمة القياس؛ وضح البيانات وسياسة الفحص دون ضمان مختلق.'
    if re.search(r'(?:تصوير|صور|صوره|صورة).{0,15}حقيقي',text) and not re.search(r'(?:تصوير|صور|صوره|صورة).{0,15}حقيقي',evidence) and not re.search(r'ما عندي|ما متوفر|غير متوفر|لا يوجد|لا املك|مو متوفر',text):
        return 'مصدر التصوير غير موثق؛ لا تدع أن الصور حقيقية. اعترف بعدم وجود فيديو موثق إذا لا يوجد.'
    if (re.search(r'وزن|كيلو', normalized(question))
            and re.search(r'(?:يناسب|البس|تلبس|يلبس).{0,20}قياس\s*\d+', text)
            and not _has_weight_size_table(product)
            and not _declared_size(question)):
        return 'لا يوجد جدول يربط الوزن بقياس محدد لهذا المنتج. اسأل عن قياس الملابس المعتاد أو القياس المطلوب ولا تخترع 42 أو 44 من الوزن.'
    if signals(question) == ['refusal'] and re.search(r'احجز|اثبت|نثبت|تحب|تحبين|اريد رقم|دز.{0,8}عنوان',text):
        return 'الزبون رفض الشراء؛ اختم بلطف دون سؤال بيع أو جمع بيانات.'
    return ''


def _explicit_purchase(text):
    return bool(re.search(r'(?<!\w)(?:ثبت(?:ي|يه|يهن|يهم|لي|ولي)|احجز(?:ي|يلي|لي|ولي)|اريد\s+(?:اطلب|احجز|اشتري))(?!\w)', text))


def premature_close(reply, question):
    text=normalized(question)
    objection=bool(re.search(r'غالي|ارخص|ميزاني|تحسبي|خصم|تخفيض',text))
    if _explicit_purchase(text) and not purchase_block_reason(question) and not re.search(r'[؟?]|(?:اذا|بشرط|شرط|بسعر|تحسبي|خصم|تخفيض)', text):
        return False
    return objection and bool(re.search(r'احجز|اثبت|نثبت|دز.{0,10}(?:عنوان|رقم)',normalized(reply)))


def purchase_block_reason(question):
    text = normalized(question)
    kind = stop_kind(question)
    if kind in {'opt_out', 'declined'}:
        return 'declined'
    if re.search(r'(?:ما\s*ا+ريد|ماريد)\s+(?:اطلب|احجز|اشتري)', text):
        return 'not_authorized'
    if re.search(r'(?:لا|مو|ما)\s*(?:تثبت\w*|ثبتي|تحجز\w*|احجزي)|بدون.{0,12}(?:تثبيت|تثبت|حجز)',text):
        return 'not_authorized'
    if kind == 'deferred':
        return 'deferred'
    if soft_indecision(text):
        return 'undecided'
    return ''


def decision_error(result, question):
    reply = normalized(result.get('reply'))
    confirmed = any(not re.search(r'(?:ما|لم|مو)\s*(?:يتم\s*)?$', reply[max(0, hit.start()-18):hit.start()])
                    for hit in re.finditer(r'تم تثبيت|تم حجز|ثبتنا|ثبتنالج', reply))
    if purchase_block_reason(question) and (result.get('create_order') or confirmed):
        return 'الرسالة الحالية تمنع الحجز أو تؤجل القرار. create_order=false ولا تؤكد تثبيتاً؛ أجب الاستفسار فقط واحترم الاختيار.'
    return ''


def _has_weight_size_table(product):
    """A broad weight range alongside sizes is not an explicit size chart."""
    facts = normalized(' '.join(str((product or {}).get(k) or '')
                                for k in ('sizes', 'notes', 'description', 'measurements', 'fit_notes')))
    number_range = r'\d{2,3}(?:\s*(?:[-–—]|الى|لحد|حتى)\s*\d{2,3})?'
    weight_first = (r'وزن\s*' + number_range + r'\s*(?:كيلو|كغم|كغ)?'
                    r'\s*(?:=|:|يناسب|يلبس|يعادل)?\s*(?:قياس|مقاس)\s*\d{2}')
    size_first = (r'(?:قياس|مقاس)\s*\d{2}\s*(?:=|:|يناسب|يلبس)?'
                  r'\s*(?:ل?وزن|للاوزان|اوزان)\s*' + number_range)
    return bool(re.search(weight_first, facts) or re.search(size_first, facts))


def _declared_size(question):
    text = normalized(question)
    return bool(re.search(r'(?:قياسي|مقاسي|البس|قياس|مقاس|سايز)\s*(?:\d{2}(?!\d)|[2-6]?xl|xxl|xxxl|[sml])(?!\w)|اريد(?:ه|ها)?\s*(?:[2-6]?xl|xxl|xxxl|xs|[sml])(?!\w)', text))


def measurement_fact(product, kind):
    """A labelled numerical garment measurement, never a staff instruction."""
    labels = {'width': r'عرض(?:ه|ها)?|تحت الابط|محيط الصدر',
              'length': r'طول(?:ه|ها)?|الطول'}
    if kind not in labels:
        return ''
    for key in ('measurements', 'notes', 'description', 'fit_notes'):
        value = customer_copy.fact((product or {}).get(key)).strip()
        for clause in re.split(r'[\n؛;]+|(?<=[.!؟?])\s+', value):
            clean = normalized(clause)
            if (len(clause) <= 220 and not customer_copy.has_instructions(clause)
                    and not re.search(r'يحتاج|تاكيد|ما عندي|غير معروف|[؟?]', clean)
                    and re.search(r'(?:' + labels[kind] + r').{0,35}?\d{1,3}(?:[.,]\d+)?\s*(?:سم|سانتي|سنتيمتر|cm)(?!\w)', clean)):
                return clause
    return ''


def detail_fact(product, kind):
    """Read a construction answer, never turn the question's wording into a fact."""
    if kind in {'width', 'length'}:
        return measurement_fact(product, kind)
    patterns = {'closure': r'لاستيك|استيك|(?<!\w)(?:دگم|دكم)(?!\w)|ازرار|أزرار|سحاب|كبس',
                'lining': r'بطان|مبطن', 'stretch': r'مطاط|ليكرا|يتمدد'}
    if kind not in patterns:
        return ''
    for key in (kind, 'faq', 'notes', 'description'):
        value = customer_copy.fact((product or {}).get(key), key)
        for clause in re.split(r'[.؟?\n؛]+', value):
            clean = normalized(clause).strip()
            if (not clean or len(clause) > 160 or customer_copy.has_instructions(clause)
                    or re.search(r'يحتاج|تاكيد|غير معروف|هل |لو |حسب الطلب|حسب الموديل', clean)):
                continue
            if (key == kind or re.search(patterns[kind], clean)):
                return clause.strip(' ،')
    return ''


def missing_fact_reply(question, product):
    if not product:
        return ''
    text=normalized(question)
    if (re.search(r'وزن|وزني|كيلو|كغم', text)
            and not _has_weight_size_table(product) and not _declared_size(question)):
        return 'حتى أضبط لج القياس، شكد قياس تلبسين؟'
    if 'فيديو' in text and not product.get('video_url'):
        return 'هسه ما عندي فيديو لهالموديل، المتوفر صوره.'
    if re.search(r'لاستيك|استيك|(?<!\w)(?:دگم|دكم)(?!\w)', text) and not re.search(
            r'لاستيك|استيك|(?<!\w)(?:دگم|دكم)(?!\w)', normalized(' '.join(
                customer_copy.fact(product.get(key)) for key in
                ('closure', 'stretch', 'description', 'faq', 'notes')))):
        return ('حقج تسألين عيني 🌷 ما أريد أخمّن عليج إذا لاستيك لو دگم.\n'
                'أخلّي البنية تشيّكه من القطعة نفسها وترد عليج بجواب مضبوط.')
    if re.search(r'عريض|واسع|ضيق', text) and re.search(r'\d{2}', text) and not measurement_fact(product, 'width'):
        size = re.search(r'(?:قياسي|مقاسي|قياس|مقاس)\s*([3-6]\d)(?!\d)|(?<!\d)([3-6]\d)\s*(?:يناسب|يجي|يلبس)', text)
        chosen = f'قياس {size[1] or size[2]}' if size else 'القياس اللي ترتاحين بيه'
        name = customer_copy.fact(product.get('product_name')).strip() or 'القطعة'
        part = 'قميص السوت' if 'قميص' in text and 'سوت' in normalized(name) else name
        if size:
            return (f'إي فهمتج عيني 🌷 يهمج تعرفين إذا {part} ب{chosen} يطلع واسع.\n'
                    'أخلّي البنية تشيّك عرضه من القطعة نفسها. تحبينه واسع شوي لو هواية؟')
        return (f'فهمتج عيني، تريدين {part} يطلع واسع ومريح 🌷.\n'
                'شنو قياس الملابس اللي ترتاحين بيه عادة؟')
    if re.search(r'عرض(?:ه|ها)|(?<!\w)و?العرض(?!\w)|عرض\s+(?:القطعة|الصدر)|عرض.{0,25}(?:سم|سانتي|سنتيمتر)|تحت الابط|محيط الصدر',text) and not measurement_fact(product, 'width'):
        return 'عرض القطعة بالسنتيمتر يحتاج قياس من المتجر حتى نتأكد من الرقم الصحيح.'
    return ''


def fact_requires_human(question, product):
    text = normalized(question)
    if not product:
        return False
    if re.search(r'لاستيك|استيك|(?<!\w)(?:دگم|دكم)(?!\w)', text) and missing_fact_reply(question, product):
        return True
    return (bool(re.search(r'عرض(?:ه|ها)|(?<!\w)و?العرض(?!\w)|عرض\s+(?:القطعة|الصدر)|عرض.{0,25}(?:سم|سانتي|سنتيمتر)|تحت الابط|محيط الصدر|عريض|واسع|ضيق', text))
            and not measurement_fact(product, 'width'))
