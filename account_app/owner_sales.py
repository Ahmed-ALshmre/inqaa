"""Owner-approved sales policy, kept separate from untrusted chat/catalog prose.

Approved October 26, 2026. These are store policies, not a universal size/weight
conversion. Operational changes still require real staff execution.
"""
import hashlib
import re

try:
    from . import customer_copy, product_variants, sales_answers, sales_strategy
    from .pricing import terms, offer_tiers, quote
except ImportError:
    import customer_copy, product_variants, sales_answers, sales_strategy
    from pricing import terms, offer_tiers, quote

GUIDE = '''[سياسة المالك المعتمدة — تتقدم على صياغات البيع القديمة المتعارضة]
اشتغلي بروح موظفة عراقية شاطرة بالمبيعات: جاوبي مباشرة وباختصار وحنية، وكملي من آخر نقطة بالمحادثة.
نوّعي الود بصورة طبيعية دون تكراره بكل رد. تصريح الزبون وتصحيحه لصيغة الخطاب يتقدمان على الاسم. عند غياب تصريح اعتمدي كلامه عن نفسه ثم الاسم الأول الواضح. في متاجر لمسة والفاتنة وخيوط النسائية صيغة الخطاب الافتراضية مؤنثة عند غياب دليل أوضح؛ هذا تفضيل مخاطبة من المالك وليس جنساً مؤكداً ولا يُحفظ كهوية. للرجل المعروف خطاب المذكر المحترم. في الأطفال والجملة وغيرهما، المجهول بصيغة محايدة فعلية مثل «شنو القياس المطلوب؟»، لا «شكد تلبس؟». جنس الطفل أو المستفيد من الهدية لا يحدد خطاب المشتري. لا تدعي أنك إنسان إذا سئلت عن هويتك.
يمكن إيموجي واحد أو اثنين في بعض الردود عند الحاجة فقط، وليس بكل رسالة أو بعد الشكوى.
الرد من رسالة إلى أربع رسائل قصيرة حسب الحاجة، لكل رسالة معنى كامل. لا تحولي كلمة أو رمزاً إلى إشعار منفصل. جاوبي كل الأسئلة، وآخر رسالة أثناء البيع تنتهي بسؤال واحد يناسب الناقص: قياس، لون إذا متعدد، محافظة، أو أحجزلج؟ لا سؤال بيع بعد التثبيت أو الشكر أو الرفض أو التحويل.
ممنوع نهائياً سؤال تحبينه مضبوط لو واسع أو أي مرادف له. رغبة الزبونة بالوسع ليست طلب قياس عرض بالسنتيمتر ولا تستدعي التدخل. قياسات متاجر الملابس النسائية متاحة من S إلى 5XL وفق تأكيد المالك، وليست تحويلاً عالمياً إلى وزن. XXL وإكسين نفس التسمية. استخدمي قياس الزبونة مباشرة ولا تعيدي سؤال الوزن بعده. 50،52 نطاق قياس للزبونة وليس طلب قطعتين أو ضرورة سؤالها أيهما.
بمتاجر الأطفال نعتمد العمر فقط. لا تطلبي وزن الطفل أو طوله. سنة ونص لموديل يبدأ بسنتين: اشرحي بشكل مشروط إذا بنيتها خشنة ممكن يجي مناسب، مع الفحص؛ لا تسجلي السنتين بدل عمرها قبل قبولها.
المنتج المربوط بالإعلان معروف: أجيبي باسمه وسعره الحالي من أول رد، دون طلب صورة أو قول مو متأكدة. فشل تحليل صورة لا يلغي ربط الإعلان. صورة تطابقت فعلاً مع منتج آخر أو تصحيح صريح من الزبونة يُحدّث الاختيار.
لا تسردي سياسة المتجر كلها في كل دور. اذكري فقط السياسة التي سأل عنها الزبون أو تزيل تردده الحالي. نوّعي تركيب الجمل بصورة طبيعية من سياق المحادثة، دون تكرار التحية والعرض والسؤال المحسوم.
طلب التخفيض مساومة طبيعية، وليس مشكلة نظام أو سبب تحويل أو شرطاً يعلق المحادثة. ارفضي التخفيض الإضافي بلطف واذكري السعر الحقيقي؛ قولي عليه عرض فقط إذا العرض مثبت. تابعي الاختيار الناقص دون إعادة الأسئلة المجابة. بعد توضيح السعر، موافقة الزبون أو إكمال بيانات الحجز يسمحان بالمتابعة بالسعر الحقيقي، دون مطالبته بعبارة «ألغي الشرط». لا تقبلي السعر الذي اقترحه من عندك ولا تثبتي خلال المساومة نفسها.
لا تستخدمي «حسب سياسة المتجر» أو «بشرط» أو لغة العقود كحشو. اشرحي المعلومة مباشرة عند السؤال عنها. الصورة المتعرف عليها تحتاج جواباً مفيداً عن الموديل والسؤال المرافق، لا «وصلت الصورة» أو «اختيارج حلو» وحدها. حافظي على القياس واللون والاختيارات المعروفة، واطلبي فقط الناقص فعلاً.
السعر مباشر؛ اذكري العرض وقلة الكمية وانتهاء العرض فقط إذا مؤكدات ببيانات المنتج. لا تعرضي فيديو من نفسج. إذا لا توجد صور إضافية قولي بس هاي الصورة متوفرة وطمئني بالفحص.
الدفع عند الاستلام، والفحص بوجود المندوب قبل الدفع. إذا القطعة لا تناسب أو بيها إشكال ترجع مع المندوب مجاناً دون دفع أي شيء حتى أجور التوصيل. لا تمددي هذه السياسة لما بعد الدفع ومغادرة المندوب؛ تلك الحالات للبشر.
الاستلام الجزئي مسموح للقطع المنفصلة ويحسب المستلم بسعر المفرد؛ البكجات والعروض المشروطة بعدد تُستلم كاملة أو ترجع كاملة، ولا تباع قطعة من بكج كامل فقط.
الألوان: اذكري الموجود بنفس الموديل فقط. عند نفاد الموديل اقترحي غيره من نفس المتجر؛ قبل إرسال البدائل اسألي عن القياس أو العمر إن كان مجهولاً فقط. اجمعي صور الموديل نفسه وألوانه بوصف واحد، والموديلات المختلفة بفقرات واضحة داخل رسالة متماسكة؛ إذا كلها غير متوفرة اختصري ذلك مرة واحدة.
لا تطلبي الاسم. عند شراء صريح وبيانات مكتملة ثبتي مباشرة، دون تأكيد شراء ثان. اطلبي الناقص فقط؛ رقم ناقص يصحح قبل التثبيت، والمحافظة وحدها لا تكفي ويكفي عنوان منطقة أو قرية معروف؛ النقطة الدالة إضافة اختيارية وليست شرط حجز. ادمجي تفاصيل العنوان المتفرقة ولا تعيدي طلب المعلومة الموجودة. آخر تصحيح للزبونة يتقدم على القديم لكل قطعة. رسالة التثبيت الحالية يرسلها النظام مرة واحدة بعد نجاح الحفظ، ولا تعاد بسبب سؤال لاحق.
الصوت الواضح يعالج مباشرة بنموذج المبيعات نفسه؛ الصوت غير الواضح أو غير القابل للقراءة يحوّل للتدخل البشري دون تخمين أو مطالبة الزبون بإعادته. حوّلي أيضاً عند طلب موظفة أو رقم المتجر أو اختلاف سعر الإعلان أو تفاصيل مجهولة مثل الطول والبطانة والمطاطية أو إلغاء أو تعديل أو إضافة بعد الحجز أو تأخر الطلب أو الشكاوى والإرجاع بعد الدفع. تفاصيل مجهولة وصوتيات غير مفهومة: لا رد بديل ولا كليشة انتظار. عند إجراء طلب/شكوى وبعد إنشاء المراجعة فعلاً: هسه أحوّل رسالتج للقسم المختص حبيبتي؛ عدلي الخطاب حسب الجنس.
التوصيل حسب إعدادات المتجر، والمعتاد يوم أو يومان. طلب باچر ليس ضماناً: إن شاء الله يوصل بس مو أكيد، إحنا نسلمه لشركة التوصيل. إذا يشترط الوصول المضمون حوّلي للبشر قبل الحجز. لا تعدي بوصول باچر إذا الوقت الحالي أو السياسة لا يدعمان ذلك.
سؤال أجور التوصيل العادي جوابه الأجور والمدة المطلوبة فقط. لا تقولي «الأجور لشركة التوصيل مو إلنا» إلا عند اعتراض صريح على الأجور أو طلب إعفائها. سؤال «السعر يشمل التوصيل؟» استيضاح وليس اعتراضاً. لا تخترعي إعفاءً. عند الاعتراض اشرحي السعر والعرض المثبت دون تغيير المنتج من نفسج.
ممنوع: وصل المرفق، تم استلام الصورة، هذه التفاصيل تحتاج تأكيداً، أخلّي البنية تشيكه، هسه يفتح الألبوم، أو ادعاء إرسال/تحويل/حجز/تعديل لم ينفذه النظام. فشل صور مؤكد يعاد مرة واحدة ثم تدخل بشري؛ الإرسال غير المؤكد لا يعاد عميانياً.
'''

AFFECTION = re.compile(r'(?<!\w)(?:حبيبتي|حبيبي|عمري|روحي|حياتي|قلبي|كلبي|عيوني|عيني)(?!\w)')
EMOJI = re.compile('[\U0001F300-\U0001FAFF\u2600-\u27BF][\uFE0F\u200D]*')
FIT_QUESTION = re.compile(r'(?:تحبين|تحب|تريدين|تريد)\s*(?:ه|ها)?\s*(?:يطلع|يكون|يجي)?\s*(?:مضبوط|ضيق|فيت|واسع)\s*(?:عليج\s*)?(?:لو|او|أو)\s*(?:مضبوط|ضيق|فيت|واسع)[؟?]?', re.I)


FORBIDDEN_REPLY = re.compile(
    r'وصل(?:ت|تني|تنا|نا)?\s*(?:المرفق|المرفقات|الصورة|الصوره)|تم استلام (?:الصورة|المرفق)|'
    r'(?:الصورة|الصوره)\s*(?:وصلت|عندي)|'
    r'هذه التفاصيل تحتاج تاكيد|اخلي البني[ةه].{0,25}(?:تشيك|ترد)|هسه يفتح.{0,15}الالبوم')


# Addressing hints only: never persist an inferred identity in customers.gender.
FEMALE_NAMES = set('ساره سارة زينب فاطمه فاطمة مريم هدى زهراء دعاء اسراء ايات آيات رقيه رقية حوراء سعاد سحر امل عبير اريج بتول حنان حنين مها رنا رشا ريم رحاب رباب شيماء شهد شذى علياء غاده غادة دلال دانيا دانيه دانية ضحى نسرين ندى نوال هناء هبه هبة هاجر ياسمين يسرى بشرى خديجه خديجة عائشه عائشة اسماء اسيل جنات سوسن لقاء ليلى لينا روان رؤى رفيف رغد'.split())
MALE_NAMES = set('احمد محمد محمود علي حسن حسين حيدر مصطفى عمر عثمان خالد وليد كريم مهدي عادل ياسر ياسين قاسم عباس عبدالعزيز عبدالله عبدالرحمن ابراهيم اسماعيل اسحاق يوسف يعقوب زكريا حامد ماجد امجد مؤيد رائد ثائر سامر ماهر طارق لؤي اكرم حمزه حمزة هاشم صادق عمار اسامه اسامة ناصر ناظم نبيل يحيى بلال اسعد سعد سيف'.split())
FEMALE_LATIN = set('sara sarah zainab zaynab fatima fatimah maryam mariam huda zahraa zahra rania rasha reem nada noorhan shahad shaimaa shayma hiba heba haneen hanan esraa israa aya ayat duaa doaa raghad rafif rafeef hajar hajer batool batul ruba lubna asmaa aseel yasmin yasmine nourhan nora nura'.split())
MALE_LATIN = set('ahmed ahmad mohamed mohammad mohammed muhammad ali hassan hasan hussein hussain haider hayder mustafa omar osman khaled waleed kareem karim mahdi adel yasser ibrahim ismail youssef yusuf'.split())

FEMALE_NAMES = {customer_copy.normalized(n) for n in FEMALE_NAMES}
MALE_NAMES = {customer_copy.normalized(n) for n in MALE_NAMES}


WOMENS_STORES = frozenset({'default', 'al-fatena', 'khuyoot'})


def explicit_gender(text):
    """Only a direct self-identification/address request may be persisted."""
    clean = customer_copy.normalized(text)
    hits = []
    for gender, nouns, request in (
        ('female', r'بنت|بني[هة]|امرا[هة]|حرم[هة]|زوج[هة]|انثي', r'ك(?:مؤنث|مونث)|بصيغ[هة] ال(?:مؤنث|مونث)'),
        ('male', r'رجل|رجال|ولد|شاب|زوج', r'كمذكر|بصيغه المذكر|بصيغة المذكر'),
    ):
        pattern = r'(?<!\w)(?:(?:اني|انا)\s+(?:' + nouns + r')|خاطب(?:ني|يني)\s+(?:' + request + r'))(?!\w)'
        hits.extend((m.start(), gender) for m in re.finditer(pattern, clean))
    return max(hits)[1] if hits else ''


def self_address_hint(text):
    """Grammatical self-description, never a recipient or address to the agent."""
    clean = customer_copy.normalized(text)
    for gender, adjectives in (
        ('female', r'محتار[هة]|متردد[هة]|خايف[هة]|لابس[هة]|محتاج[هة]|مستعجل[هة]|حامل|مرضع[هة]|موظف[هة]|طالب[هة]|متاكد[هة]|حاجز[هة]'),
        ('male', r'محتار|متردد|خايف|لابس|محتاج|مستعجل|موظف|طالب|متاكد|حاجز'),
    ):
        if re.search(r'(?<!\w)(?:اني|انا)\s+(?:مو\s+)?(?:' + adjectives + r')(?!\w)', clean):
            return gender
    return ''


def address_gender(name, explicit='', *, text='', history=(), store_id=''):
    """Conservative name hint for wording; explicit customer/staff choice wins.

    Shared names and aliases use the store's wording default, never an identity.
    A female customer's father's name later in the display name never wins.
    """
    latest = explicit_gender(text)
    if latest:
        return latest
    if explicit in {'male', 'female'}:
        return explicit
    incoming = [str(m.get('text') or m.get('content') or '') for m in history
                if isinstance(m, dict) and (m.get('direction') == 'incoming' or m.get('role') == 'user')]
    for previous in reversed(incoming):
        stated = explicit_gender(previous)
        if stated:
            return stated
    for previous in [text] + list(reversed(incoming)):
        hint = self_address_hint(previous)
        if hint:
            return hint
    clean = customer_copy.normalized(name)
    words = re.findall(r'[^\W\d_]+', clean)
    fallback = 'female' if store_id in WOMENS_STORES else ''
    if not words or any(w in {'متجر', 'ستور', 'store', 'shop', 'بوتيك', 'شركة', 'مؤسسة'} for w in words):
        return fallback
    if words[0] in {'its', 'mrs', 'ms', 'miss'} and len(words) > 1:
        words = words[1:]
    # Honorifics must be standalone words: أمجد is not أم + a child's name.
    if words[0] in {'ام', 'umm', 'um'} and len(words) > 1:
        return 'female'
    if words[0] in {'ابو', 'ابي', 'abu', 'abo'} and len(words) > 1:
        return 'male'
    first = words[0]
    if first in FEMALE_NAMES or first in FEMALE_LATIN:
        return 'female'
    if first in MALE_NAMES or first in MALE_LATIN:
        return 'male'
    return fallback


def address_guidance(gender):
    if gender == 'female':
        return 'صيغة الخطاب المعتمدة لهذه المحادثة: مؤنث. قولي «شكد قياس تلبسين؟» و«قياسج» و«تحبين». لا ترجعي للمذكر بسبب اسم حساب مستعار أو مجهول.'
    if gender == 'male':
        return 'صيغة الخطاب المعتمدة لهذه المحادثة: مذكر محترم. احترمي تصريح الزبون ولا تحوّلي خطابه للمؤنث بسبب الملابس النسائية أو جنس الطفل.'
    return 'صيغة الخطاب غير محددة: استخدمي جملاً محايدة مثل «شنو القياس المطلوب؟» و«من يا محافظة؟». «تلبس» و«تحب» و«تفضل» خطاب مذكر وليست صيغة محايدة.'


def neutral_address(text):
    text = AFFECTION.sub('', text)
    text = re.sub(r'(هلا)\s+بي[كج](?!\w)', r'\1', text)
    text = re.sub(r'شنو\s+حاب[هة]?\s+تشوف(?:ين)?(?:\s+من\s+الموديلات)?[؟?]?', 'أكو موديل معين؟', text)
    text = re.sub(r'(?:شكد|شنو)\s+(?:(?:القياس|قياس)\s+)?تلبس(?:ين)?(?:\s+قياس)?[؟?]?',
                  'شنو القياس المطلوب؟', text)
    text = re.sub(r'تحب(?:ين)?\s+تشوف(?:ين)?', 'أعرض', text)
    for before, after in [('قياس[كج]', 'القياس'), ('عنوان[كج]', 'العنوان'), ('رقم[كج]', 'رقم الموبايل'),
                          ('اختيار[كج]', 'الاختيار'), ('رسالت[كج]', 'الرسالة'),
                          (r'(?:أحجز|احجز|أثبت|اثبت)ل[كج](?:\s+(?:ال)?حجز)?', 'أثبت الحجز'),
                          ('(?:دزي|دز|ارسلي|ارسل|أرسلي)(?:لي)?', 'ممكن إرسال')]:
        text = re.sub(r'(?<!\w)' + before + r'(?!\w)', after, text)
    return text


def warmth_allowed(question, history=()):
    clean=customer_copy.normalized(question)
    if re.search(r'شكوي|شكوى|انزعج|زعلان|ليش.*(?:طلب|رد|ثبت)|ما وصل|تاخر|رسمي|بدون.*(?:القاب|ألقاب|حبيبتي|عمري)|بلا.*(?:القاب|ألقاب)',clean):
        return False
    incoming=[str(m.get('text') or m.get('content') or '') for m in history
              if m.get('direction')=='incoming' or m.get('role')=='user']
    if any(re.search(r'رسمي|(?:بدون|بلا).*(?:القاب|ألقاب|حبيبتي|عمري)',customer_copy.normalized(t)) for t in incoming[-20:]):
        return False
    outgoing=[str(m.get('text') or m.get('content') or '') for m in history
              if m.get('direction')=='outgoing' or m.get('role')=='assistant']
    return not any(AFFECTION.search(t) for t in outgoing[-4:])


def surface(text, gender='', seed='', *, warm=True, store_id='', allow_endearments=True):
    """One coherent surface for local and model replies; never alters receipts."""
    text = str(text or '').strip()
    if not text or re.match(r'^تم\s+(?:تثبيت|تسجيل|حجز|تأكيد|تاكيد)', text):
        return text
    text = FIT_QUESTION.sub('', text).strip()
    if not allow_endearments:
        text=AFFECTION.sub('',text).strip(' ،,')
        warm=False
    female = gender == 'female'
    if gender == 'male':
        text = AFFECTION.sub('', text)
        for before, after in [('تدللين', 'تدلل'), ('تحبين', 'تحب'), ('تلبسين', 'تلبس'),
                              ('تشوفين', 'تشوف'),
                              ('تريدين', 'تريد'), ('تكدرين', 'تكدر'), ('تفحصين', 'تفحص'),
                              ('تتأكدين', 'تتأكد'), ('تتاكدين', 'تتاكد'), ('ترجعين', 'ترجع'),
                              ('تدفعين', 'تدفع'), ('دزيلي', 'دزلي'), ('قياسج', 'قياسك'),
                              ('عنوانج', 'عنوانك'), ('رقمج', 'رقمك'), ('عليج', 'عليك'),
                              ('اختيارج', 'اختيارك'), ('يعجبج', 'يعجبك'), ('يناسبج', 'يناسبك'),
                              ('دزي', 'دز'), ('ارسلي', 'ارسل'), ('أرسلي', 'أرسل'),
                              ('تفضلي', 'تفضل'), ('تفضلين', 'تفضل'), ('تختارين', 'تختار'),
                              ('أدزلج', 'أدزلك'), ('ادزلج', 'ادزلك'), ('أشوفلج', 'أشوفلك'),
                              ('اشوفلج', 'اشوفلك'), ('أرسلج', 'أرسلك'), ('ارسلج', 'ارسلك'),
                              ('لج', 'لك')]:
            text = re.sub(r'(?<!\w)(و?)' + before + r'(ه|ها|هم|هن)?(?!\w)',
                          lambda match: match[1] + after + (match[2] or ''), text)
        # "بيج" is also a catalog color; only change it in a greeting.
        text = re.sub(r'(هلا\s+)بيج\b', r'\1بيك', text)
        text = text.replace('أحجزلج', 'أحجزلك').replace('أثبتلج', 'أثبتلك').replace('رسالتج', 'رسالتك')
    elif female:
        for before, after in [('تدلل', 'تدللين'), ('تحب', 'تحبين'), ('تلبس', 'تلبسين'),
                              ('حاب', 'حابة'), ('محتاج', 'محتاجة'),
                              ('تشوف', 'تشوفين'),
                              ('تريد', 'تريدين'), ('تكدر', 'تكدرين'), ('تفحص', 'تفحصين'),
                              ('دزلي', 'دزيلي'), ('اختيارك', 'اختيارج'), ('يعجبك', 'يعجبج'),
                              ('يناسبك', 'يناسبج'), ('قياسك', 'قياسج'), ('عنوانك', 'عنوانج'),
                              ('لك', 'لج'), ('عندك', 'عندج'), ('بيك', 'بيج'),
                              ('أحجزلك', 'أحجزلج'), ('أثبتلك', 'أثبتلج'),
                              ('أدزلك', 'أدزلج'), ('ادزلك', 'ادزلج'), ('أشوفلك', 'أشوفلج'),
                              ('اشوفلك', 'اشوفلج'), ('تتأكد', 'تتأكدين'), ('تتاكد', 'تتاكدين'),
                              ('ترجع', 'ترجعين'), ('تدفع', 'تدفعين'),
                              ('رقمك', 'رقمج'), ('عليك', 'عليج'), ('رسالتك', 'رسالتج'),
                              ('ارسل', 'ارسلي'), ('دز', 'دزي'),
                              ('تفضل', 'تفضلي')]:
            text = re.sub(r'(?<!\w)(و?)' + before + r'(ه|ها|هم|هن)?(?!\w)',
                          lambda match: match[1] + after + (match[2] or ''), text)
        choice = ('حبيبتي', 'عمري', 'روحي', 'حياتي', 'عيني')[int(hashlib.sha256(str(seed).encode()).hexdigest()[:8], 16) % 5]
        matches = list(AFFECTION.finditer(text))
        if matches:
            text = AFFECTION.sub(lambda m: choice if m.start() == matches[0].start() else '', text)
        elif warm:
            text = choice + '، ' + text
    else:
        text = neutral_address(text)
    if gender == 'male' and store_id in WOMENS_STORES:
        # A man buying women's clothing need not be the wearer. Ask for the
        # requested size without turning his own address feminine.
        text = re.sub(r'(?:شكد|شنو)\s+(?:قياس\s+)?تلبس(?:\s+قياس)?\s*[؟?]',
                      'شنو القياس المطلوب؟', text)
    # Across the entire turn, not two emoji per bubble.
    count = 0
    def limit_emoji(match):
        nonlocal count
        count += 1
        return match[0] if count <= 2 else ''
    text = EMOJI.sub(limit_emoji, text)
    text = re.sub(r'[ \t]{2,}', ' ', text)
    text = re.sub(r'([،,])\s*[،,]', r'\1', text)
    text = re.sub(r'\s+([،,.؟?])', r'\1', text)
    return text.strip(' ،,')


def human_reason(text, *, post_order=False, message_type='text'):
    clean = customer_copy.normalized(text)
    if message_type == 'audio':
        return 'voice_message'
    if re.search(r'(?:رقم|موبايل|هاتف).{0,20}(?:المتجر|المحل|الصفحة|الصفحه|عدكم|عندكم)|(?:رقمكم|رقمج حتى اتصل|اريد رقم اتصل)', clean):
        return 'store_phone_requested'
    if re.search(r'(?:اريد|ممكن|خليني|حولني).{0,25}(?:موظف|بشر|انسان)|ما اريد.{0,12}(?:بوت|ذكاء)', clean):
        return 'customer_requested_human'
    if re.search(r'(?:اذا|لازم|ضروري|اكيد|تضمن).{0,25}(?:باچر|باجر|غدا|غداً).{0,25}(?:اكيد|مضمون|ما اريد|ماريد)|(?:اذا|لازم).{0,12}(?:مو اكيد|مو مضمون)', clean):
        return 'guaranteed_arrival_required'
    conditional_inspection = bool(re.search(r'اذا|لو', clean) and re.search(r'نفس الصور|افحص|فحص', clean) and not re.search(r'استلمت|دفعت|راح المندوب', clean))
    if post_order and not conditional_inspection:
        if re.search(r'(?:جاوبي|كلمي|اتصلي|ردي).{0,20}المندوب|المندوب.{0,20}(?:ينتظر|يتصل)|وصل\s*(?:لولا|لو لا)|(?:يوصل|واصل).{0,12}اليوم', clean):
            return 'post_order_delivery_status'
        if re.search(r'بدل|بدّل|غيري|غيرلي|تغيير|الغ|الغاء|اضيف|ضيف|زيد|زودي|ارجع|استبد|اشتكي|شكوى|ما وصل|ماوصل|مو واصل|تاخر|صار.{0,12}(?:يومين|ايام)|وين طلبي|وين الطلب|استلمت|وصلني.{0,25}(?:صغير|ضيّق|ضيق|مو|غير)', clean):
            return 'post_order_action'
    return ''


def result(text='', *, human='', silent=False):
    return dict(reply='' if silent else text, reply_parts=[] if silent else [text],
                create_order=False, order={}, requires_human=bool(human),
                failed=bool(human), failure_reason=human, handoff_reason=human,
                _needs_fact_review=bool(human),
                _owner_policy=True, _local_reply=True, _suppress_product_images=True,
                _semantic_parts=True)


def next_step(product, context, customer):
    try:
        from .sales_playbook import customer_selection, weight_cart_choices
    except ImportError:
        from sales_playbook import customer_selection, weight_cart_choices
    known = customer_selection(context, customer)
    years = product_variants.size_data((product or {}).get('sizes'))['years']
    if years and known['age'] is None:
        return 'شكد العمر المطلوب؟'
    if not years and not known['size'] and product_variants.weight_only_ranges(product):
        plan=weight_cart_choices(product,context,customer)
        if plan and plan.get('question'):return plan['question']
    elif not years and not known['size']:
        return 'شكد قياس تلبسين؟'
    colors = product_variants.colors((product or {}).get('colors'))
    if len(colors) > 1 and not known['color']:
        return 'أي لون تحبين؟'
    if not (customer or {}).get('province'):
        return 'من يا محافظة؟'
    return 'أثبّت لج حجز؟'


def delivery_objection(text):
    text = customer_copy.normalized(text)
    if re.search(r'ارجع|يرجع|ترجع|فحص|افحص', text):
        return False
    return bool(re.search(
        r'(?:اريد|اريده|اريدها|خلي|خلو|سووا|سوي|يصير).{0,25}(?:توصيل مجاني|التوصيل مجاني|بدون اجور|التوصيل عليكم)|'
        r'(?:ما ادفع|ما انطي|ماانطي).{0,18}(?:توصيل|اجور)|'
        r'(?:التوصيل|اجور التوصيل).{0,15}(?:غالي|غاليه|هواي|وايه|عليكم)|'
        r'ليش.{0,15}(?:اجور|التوصيل).{0,10}(?:غالي|خمسه|خمسة|5000|5)', text))


def delivery_wording(reply, question):
    if delivery_objection(question):
        return reply
    # Keep the fee itself while suppressing an unsolicited defensive clause,
    # including when an older saved prompt makes the model generate it.
    return re.sub(
        r'(?:و\s*)?(?:هاي\s+)?(?:(?:أ?جور(?:ه|ها)?|الأجور|الاجور)\s+)?التوصيل\s+(?:تابع[ةه]\s+)?لشركة\s+التوصيل(?:\s+مو\s+(?:إلنا|النا|إلنه|النه))?[،.!؛]*|'
        r'(?:و\s*)?(?:أ?جور(?:ه|ها)?|الأجور|الاجور)\s+لشركة\s+التوصيل(?:\s+مو\s+(?:إلنا|النا|إلنه|النه))?[،.!؛]*',
        '', reply).strip()


def conversational_wording(reply):
    """Remove policy-label filler; retain the actual fee, inspection or condition."""
    reply=re.sub(r'((?:التوصيل|الشحن)\s+)بالعالم\s+العادي(?!\w)',r'\1بالوضع العادي',reply)
    return re.sub(r'(?:حسب|وفق|ضمن|بموجب)\s+(?:سياس[ةه]|شروط)\s+(?:المتجر|المحل|الصفحة)(?:\s+المعتمدة)?[،,]?\s*',
                  '', reply).strip()


def negotiation_kind(text):
    clean = customer_copy.normalized(text)
    if re.search(r'ارخص|ميزاني|بديل|(?<!\w)(?:باجر|باچر|غدا)(?!\w)|يلحق|يوصل|موعد|لازم اليوم', clean):
        return ''
    if delivery_objection(clean):
        return 'free_delivery'
    if re.search(r'خصم|تخفيض|تنزل\w*.{0,15}سعر|تنقص\w*.{0,15}سعر|يصير\s*(?:اقل|ارخص)|'
                 r'(?:اخر|آخر)\s*سعر|سعر\w*\s*نهائي|خلي\w*\s*(?:ب|بـ)\s*\d', clean):
        return 'price'
    return ''


def negotiation_reply(ev, product, customer, context, history, *, delivery_options=(), kind=''):
    """Decline ordinary haggling, quote current facts, then continue the dialogue."""
    kind = kind or negotiation_kind(ev.get('text'))
    if not kind or not product or ev.get('_post_order'):
        return None
    selected = set(sales_answers.topics(ev.get('text')))
    if selected - {'price', 'delivery', 'fabric', 'colors', 'sizes'}:
        return None  # Preserve the normal route for independent unknown facts.
    try:
        count, price, _ = terms(product)
    except (ValueError, TypeError, KeyError):
        return None
    unit = 'البكج' if count > 1 else 'القطعة'
    label = 'سعر البكج' if count > 1 else 'سعره'
    if kind == 'price':
        offered = bool(customer_copy.fact(product.get('offer')) or
                       re.search(r'(?<!\w)بدل(?!\w)', customer_copy.normalized(product.get('price'))))
        refusal = ('ما نكدر ننزل أكثر، عليه عرض حاليًا' if offered else 'هذا السعر النهائي، ما عدنا تخفيض حاليًا')
        opening = f'{refusal}، {label} {customer_copy.chat_amount(price)} و{unit} تستاهل.'
    elif kind == 'free_delivery' and delivery_options:
        try:
            fees = [(region, quote([dict(product_id=product['product_id'], quantity=count)], [product], fee)['delivery_fee'])
                    for region, fee in delivery_options]
        except (ValueError, TypeError, KeyError):
            return None
        if all(fee == 0 for _, fee in fees):
            opening = f'إي، التوصيل مجاني بهذا العرض، {label} {customer_copy.chat_amount(price)}.'
        else:
            costs = ' و'.join('التوصيل' + region + ' ' + (customer_copy.chat_amount(fee) if fee else 'مجاني') for region, fee in fees)
            opening = f'ما نكدر نخلي التوصيل مجاني، {costs}. {label} {customer_copy.chat_amount(price)}.'
    else:
        return None
    parts = [opening]
    detail_topics = selected - {'price', 'delivery'}
    if detail_topics:
        facts, gaps = sales_answers.product_fragments(ev.get('text'), product)
        if gaps:
            return None
        parts.extend(fact for fact in facts if not re.search(r'سعر|توصيل|د\.\s*ع', fact))
    if not sales_strategy.purchase_block_reason(ev.get('text')):
        try:
            from .sales_playbook import price_next_step
        except ImportError:
            from sales_playbook import price_next_step
        step = price_next_step(product, context, customer, history)
        if step:
            parts.append(step)
    reply = '\n'.join(parts)
    return dict(result(reply), reply_parts=parts, _browse_only=True,
                _negotiation_declined=kind, _semantic_parts=True)


def reply(ev, product, customer, context, history, *, photo_count=0):
    """Narrow, owner-approved turns before generic generation/recovery."""
    text = customer_copy.normalized(ev.get('text')).strip(' .،!؟?')
    reason = human_reason(text, post_order=bool(ev.get('_post_order')))
    if reason:
        return result(human=reason, silent=True)
    if re.search(r'(?:كاتبين|مكتوب|بالاعلان|بالاعلان).{0,45}(?:ليش|سعر|الف|\d)|ليش.{0,35}(?:اعلان|كاتبين)', text) and re.search(r'ليش|غير|فرق', text):
        return dict(result(human='advertisement_price_mismatch', silent=True), _needs_fact_review=True)
    if not product or ev.get('_post_order') or len(ev.get('_image_matches') or []) > 1:
        return None
    try:
        from .product_albums import stock_state
    except ImportError:
        from product_albums import stock_state
    if re.search(r'احجز|ثبت|اطلب|اشتري|07[0-9]{7,}', text):
        return None
    step = next_step(product, context, customer)
    try:
        from .conversation_quality import child_ages
    except ImportError:
        from conversation_quality import child_ages
    if len(child_ages(text)) > 1:
        return None
    if re.search(r'غيره|غيرها|غير موديل|بديل|موديلات|موديل ثاني', text):
        return None  # Search the catalogue instead of repeating the rejected model.
    if stock_state(product) == 'out' and not re.fullmatch(r'(?:اي|نعم|تمام|دزيلي|دزي|ارسلي)[ ؟?!.،]*', text):
        return result('للأسف هذا الموديل ما متوفر، تحبين أقترحلج غيره؟')
    if stock_state(product) != 'available':
        return None
    if ev.get('_ad_identity_fallback') and not set(sales_answers.topics(text)) - {'price'}:
        count, price, _ = terms(product)
        unit = ' للبكج الكامل' if count > 1 else ''
        offer = '، عليه عرض حالياً' if product.get('offer') else ''
        return result(customer_copy.fact(product.get('product_name')) + ' متوفر، سعره ' + customer_copy.chat_amount(price) + unit + offer + '. ' + step)
    if re.search(r'(?<!\w)(?:باچر|باجر|غدا|غداً)(?!\w)', text) and re.search(r'يوصل|توصيل|اريد|ضروري|مناسب', text):
        return result('إن شاء الله يوصلج باچر بس مو أكيد، إحنا نسلّمه لشركة التوصيل، يناسبج؟')
    years = product_variants.size_data(product.get('sizes'))['years']
    if years and min(lo for lo, hi in years) == 2 and re.search(r'سن[ةه]\s*(?:و\s*)?(?:نص|نصف)|سنه ونص|18\s*شهر', text):
        return result('إذا بنيتها خشنة، قياس السنتين ممكن يجي فيت ومناسب عليها، وتكدرين تفحصينه بوجود المندوب قبل الدفع، تحبين قياس السنتين؟')
    if re.search(r'اخذ.{0,15}(?:وحده|قطعه).{0,25}(?:ارجع|الثاني)|ارجع.{0,12}(?:الثاني|وحده)|استلام جزئي', text):
        count, _, _ = terms(product)
        restricted = count > 1 or bool(offer_tiers(product))
        return result(('هذا عرض كامل، ما نكدر نجزّئه؛ تستلمينه كامل أو ترجعينه كامل. ' if restricted else
                       'إي تكدرين تستلمين اللي يناسبج وترجعين الثانية ويا المندوب، والمستلمة تنحسب بسعر المفرد. ') + step)
    if re.search(r'(?:وحده|قطعه).{0,15}(?:بكج|البكج)|(?:بكج|البكج).{0,20}(?:مفرد|وحده|قطعه)', text):
        if terms(product)[0] > 1:
            return result('للأسف يجي بكج كامل، ما نكدر نبيعه مفرد، أحجزلج البكج؟')
    if re.search(r'صور.{0,20}(?:اكثر|اضافي|ثاني|غير)|غير.{0,15}صور|صوري|تصوير حقيقي|صورة حقيقية|تدزين نفس الصوره', text) and photo_count <= 1 and not re.search(r'قماش|خامة|سعر|طول|عرض|بطان|مطاط', text):
        return result('بس هاي الصورة متوفرة، بس تكدرين تفحصين الطلب بوجود المندوب قبل الدفع، وإذا بيه أي إشكال يرجع مجاناً بدون أجور توصيل. ' + step)
    if re.search(r'دفع|مقدم|مقدّم|احول|تحويل مبلغ|اخاف|مو نفس الصور|تضمن.{0,20}الصور|يختلف.{0,12}الصور|افحص|فحص|ارجعه.{0,20}(?:اذا|لو)|(?:اذا|لو).{0,15}(?:يرجع|ترجع)', text) and not set(sales_answers.topics(text)) - {'payment', 'inspection', 'delivery'}:
        return result('الدفع عند الاستلام، تفحصين القطعة وتتأكدين منها بوجود المندوب قبل الدفع، وإذا بيها أي إشكال ترجعينها مجاناً وما تدفعين أي شي حتى أجور التوصيل. ' + step)
    if delivery_objection(text):
        if not re.search(r'مجاني', str(product.get('delivery') or '')):
            offer = 'القطع عليها عرض حالياً، ' if product.get('offer') else ''
            return result(offer + 'وأجور التوصيل لشركة التوصيل مو إلنا. ' + step)
    if re.search(r'غالي|يصير اقل|خصم|تخفيض', text) and not re.search(r'ارخص|ميزاني|بديل|قماش|خامة', text) and sales_answers.known_budget(text, context) is None:
        offer = 'حالياً عليه عرض والقطعة تستاهل. ' if product.get('offer') else 'هذا سعر القطعة الحالي والقطعة تستاهل. '
        return result(offer + step)
    # Casual fit is not a request for the missing centimetre width.
    if re.search(r'سم|سانتي|سنتيمتر|محيط|عرض|بطان|سحاب|مطاط|لاستيك|طول', text):
        return None
    requested = product_variants.requested(text)['sizes']
    declared = re.search(r'(?:قياسي|مقاسي|البس قياس)\s*(\d{2})(?!\d)', text)
    if declared:
        requested = requested + [declared[1]]
    range_match = re.search(r'(?:قياس|قياسي|البس)\s*(\d{2})\s*[،,/ ]+\s*(\d{2})(?!\d)', text)
    if re.search(r'قماش|سعر|توصيل|لون|قياسات|فحص|الوان|الفرق', text):
        return None
    if requested or range_match or re.fullmatch(r'(?:اريده?\s+)?(?:واسع|فيت|راهي)', text):
        if years:
            return None  # Child availability answers the exact age, not adult fit.
        checks = product_variants.assess(product, text)
        if any(check['supported'] is False for check in checks):
            return None
        # Actual missing measurements still take the silent detail-review path.
        if (context or {}).get('open_details'):
            return None
        if requested or range_match:
            choice = range_match[1] + '-' + range_match[2] if range_match else requested[-1]
            step = next_step(product, context, dict(customer or {}, size=choice))
            return result('إي، قياس ' + choice + ' متوفر ويجي فيت ومرتب عالجسم، ' + step)
        return result('إي متوفر ويجي فيت ومرتب عالجسم، ' + step)
    return None
