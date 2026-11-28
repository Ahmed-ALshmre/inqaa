"""Hybrid Sales Engine playbook distilled from winning Iraqi human-staff chats.

Combines deterministic situation detection, verified multi-piece upsell math,
and dynamic few-shot winning Iraqi staff examples so the sales writer adapts
its tactics and conciseness to each turn without ever inventing prices,
stock, or unauthorized discounts.
"""
import re

from .. import checkout, pricing, product_albums

INSPECTION_RISK_RE = re.compile(
    r'(?:اذا|لو)\s+(?:ما\s*عجب|ماعجب|ما\s*يصير|مايصير|ما\s*رهم|مارهم|طلع\s+(?:اي\s+)?اشكال|مو\s+مضبوط)'
    r'|(?:هم\s+)?(?:ادفع|ندفع|اندفع|ماندفع|ما\s*ندفع)\s*(?:اجور|اجرة|فلوس)?\s*(?:ال)?توصيل'
    r'|يترجع|يرجع\s+(?:مع|ويا)\s+المندوب|اذا\s+رجعت'
)
DELIVERY_OBJECTION_RE = re.compile(
    r'توصيل\s+مجاني|مو\s+مجاني|التوصيل.{0,24}(?:عليكم|غالي|هواي|نص\s+سعر|بلاش|مجاني)'
    r'|ليش\s+(?:ال)?توصيل|اجور\s+التوصيل\s+(?:غالي|هواي)|توصيله?\s+غالي'
)
BARGAINING_RE = re.compile(
    r'(?:بي[هة]|اكو)\s+مجال|تنزل(?:ين|ون)?\s*(?:من\s+)?(?:ال)?سعر|خصم\s+اضافي|تخفيض'
    r'|شكد\s+اخر[هة]|كم\s+اخر[هة]|غالي\s*(?:كلش|شوي|علي)|ما\s*يصير\s+بـ?\s*\d'
)
POSTPONE_RE = re.compile(
    r'بعد\s+(?:اسبوع|فتر[هة]|كم\s+يوم)|خل(?:ي)?\s+افكر|افكر\s+وارد|مو\s+هسه|بعدين\s+اطلب|انتظر\s+الراتب|اشاور'
)
TOTAL_WITH_DELIVERY_RE = re.compile(
    r'(?:مع|ويا|ويه|وي)\s+(?:ال)?توصيل|(?:يعني|يصير)\s+(?:ال)?(?:حساب|مجموع|سعر)\s*\d*'
    r'|المجموع\s*(?:شكد|كم|\d+)|شكد\s+(?:يصير|تصير|الحساب|المجموع)'
)
INCLUDED_ITEMS_RE = re.compile(
    r'(?:ويا[هة]?|مع|يجي\s+ويا[هة]?)\s+(?:ال)?(?:حذاء|جنط[هة]|حقيب[هة]|جواريب|شال|حزام)'
    r'|شنو\s+يجي\s+ويا[هة]?|كم\s+قطع[هة]\s+(?:الطقم|البدل[هة])'
)
SIZE_HESITATION_RE = re.compile(
    r'اخاف\s+(?:فصال|قياس|صغير|كبير|ما\s*يرهم|ما\s*يكفي)|خاف\s+(?:صغير|كبير|ما\s*يرهم)'
    r'|ناعم[هة]|ضعيف[هة]|مربرب[هة]|سمين[هة]|مو\s+مضبوط'
)

# Real anonymized winning Iraqi human-staff patterns distilled from Zernio + Inqaa DB corpus.
# Kept concise (<= 110 chars per example) to anchor the writer's length and warmth.
SITUATION_PLAYBOOK = {
    'multi_photo_inquiry': {
        'tactic': (
            'للصور المتعددة: اعطي حالة كل موديل بسطر قصير (متوفر + السعر/العرض، أو مخلص) '
            'واقترحي تاخذ المتوفرات سوة بنفس التوصيل.'
        ),
        'example': (
            'الأول متوفر وسعره مناسب، والثاني هم متوفر وتكدرين تاخذيهم سوة بنفس التوصيل 🌷 '
            'شكد العمر/القياس اللي تحتاجينه؟'
        ),
    },
    'oos_or_size_pivot': {
        'tactic': (
            'إذا الموديل أو القياس مخلص: لا توكفين عند "مخلص"! '
            'وضحي باختصار واطرحي فوراً بديل متوفر من الخيارات المثبتة.'
        ),
        'example': (
            'حبيبتي هالموديل مخلص حالياً، بس متوفر عدنا موديلات قريبة إله وبنفس القياس، '
            'تحبين أدزلج صورهم؟'
        ),
    },
    'objection_inspection_risk': {
        'tactic': (
            'إذا سألت "إذا ما عجبني أدفع توصيل؟": جاوبي بوضوح تام وطمأنة صفر-مخاطرة '
            'حسب سياسة الفحص: تفحص وتقيس كدام المندوب وإذا ما طابق يرجع وياه بدون أي دفع حتى توصيل.'
        ),
        'example': (
            'أي عيوني أكيد، تفحصين وتقيسين كدام المندوب براحتج؛ عجبج بالعافية، '
            'وما طابق يرجع وياه وكلشي ما تدفعين حتى توصيل ما تدفعين 🌷'
        ),
    },
    'objection_delivery_fee': {
        'tactic': (
            'عند الاعتراض على أجور التوصيل: تعاطفي بلطف، وضحي أن التوصيل ثابت لكل الطلب، '
            'وذوّبي الكلفة باقتراح قطعتين بنفس التوصيل.'
        ),
        'example': (
            'عيوني التوصيل ثابت لكل الطلب، فحتى يطلع عليج أوفر هواية تكدرين تاخذين قطعتين '
            'سوة بنفس أجور التوصيل 🌷'
        ),
    },
    'objection_bargaining': {
        'tactic': (
            'عند المساومة على السعر: كوني لطيفة وحاسمة باختصار (الأسعار ثابتة/بالمعرض)، '
            'ولا تخترعين خصم شخصي؛ وجّهيها لعرض القطعتين أو بديل أوفر إن وجد.'
        ),
        'example': (
            'حياتي الأسعار ثابتة لأن القطع بسعر العرض، وإذا تاخذين قطعتين يطلع عليج أوفر '
            'وبنفس التوصيل 🌷'
        ),
    },
    'objection_postpone': {
        'tactic': (
            'عند المماطلة ("بعد أسبوع / أفكر"): احترمي قرارها بلطف وباختصار بدون إلحاح، '
            'ومع تذكير هادئ أن الموديلات والقياسات تخلص بسرعة.'
        ),
        'example': (
            'براحتج عيوني، بس حبيت أنبهج الموديل عليه طلب والقياسات تخلص بسرعة، '
            'وبأي وقت تحتاجين إحنا بالخدمة 🌷'
        ),
    },
    'total_with_delivery_check': {
        'tactic': (
            'إذا سألت عن المجموع مع التوصيل: اعطي المجموع النهائي الواضح مباشرة بفقاعة قصيرة '
            'باستعمال رموز amounts المثبتة بدون لف ودوران.'
        ),
        'example': (
            'إي عيوني، يصير المجموع كامل ويا التوصيل {{total}} والدفع عند الاستلام بعد الفحص 🌷'
        ),
    },
    'upsell_multi_piece': {
        'tactic': (
            'لرفع السلة لأكثر من قطعة: أبرزِي عرض القطعتين أو توفير التوصيل المشترك '
            'بجملة عفوية واحدة ثم سؤال اختيار ثنائي سهل.'
        ),
        'example': (
            'القطعة بـ{{unit_price}} وعليه عرض القطعتين بـ{{offer_total}} بنفس التوصيل يطلع عليج أوفر، '
            'تاخذين وحدة لو تستفادين من العرض؟'
        ),
    },
    'fast_close_contact': {
        'tactic': (
            'إغلاق سريع بأسلوب موظفة المحل: بمجرد اكتمال الاختيار، اختصري بفقاعة واحدة دافئة '
            'واطلبي العنوان والرقم مباشرة لتثبيت الحجز.'
        ),
        'example': (
            'من عيوني، بس دزيلي العنوان الكامل ورقم الموبايل حتى أثبتلج الحجز مباشرة 🌷'
        ),
    },
    'size_fit_reassurance': {
        'tactic': (
            'عند القلق من القياس أو القصة: طمأنيها من بيانات القياس المثبتة وذكّريها '
            'إن الفحص والقياس كدام المندوب قبل الدفع.'
        ),
        'example': (
            'لا تشيلين هم عيوني، هالقياس ضمن نطاق الموديل، وهم تفحصينه كدام المندوب قبل الدفع 🌷'
        ),
    },
}


def is_inspection_risk_question(text: str) -> bool:
    return bool(INSPECTION_RISK_RE.search(checkout.normalized(text or '')))


def is_total_with_delivery_question(text: str) -> bool:
    return bool(TOTAL_WITH_DELIVERY_RE.search(checkout.normalized(text or '')))


def compute_upsell_context(product, policy, state, amounts):
    """Return verified multi-piece / shared-delivery upsell metadata, or None."""
    if not product or (state or {}).get('order_id'):
        return None
    if product_albums.stock_state(product) != 'available':
        return None
    try:
        unit_pieces, retail_price, _ = pricing.terms(product)
        tiers = pricing.offer_tiers(product)
    except (TypeError, ValueError):
        return None

    fees = (policy or {}).get('delivery_fee')
    flat_fee = None
    if isinstance(fees, dict) and fees and len(set(fees.values())) == 1:
        flat_fee = next(iter(fees.values()))
    elif isinstance(fees, int):
        flat_fee = fees

    upsell = {}
    if tiers:
        qty, total = tiers[0]
        if qty > unit_pieces and retail_price and total < retail_price * (qty / max(1, unit_pieces)):
            upsell['has_bundle_offer'] = True
            upsell['bundle_qty'] = qty
            upsell['bundle_savings_note'] = f'أخذ {qty} قطع بعرض أوفر وبنفس أجور التوصيل'
    if flat_fee and flat_fee > 0:
        upsell['shared_delivery_benefit'] = 'أجور التوصيل ثابتة لكل الطلب حتى لو أخذت قطعتين أو أكثر سوة'
    return upsell or None


def detect_situations(
    customer_message,
    *,
    state=None,
    product=None,
    catalog_stock='unknown',
    question=None,
    amounts=None,
    recommendations=None,
    photo_clarification=None,
    market=None,
    recent_messages=None,
):
    """Deterministically detect active sales situations for the current turn."""
    clean = checkout.normalized(customer_message or '')
    situations = []

    if photo_clarification and (
        photo_clarification.get('multiple')
        or len(photo_clarification.get('matched_products') or []) >= 2
    ):
        situations.append('multi_photo_inquiry')

    if (
        catalog_stock == 'out'
        or question in {'alternatives.photos', 'inventory.alternative', 'alternative.color'}
        or (photo_clarification and photo_clarification.get('all_sold_out'))
    ):
        situations.append('oos_or_size_pivot')

    if INSPECTION_RISK_RE.search(clean):
        situations.append('objection_inspection_risk')

    if DELIVERY_OBJECTION_RE.search(clean):
        situations.append('objection_delivery_fee')

    if BARGAINING_RE.search(clean):
        situations.append('objection_bargaining')

    if POSTPONE_RE.search(clean):
        situations.append('objection_postpone')

    if TOTAL_WITH_DELIVERY_RE.search(clean) and (amounts or {}).get('total') is not None:
        situations.append('total_with_delivery_check')

    if SIZE_HESITATION_RE.search(clean):
        situations.append('size_fit_reassurance')

    if (
        (amounts or {}).get('offer_total') is not None
        or question == 'offer.quantity'
        or ('objection_delivery_fee' in situations and product and catalog_stock == 'available')
    ):
        situations.append('upsell_multi_piece')

    if question in {'contact.details', 'contact.phone', 'contact.address', 'contact.province'}:
        situations.append('fast_close_contact')

    # Deduplicate preserving priority order
    return list(dict.fromkeys(situations))


def build_playbook(
    customer_message,
    *,
    state=None,
    product=None,
    policy=None,
    catalog_stock='unknown',
    question=None,
    amounts=None,
    recommendations=None,
    photo_clarification=None,
    market=None,
    recent_messages=None,
):
    """Build the compact situation-aware Hybrid Sales Playbook for writer context."""
    situations = detect_situations(
        customer_message,
        state=state,
        product=product,
        catalog_stock=catalog_stock,
        question=question,
        amounts=amounts,
        recommendations=recommendations,
        photo_clarification=photo_clarification,
        market=market,
        recent_messages=recent_messages,
    )
    upsell = compute_upsell_context(product, policy, state, amounts)
    if not situations and not upsell:
        return None

    selected = situations[:3]
    tactics = [SITUATION_PLAYBOOK[s]['tactic'] for s in selected if s in SITUATION_PLAYBOOK]
    examples = [
        SITUATION_PLAYBOOK[s]['example']
        for s in selected[:2]
        if s in SITUATION_PLAYBOOK
    ]

    # Never inject an example containing a question mark when next_step.field is None,
    # or an example containing {{...}} tokens not present in amounts.
    safe_examples = []
    valid_tokens = set((amounts or {}).keys())
    for ex in examples:
        tokens = set(re.findall(r'\{\{([a-z_]+)\}\}', ex))
        if tokens - valid_tokens:
            continue
        if question is None and ('؟' in ex or '?' in ex):
            continue
        safe_examples.append(ex)

    playbook = {
        'situations': selected,
        'style_target': {
            'max_bubble_chars': 110,
            'target_bubbles': '1-2',
            'voice': 'iraqi_human_concise',
        },
        'tactics': tactics,
    }
    if safe_examples:
        playbook['winning_examples'] = safe_examples
    if upsell and not (state or {}).get('order_id'):
        playbook['upsell_angle'] = upsell
    return playbook


def deterministic_inspection_risk_line(policy):
    """Clear zero-risk fallback line when a customer asks if they pay delivery on return."""
    policy_text = checkout.normalized((policy or {}).get('inspection_policy') or '')
    if policy_text and re.search(r'فحص|يرجع|بدون\s*دفع|قبل\s*الدفع', policy_text):
        return (
            'إي عيوني أكيد، تفحصين وتقيسين كدام المندوب براحتج؛ '
            'إذا ما طابق أو ما عجبج يرجع وياه مباشرة وما تدفعين أي شي حتى التوصيل ما تدفعين 🌷'
        )
    return None
