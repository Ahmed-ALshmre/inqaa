"""Keep approved replies coherent and limit customer notifications.

The approved ``reply`` is authoritative. Model-proposed parts are only layout
hints: they cannot replace, omit or resurrect facts after a reply correction.
"""
import re
import math

MAX_PARTS = 3
# Most sales replies fit comfortably in one bubble, including the next question.
# This is a grouping target, never a hard cut or permission to truncate a fact.
TARGET_LENGTH = 600
MAX_SALES_MESSAGES = 5
SALES_TARGET_LENGTH = 80
LIST_ITEM = re.compile(r"^\s*(?:[-*•]|\d+[.)\-])\s+")


def polish_surface_punctuation(reply):
    """Remove adjacent punctuation artifacts without changing approved facts."""
    if not isinstance(reply, str):
        return reply
    reply = re.sub(r'([.!؟?؛])\s*[،؛]', r'\1', reply)
    reply = re.sub(r'،\s*،', '،', reply)
    # Price starts a distinct sales function; make its preceding sentence
    # complete before a planner sends it in a separate notification.
    reply = re.sub(r'،\s+(?=(?:السعر|سعره|سعرها)\b)', '. ', reply)
    return reply


def semantic_units(text):
    """Use complete sentences/clauses, never a character or word-count cut."""
    blocks = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if LIST_ITEM.match(line) and blocks and (
                LIST_ITEM.match(blocks[-1]) or blocks[-1].endswith(":")
                or "\n" in blocks[-1]):
            blocks[-1] += "\n" + line
        else:
            blocks.append(line)
    units = []
    for block in blocks:
        if LIST_ITEM.match(block) or "\n" in block:
            units.append(block)
            continue
        # Protect URLs, decimals, currency abbreviations and list numbering.
        spans = [m.span() for m in re.finditer(r"https?://\S+|\b\d+[.)]\s|د\.\s*ع\.", block)]
        cuts = []
        for match in re.finditer(r"(?<=[.!؟?؛])\s+", block):
            if not any(start <= match.start() < end for start, end in spans):
                cuts.append(match.span())
        start = 0
        sentences = []
        for left, right in cuts:
            sentences.append(block[start:left].strip())
            start = right
        sentences.append(block[start:].strip())
        # A comma or a change of subject is not a message boundary. In
        # particular, keep price + delivery + total and answer + question intact.
        units.extend(sentence for sentence in sentences if sentence)
    return units


def compact(text):
    return re.sub(r"\s+", " ", text or "").strip()


def topic(text):
    groups = (
        ("contact", r"رقم|موبايل|هاتف|عنوان|محافظة"),
        ("delivery", r"توصيل|يوصل|شحن|مدة"),
        ("price", r"سعر|السعر|المجموع|دينار|د\.ع|ألف|الف"),
        ("fit", r"قياس|مقاس|وزن|كيلو|يلبس"),
        ("fabric", r"قماش|خامة|لينن|باربي"),
        ("inspection", r"فحص|استلام|مندوب|ترجع|يرجع"),
    )
    return next((name for name, pattern in groups if re.search(pattern, text)), "")


def is_greeting(text):
    return bool(re.fullmatch(
        r"(?:أهلا|اهلا|هلا|مرحبا|مراحب|وعليكم السلام|السلام عليكم|من عيوني|تدللين|تدلل|يا هلا)"
        r"(?:\s+(?:عيني|حبيبتي|عمري|كلبي))*[.!،🌸🌷✨\s]*",
        compact(text),
    ))


def receipt(text):
    return bool(re.match(r"^تم\s+(?:تثبيت|تأكيد|تاكيد|تسجيل|حجز).{0,25}(?:الطلب|الحجز)", text.strip()))


def cost_detail(text):
    return bool(re.search(r"سعر|السعر|المجموع|الإجمالي|الاجمالي", text)
                or re.search(r"\d[\d,.٬]*\s*(?:د\.\s*ع\.?|دينار|ألف|الف)", text)
                or (re.search(r"توصيل|شحن", text) and re.search(r"\d|مجاني", text)))


def approved_parts(result, reply):
    if result.get('_sales_bubble_plan'):
        return plan_sales_bubbles(result, reply)['reply_parts']
    if not isinstance(reply, str) or not reply.strip():
        return []
    reply = reply.strip()
    if result.get("_single_message") or receipt(reply):
        return [reply]
    proposed = result.get('reply_parts')
    valid = (isinstance(proposed, list) and proposed
             and all(isinstance(p, str) and p.strip() for p in proposed)
             and compact(" ".join(proposed)) == compact(reply))
    # Blank paragraphs and model hints must not turn a short, connected answer
    # into a stream of notifications, even when each sentence has a new topic.
    if len(compact(reply)) <= TARGET_LENGTH:
        return [reply]
    album = bool(result.get('_ordered_album') and valid)
    # An album item is indivisible, but several photo descriptions can share one
    # bubble. For ordinary replies derive boundaries from the approved text so
    # the same reply has the same delivery plan despite arbitrary model parts.
    parts = [p.strip() for p in proposed] if album else semantic_units(reply)
    units = []
    for part in parts:
        if units and not album:
            previous = units[-1]
            continuation = bool(re.match(r"^(?:لأن|لان|و?إذا|و?اذا|يعني|بس|لكن|و?بشرط|و?حتى|أو|او)\s", part))
            contact_fields = topic(previous) == topic(part) == "contact"
            price_total = cost_detail(previous) and cost_detail(part)
            question = bool(re.search(r"[؟?]$", part))
            # A soft line wrap cannot split a sentence or a conditional clause.
            fragment = (not re.search(r"[.!؟?؛:]$", previous)
                        or (len(compact(previous)) < 30 and not re.search(r"[؟?]$", previous)))
            if is_greeting(previous) or fragment or contact_fields or price_total or continuation or question:
                units[-1] = previous + "\n\n" + part
                continue
        units.append(part)
    if len(units) < 2:
        return [reply]
    grouped = []
    for unit in units:
        if grouped and len(compact(grouped[-1] + " " + unit)) <= TARGET_LENGTH:
            grouped[-1] += "\n\n" + unit
        else:
            grouped.append(unit)
    # Do not send a tiny final fragment as another notification.
    if len(grouped) > 1 and len(compact(grouped[-1])) < 100:
        grouped[-2:] = ["\n\n".join(grouped[-2:])]
    while len(grouped) > MAX_PARTS:
        # Merge whole adjacent sections; retain order and every approved fact.
        def cost(index):
            left, right = grouped[index:index + 2]
            related = bool(topic(left) and topic(left) == topic(right))
            return (not related, len(left) + len(right))
        index = min(range(len(grouped) - 1), key=cost)
        grouped[index:index + 2] = ["\n\n".join(grouped[index:index + 2])]
    return grouped


def _single_sales_kind(result, reply):
    """System-owned phase markers outrank sales grouping."""
    meta = result.get('meta') or {}
    phase = str(result.get('_response_kind') or result.get('intent') or '').lower()
    if receipt(reply) or phase in {'confirmation', 'order_confirmation'}:
        return 'confirmation'
    if (result.get('_service_reply') or result.get('_post_order') or meta.get('post_order_followup')
            or phase in {'service', 'post_order', 'post_order_followup', 'complaint', 'support'}):
        return 'service'
    clean = compact(reply).strip(' .!،🌸🌷✨💐❤️🤍')
    if re.fullmatch(r'(?:شكرا|شكراً|تسلمين|مشكورة|تدللين|تدلل|العفو|بالخدمة)(?:\s+(?:عيني|حبيبتي|عمري|دائماً|دائما|كلبي))*', clean):
        return 'thanks'
    if (phase in {'optout', 'opt_out', 'refusal', 'decline', 'thanks', 'thank_you'}
            or (len(clean) <= 180 and not cost_detail(clean) and not re.search(r'[؟?]', clean)
                and re.search(r'ما\s+راح\s+(?:نراسل|أ?ثبت|نثبت)|براحت[جك]|بدون ضغط', clean))):
        return 'optout'
    return ''


def _sales_units(reply):
    """Recognize independent sales functions, keeping every approved character."""
    markers = (r'(?:و?(?:السعر|سعره|سعرها|القماش|قماشه|قماشها|الخامة|القياسات|قياساته|'
               r'الألوان|الالوان|ألوانه|التوصيل|الفحص|المجموع|الإجمالي|الاجمالي)\b|'
               r'شنو\b|شكد\b|تحبين\b|تحب\b|تكدرين\b|تقدرين\b|ممكن\b|بس\s+(?:كوليلي|كليلي|قوليلي)\b|(?:و?إذا|و?اذا)\s+(?:تحبين|تكليلي|تكليني|تكدرين|تقدرين)\b|أي قياس\b|اي قياس\b)')
    pieces = []
    for unit in semantic_units(reply):
        if '\n' in unit or LIST_ITEM.match(unit):
            pieces.append(unit)
            continue
        # A comma can separate a full fact from the next sales function, but a
        # number/currency, URL, arbitrary word count or soft wrap cannot.
        cuts = list(re.finditer(r'(?<=[،,.!?؟؛])\s+(?=' + markers + r')', unit))
        start = 0
        for cut in cuts:
            pieces.append(unit[start:cut.start()].strip())
            start = cut.end()
        pieces.append(unit[start:].strip())
    atomic = []
    for piece in pieces:
        if not piece:
            continue
        if atomic:
            previous = atomic[-1]
            question = bool(re.search(r'[؟?]$', piece))
            costs = (cost_detail(previous) and (cost_detail(piece) or topic(piece) == 'delivery'))
            contact_fields = topic(previous) == topic(piece) == 'contact'
            fit_explanation = topic(previous) == topic(piece) == 'fit' and not question
            continuation = bool(re.match(r'^(?:لأن|لان|و?إذا|و?اذا|يعني|بس|لكن|و?بشرط|أو|او)\s', piece))
            # A full answer followed by a new "بس" concern is a useful second
            # bubble; joining it creates the very long photo/fit replies seen
            # in delivered conversations. Short conditional clauses stay whole.
            if continuation and re.match(r'^(?:بس|لكن)\s', piece) and len(compact(previous)) >= 70:
                continuation = False
            if question and (re.match(r'^بس\s+(?:حتى\s+(?:أ?ساعدج|نساعدج)|كوليلي|كليلي|قوليلي)', piece)
                             or re.match(r'^(?:و?إذا|و?اذا)\s+(?:تحبين|تكليلي|تكليني|تكدرين|تقدرين)', piece)):
                continuation = False  # A new decision request is its own useful bubble.
            fresh_function = bool(re.match(
                r'^(?:و?(?:السعر|سعره|سعرها|القماش|قماشه|قماشها|الخامة|القياسات|قياساته|'
                r'الألوان|الالوان|ألوانه|التوصيل|الفحص|المجموع|الإجمالي|الاجمالي)\b|'
                r'شنو\b|شكد\b|تحبين\b|تحب\b|تكدرين\b|تقدرين\b|ممكن\b|حتى نكمل\b)', piece))
            unfinished = not re.search(r'[.!؟?؛:،,]$', previous) and not fresh_function
            soft_intro = (len(compact(previous)) <= 24 and not topic(previous)
                          and not re.search(r'[؟?]', previous))
            if is_greeting(previous) or soft_intro or costs or contact_fields or fit_explanation or continuation or unfinished:
                atomic[-1] = previous + '\n\n' + piece
                continue
        atomic.append(piece)
    # If model prose interleaved another fact with quoted cost components,
    # preserve its order while keeping the complete quote in one bubble.
    cost_indices = [index for index, unit in enumerate(atomic) if cost_detail(unit)]
    if len(cost_indices) > 1 and re.search(r'المجموع|الإجمالي|الاجمالي', reply):
        first, last = cost_indices[0], cost_indices[-1]
        atomic[first:last + 1] = ['\n\n'.join(atomic[first:last + 1])]
    return atomic


def plan_sales_bubbles(result, reply, image_urls=None):
    """Plan 2–5 visible sales messages, counting photos in the same budget.

    The caller must persist and send this exact plan. ``needs_rewrite`` is a
    validation failure, never permission to pad the reply, silently remove
    facts, or claim deferred photos have been sent. This function has no I/O.
    Exceptions keep one text bubble; media that would exceed a one-message
    exception need a caption-capable sender or a corrected response upstream.
    """
    result = result or {}
    reply = reply.strip() if isinstance(reply, str) else ''
    if image_urls is None:
        image_urls = (result.get('product_image_urls') or result.get('image_urls')
                      or ([result['product_image_url']] if result.get('product_image_url') else []))
        if not result.get('send_image'):
            image_urls = []
    if isinstance(image_urls, str):
        image_urls = [image_urls]
    images = list(dict.fromkeys(value.strip() for value in image_urls or []
                                if isinstance(value, str) and value.strip()))
    kind = _single_sales_kind(result, reply)
    # Reserve a text slot whenever a validated reply exists. An image-only
    # response can use all five slots without inventing a caption.
    image_cap = MAX_SALES_MESSAGES - bool(reply)
    selected, deferred = images[:image_cap], images[image_cap:]
    limit = max(1, MAX_SALES_MESSAGES - len(selected))
    reasons = []
    proposed = result.get('reply_parts')
    valid = (isinstance(proposed, list) and proposed
             and all(isinstance(part, str) and part.strip() for part in proposed)
             and compact(' '.join(proposed)) == compact(reply))
    if kind or result.get('_single_message'):
        parts = [reply] if reply else []
    else:
        # Album descriptions are atomic. A model's arbitrary parts never alter
        # the approved text or override semantic protection of cost/conditions.
        structured = bool(result.get('_semantic_parts') and valid)
        units = ([part.strip() for part in proposed] if (result.get('_ordered_album') or structured) and valid
                 else _sales_units(reply))
        # The same size/color can legitimately describe two distinct photos.
        # Check repetition within each product section, not between products.
        duplicate_units = units if result.get('_ordered_album') else semantic_units(reply)
        within_album_duplicate = bool(result.get('_ordered_album') and any(
            len({compact(part) for part in semantic_units(unit)}) < len(semantic_units(unit))
            for unit in units))
        if (len({compact(unit) for unit in units}) < len(units)
                or len({compact(unit) for unit in duplicate_units}) < len(duplicate_units)
                or within_album_duplicate):
            reasons.append('duplicate_sales_content')
        parts = list(units)
        desired = min(limit, len(parts) if structured else max(2, math.ceil(len(compact(reply)) / SALES_TARGET_LENGTH)))
        while len(parts) > desired:
            def merge_cost(index):
                left, right = parts[index:index + 2]
                final_question = index + 1 == len(parts) - 1 and bool(re.search(r'[؟?]$', right))
                related = bool(topic(left) and topic(left) == topic(right))
                return (final_question, not related, len(left) + len(right))
            index = min(range(len(parts) - 1), key=merge_cost)
            parts[index:index + 2] = ['\n\n'.join(parts[index:index + 2])]
        if not parts and reply:
            parts = [reply]
        elif len(parts) == 1:
            parts = [reply]
    # Line breaks generated by the model are prose wrapping, not extra
    # pauses inside one Messenger bubble.
    parts = [part if re.search(r'(?m)^\s*(?:[-*•]|\d+[.)\-])\s+', part)
             else compact(part) for part in parts]
    total = len(parts) + len(selected)
    if deferred:
        reasons.append('deferred_images_require_pagination')
    if kind and total > 1:
        reasons.append('exception_requires_one_visible_message')
    elif not kind and total and total < 2:
        reasons.append('insufficient_distinct_sales_messages')
    if parts and compact(' '.join(parts)) != compact(reply):
        raise ValueError('Sales layout changed the approved reply')
    return {'reply': reply, 'reply_parts': parts, 'image_urls': selected,
            'deferred_image_urls': deferred, 'total_messages': total,
            'exception': kind or None, 'needs_rewrite': bool(reasons), 'rewrite_reasons': reasons}
