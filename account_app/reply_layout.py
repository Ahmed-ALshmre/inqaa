"""Group approved text into 1–5 meaningful bubbles without rewriting facts."""
import re

MAX_PARTS = 5
TARGET_LENGTH = 220
LIST_ITEM = re.compile(r"^\s*(?:[-*•]|\d+[.)\-])\s+")
NEXT_QUESTION = r"(?:شنو|شكد|يا قياس|أي قياس|اي قياس|تحبين|تفضلين|تفضلينه|من يا محافظة)\b"


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
        for sentence in sentences:
            # Iraqi replies often separate independent ideas with commas only.
            boundaries = list(re.finditer(
                r"(?<=[،,])\s+(?=(?:و?(?:السعر|سعره|سعرها|القماش|قماشه|قماشها|التوصيل|الفحص|القياسات|الألوان)\b|"
                + NEXT_QUESTION + r"))|\s+(?=" + NEXT_QUESTION + r")", sentence))
            start = 0
            for boundary in boundaries:
                left = sentence[start:boundary.start()].strip()
                right = sentence[boundary.end():].strip()
                question = bool(re.match(NEXT_QUESTION, right) and re.search(r"[؟?]$", right))
                changed_topic = bool(topic(left) and topic(right) and topic(left) != topic(right))
                if len(compact(left)) >= 45 and (question or changed_topic or len(left) > TARGET_LENGTH):
                    units.append(left)
                    start = boundary.end()
            if sentence[start:].strip():
                units.append(sentence[start:].strip())
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
        r"(?:أهلا|اهلا|هلا|مرحبا|مراحب|وعليكم السلام|السلام عليكم|من عيوني|تدللين|تدلل|يا هلا)[^؟?\d]{0,32}[.!،🌸✨\s]*",
        compact(text),
    ))


def receipt(text):
    return bool(re.match(r"^تم\s+(?:تثبيت|تأكيد|تاكيد|تسجيل|حجز).{0,25}(?:الطلب|الحجز)", text.strip()))


def cost_detail(text):
    return bool(re.search(r"سعر|السعر|المجموع|الإجمالي|الاجمالي", text)
                or (re.search(r"توصيل|شحن", text) and re.search(r"\d|مجاني", text)))


def approved_parts(result, reply):
    if not reply or not reply.strip():
        return []
    if result.get("_single_message") or receipt(reply):
        return [reply]
    # Short answers and courtesies do not benefit from extra message bubbles.
    if len(compact(reply)) <= 100:
        return [reply]
    proposed = result.get("reply_parts")
    valid = (isinstance(proposed, list) and proposed
             and all(isinstance(p, str) and p.strip() for p in proposed)
             and compact(" ".join(proposed)) == compact(reply))
    if valid:
        # A model-proposed single wall of text still needs semantic layout.
        parts = [unit for p in proposed for unit in semantic_units(p)]
    else:
        # Never resurrect unchecked model parts after a checker replaced reply.
        parts = semantic_units(reply)

    grouped = []
    for part in parts:
        if grouped:
            previous = grouped[-1]
            same_topic = bool(topic(previous) and topic(previous) == topic(part))
            previous_fragment = (len(compact(previous)) < 30 and not re.search(r"[؟?]$", previous))
            continuation = bool(re.match(r"^(?:لأن|لان|وإذا|واذا|يعني)\s", part))
            separate_question = bool(re.search(r"[؟?]$", part) and not re.search(r"[؟?]$", previous))
            combined_length = len(compact(previous)) + len(compact(part))
            contact_fields = topic(previous) == topic(part) == "contact"
            price_total = cost_detail(previous) and cost_detail(part)
            # Keep related explanations, contact fields and greeting+answer together.
            if (is_greeting(previous) or contact_fields
                    or (not separate_question and (price_total or continuation
                        or ((previous_fragment or same_topic) and combined_length <= TARGET_LENGTH)))):
                grouped[-1] = previous + "\n\n" + part
                continue
        grouped.append(part)

    if len(grouped) > 1 and len(compact(grouped[-1])) < 20 and not re.search(r"[؟?]$", grouped[-1]):
        grouped[-2:] = ["\n\n".join(grouped[-2:])]
    while len(grouped) > MAX_PARTS:
        # Merge adjacent related/short sections; retain order and every fact.
        def cost(index):
            left, right = grouped[index:index + 2]
            related = bool(topic(left) and topic(left) == topic(right))
            final_question = index + 1 == len(grouped) - 1 and re.search(r"[؟?]$", right)
            return (bool(final_question), not related, len(left) + len(right))
        index = min(range(len(grouped) - 1), key=cost)
        grouped[index:index + 2] = ["\n\n".join(grouped[index:index + 2])]
    return grouped
