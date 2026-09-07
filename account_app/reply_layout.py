"""Group an approved reply into at most four complete, ordered messages."""
import re


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
        parts = [p.strip() for p in proposed]
    else:
        # Never resurrect unchecked model parts after a checker replaced reply.
        parts = [p.strip() for p in re.split(r"\n\s*\n", reply) if p.strip()]
        if len(parts) == 1 and "\n" not in reply:
            # Leave decimals, URLs, numbered lists and unpunctuated text intact.
            parts = [p.strip() for p in re.split(r"(?<=[.!؟?])\s+(?=\S)", reply) if p.strip()]

    grouped = []
    for part in parts:
        if grouped:
            previous = grouped[-1]
            same_topic = bool(topic(previous) and topic(previous) == topic(part))
            previous_fragment = (len(compact(previous)) < 30 and not re.search(r"[؟?]$", previous))
            continuation = bool(re.match(r"^(?:لأن|لان|وإذا|واذا|يعني)\s", part))
            # Keep related explanations, contact fields and greeting+answer together.
            if (is_greeting(previous) or previous_fragment or continuation
                    or (same_topic and (topic(part) == "contact" or len(previous) + len(part) <= 300))):
                grouped[-1] = previous + "\n\n" + part
                continue
        grouped.append(part)

    if len(grouped) > 1 and len(compact(grouped[-1])) < 20 and not re.search(r"[؟?]$", grouped[-1]):
        grouped[-2:] = ["\n\n".join(grouped[-2:])]
    while len(grouped) > 4:
        # Merge adjacent related/short sections; retain order and every fact.
        def cost(index):
            left, right = grouped[index:index + 2]
            related = bool(topic(left) and topic(left) == topic(right))
            final_question = index + 1 == len(grouped) - 1 and re.search(r"[؟?]$", right)
            return (bool(final_question), not related, len(left) + len(right))
        index = min(range(len(grouped) - 1), key=cost)
        grouped[index:index + 2] = ["\n\n".join(grouped[index:index + 2])]
    return grouped
