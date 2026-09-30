from pathlib import Path
p=Path('account_app/app.py');s=p.read_text(encoding='utf-8')
start=s.index('def select_customer_context_product(');end=s.index('\n\ndef select_product_mentioned_in_reply',start)
s=s[:start]+'''def _context_product_words(text):
    # Normalize spelling only for an already-known product's context. Keep the
    # catalog's general search and image matching rules independent.
    return {word.translate(str.maketrans("أإآىة", "ااايه")) for word in _catalog_words(text)}


def select_customer_context_product(text, customer_products):
    if not customer_products:
        return None
    ordered = _customer_products_display_order(customer_products)
    words = _catalog_words(text)
    for index, aliases in _ORDINAL_WORDS.items():
        ordinal_aliases = [alias for alias in aliases if alias.startswith("ال")]
        if any(alias in words for alias in ordinal_aliases) and len(ordered) >= index:
            return ordered[index - 1]

    explicit = _text_match_product(text or "", ordered)
    if explicit:
        return explicit

    request_info = _extract_product_request(text)
    terms = _context_product_words(" ".join(request_info.get("product_terms") or []))
    terms -= {"قطعه", "ملابس"}
    # "ست نفس الصورة؟" addresses the seller; "أريد ست" still requests a set.
    if re.match(r"^\\s*ست(?:\\s|[،,:])", str(text or "")) and not re.search(r"اريد|أريد|اشتري|احجز|أحجز", str(text or "")):
        terms.discard("ست")
    if terms:
        ordered = [p for p in ordered if terms & _context_product_words(
            " ".join(str(p.get(k) or "") for k in ("product_name", "category")))]
        if not ordered:
            return None
    if request_info.get("has_specific_filter"):
        matches = []
        for product in ordered:
            match = _product_matches_request(product, request_info)
            if match:
                matches.append(match)
        if len(matches) == 1:
            return matches[0]["product"]
    return ordered[0] if len(ordered) == 1 else None


def is_product_detail_followup(text):
    """Questions and polite closure do not reopen product selection."""
    text = str(text or "").strip()
    if is_conditional_return_question(text) or requests_alternative_photo(text):
        return True
    if re.fullmatch(r"لا\\s+(?:(?:حبي|عيني|حياتي|عمري)\\s+)?(?:تسلمين|شكرا|شكراً)[.!،\\s]*", text):
        return True
    # Preserve explicit changes, rejection, and requests for a different item.
    if re.search(r"ماريد|ما\\s*(?:اريد|أريد|عجب)|ماعجب|عوف|الغ|ألغ|بدل|خلي|مو\\s+(?:هذا|هاي|ذوق)|(?:اريد|أريد|اختار|أختار)", text):
        return False
    return bool(re.search(r"قماش|خام|قياس|مقاس|طول|سعر|توصيل|صور|تصوير|فحص|لون", text))
'''+s[end:]
s=s.replace('    if requests_alternative_photo(text) or is_conditional_return_question(text):\n        return None','    if is_product_detail_followup(text):\n        return None',1)
needle='    known = {r["product_id"]: dict(r) for r in rows}'
s=s.replace(needle,'''    reliable_ids = {p["product_id"] for p in load_customer_products(db, ev["sender_id"], limit=50)}
    mentioned_ids = {p["product_id"] for p in products if _text_match_product(text, [p])}
    # Suggested/rejected items only re-enter when explicitly mentioned or part
    # of the pending add/replace question, never as automatic alternatives.
    known = {r["product_id"]: dict(r) for r in rows
             if r["product_id"] in reliable_ids or r["product_id"] in mentioned_ids}''',1)
s=s.replace('(?:^|\\s)(?:اذا|إذا|لو|في حال)(?:\\s|$)', '(?:^|\\s)(?:و)?(?:اذا|إذا|لو|في حال)(?:\\s|$)',1)
# Avoid recording fresh alternative suggestions for a known-item question.
s=s.replace('customer_products and _is_contextual_product_question(ev.get("text", ""))','customer_products and (_is_contextual_product_question(ev.get("text", "")) or\n                (is_product_detail_followup(ev.get("text", "")) and select_customer_context_product(ev.get("text", ""), customer_products)))',1)
p.write_text(s,encoding='utf-8')
