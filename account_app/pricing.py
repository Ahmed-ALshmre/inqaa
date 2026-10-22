"""Catalog pricing shared by checkout, receipts and exported orders."""
import re
from decimal import Decimal

DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


def clean(value):
    return str(value or "").translate(DIGITS).replace("أ", "ا").replace("إ", "ا")


def money(value):
    text = clean(value).replace(",", "").replace("٬", "")
    matches = list(re.finditer(r"\d+(?:\.\d+)?", text))
    if len(matches) != 1:
        raise ValueError("سعر المنتج غير واضح؛ حدّث السعر قبل تثبيت الطلب")
    amount = Decimal(matches[0].group())
    if re.match(r"\s*(?:الف|الاف|آلاف)", text[matches[0].end():]):
        amount *= 1000
    if amount < 0 or amount != int(amount) or text.strip().startswith("-"):
        raise ValueError("سعر المنتج غير صحيح")
    return int(amount)


def terms(product):
    raw = clean(product.get("price"))
    # The price field defines the sale unit. Notes and offers may describe the
    # contents of a package or a multi-item discount without changing that unit.
    counts = {int(n) for n in re.findall(r"(\d+)\s*قطع", raw)}
    bundle = bool(re.search(r"بكج|باكج", raw)) and bool(counts)
    if bundle and len(counts) != 1:
        raise ValueError("عدد قطع البكج غير واضح؛ راجع بيانات المنتج")
    size = counts.pop() if bundle else 1
    # Remove the piece count, never concatenate it with the monetary amount.
    price = money(re.sub(r"\d+\s*قطع", "", raw))
    if price <= 0 or size <= 0:
        raise ValueError("سعر المنتج أو عدد قطع البكج غير صحيح")
    shipping = clean(product.get("delivery"))
    free = bool(re.search(r"توصيل\s+مجاني", shipping + " " + clean(product.get("offer"))))
    return size, price, free


_OFFER_QUANTITY = re.compile(
    r"(?<!\w)(?:(\d+)\s*قطع(?:ة)?|(?:ال)?قطعتين|(?:ال)?قطعتان)\s*"
    r"(?:بسعر\s*|بـ?\s*|سعر(?:ه|ها|هن|هم)?\s*(?:بـ?\s*)?)"
    r"(\d+(?:[,.٬]\d{3})*(?:\.\d+)?)\s*(الف|الاف|آلاف)?"
)


def offer_tiers(product):
    """Return explicit quantity/total promotions; never infer a discount."""
    size, regular, _ = terms(product)
    if size != 1:
        return []
    tiers = {}
    for key in ("offer", "bundle_contents", "notes"):
        source = clean(product.get(key)).replace("،", " ")
        for match in _OFFER_QUANTITY.finditer(source):
            count = int(match[1]) if match[1] else 2
            if not 2 <= count <= 20:
                continue
            total = money(match[2] + (" الف" if match[3] else ""))
            if total <= 0 or total >= count * regular:
                continue
            if count in tiers and tiers[count] != total:
                raise ValueError("أسعار العرض متعارضة؛ راجع بيانات المنتج قبل تثبيت الطلب")
            tiers[count] = total
    return sorted(tiers.items())


def _best_price(count, regular, tiers):
    """Find the cheapest valid combination of published offers and single units."""
    choices = [(1, regular)] + tiers
    best = [(0, [])]
    for quantity in range(1, count + 1):
        candidates = [(best[quantity - n][0] + amount,
                       best[quantity - n][1] + [(n, amount)])
                      for n, amount in choices if n <= quantity]
        best.append(min(candidates, key=lambda candidate: candidate[0]))
    amount, units = best[count]
    promotions = []
    for size, total in tiers:
        bundles = units.count((size, total))
        if bundles:
            promotions.append(dict(quantity=size, unit_total=total,
                                   bundles=bundles, amount=bundles * total))
    return amount, promotions, units.count((1, regular))


def expand_bundles(items, catalog):
    """Convert an explicitly labelled package to pieces without guessing variants."""
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise ValueError("تفاصيل القطع غير واضحة؛ حددي القطع المطلوبة")
    products = {str(p["product_id"]): p for p in catalog}
    expanded = []
    for item in items:
        product = products.get(str(item.get("product_id")), {})
        if not product:
            expanded.append(dict(item))
            continue
        size, _, _ = terms(product)
        count = int(item.get("quantity", 1))
        if not 1 <= count <= 20:
            raise ValueError("عدد القطع غير صحيح")
        marker = clean(item.get("notes"))
        same_product_rows = sum(str(row.get("product_id")) == str(item.get("product_id")) for row in items)
        package = item.get("quantity_unit") == "bundle" or (item.get("quantity_unit") != "piece" and same_product_rows == 1 and count == 1 and bool(re.search(r"بكج|باكج", marker)))
        color = str(item.get("color") or "").strip()
        colors = [v.strip() for v in re.split(r"[،,]" if re.search(r"[،,]", color) else r"\s+و(?=ال|ماروني|وردي|تركوازي|ازرق|أزرق|اسود|أسود)", color) if v.strip()]
        if size > 1 and (package or (count == 1 and len(colors) == size)):
            if re.search(r"[,،]|\d\s+و\s*\d", str(item.get('size') or '')):
                raise ValueError("سجلي كل قياس من البكج في سطر مستقل مع لونه")
            if len(colors) == size:
                for choice in colors:
                    expanded.append(dict(item, color=choice, quantity=count, quantity_unit="piece", notes=re.sub(r"بكج|باكج", "مجموعة", str(item.get("notes") or ""))))
            elif len(colors) == 1:
                expanded.append(dict(item, quantity=count * size, quantity_unit="piece", notes=re.sub(r"بكج|باكج", "مجموعة", str(item.get("notes") or ""))))
            else:
                raise ValueError("حددي ألوان وقياسات قطع البكج حتى أثبتها بصورة صحيحة")
        else:
            expanded.append(dict(item))
    return expanded


def quote(items, catalog, default_delivery):
    products = {str(p["product_id"]): p for p in catalog}
    groups = {}
    for item in items:
        groups.setdefault(str(item["product_id"]), []).append(item)
    priced, summaries, total, all_free = [], [], 0, True
    for pid, rows in groups.items():
        size, price, free = terms(products[pid])
        count = sum(int(i.get("quantity") or 1) for i in rows)
        if count % size:
            raise ValueError(f"{products[pid].get('product_name', '')} يباع بكج من {size} قطع؛ اختاري عدد قطع من مضاعفات {size} قبل التثبيت")
        if size == 1:
            amount, promotions, regular_count = _best_price(count, price, offer_tiers(products[pid]))
        else:
            amount, promotions, regular_count = count // size * price, [], 0
        total += amount
        all_free = all_free and free
        summaries.append(dict(product_id=pid, bundle_size=size, bundles=count // size,
                              amount=amount, items=rows, promotions=promotions,
                              regular_count=regular_count, regular_price=price))
        base, remainder = divmod(amount, count)
        # Integer dinars must add up exactly, even when 25000 / 3 is recurring.
        for row in rows:
            quantity = int(row.get("quantity") or 1)
            high = min(quantity, remainder)
            for n, unit in ((high, base + 1), (quantity - high, base)):
                if n:
                    basis = "offer_allocation" if promotions else "package_allocation" if size > 1 else "unit"
                    priced.append(dict(row, quantity=n, unit_price=unit, price_basis=basis))
            remainder -= high
    return dict(items=priced, groups=summaries, product_total=total,
                delivery_fee=0 if all_free and items else int(default_delivery or 0))
