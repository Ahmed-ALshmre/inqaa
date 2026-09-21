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
    context = " ".join(clean(product.get(k)) for k in ("price", "offer", "notes"))
    counts = {int(n) for n in re.findall(r"(?:بكج\s*)?(\d+)\s*قطع", context)}
    bundle = bool(re.search(r"بكج|باكج", context)) and bool(counts)
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
        amount = count // size * price
        total += amount
        all_free = all_free and free
        summaries.append(dict(product_id=pid, bundle_size=size, bundles=count // size, amount=amount, items=rows))
        base, remainder = divmod(amount, count)
        # Integer dinars must add up exactly, even when 25000 / 3 is recurring.
        for row in rows:
            quantity = int(row.get("quantity") or 1)
            high = min(quantity, remainder)
            for n, unit in ((high, base + 1), (quantity - high, base)):
                if n:
                    priced.append(dict(row, quantity=n, unit_price=unit))
            remainder -= high
    return dict(items=priced, groups=summaries, product_total=total,
                delivery_fee=0 if all_free and items else int(default_delivery or 0))
