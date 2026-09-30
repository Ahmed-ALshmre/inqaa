from pathlib import Path
p=Path('account_app/app.py');s=p.read_text(encoding='utf-8')
s=s.replace('    _add_column_if_missing("messages", "media_json", "TEXT")','    for column in ("product_total", "delivery_fee", "total_amount"):\n        _add_column_if_missing("orders", column, "INTEGER")\n    _add_column_if_missing("messages", "media_json", "TEXT")',1)
# Persist automatic checkout totals without modifying its existing validation.
s=s.replace('    db.execute(\n        """INSERT INTO orders\n', '    order_cursor = db.execute(\n        """INSERT INTO orders\n',1)
needle='    db.execute(\n        """UPDATE customers SET\n           phone'
s=s.replace(needle,'    db.execute("UPDATE orders SET product_total=?,delivery_fee=?,total_amount=? WHERE id=?",\n               (product_total, delivery_fee, total_amount, order_cursor.lastrowid))\n\n'+needle,1)
# Manual pricing is server-controlled and snapshotted at creation.
needle='    product_id_text = ", ".join(item["product_id"] for item in items if item.get("product_id"))'
pos=s.index(needle,s.index('def api_create_order('))
s=s[:pos]+'''    by_id = {str(p.get("product_id")): p for p in catalog}
    product_total = 0
    for item in items:
        raw_price = str(by_id.get(item["product_id"], {}).get("price") or "").translate(_ARABIC_DIGIT_TRANS)
        price = int(re.sub(r"\\D", "", raw_price) or "0")
        if price <= 0:
            return jsonify({"ok": False, "error": "سعر إحدى القطع غير محدد؛ حدّث سعر المنتج قبل تثبيت الطلب"}), 400
        item["unit_price"] = price
        product_total += price * item["quantity"]
    delivery_fee = int(delivery_fee_for_province(data["province"], db) or 0)
    total_amount = product_total + delivery_fee
'''+s[pos:]
pos=s.index('    order_info = {',s.index('def api_create_order('));end=s.index('@app.route("/api/conversations/<sender_id>/mark_reviewed"',pos)
chunk=s[pos:end].replace('        "created_at": now,','        "created_at": now,\n        "store_id": current_store_id(),\n        "product_total": product_total,\n        "delivery_fee": delivery_fee,\n        "total_amount": total_amount,',1)
chunk=chunk.replace('    db.execute(\n        "INSERT INTO orders','    order_cursor = db.execute(\n        "INSERT INTO orders',1)
chunk=chunk.replace('    db.execute(\n        "UPDATE customers', '    db.execute("UPDATE orders SET product_total=?,delivery_fee=?,total_amount=? WHERE id=?",\n               (product_total, delivery_fee, total_amount, order_cursor.lastrowid))\n    db.execute(\n        "UPDATE customers',1)
s=s[:pos]+chunk+s[end:]
p.write_text(s,encoding='utf-8')
