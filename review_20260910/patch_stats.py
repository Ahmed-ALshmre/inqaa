from pathlib import Path
p=Path('account_app/app.py');s=p.read_text(encoding='utf-8');start=s.index('def _orders_payload(');end=s.index('\n\n@app.route("/orders"',start)
s=s[:start]+'''def _orders_payload(db, limit=500, date_from=None, date_to=None, offset=0, store_id=None, search=""):
    conditions, params = [], []
    for value, operator in ((date_from, ">="), (date_to, "<=")):
        if value:
            datetime.strptime(value, "%Y-%m-%d")
            conditions.append(f"substr(o.created_at,1,10) {operator} ?")
            params.append(value)
    if date_from and date_to and date_from > date_to:
        raise ValueError("Invalid date range")
    if store_id and store_id != "all":
        conditions.append("COALESCE(o.store_id,'default')=?")
        params.append(store_id)
    where = " AND ".join(conditions) or "1=1"
    totals = db.execute(f"SELECT COUNT(*) total, SUM(CASE WHEN COALESCE(o.status,'new')='new' THEN 1 ELSE 0 END) new_count, COUNT(DISTINCT CASE WHEN COALESCE(o.status,'new')!='cancelled' THEN NULLIF(o.sender_id,'') END) buyers FROM orders o WHERE {where}", params).fetchone()
    # The denominator uses the same dates and store, including manual buyers
    # without incoming messages. Multiple orders by one person count once.
    people_conditions, people_params = [], []
    for value, operator in ((date_from, ">="), (date_to, "<=")):
        if value:
            people_conditions.append(f"substr(activity_time,1,10) {operator} ?")
            people_params.append(value)
    if store_id and store_id != "all":
        people_conditions.append("COALESCE(store_id,'default')=?")
        people_params.append(store_id)
    people_where = " AND ".join(people_conditions) or "1=1"
    people = db.execute(f"""SELECT COUNT(DISTINCT NULLIF(sender_id,'')) FROM (
        SELECT sender_id,store_id,created_at activity_time FROM messages WHERE direction='incoming'
        UNION ALL SELECT sender_id,store_id,first_seen_at FROM customers
        UNION ALL SELECT sender_id,store_id,created_at FROM orders
    ) WHERE {people_where}""", people_params).fetchone()[0]
    if search:
        conditions.append("(" + " OR ".join(f"instr(lower(COALESCE({column},'')),lower(?))>0" for column in ("o.id", "o.sender_id", "o.customer_name", "c.name", "o.phone", "o.product_name", "o.province", "o.address", "o.status")) + ")")
        params.extend([search] * 9)
    where = " AND ".join(conditions) or "1=1"
    rows = db.execute(f"""SELECT o.*, c.name customer_display_name, c.page_id,
        COALESCE(c.platform,'facebook') platform FROM orders o
        LEFT JOIN customers c ON c.sender_id=o.sender_id WHERE {where}
        ORDER BY o.id DESC LIMIT ? OFFSET ?""", params + [limit + 1, offset]).fetchall()
    orders = []
    for row in rows[:limit]:
        order = dict(row)
        try:
            order["items"] = json.loads(order.get("order_items") or "[]")
        except (ValueError, TypeError):
            order["items"] = []
        orders.append(order)
    conversion = round(100 * totals["buyers"] / people, 2) if people else 0
    return {"orders": orders, "total": totals["total"], "new_count": totals["new_count"] or 0,
            "people_count": people, "ordering_people_count": totals["buyers"],
            "people_to_order_conversion": conversion, "conversion_rate": conversion,
            "has_more": len(rows) > limit, "next_offset": offset + len(orders)}
'''+s[end:]
start=s.index('    limit = request.args.get("limit", "500")',s.index('def api_orders():'));end=s.index('\n\ndef _order_payload_by_id',start)
s=s[:start]+'''    try:
        limit = max(1, min(int(request.args.get("limit", 100)), 2000))
        offset = max(0, int(request.args.get("offset", 0)))
        return jsonify(_orders_payload(get_db(), limit=limit, offset=offset,
            date_from=request.args.get("date_from"), date_to=request.args.get("date_to"),
            store_id=request.args.get("store_id"), search=request.args.get("q", "").strip()))
    except ValueError:
        return jsonify({"error": "تحقق من الفترة الزمنية وأرقام الصفحات"}), 400
'''+s[end:]
p.write_text(s,encoding='utf-8')
