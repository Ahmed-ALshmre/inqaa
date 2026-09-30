from pathlib import Path
p=Path('account_app/app.py');s=p.read_text(encoding='utf-8')
s=s.replace('elif status == "problems": conditions.append("human_attention_count > 0")','elif status == "problems": conditions.append("human_service_count > 0")\n    elif status == "system_issues": conditions.append("system_issue_count > 0")',1)
s=s.replace('    rows = db.execute(f"SELECT * FROM ({base_query}) WHERE {where}', '''    # Technical failures are distinct from deliberate human handoffs. Repeated
    # checkout prompts are indicators for review, not automatic order changes.
    technical = "(hr.reason LIKE '%exception:%' OR hr.reason LIKE '%empty_reply%' OR hr.reason LIKE '%could not produce a reply%')"
    base_query = f"""SELECT base.*,
        (base.problem_count + (SELECT COUNT(*) FROM human_reviews hr WHERE hr.sender_id=base.sender_id AND hr.status='pending' AND NOT {technical})) human_service_count,
        ((SELECT COUNT(*) FROM human_reviews hr WHERE hr.sender_id=base.sender_id AND hr.status='pending' AND {technical}) +
         CASE WHEN (SELECT COUNT(*) FROM messages issue WHERE issue.sender_id=base.sender_id AND issue.direction='outgoing'
             AND (issue.text LIKE 'هذه القطع المقترحة للحجز%' OR issue.text LIKE 'باقي نحدد لون%' OR issue.text LIKE 'أثبتلج%وحده، لو وياه%')
             AND datetime(issue.created_at)>=datetime(base.last_time,'-1 day')
             AND NOT EXISTS (SELECT 1 FROM orders done WHERE done.sender_id=base.sender_id AND datetime(done.created_at)>=datetime(issue.created_at)))>=3 THEN 1 ELSE 0 END
        ) system_issue_count
        FROM ({base_query}) base"""
    rows = db.execute(f"SELECT * FROM ({base_query}) WHERE {where}''',1)
# Do not transmit a fabricated zero for legacy records without a price snapshot.
start=s.index('def api_resend_order_telegram(');end=s.index('\n\n',s.index('    if not order:',start))
# insert immediately before send, inspect using generic known call
pos=s.index('    telegram_sent = send_order_to_telegram(order)',start) if '    telegram_sent = send_order_to_telegram(order)' in s[start:start+1400] else -1
if pos>=0:s=s[:pos]+'''    if order.get("total_amount") is None:
        return jsonify({"ok": False, "error": "هذا طلب قديم بلا سعر محفوظ؛ يلزم مراجعة السعر قبل إعادة إرساله"}), 409
'''+s[pos:]
p.write_text(s,encoding='utf-8')
p=Path('account_app/static/js/dashboard.js');s=p.read_text(encoding='utf-8').replace("['all', 'booked', 'problems', 'unanswered']", "['all', 'booked', 'problems', 'system_issues', 'unanswered']")
s=s.replace("${leadMeta}${badge}${problemBadge}","${leadMeta}${badge}${problemBadge}${c.system_issue_count > 0 ? '<span class=\"badge bg-danger\" title=\"خطأ تقني أو تكرار خطوات الحجز؛ يحتاج مراجعة\">مشكلة نظام</span>' : ''}")
p.write_text(s,encoding='utf-8')
p=Path('account_app/templates/dashboard.html');s=p.read_text(encoding='utf-8');needle='        <button class="btn btn-outline-secondary active" data-filter="all"'
s=s.replace(needle,'        <button class="btn btn-outline-danger" data-filter="system_issues" onclick="setFilter(\'system_issues\',this)" title="أخطاء تقنية أو تكرار خطوات الحجز، منفصلة عن التحويل البشري">مشاكل النظام</button>\n'+needle,1);p.write_text(s,encoding='utf-8')
