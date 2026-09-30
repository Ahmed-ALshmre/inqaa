from pathlib import Path
p=Path('account_app/app.py');s=p.read_text(encoding='utf-8')
a=s.index('def reject_current_binding(');b=s.index('\n_PRODUCT_OBJECTION_KEYWORDS',a);t=s[a:b].replace('    db.commit()','    ai_efficiency.invalidate_product(db, current_store_id(), sender_id, binding.get("product_id"))\n    db.commit()',1);s=s[:a]+t+s[b:]
s=s.replace('            for pid in rejected:\n                db.execute(', '            for pid in rejected:\n                ai_efficiency.invalidate_product(db, current_store_id(), ev["sender_id"], pid)\n                db.execute(',1)
p.write_text(s,encoding='utf-8')
