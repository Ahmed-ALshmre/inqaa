import zipfile, sqlite3, pathlib, csv, json
root=pathlib.Path('outputs/token-audit-20260916')
with zipfile.ZipFile(r'C:\Users\Ghost الشبح\Downloads\lamsa-store-backup-20260916-105632.zip') as z:
 root.joinpath('sales.db').write_bytes(z.read('sales.db'))
db=sqlite3.connect('file:'+root.joinpath('sales.db').resolve().as_posix()+'?mode=ro',uri=True)
tables=[x[0] for x in db.execute("select name from sqlite_master where type='table'")]
print('TABLES',tables)
for t in tables:
 if 'setting' in t:
  print('SCHEMA',t,list(db.execute('pragma table_info("'+t+'")')))
for k,v in db.execute('select key,value from app_settings'):
 if any(s in k for s in ['main_model','checker_model','max_tokens','checker_enabled','history','reasoning']): print(k, v)
print('MEMORY_SCHEMA',list(db.execute('pragma table_info(conversation_memory)')))
with open(r'C:\Users\Ghost الشبح\Downloads\openrouter_activity_2026-09-16.csv',newline='',encoding='utf-8-sig') as f: rows=list(csv.DictReader(f))
for col in ['cost_total','tokens_prompt','tokens_completion','tokens_reasoning','tokens_cached']: print(col,sum(float(r[col] or 0) for r in rows))
print('finish', {k:sum(r['finish_reason_normalized']==k for r in rows) for k in set(r['finish_reason_normalized'] for r in rows)})
