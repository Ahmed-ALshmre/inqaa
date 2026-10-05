"""Reusable uploads and bounded cleanup; conversation history is never pruned."""
import hashlib
import json
import os
import re
import secrets
import sqlite3
import time
from pathlib import Path
from urllib.parse import unquote

DAY = 86400
FILE_RE = re.compile(r"product_\d+_\d+\.(?:png|jpe?g|webp)", re.I)


def init(db):
    db.execute("""CREATE TABLE IF NOT EXISTS upload_assets (
        filename TEXT PRIMARY KEY, store_id TEXT NOT NULL, original_name TEXT,
        sha256 TEXT, library INTEGER NOT NULL DEFAULT 1, source TEXT NOT NULL,
        created_at REAL NOT NULL, updated_at REAL NOT NULL)""")
    db.execute("CREATE TABLE IF NOT EXISTS retired_product_images (path TEXT PRIMARY KEY, retired_at REAL NOT NULL)")
    db.execute("CREATE INDEX IF NOT EXISTS upload_assets_hash ON upload_assets(store_id,sha256)")
    db.execute("CREATE TABLE IF NOT EXISTS storage_maintenance_state (key TEXT PRIMARY KEY,value TEXT)")
    db.commit()


def names(value):
    text=unquote(str(value or '')).replace('\\', '/')
    result=set(FILE_RE.findall(text))
    for match in re.finditer(r"product_image/+([^\"'<>?]+?\.(?:png|jpe?g|webp|gif))",text,re.I):
        result.add(match.group(1).rsplit('/',1)[-1])
    return result


def catalog_snapshot(path):
    with open(path,encoding='utf-8') as source:
        products=json.load(source)
    if not isinstance(products,list):
        raise ValueError('كتالوج المنتجات غير صالح؛ أوقف التنظيف لحماية الصور')
    return products


def catalog_names(products):
    return names(json.dumps(products,ensure_ascii=False))


def safe_file(root, filename):
    root=Path(root).resolve()
    path=root / filename
    if not FILE_RE.fullmatch(filename) or path.is_symlink() or path.resolve().parent != root:
        return None
    return path


def register_legacy(db, root, products, limit=1000):
    """Old unclassified uploads are reusable; absence of metadata is not deletion consent."""
    used=catalog_names(products)
    owners={name:str(p.get('store_id') or 'default') for p in products for name in names(json.dumps(p,ensure_ascii=False))}
    registered={row[0] for row in db.execute('SELECT filename FROM upload_assets')}
    if not Path(root).is_dir(): return
    paths=[Path(root)/name for name in used] + list(Path(root).iterdir())
    if not any(path.name not in registered and FILE_RE.fullmatch(path.name) and path.is_file() for path in paths): return
    for row in db.execute("SELECT m.image_url,COALESCE(c.store_id,'default') FROM messages m LEFT JOIN customers c ON c.sender_id=m.sender_id WHERE m.image_url LIKE '%product_image/uploads/product_%'"):
        for name in names(row[0]): owners.setdefault(name,str(row[1]))
    count=0
    if not Path(root).is_dir(): return
    for path in paths:
        if path.name in registered or not FILE_RE.fullmatch(path.name) or not path.is_file() or path.is_symlink(): continue
        stamp=path.stat().st_mtime
        db.execute('INSERT OR IGNORE INTO upload_assets VALUES(?,?,?,?,?,?,?,?)',
                   (path.name,owners.get(path.name,'default'),path.name,None,0 if path.name in used else 1,
                    'product' if path.name in used else 'legacy',stamp,stamp))
        registered.add(path.name)
        if path.name not in used:
            count+=1
            if count>=limit: break
    db.commit()


def retire_product_images(module, db, old_products, new_products):
    def paths(products):
        found=set()
        for product in products:
            for url in module.product_image_urls(product):
                if '/product_image/' not in str(url).replace('\\','/'): continue
                path=Path(module._local_image_path_from_reference(url)).resolve()
                if any(path.is_relative_to(Path(root).resolve()) for root in [module.UPLOADS_DIR,module.PRODUCT_IMAGE_DIR]):
                    found.add(str(path))
        return found
    now=time.time()
    for path in paths(old_products)-paths(new_products):
        db.execute('INSERT OR REPLACE INTO retired_product_images VALUES(?,?)',(path,now))
    db.commit()


def store_upload(db, root, store, original_name, content, extension, library=True):
    digest=hashlib.sha256(content).hexdigest()
    db.execute('BEGIN IMMEDIATE')
    try:
        rows=db.execute('SELECT filename FROM upload_assets WHERE store_id=? AND sha256=? ORDER BY updated_at DESC', (store,digest)).fetchall()
        for row in rows:
            path=safe_file(root,row[0])
            if path and path.is_file():
                db.execute('UPDATE upload_assets SET library=MAX(library,?),updated_at=? WHERE filename=?',(int(library),time.time(),row[0]))
                db.commit()
                return row[0],True
        filename=f'product_{time.time_ns()}_{secrets.randbits(64)}{extension}'
        Path(root).mkdir(parents=True,exist_ok=True)
        path=safe_file(root,filename)
        with path.open('xb') as output: output.write(content)
        now=time.time()
        db.execute('INSERT INTO upload_assets VALUES(?,?,?,?,?,?,?,?)',
                   (filename,store,Path(original_name).name,digest,int(library),'library' if library else 'product',now,now))
        db.commit()
        return filename,False
    except Exception:
        db.rollback();raise


def library_page(db, root, store, offset=0, limit=40):
    rows=db.execute('SELECT * FROM upload_assets WHERE library=1 AND store_id=? ORDER BY created_at DESC,filename LIMIT ? OFFSET ?',
                    (store,limit+1,offset)).fetchall()
    result=[]
    for row in rows[:limit]:
        path=safe_file(root,row['filename'])
        if path and path.is_file():
            result.append({'filename':row['filename'],'name':row['original_name'] or row['filename'],
                           'image_url':'/product_image/uploads/'+row['filename'],'bytes':path.stat().st_size})
    return {'images':result,'has_more':len(rows)>limit,'next_offset':offset+len(rows[:limit])}


def referenced_names(db, products):
    protected=catalog_names(products)
    tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    for table in tables:
        if table in {'upload_assets','retired_product_images','storage_maintenance_state','products'}: continue
        quoted='"'+table.replace('"','""')+'"'
        cols=['"'+r[1].replace('"','""')+'"' for r in db.execute(f'PRAGMA table_info({quoted})') if any(t in (r[2] or '').upper() for t in ('TEXT','CHAR','CLOB'))]
        if not cols: continue
        where=' OR '.join(f"({col} LIKE '%product_%' OR {col} LIKE '%product_image%')" for col in cols)
        for row in db.execute(f'SELECT {",".join(cols)} FROM {quoted} WHERE {where}'):
            for value in row: protected.update(names(value))
    return protected


def cleanup(db, root, products, preview=False, force=False, now=None, product_root=None):
    init(db)
    now=time.time() if now is None else now
    last=db.execute("SELECT value FROM storage_maintenance_state WHERE key='last_run'").fetchone()
    if not force and not preview and last and now-float(last[0])<DAY:
        return {'skipped':True}
    register_legacy(db, root, products)
    db.execute('BEGIN IMMEDIATE')
    try:
        last=db.execute("SELECT value FROM storage_maintenance_state WHERE key='last_run'").fetchone()
        if not force and not preview and last and now-float(last[0])<DAY:
            db.rollback();return {'skipped':True}
        report={'preview':preview,'deleted_rows':{},'images':0,'image_bytes':0,'batch_limit':5000}
        cutoff=now-30*DAY
        for table,predicate in [
            ('ai_jobs',"status IN ('done','dismissed') AND COALESCE(finished_at,created_at)<?"),
            ('ai_request_events','created_at<?'),
        ]:
            count=db.execute(f'DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE {predicate} ORDER BY rowid LIMIT 5000)',(cutoff,)).rowcount
            report['deleted_rows'][table]=count
        # Keep delivery keys and processed message IDs: they prevent duplicate sends.
        protected=referenced_names(db,products)
        candidates=[]
        for row in db.execute("SELECT filename FROM upload_assets WHERE library=0 AND source='product' AND updated_at<?", (now-7*DAY,)):
            if row[0] in protected: continue
            path=safe_file(root,row[0])
            if path and db.execute('SELECT 1 FROM retired_product_images WHERE path=? AND retired_at>=?',(str(path.resolve()),now-7*DAY)).fetchone(): continue
            if path and path.is_file() and path.stat().st_mtime<now-7*DAY:
                candidates.append((path,path.stat().st_size))
                if len(candidates)>=500: break
        chosen={path.resolve() for path,_ in candidates}
        roots=[Path(root).resolve()]
        if product_root: roots.append(Path(product_root).resolve())
        for row in db.execute('SELECT path FROM retired_product_images WHERE retired_at<? ORDER BY retired_at',(now-7*DAY,)):
            if len(candidates)>=500: break
            path=Path(row[0])
            resolved=path.resolve()
            if path.name in protected or resolved in chosen or path.is_symlink(): continue
            if not any(resolved.is_relative_to(allowed) for allowed in roots): continue
            asset=db.execute('SELECT library FROM upload_assets WHERE filename=?',(path.name,)).fetchone()
            if asset and asset[0]: continue
            if path.is_file() and path.stat().st_mtime<now-7*DAY:
                candidates.append((path,path.stat().st_size));chosen.add(resolved)
                if len(candidates)>=500: break
        report['images']=len(candidates)
        report['image_bytes']=sum(size for _,size in candidates)
        if preview:
            db.rollback();return report
        for path,_ in candidates:
            path.unlink()
            db.execute('DELETE FROM upload_assets WHERE filename=?',(path.name,))
            db.execute('DELETE FROM retired_product_images WHERE path=?',(str(path.resolve()),))
        db.execute("INSERT OR REPLACE INTO storage_maintenance_state VALUES('last_run',?)",(str(now),))
        db.execute("INSERT OR REPLACE INTO storage_maintenance_state VALUES('last_report',?)",(json.dumps(report),))
        db.commit()
        db.execute('PRAGMA optimize')
        return report
    except Exception:
        db.rollback();raise


def compact_database(db):
    """Manual compaction only while no AI job is executing; fail quickly when busy."""
    if db.execute("SELECT 1 FROM ai_jobs WHERE status='running' LIMIT 1").fetchone():
        return False
    old_timeout=db.execute('PRAGMA busy_timeout').fetchone()[0]
    try:
        db.execute('PRAGMA busy_timeout=200')
        db.execute('VACUUM')
        return True
    except sqlite3.OperationalError:
        return False
    finally:
        db.execute(f'PRAGMA busy_timeout={old_timeout}')
