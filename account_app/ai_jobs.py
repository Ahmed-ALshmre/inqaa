"""SQLite work queue. One job per conversation; different customers run in parallel."""
import hashlib
import json
import sqlite3
import threading
import time
import uuid
from contextvars import ContextVar

active_job = ContextVar('active_ai_job', default=None)


class Busy(Exception):
    pass


def init_db(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS ai_jobs (
            id TEXT PRIMARY KEY, store_id TEXT NOT NULL, sender_id TEXT NOT NULL,
            kind TEXT NOT NULL, dedupe_key TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued', result TEXT,
            created_at REAL NOT NULL, available_at REAL NOT NULL, started_at REAL,
            finished_at REAL, owner TEXT);
        CREATE INDEX IF NOT EXISTS ai_jobs_ready ON ai_jobs(status,available_at);
        CREATE TABLE IF NOT EXISTS ai_service_incidents (
            store_id TEXT NOT NULL, code TEXT NOT NULL, occurrences INTEGER NOT NULL DEFAULT 1,
            updated_at REAL NOT NULL, PRIMARY KEY(store_id,code));
        CREATE TABLE IF NOT EXISTS ai_deliveries (
            delivery_key TEXT PRIMARY KEY, job_id TEXT NOT NULL, status TEXT NOT NULL,
            result TEXT, updated_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS manual_image_links (
            store_id TEXT NOT NULL, sender_id TEXT NOT NULL, image_url TEXT NOT NULL,
            product_id TEXT NOT NULL, message_id INTEGER NOT NULL,
            PRIMARY KEY(store_id,sender_id,image_url));
        CREATE TABLE IF NOT EXISTS ai_request_events (
            id INTEGER PRIMARY KEY, store_id TEXT, job_id TEXT, model TEXT,
            queue_ms INTEGER, elapsed_ms INTEGER, attempts INTEGER,
            http_status INTEGER, error TEXT, created_at REAL NOT NULL);
    ''')


def enqueue(db, store, sender, kind, payload, key, delay=0):
    identity = hashlib.sha256(f'{store}:{sender}:{kind}:{key}'.encode()).hexdigest()
    now = time.time()
    old = db.execute('SELECT * FROM ai_jobs WHERE dedupe_key=?', (identity,)).fetchone()
    if old and old['status'] == 'done' and kind == 'preview':
        # A deliberate new request may produce a new draft; pending duplicates coalesce.
        db.execute("UPDATE ai_jobs SET dedupe_key=? WHERE id=? AND status='done'", (old['id'], old['id']))
    elif old and old['status'] == 'failed' and not db.execute(
            'SELECT 1 FROM ai_deliveries WHERE job_id=? LIMIT 1', (old['id'],)).fetchone():
        db.execute("UPDATE ai_jobs SET status='queued',result=NULL,available_at=?,owner=NULL WHERE id=? AND status='failed'",
                   (now + delay, old['id']))
    db.execute('''INSERT OR IGNORE INTO ai_jobs
        (id,store_id,sender_id,kind,dedupe_key,payload,created_at,available_at)
        VALUES(?,?,?,?,?,?,?,?)''',
        (uuid.uuid4().hex, store, sender, kind, identity,
         json.dumps(payload, ensure_ascii=False), now, now + delay))
    db.commit()
    return dict(db.execute('SELECT * FROM ai_jobs WHERE dedupe_key=?', (identity,)).fetchone())


def claim(db, owner):
    with db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('''SELECT * FROM ai_jobs j WHERE status='queued' AND available_at<=?
            AND NOT EXISTS (SELECT 1 FROM ai_jobs r WHERE r.store_id=j.store_id
                AND r.sender_id=j.sender_id AND r.status='running')
            ORDER BY j.created_at,j.rowid LIMIT 1''', (time.time(),)).fetchone()
        if not row:
            return None
        db.execute("UPDATE ai_jobs SET status='running',started_at=?,owner=? WHERE id=?",
                   (time.time(), owner, row['id']))
    return dict(row)


def finish(db, job_id, status, result):
    db.execute('UPDATE ai_jobs SET status=?,result=?,finished_at=? WHERE id=?',
               (status, json.dumps(result, ensure_ascii=False), time.time(), job_id))
    db.commit()


def deliver(db, identity, payload, send):
    key = hashlib.sha256((identity + json.dumps(payload, sort_keys=True, ensure_ascii=False)).encode()).hexdigest()
    with db:
        inserted = db.execute("INSERT OR IGNORE INTO ai_deliveries VALUES(?,?,'sending',NULL,?)",
                              (key, identity, time.time())).rowcount
    if not inserted:
        row = db.execute('SELECT status,result FROM ai_deliveries WHERE delivery_key=?', (key,)).fetchone()
        if row['status'] == 'sent':
            return json.loads(row['result'])
        if row['status'] == 'rejected':
            with db:
                inserted = db.execute("UPDATE ai_deliveries SET status='sending',updated_at=? WHERE delivery_key=? AND status='rejected'",
                                      (time.time(), key)).rowcount
        if not inserted:
            return {'ok': False, 'status': 'delivery_uncertain',
                    'message': 'تحقق من الرسالة السابقة قبل إعادة الإرسال؛ لم يتأكد تسليمها.'}
    result = send()
    status = result.get('status_code') or 0
    rejected = (isinstance(status, int) and 400 <= status < 500 and status != 408) or result.get('status') == 'missing_key'
    db.execute('UPDATE ai_deliveries SET status=?,result=?,updated_at=? WHERE delivery_key=?',
               ('sent' if result.get('ok') else 'rejected' if rejected else 'failed', json.dumps(result, ensure_ascii=False), time.time(), key))
    db.commit()
    return result


def recover_interrupted(db):
    # Delivery may have succeeded immediately before process death. Never replay
    # an uncertain send automatically. Pure previews are safe to recompute.
    db.execute("UPDATE ai_jobs SET status='queued',owner=NULL WHERE status='running' AND kind='preview'")
    db.execute("""UPDATE ai_jobs SET status='failed',finished_at=?,result=?
        WHERE status='running'""", (time.time(), json.dumps({
            'ok': False, 'error': 'توقفت المعالجة عند إعادة تشغيل السيرفر؛ تحقق من الإرسال قبل المحاولة مجدداً.',
            'delivery_uncertain': True}, ensure_ascii=False)))
    db.commit()


_start_lock = threading.Lock()
_started = set()


def start(path, handler, workers=8):
    with _start_lock:
        if path in _started:
            return
        db = sqlite3.connect(path, timeout=30)
        recover_interrupted(db)
        db.close()
        _started.add(path)
    def work():
        owner = uuid.uuid4().hex
        while True:
            db = sqlite3.connect(path, timeout=30)
            db.row_factory = sqlite3.Row
            job = None
            try:
                job = claim(db, owner)
                if job:
                    result = handler(job, json.loads(job['payload'])) or {}
                    finish(db, job['id'], 'failed' if result.get('error') or result.get('ok') is False else 'done', result)
            except Busy:
                db.rollback()
                db.execute("UPDATE ai_jobs SET status='queued',available_at=?,owner=NULL WHERE id=?",
                           (time.time() + 1, job['id']))
                db.commit()
            except Exception as exc:
                db.rollback()
                if job:
                    finish(db, job['id'], 'failed', {'ok': False, 'error': 'تعذر إكمال المهمة: ' + type(exc).__name__})
            finally:
                db.close()
            if not job:
                time.sleep(.5)
    for _ in range(workers):
        threading.Thread(target=work, daemon=True, name='ai-job-worker').start()
