"""Lossless prompt compaction and cached image recognition with bounded recovery."""
import base64
import hashlib
import io
import json
import time
import uuid
from collections import Counter


def merge_history(history, memory):
    """Retain legacy memory entries absent from the authoritative message history."""
    counts = Counter((m.get('direction'), str(m.get('text') or '').strip()) for m in history)
    missing = []
    for message in memory or []:
        direction = 'incoming' if message.get('role') == 'user' else 'outgoing'
        text = str(message.get('content') or '').strip()
        key = (direction, text)
        if counts[key]:
            counts[key] -= 1
        elif text:
            missing.append({'direction': direction, 'text': text, 'created_at': '',
                            'message_type': 'text'})
    return missing + list(history)


def compact(value):
    if isinstance(value, dict):
        return {k: compact(v) for k, v in value.items() if v is not None and v != '' and v != [] and v != {}}
    if isinstance(value, list):
        return [compact(v) for v in value]
    return value


def dumps(value):
    return json.dumps(compact(value), ensure_ascii=False, separators=(',', ':'))


def init_db(db):
    db.execute('''CREATE TABLE IF NOT EXISTS conversation_pause_state (
        sender_id TEXT PRIMARY KEY, reason TEXT NOT NULL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
    db.execute('''CREATE TABLE IF NOT EXISTS image_recognition_attempts (
        store_id TEXT NOT NULL, sender_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
        status TEXT NOT NULL, result TEXT, catalog_version TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(store_id, sender_id, fingerprint))''')
    db.execute('''CREATE TABLE IF NOT EXISTS ai_usage_events (
        id INTEGER PRIMARY KEY, store_id TEXT, purpose TEXT, model TEXT,
        prompt_tokens INTEGER, completion_tokens INTEGER, reasoning_tokens INTEGER,
        cached_tokens INTEGER, finish_reason TEXT, elapsed_ms INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
    columns = {row[1] for row in db.execute('PRAGMA table_info(image_recognition_attempts)')}
    for name, definition in [('updated_epoch', 'REAL NOT NULL DEFAULT 0'),
                             ('attempt_count', 'INTEGER NOT NULL DEFAULT 1'), ('owner', 'TEXT')]:
        if name not in columns:
            db.execute(f'ALTER TABLE image_recognition_attempts ADD COLUMN {name} {definition}')


def fingerprint(data_url):
    from PIL import Image, ImageOps
    raw = base64.b64decode(data_url.split(',', 1)[1], validate=True)
    with Image.open(io.BytesIO(raw)) as source:
        image = ImageOps.exif_transpose(source).convert('RGBA')
        return hashlib.sha256(str(image.size).encode() + image.tobytes()).hexdigest()


def recognize_once(db, store, sender, key, catalog, recognize):
    """Cache verified outcomes, coalesce duplicates, recover stale transient failures."""
    version = hashlib.sha256(dumps(catalog).encode()).hexdigest()
    now, owner = time.time(), uuid.uuid4().hex
    inserted = db.execute('''INSERT OR IGNORE INTO image_recognition_attempts
        (store_id,sender_id,fingerprint,status,catalog_version,updated_epoch,owner)
        VALUES(?,?,?,'started',?,?,?)''', (store, sender, key, version, now, owner)).rowcount
    db.commit()
    if not inserted:
        row = db.execute('''SELECT result,status,catalog_version,updated_epoch,attempt_count,owner
            FROM image_recognition_attempts
            WHERE store_id=? AND sender_id=? AND fingerprint=?''', (store, sender, key)).fetchone()
        result = json.loads(row[0]) if row and row[0] else None
        retryable = bool(result and result.get('service_error'))
        cooldown = 300 if result and result.get('error_code') in {'authentication', 'billing', 'missing_key'} else 60
        stale = row and row[1] == 'started' and row[3] > 0 and now - row[3] > 180
        changed = row and row[1] == 'finished' and row[2] != version
        retry = row and row[1] != 'invalidated' and (changed or (stale and row[4] < 2)
                    or (retryable and row[1] == 'finished' and now - row[3] >= cooldown))
        if retry:
            inserted = db.execute('''UPDATE image_recognition_attempts SET status='started',result=NULL,
                catalog_version=?,updated_epoch=?,owner=?,attempt_count=?
                WHERE store_id=? AND sender_id=? AND fingerprint=? AND updated_epoch=?
                AND status=? AND owner IS ?''',
                (version, now, owner, 1 if changed else row[4] + 1,
                 store, sender, key, row[3], row[1], row[5])).rowcount
            db.commit()
        if not inserted:
            return result or {
            'product_found': False, 'service_error': True,
            'pending': True, 'reason': 'Image recognition already in progress',
            'error_code': 'attempt_already_started'}
    try:
        result = recognize()
    except Exception:
        result = {'product_found': False, 'service_error': True,
                  'reason': 'Image recognition failed; manual review required', 'error_code': 'service_error'}
    db.execute('''UPDATE image_recognition_attempts SET status='finished', result=?,updated_epoch=?
        WHERE store_id=? AND sender_id=? AND fingerprint=? AND owner=? AND status='started' ''',
        (dumps(result), time.time(), store, sender, key, owner))
    db.commit()
    return result


def vision_choice(payload, products):
    """An incomplete/invalid answer is a service failure, never evidence of no match."""
    try:
        choice = payload['choices'][0]
        raw = choice['message'].get('content')
        if isinstance(raw, list):
            raw = ''.join(p.get('text', '') for p in raw if isinstance(p, dict) and p.get('type') == 'text')
        if choice.get('finish_reason') in {'length', 'error', 'content_filter'} or not isinstance(raw, str) or not raw.strip():
            raise ValueError('incomplete')
        text = raw.strip().removeprefix('```json').removeprefix('```').removesuffix('```').strip()
        if text.startswith('{'):
            value = json.loads(text)['product_id']
        else:
            value = text.strip('"\'')
        if value is None or str(value).upper() in {'NONE', 'NO_MATCH', 'NULL'}:
            return {'product_found': False, 'product_id': '', 'confidence': 0,
                    'reason': 'No clear visual match', 'raw': raw}
        ids = {str(p['product_id']) for p in products}
        if value not in ids:
            raise ValueError('unknown product')
        return {'product_found': True, 'product_id': value, 'confidence': 100,
                'reason': 'Catalog visual match', 'raw': raw}
    except (KeyError, IndexError, TypeError, ValueError):
        return {'product_found': False, 'product_id': '', 'service_error': True,
                'error_code': 'invalid_vision_response', 'reason': 'Incomplete or invalid image analysis response'}


def invalidate_product(db, store, sender, product_id):
    """A rejected association cannot be resurrected by replaying its image."""
    rows = db.execute('''SELECT fingerprint,result FROM image_recognition_attempts
        WHERE store_id=? AND sender_id=? AND status='finished' ''', (store, sender)).fetchall()
    for key, raw in rows:
        result = json.loads(raw or '{}')
        if result.get('product_id') == product_id:
            result = {'product_found': False, 'reason': 'Previous image association rejected; clarify or review manually',
                      'error_code': 'association_rejected'}
            db.execute('''UPDATE image_recognition_attempts SET status='invalidated',result=?
                WHERE store_id=? AND sender_id=? AND fingerprint=?''', (dumps(result), store, sender, key))


def record_usage(db, store, purpose, model, payload, elapsed_ms):
    usage = payload.get('usage') or {}
    completion = usage.get('completion_tokens_details') or {}
    prompt = usage.get('prompt_tokens_details') or {}
    choices = payload.get('choices') or [{}]
    db.execute('''INSERT INTO ai_usage_events
        (store_id,purpose,model,prompt_tokens,completion_tokens,reasoning_tokens,
         cached_tokens,finish_reason,elapsed_ms) VALUES(?,?,?,?,?,?,?,?,?)''',
        (store, purpose, model, usage.get('prompt_tokens'), usage.get('completion_tokens'),
         completion.get('reasoning_tokens'), prompt.get('cached_tokens'),
         choices[0].get('finish_reason'), elapsed_ms))
    db.commit()
