"""A generated answer is context only after the transport accepts it.

Legacy records have no delivery metadata and keep their previous semantics.
This tracks provider acceptance, never customer receipt or read status.
"""
import json
import re

VISIBLE_SQL = """(direction<>'outgoing' OR COALESCE(json_extract(
    CASE WHEN json_valid(raw_payload) THEN raw_payload ELSE '{}' END,
    '$._delivery_status'),'accepted')='accepted')"""
ANSWERED_SQL = VISIBLE_SQL + """ AND COALESCE(json_extract(
    CASE WHEN json_valid(raw_payload) THEN raw_payload ELSE '{}' END,
    '$._partial_delivery_reply'),0)=0"""


def metadata(value):
    try:
        result = json.loads(value or '{}')
        return result if isinstance(result, dict) else {}
    except (TypeError, ValueError):
        return {}


def compact(value):
    return re.sub(r'\s+', ' ', str(value or '')).strip()


def prepare(db, sender, parts, images, message_ids=None):
    """Identify this answer's saved rows, scoped to sender and generated output.

    Pipeline results provide their exact saved IDs. Legacy/direct callers use
    the latest matching generated answer's turn, even if intake has already
    persisted a newer incoming bubble before transport starts.
    """
    wanted = {compact(part) for part in parts}

    def generated_match(row):
        data = metadata(row['raw_payload'])
        return not (data.get('sender_type') == 'staff' or data.get('manual_reply')) and (
            compact(row['text']) in wanted or row['image_url'] in images)

    if message_ids is not None:
        ids = list(dict.fromkeys(value for value in message_ids
                                if isinstance(value, int) and not isinstance(value, bool) and value > 0))
        if not ids:
            return []
        placeholders = ','.join('?' for _ in ids)
        rows = db.execute('''SELECT id,text,image_url,raw_payload FROM messages
            WHERE sender_id=? AND direction='outgoing' AND id IN (''' + placeholders + ') ORDER BY id',
            [sender, *ids]).fetchall()
    else:
        anchor = next((row for row in db.execute('''SELECT id,text,image_url,raw_payload FROM messages
            WHERE sender_id=? AND direction='outgoing' ORDER BY id DESC''', (sender,))
            if generated_match(row)), None)
        if anchor is None:
            return []
        boundary = db.execute("SELECT COALESCE(MAX(id),0) FROM messages WHERE sender_id=? AND direction='incoming' AND id<?",
                              (sender, anchor['id'])).fetchone()[0]
        rows = db.execute('''SELECT id,text,image_url,raw_payload FROM messages
            WHERE sender_id=? AND direction='outgoing' AND id>? AND id<=? ORDER BY id''',
            (sender, boundary, anchor['id'])).fetchall()
    matches = []
    for row in rows:
        if not generated_match(row):
            continue
        data = metadata(row['raw_payload'])
        previous = data.get('_delivery_status')
        if previous not in {'accepted', 'pending', 'unconfirmed'}:
            data['_delivery_status'] = 'pending'
            db.execute('UPDATE messages SET raw_payload=? WHERE id=?',
                       (json.dumps(data, ensure_ascii=False), row['id']))
        matches.append(dict(id=row['id'], text=row['text'], image_url=row['image_url'], previous=previous))
    db.commit()
    return matches


def update(db, rows, status, *, text=None, image=None):
    selected = [row for row in rows if (text is None and image is None)
                or (text is not None and compact(row['text']) == compact(text))
                or (image is not None and row['image_url'] == image)]
    for row in selected:
        record = db.execute('SELECT raw_payload FROM messages WHERE id=?', (row['id'],)).fetchone()
        data = metadata(record['raw_payload'])
        if status == 'skipped' and data.get('_delivery_status') != 'pending':
            continue
        data['_delivery_status'] = status
        db.execute('UPDATE messages SET raw_payload=? WHERE id=?',
                   (json.dumps(data, ensure_ascii=False), row['id']))
    if selected:
        db.execute('DELETE FROM sales_conversation_context WHERE sender_id=(SELECT sender_id FROM messages WHERE id=?)',
                   (selected[0]['id'],))
    db.commit()


def complete(db, rows, accepted_all):
    for row in rows:
        record = db.execute('SELECT raw_payload FROM messages WHERE id=?', (row['id'],)).fetchone()
        data = metadata(record['raw_payload'])
        data['_partial_delivery_reply'] = not accepted_all
        db.execute('UPDATE messages SET raw_payload=? WHERE id=?',
                   (json.dumps(data, ensure_ascii=False), row['id']))
    if rows and not accepted_all:
        sender = db.execute('SELECT sender_id FROM messages WHERE id=?', (rows[0]['id'],)).fetchone()[0]
        db.execute("UPDATE followups SET status='cancelled' WHERE sender_id=? AND status='pending'", (sender,))
    db.commit()
