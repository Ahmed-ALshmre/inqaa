"""TikTok DM limits; receipts survive retries and remain scoped to a customer."""
from datetime import datetime, timedelta, timezone
import hashlib
import io
import os
from pathlib import Path
import tempfile

from PIL import Image, ImageOps


def init_db(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS zernio_tiktok_inbound (
            sender_id TEXT NOT NULL, message_id TEXT NOT NULL, sent_at TEXT NOT NULL,
            PRIMARY KEY(sender_id, message_id));
        CREATE INDEX IF NOT EXISTS zernio_tiktok_latest ON zernio_tiktok_inbound(sender_id, sent_at);
        CREATE TABLE IF NOT EXISTS zernio_tiktok_sends (
            delivery_key TEXT PRIMARY KEY, sender_id TEXT NOT NULL, incoming_id TEXT NOT NULL,
            status TEXT NOT NULL, provider_id TEXT);
        CREATE INDEX IF NOT EXISTS zernio_tiktok_budget ON zernio_tiktok_sends(sender_id, incoming_id, status);
    ''')


def record_incoming(db, sender, message_id, sent_at):
    # Duplicate and older webhooks must not reopen/reset the latest reply window.
    db.execute('INSERT OR IGNORE INTO zernio_tiktok_inbound VALUES(?,?,?)', (sender, message_id, sent_at))
    db.commit()


def prepare(messages):
    """TikTok accepts text OR one image. Keep captions as a separate message."""
    result = []
    for message in messages:
        if not isinstance(message, dict) or message.get('type') not in {'text', 'image'}:
            raise ValueError('TikTok DMs support text and images only')
        if message['type'] == 'text':
            text = message.get('text')
            if not isinstance(text, str) or not text.strip() or len(text) > 6000:
                raise ValueError('TikTok text must contain 1 to 6000 characters')
            result.append(message)
        else:
            image = dict(message)
            caption = image.pop('caption', '')
            result.append(image)
            if caption:
                if not isinstance(caption, str) or len(caption) > 6000:
                    raise ValueError('Invalid TikTok image caption')
                result.append(dict(type='text', text=caption))
    if not result or len(result) > 10:
        raise ValueError('TikTok accepts at most 10 replies per customer turn')
    return result


def image_reference(module, path, reference):
    """Convert an already validated local catalogue image, never fetch a remote file."""
    with Image.open(path) as image:
        if image.format in {'JPEG', 'PNG'} and path.stat().st_size <= 3 * 1024 * 1024:
            return reference
        image = ImageOps.exif_transpose(image)
        image.thumbnail((2048, 2048))
        background = Image.new('RGB', image.size, 'white')
        if 'A' in image.getbands():
            background.paste(image.convert('RGB'), mask=image.getchannel('A'))
        else:
            background.paste(image.convert('RGB'))
        buffer = io.BytesIO()
        background.save(buffer, format='JPEG', quality=85, optimize=True)
    data = buffer.getvalue()
    if len(data) > 3 * 1024 * 1024:
        raise ValueError('TikTok image exceeds 3 MB')
    filename = 'tiktok-' + hashlib.sha256(data).hexdigest() + '.jpg'
    root = Path(module.PRODUCT_IMAGE_DIR)
    root.mkdir(parents=True, exist_ok=True)
    target = root / filename
    if not target.exists():
        with tempfile.NamedTemporaryFile(dir=root, suffix='.tmp', delete=False) as temporary:
            temporary.write(data)
            temp_path = Path(temporary.name)
        try:
            os.replace(temp_path, target)
        finally:
            temp_path.unlink(missing_ok=True)
    return module.PUBLIC_URL.rstrip('/') + '/product_image/' + filename


def reserve(db, sender, key):
    """Reserve before HTTP so concurrent jobs cannot overspend the ten-message budget."""
    old = db.execute('SELECT status,provider_id FROM zernio_tiktok_sends WHERE delivery_key=? AND sender_id=?',
                     (key, sender)).fetchone()
    if old and old['status'] == 'sent':
        return dict(ok=True, cached=True, provider_id=old['provider_id'])
    if old and old['status'] != 'rejected':
        return dict(ok=False, status='delivery_uncertain')
    incoming = db.execute('''SELECT message_id,sent_at FROM zernio_tiktok_inbound
        WHERE sender_id=? ORDER BY sent_at DESC,rowid DESC LIMIT 1''', (sender,)).fetchone()
    if not incoming:
        return dict(ok=False, status='tiktok_no_customer_message')
    sent_at = datetime.fromisoformat(incoming['sent_at'])
    age = datetime.now(timezone.utc) - sent_at
    if age < timedelta(0) or age >= timedelta(hours=48):
        return dict(ok=False, status='tiktok_reply_window_closed')
    count_sql = '''SELECT COUNT(*) FROM zernio_tiktok_sends WHERE sender_id=? AND incoming_id=?
        AND status IN ('reserved','sent','uncertain')'''
    with db:
        if old:
            changed = db.execute('''UPDATE zernio_tiktok_sends SET status='reserved',incoming_id=?
                WHERE delivery_key=? AND status='rejected' AND (''' + count_sql + ')<10',
                (incoming['message_id'], key, sender, incoming['message_id'])).rowcount
        else:
            changed = db.execute('''INSERT OR IGNORE INTO zernio_tiktok_sends
                SELECT ?,?,?,'reserved',NULL WHERE (''' + count_sql + ')<10',
                (key, sender, incoming['message_id'], sender, incoming['message_id'])).rowcount
    if not changed:
        return dict(ok=False, status='tiktok_reply_limit')
    return dict(ok=True, cached=False)


def finish(db, key, *, provider_id=None, rejected=False):
    with db:
        db.execute('UPDATE zernio_tiktok_sends SET status=?,provider_id=? WHERE delivery_key=?',
                   ('sent' if provider_id else 'rejected' if rejected else 'uncertain', provider_id, key))
