"""Allowlisted ad referral metadata; never infer ads from customer attachments."""
import json
from urllib.parse import urlsplit


def safe_url(value):
    if not isinstance(value, str):
        return ''
    value = value.strip()
    try:
        url = urlsplit(value)
        return value if url.scheme in ('https', 'http') and url.hostname and not url.username and not url.password else ''
    except ValueError:
        return ''


def normalize(node):
    if not isinstance(node, dict):
        return {}
    aliases = {
        'ad_id': ('ad_id', 'adId'), 'ref': ('ref',),
        'source_id': ('source_id', 'sourceId', 'referral_source_id'),
        'source': ('source',), 'source_type': ('source_type', 'sourceType', 'referral_source_type'),
        'headline': ('headline', 'ad_title', 'title', 'referral_headline'),
        'body': ('body', 'description', 'referral_body'),
        'image_url': ('thumbnail_url', 'thumbnailUrl', 'image_url', 'imageUrl', 'photo_url'),
        'source_url': ('source_url', 'sourceUrl', 'referral_source_url', 'ad_url'),
        'campaign_id': ('campaign_id',), 'campaign_name': ('campaign_name', 'utm_campaign'),
        'post_id': ('post_id',), 'ctwa_clid': ('ctwa_clid', 'referral_ctwa_clid'),
    }
    result = {}
    for key, names in aliases.items():
        for name in names:
            value = node.get(name)
            if isinstance(value, (str, int)) and not isinstance(value, bool) and str(value).strip():
                value = str(value).strip()
                value = safe_url(value) if key.endswith('_url') else value[:1000 if key == 'body' else 250]
                if value:
                    result[key] = value
                    break
    if not result.get('image_url') and str(node.get('referral_media_content_type', '')).startswith('image/'):
        value = safe_url(node.get('referral_media_url'))
        if value:
            result['image_url'] = value
    if not result.get('ad_id') and result.get('source_type', '').lower() == 'ad' and result.get('source_id'):
        result['ad_id'] = result['source_id']
    return result


def extract(payload):
    if not isinstance(payload, dict):
        return {}
    # Only containers explicitly used for attribution are candidates.
    containers = [payload]
    for key in ('content_attributes', 'additional_attributes', 'custom_attributes'):
        if isinstance(payload.get(key), dict):
            containers.append(payload[key])
    conversation = payload.get('conversation') or {}
    if isinstance(conversation, dict):
        for key in ('additional_attributes', 'custom_attributes'):
            if isinstance(conversation.get(key), dict):
                containers.append(conversation[key])
    try:
        event = payload['entry'][0]['messaging'][0]
        containers.extend([event, event.get('message', {}), event.get('postback', {})])
    except (KeyError, IndexError, TypeError):
        pass
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in ('_ad_context', 'referral', 'ad_context', 'external_ad_reply'):
            node = container.get(key)
            if isinstance(node, dict):
                merged = dict(node)
                if isinstance(node.get('ads_context_data'), dict):
                    merged.update(node['ads_context_data'])
                result = normalize(merged)
                if result:
                    return result
        if any(container.get(key) for key in ('ad_id', 'ref', 'referral_source_id')):
            # Flat payload content/title/image may be the customer's own media.
            allowed = {key: value for key, value in container.items()
                       if key in ('ad_id', 'ref', 'campaign_id', 'campaign_name', 'ad_url') or key.startswith('referral_')}
            result = normalize(allowed)
            if result:
                return result
    return {}


def init_db(db):
    db.execute('''CREATE TABLE IF NOT EXISTS conversation_ad_context (
        sender_id TEXT PRIMARY KEY, message_id INTEGER, data TEXT NOT NULL)''')


def save(db, sender_id, message_id, payload, ad_id=None, ref=None):
    context = extract(payload)
    if not context:
        context = normalize({'ad_id': ad_id, 'ref': ref})
    if context:
        previous = db.execute('SELECT data FROM conversation_ad_context WHERE sender_id=?', (sender_id,)).fetchone()
        if previous:
            old = json.loads(previous['data'])
            identity = lambda item: next(((key, item[key]) for key in ('ad_id', 'source_id', 'post_id', 'ref') if item.get(key)), None)
            if identity(context) and identity(context) == identity(old):
                context = {**old, **context}
        db.execute('''INSERT INTO conversation_ad_context(sender_id,message_id,data) VALUES (?,?,?)
            ON CONFLICT(sender_id) DO UPDATE SET message_id=excluded.message_id,data=excluded.data
            WHERE excluded.message_id >= COALESCE(conversation_ad_context.message_id,0)''',
            (sender_id, message_id, json.dumps(context, ensure_ascii=False)))


def load(db, sender_id):
    cached = db.execute('SELECT data FROM conversation_ad_context WHERE sender_id=?', (sender_id,)).fetchone()
    if cached:
        return json.loads(cached['data']) or None
    # Backfill existing conversations once, independently of message pagination.
    rows = db.execute('''SELECT id,ad_id,ref,raw_payload FROM messages
        WHERE sender_id=? AND direction='incoming' AND
        (COALESCE(ad_id,'')!='' OR COALESCE(ref,'')!='' OR raw_payload LIKE '%referral%' OR raw_payload LIKE '%_ad_context%')
        ORDER BY id DESC''', (sender_id,))
    for row in rows:
        try:
            payload = json.loads(row['raw_payload'] or '{}')
        except (ValueError, TypeError):
            payload = {}
        context = extract(payload) or normalize({'ad_id': row['ad_id'], 'ref': row['ref']})
        if context:
            save(db, sender_id, row['id'], {'_ad_context': context})
            db.commit()
            return context
    db.execute('INSERT OR IGNORE INTO conversation_ad_context(sender_id,data) VALUES (?,?)', (sender_id, '{}'))
    db.commit()
    return None
