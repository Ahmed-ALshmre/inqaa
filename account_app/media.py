"""Normalize incoming attachments without assuming that CDN URLs are images."""
import json
from pathlib import PurePosixPath
from urllib.parse import urlparse

AUDIO_EXTENSIONS = {'.mp3', '.m4a', '.aac', '.ogg', '.oga', '.opus', '.wav', '.amr'}
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.avif', '.bmp'}
VIDEO_EXTENSIONS = {'.mp4', '.mov', '.webm', '.mkv', '.3gp'}


def media_type(url, hint=''):
    if not isinstance(url, str):
        return ''
    parsed = urlparse(url.strip())
    if parsed.scheme not in ('http', 'https') and not (url.startswith('/') and not url.startswith('//')):
        return ''
    path = parsed.path.lower()
    ext = PurePosixPath(path).suffix
    hint = str(hint or '').lower()
    # Messenger voice notes are often MP4 served as video/mp4.
    if ext in AUDIO_EXTENSIONS or 'audioclip' in path or hint.startswith(('audio', 'voice')):
        return 'audio'
    if ext in VIDEO_EXTENSIONS or hint.startswith('video'):
        return 'video'
    if ext in IMAGE_EXTENSIONS or hint.startswith(('image', 'photo')):
        return 'image'
    if parsed.hostname == 'lookaside.fbsbx.com' and path.startswith('/ig_messaging_cdn/'):
        return 'file'
    return 'file' if hint in ('file', 'attachment', 'media') or hint.startswith('application/') else ''


def extract_media(data, text='', image_url=''):
    found = {}
    def add(url, hint=''):
        if not isinstance(url, str):
            return
        url = url.strip()
        kind = media_type(url, hint)
        if kind:
            previous = found.get(url)
            if not previous or kind in ('audio', 'video'):
                found[url] = {'type': kind, 'url': url}
    def visit(value, hint='', depth=0):
        if depth > 12:
            return
        if isinstance(value, list):
            for item in value:
                visit(item, hint, depth+1)
        elif isinstance(value, str):
            if value.lstrip().startswith(('[', '{')):
                try: visit(json.loads(value), hint, depth+1)
                except (ValueError, TypeError): pass
            else:
                add(value, hint)
        elif isinstance(value, dict):
            kind = value.get('mime_type') or value.get('mime') or value.get('content_type') or value.get('type') or value.get('attachment_type') or value.get('last_input_type') or hint
            url = value.get('url') or value.get('file_url') or value.get('media_url')
            if url: add(url, kind)
            for key, item in value.items():
                if key in ('url','file_url','media_url'): continue
                if key in ('text','last_input_text','last_text'):
                    add(item)
                elif key in ('entry','messaging','message','payload','last_input','data'):
                    visit(item, kind, depth+1)
                elif key in ('attachments','attachment','last_input_attachments','media','files','last_input_attachment','last_input_attachment_url','attachment_url'):
                    visit(item, kind if kind in ('image','audio','video') else 'attachment', depth+1)
                elif key in ('images','image_urls','image_url','last_input_image','last_input_image_url','photo_url','picture_url','last_image_url','ig_last_input_image_url','instagram_image_url'):
                    visit(item, 'image', depth+1)
                elif key in ('audio','audio_url','voice_url','last_input_audio','last_input_audio_url','voice','voice_note'):
                    visit(item, 'audio', depth+1)
                elif key in ('video','video_url','last_input_video_url'):
                    visit(item, 'video', depth+1)
    visit(data)
    add(text)
    add(image_url, 'image')
    return list(found.values())


def message_media(message):
    row = dict(message)
    try:
        saved = json.loads(row.get('media_json') or '[]')
    except (ValueError, TypeError):
        saved = []
    if saved:
        return extract_media({'attachments': saved})
    raw = {}
    if row.get('direction') == 'incoming':
        try: raw = json.loads(row.get('raw_payload') or '{}')
        except (ValueError, TypeError): pass
    return extract_media(raw, row.get('text') or '', row.get('image_url') or '')
