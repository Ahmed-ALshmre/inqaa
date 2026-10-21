"""Safely preserve provider-hosted incoming media before signed URLs expire."""
import ipaddress
from urllib.parse import urljoin, urlparse

import requests

MAX_BYTES = 25 * 1024 * 1024
MAX_REDIRECTS = 4
CHUNK_SIZE = 64 * 1024

_MIME_SUFFIX = {
    'image/jpeg': ('image', '.jpg'), 'image/png': ('image', '.png'),
    'image/gif': ('image', '.gif'), 'image/webp': ('image', '.webp'),
    'image/avif': ('image', '.avif'),
    'audio/mpeg': ('audio', '.mp3'), 'audio/mp4': ('audio', '.m4a'),
    'audio/ogg': ('audio', '.ogg'), 'audio/webm': ('audio', '.webm'),
    'audio/aac': ('audio', '.aac'), 'audio/wav': ('audio', '.wav'),
    'audio/x-wav': ('audio', '.wav'), 'audio/amr': ('audio', '.amr'),
    'application/ogg': ('audio', '.ogg'), 'audio/x-m4a': ('audio', '.m4a'),
    'video/mp4': ('video', '.mp4'), 'video/webm': ('video', '.webm'),
    'video/quicktime': ('video', '.mov'), 'video/3gpp': ('video', '.3gp'),
}


def _host(url):
    try:
        parsed = urlparse(str(url or ''))
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
            return ''
        if parsed.port not in (None, 443):
            return ''
        host = parsed.hostname.lower().rstrip('.')
        try:
            ipaddress.ip_address(host)
            return ''
        except ValueError:
            return host
    except ValueError:
        return ''


def _suffix(host, domain):
    return host == domain or host.endswith('.' + domain)


def trusted_source(url, chatwoot_base=''):
    host = _host(url)
    if not host:
        return False
    known = ('lookaside.fbsbx.com', 'fbcdn.net', 'fbsbx.com',
             'cdninstagram.com', 'app.chatwoot.com')
    configured = _host(chatwoot_base)
    return any(_suffix(host, domain) for domain in known) or bool(configured and host == configured)


def trusted_redirect(url, chatwoot_base=''):
    host = _host(url)
    return (trusted_source(url, chatwoot_base) or bool(host and any(
        _suffix(host, domain) for domain in ('amazonaws.com', 'cloudfront.net',
                                             'storage.googleapis.com'))))


def _sniff(data, hint):
    """Classify binary downloads from CDNs that send application/octet-stream."""
    if data.startswith(b'\xff\xd8\xff'): return 'image', '.jpg', 'image/jpeg'
    if data.startswith(b'\x89PNG\r\n\x1a\n'): return 'image', '.png', 'image/png'
    if data.startswith((b'GIF87a', b'GIF89a')): return 'image', '.gif', 'image/gif'
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP': return 'image', '.webp', 'image/webp'
    if data[4:8] == b'ftyp' and b'avif' in data[8:32]: return 'image', '.avif', 'image/avif'
    if data.startswith(b'OggS'): return 'audio', '.ogg', 'audio/ogg'
    if data.startswith((b'ID3', b'\xff\xfb', b'\xff\xf3', b'\xff\xf2')): return 'audio', '.mp3', 'audio/mpeg'
    if data[:4] == b'RIFF' and data[8:12] == b'WAVE': return 'audio', '.wav', 'audio/wav'
    if data.startswith(b'#!AMR\n'): return 'audio', '.amr', 'audio/amr'
    if len(data) > 2 and data[0] == 0xff and data[1] & 0xf6 == 0xf0:
        return 'audio', '.aac', 'audio/aac'
    if data[4:8] == b'ftyp':
        return ('audio', '.m4a', 'audio/mp4') if hint == 'audio' else ('video', '.mp4', 'video/mp4')
    if data.startswith(b'\x1a\x45\xdf\xa3'):
        return ('audio', '.webm', 'audio/webm') if hint == 'audio' else ('video', '.webm', 'video/webm')
    return None


def fetch(url, hint='', chatwoot_base=''):
    """Return (bytes, media kind, suffix, MIME) for a bounded trusted URL."""
    if not trusted_source(url, chatwoot_base):
        return None
    current = url
    for _ in range(MAX_REDIRECTS + 1):
        if not trusted_redirect(current, chatwoot_base):
            return None
        try:
            with requests.get(current, stream=True, allow_redirects=False,
                              timeout=(3, 12), headers={'Accept': 'image/*, audio/*, video/*'}) as response:
                if response.status_code in (301, 302, 303, 307, 308):
                    location = response.headers.get('Location')
                    if not location:
                        return None
                    current = urljoin(current, location)
                    continue
                if response.status_code != 200:
                    return None
                mime = response.headers.get('Content-Type', '').split(';', 1)[0].strip().lower()
                declared = response.headers.get('Content-Length', '')
                if declared.isdecimal() and int(declared) > MAX_BYTES:
                    return None
                chunks = []
                total = 0
                for chunk in response.iter_content(CHUNK_SIZE):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > MAX_BYTES:
                        return None
                    chunks.append(chunk)
                data = b''.join(chunks)
                sniffed = _sniff(data, hint)
                if not sniffed:
                    return None
                kind, suffix, actual_mime = sniffed
                classified = _MIME_SUFFIX.get(mime)
                if classified and classified[0] != kind:
                    # Audio-only MP4 voice notes are often labelled video/mp4.
                    if not (mime == 'video/mp4' and hint == 'audio' and kind == 'audio'):
                        return None
                if mime not in _MIME_SUFFIX and mime not in ('application/octet-stream', 'binary/octet-stream', ''):
                    return None
                if hint in ('image', 'audio', 'video') and hint != kind:
                    return None
                mime = actual_mime
                return data, kind, suffix, mime
        except requests.RequestException:
            return None
    return None
