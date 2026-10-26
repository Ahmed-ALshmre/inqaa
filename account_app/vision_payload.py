"""Bound vision uploads without dropping catalogue references or changing originals."""
import base64
import io
import json

from PIL import Image, ImageOps

# The live upload path resets connections around 3 MB even when the model's
# theoretical context limit is higher. Bound the actual JSON body, not pixels.
MAX_REQUEST_BYTES = 1_500_000


def image_url(reference, *, sheet=False):
    """Compress inline images only; public URLs stay URLs for the provider."""
    if not str(reference).startswith('data:'):
        return reference
    header, encoded = reference.split(',', 1)
    if ';base64' not in header:
        raise ValueError('Unsupported inline image')
    raw = base64.b64decode(encoded, validate=True)
    limit = 700_000 if sheet else 350_000
    with Image.open(io.BytesIO(raw)) as source:
        edge = 2560 if sheet else 1600
        if len(raw) <= limit and max(source.size) <= edge and source.getexif().get(274, 1) == 1:
            return reference
        picture = ImageOps.exif_transpose(source).convert('RGB')
        picture.thumbnail((2560, 2560) if sheet else (1600, 1600), Image.Resampling.LANCZOS)
        for _ in range(8):
            output = io.BytesIO()
            picture.save(output, format='JPEG', quality=88 if sheet else 84, optimize=True)
            data = output.getvalue()
            if len(data) <= limit:
                return 'data:image/jpeg;base64,' + base64.b64encode(data).decode('ascii')
            picture.thumbnail((int(picture.width * .8), int(picture.height * .8)), Image.Resampling.LANCZOS)
    raise ValueError('Image cannot fit vision upload budget')


def request_batches(payload, prefix, groups, suffix=()):
    """Every labelled group appears once; measure the actual UTF-8 JSON body.

    Prefix (including customer photo) repeats in each request. A caller must
    combine all results and reject conflicting identities or partial failures.
    """
    def body(content):
        return dict(payload, messages=payload.get('messages', []) + [
            {'role': 'user', 'content': list(prefix) + content + list(suffix)}])

    def fits(content):
        return len(json.dumps(body(content), ensure_ascii=True).encode('utf-8')) <= MAX_REQUEST_BYTES

    result, current = [], []
    for group in groups:
        if not fits(group):
            raise ValueError('Vision reference group exceeds upload budget')
        if current and not fits(current + group):
            result.append(body(current))
            current = []
        current.extend(group)
    if current:
        result.append(body(current))
    return result


def consensus(results):
    """Never turn an incomplete comparison or conflicting matches into stock truth."""
    failed = next((r for r in results if r.get('service_error') or r.get('pending')), None)
    if failed:
        return failed
    matches = {r['product_id']: r for r in results if r.get('product_found') and r.get('product_id')}
    if len(matches) == 1:
        result = dict(next(iter(matches.values())))
        colors = {r['observed_color'] for r in results if r.get('product_found') and r.get('observed_color')}
        result.pop('observed_color', None)
        if len(colors) == 1:
            result['observed_color'] = next(iter(colors))
        return result
    return {'product_found': False, 'product_id': '', 'confidence': 0,
            'reason': 'Conflicting vision matches' if matches else 'No confident vision match'}
