"""Recognize copies of catalog photos after messaging apps recompress them."""
import base64
import binascii
import io
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageChops, ImageOps, ImageStat

from .inbound_media import MAX_BYTES


SIDE = 96
TILE = SIDE // 4
MAX_PIXELS = 24_000_000


def _thumbnail(source):
    with Image.open(source) as image:
        if image.width * image.height > MAX_PIXELS or getattr(image, 'is_animated', False):
            return None
        image = ImageOps.exif_transpose(image)
        if min(image.size) < SIDE:
            return None
        aspect = image.width / image.height
        thumbnail = image.convert('RGB').resize((SIDE, SIDE), Image.Resampling.LANCZOS)
        return aspect, thumbnail


@lru_cache(maxsize=512)
def _catalog_thumbnail(path, modified_ns, size):
    return _thumbnail(path)


def _same_photo(customer, catalog):
    ratio, photo = customer
    reference_ratio, reference = catalog
    if abs(ratio / reference_ratio - 1) > 0.01:
        return False
    difference = ImageChops.difference(photo, reference)
    if difference.getbbox() is None:
        return True
    # Require very small changes across the image and within each region.
    # A different garment, new crop or recolored photo goes to normal vision.
    if max(ImageStat.Stat(difference).rms) > 2.5 or max(ImageStat.Stat(reference).stddev) < 8:
        return False
    return all(
        max(ImageStat.Stat(difference.crop((x, y, x + TILE, y + TILE))).rms) <= 6
        for y in range(0, SIDE, TILE) for x in range(0, SIDE, TILE)
    )


def match_inline_photo(image, references):
    """Return one product and any consistent color from store-local references."""
    if not isinstance(image, str) or not image.startswith('data:image/'):
        return None
    header, _, encoded = image.partition(',')
    if not header.endswith(';base64') or len(encoded) > 4 * ((MAX_BYTES + 2) // 3):
        return None
    try:
        customer = _thumbnail(io.BytesIO(base64.b64decode(encoded, validate=True)))
    except (binascii.Error, OSError, ValueError, Image.DecompressionBombError):
        return None
    if not customer:
        return None

    matches = {}
    for product_id, color, filename in references:
        try:
            path = Path(filename)
            stat = path.stat()
            if not 0 < stat.st_size <= MAX_BYTES:
                continue
            catalog = _catalog_thumbnail(path, stat.st_mtime_ns, stat.st_size)
        except (OSError, ValueError, Image.DecompressionBombError):
            continue
        if catalog and _same_photo(customer, catalog):
            matches.setdefault(product_id, set()).add(color)
            if len(matches) > 1:
                return None
    if not matches:
        return None
    product_id, colors = next(iter(matches.items()))
    colors.discard('')
    return product_id, next(iter(colors)) if len(colors) == 1 else ''
