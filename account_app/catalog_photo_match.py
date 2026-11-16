"""Recognize copies of catalog photos after messaging apps recompress them."""
import base64
import binascii
import io
from functools import lru_cache
from heapq import nsmallest
from pathlib import Path

from PIL import Image, ImageChops, ImageOps, ImageStat

from .inbound_media import MAX_BYTES


SIDE = 96
TILE = SIDE // 4
MAX_PIXELS = 24_000_000


def _picture(source):
    with Image.open(source) as image:
        if image.width * image.height > MAX_PIXELS or getattr(image, 'is_animated', False):
            return None
        image = ImageOps.exif_transpose(image)
        if min(image.size) < SIDE:
            return None
        return image.convert('RGB')


@lru_cache(maxsize=512)
def _catalog_thumbnail(path, modified_ns, size):
    photo = _picture(path)
    return (photo.width / photo.height, photo.resize((SIDE, SIDE), Image.Resampling.LANCZOS)) if photo else None


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


def _screenshot_match(picture, screen, catalog):
    """Locate a near full-width photo inside a portrait messaging screenshot."""
    ratio, reference = catalog
    if max(ImageStat.Stat(reference).stddev) < 16:
        return False
    width, height = screen.size
    pixels = screen.load()
    points = [(x, y, reference.getpixel((round(x * SIDE), round(y * SIDE))))
              for y in (.20, .38, .56, .74) for x in (.23, .5, .77)]

    def score(left, top, w, h):
        total = 0
        for x, y, expected in points:
            seen = pixels[left + round(x * w), top + round(y * h)]
            total += sum(abs(a - b) for a, b in zip(seen, expected))
        return total

    def windows():
        for w in range(width, int(width * .84), -3):
            h = round(w / ratio)
            if not width * .40 <= h <= height * .90:
                continue
            left = (width - w) // 2
            for top in range(0, height - h + 1, 4):
                yield score(left, top, w, h), left, top, w

    def refined(coarse):
        for _, left, top, w in coarse:
            for new_width in range(w - 2, min(width, w + 2) + 1):
                h = round(new_width / ratio)
                for x in range(max(0, left - 2), min(width - new_width, left + 2) + 1):
                    for y in range(max(0, top - 3), min(height - h, top + 3) + 1):
                        yield score(x, y, new_width, h), x, y, new_width, h

    scale = picture.width / width
    margin = 7
    interior = (margin, margin, SIDE - margin, SIDE - margin)
    expected = reference.crop(interior)
    original = picture.load()

    def boundary(position, start, end, *, horizontal):
        radius = round(4 * scale)
        limit = picture.height if horizontal else picture.width
        if position in (0, limit):
            return position
        samples = [round(start + (end - start) * i / 12) for i in range(2, 11)]

        def edge(pos):
            total = 0
            for point in samples:
                before = original[point, pos - 2] if horizontal else original[pos - 2, point]
                after = original[point, pos + 1] if horizontal else original[pos + 1, point]
                total += sum(abs(a - b) for a, b in zip(before, after))
            return total, -abs(pos - position)

        return max(range(max(2, position - radius), min(limit - 2, position + radius) + 1),
                   key=edge, default=position)

    def closest(bounds):
        x, y, r, b = bounds
        return (boundary(x, y, b, horizontal=False), boundary(y, x, r, horizontal=True),
                boundary(r, y, b, horizontal=False), boundary(b, x, r, horizontal=True))

    coarse = nsmallest(6, windows())
    if not coarse or coarse[0][0] > len(points) * 3 * 24:
        return False
    for cost, x, y, w, h in nsmallest(12, set(refined(coarse))):
        if cost > len(points) * 3 * 18:
            break
        bounds = (round(x * scale), round(y * scale),
                  min(picture.width, round((x + w) * scale)), min(picture.height, round((y + h) * scale)))
        for box in (closest(bounds), bounds):
            left, top, right, bottom = box
            if right <= left or bottom <= top or abs((right - left) / (bottom - top) / ratio - 1) > .04:
                continue
            actual = picture.resize((SIDE, SIDE), Image.Resampling.LANCZOS, box=box).crop(interior)
            diff = ImageChops.difference(actual, expected)
            if max(ImageStat.Stat(diff).rms) <= 5.5 and all(
                max(ImageStat.Stat(diff.crop((a, b, min(a + TILE, diff.width), min(b + TILE, diff.height)))).rms) <= 11
                for b in range(0, diff.height, TILE) for a in range(0, diff.width, TILE)
            ):
                return True
    return False


def match_inline_photo(image, references):
    """Return one product and any consistent color from store-local references."""
    if not isinstance(image, str) or not image.startswith('data:image/'):
        return None
    header, _, encoded = image.partition(',')
    if not header.endswith(';base64') or len(encoded) > 4 * ((MAX_BYTES + 2) // 3):
        return None
    try:
        picture = _picture(io.BytesIO(base64.b64decode(encoded, validate=True)))
    except (binascii.Error, OSError, ValueError, Image.DecompressionBombError):
        return None
    if not picture:
        return None

    aspect = picture.width / picture.height
    customer = aspect, picture.resize((SIDE, SIDE), Image.Resampling.LANCZOS)
    screen = (picture.resize((224, round(224 / aspect)), Image.Resampling.LANCZOS)
              if .25 <= aspect <= .77 else None)
    matches = {}
    candidates = []
    for product_id, color, filename in references:
        try:
            path = Path(filename)
            stat = path.stat()
            if not 0 < stat.st_size <= MAX_BYTES:
                continue
            catalog = _catalog_thumbnail(path, stat.st_mtime_ns, stat.st_size)
        except (OSError, ValueError, Image.DecompressionBombError):
            continue
        if not catalog:
            continue
        candidates.append((product_id, color, catalog))
        if _same_photo(customer, catalog):
            matches.setdefault(product_id, set()).add(color)
            if len(matches) > 1:
                return None
    if not matches and screen:
        for product_id, color, catalog in candidates:
            if _screenshot_match(picture, screen, catalog):
                matches.setdefault(product_id, set()).add(color)
                if len(matches) > 1:
                    return None
    if not matches:
        return None
    product_id, colors = next(iter(matches.items()))
    colors.discard('')
    return product_id, next(iter(colors)) if len(colors) == 1 else ''
