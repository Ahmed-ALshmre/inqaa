"""Resolve owner ad mappings and references to accepted product presentations."""
from copy import deepcopy
import re
from urllib.parse import urlsplit

from .. import ad_attribution, product_albums, product_variants, variant_inventory
from .store import Conflict


WEB_LINK = re.compile(r'https?://[^\s<>"\]\)]+', re.I)
CATALOG_LINK_FIELDS = ('product_url', 'catalog_url', 'permalink', 'ref', 'image_url')


def link_identity(value):
    url = ad_attribution.safe_url(value)
    if not url:
        return None
    parts = urlsplit(url)
    return parts.scheme, parts.netloc.lower(), parts.path.rstrip('/'), parts.query


class References:
    def __init__(self, store, catalog):
        self.store, self.catalog = store, catalog

    def resolve(self, scope, *, ad=None, media=None, presentation=None, position=None, text=''):
        if scope.store != self.catalog.store:
            raise Conflict('catalog_scope_mismatch')
        state = self.store.snapshot(scope)['data']
        if presentation is not None:
            choices = state.get('presentations', {}).get(presentation, [])
            if type(position) is not int or not 1 <= position <= len(choices):
                raise Conflict('presentation_reference_unresolved')
            choice = deepcopy(choices[position - 1])
            self.catalog.get(choice['product_id'])
            return dict(status='known', source='presentation', **choice)
        if media and media.get('status') == 'matched':
            product_id, = media['product_ids']
            product = self.catalog.get(product_id)
            reference = dict(status='known', source='image', product_id=product_id)
            observed = product_variants.single_color(media.get('observed_color'))
            if observed:
                reference['observed_color'] = observed
                if (product_albums.stock_state(product) == 'available'
                        and observed in product_variants.colors(product.get('colors'))
                        and (not product.get('variant_stock') or variant_inventory.choices(product, color=observed))
                        and not product_variants.order_choice_error(product, 'color', observed)):
                    reference['available_photo_color'] = observed
            return reference
        direct_links = [match[0].rstrip('.,،؛!?؟') for match in WEB_LINK.finditer(text or '')]
        match = self.match_links(direct_links)
        if match:
            return match
        if ad:
            matches = ad_attribution.product_candidates(list(self.catalog.products.values()), ad, scope.store)
            matches = [p for p in matches if p.get('status', 'active') == 'active']
            if len(matches) == 1:
                return dict(status='known', source='ad', product_id=matches[0]['product_id'])
            match = self.match_links([ad.get('source_url')])
            if match:
                return match
            # Unknown/ambiguous attribution never overwrites a previous selection.
            return dict(status='ambiguous' if matches else 'unknown', source='ad')
        if direct_links:
            return dict(status='unknown', source='product_link')
        return dict(status='none')

    def match_links(self, links):
        identities = {key for link in links if (key := link_identity(link))}
        if not identities:
            return None
        matches = [product['product_id'] for product in self.catalog.products.values()
                   if product.get('status', 'active') == 'active' and
                   any(link_identity(product.get(field)) in identities for field in CATALOG_LINK_FIELDS)]
        if len(matches) == 1:
            return dict(status='known', source='product_link', product_id=matches[0])
        return dict(status='ambiguous', source='product_link') if matches else None

    def record_accepted(self, scope, key, version, presentation, outbox_ids, choices):
        """Call only after all product media parts were accepted by the channel."""
        if scope.store != self.catalog.store:
            raise Conflict('catalog_scope_mismatch')
        if not presentation or not choices or len(choices) != len(outbox_ids):
            raise ValueError('invalid_presentation')
        for choice in choices:
            product = self.catalog.get(choice['product_id'])
            if choice.get('color'):
                from ..product_variants import order_choice_error
                if order_choice_error(product, 'color', choice['color']):
                    raise Conflict('presentation_color_unavailable')
        def handle(state):
            for row_id, choice in zip(outbox_ids, choices):
                row = self.store.db.execute('SELECT status,payload FROM sales_v2_outbox WHERE scope=? AND id=?',
                                            (scope.key, row_id)).fetchone()
                import json
                payload = json.loads(row[1]) if row else {}
                if (not row or row[0] != 'accepted' or payload.get('type') != 'image'
                        or payload.get('product_id') != choice['product_id']
                        or payload.get('color', '') != choice.get('color', '')):
                    raise Conflict('presentation_requires_accepted_matching_media')
            state.setdefault('presentations', {})[presentation] = deepcopy(choices)
            return dict(presentation=presentation)
        return self.store.apply(scope, key, version,
                                dict(presentation=presentation, ids=outbox_ids, choices=choices), handle)
