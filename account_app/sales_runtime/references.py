"""Resolve owner ad mappings and references to accepted product presentations."""
from copy import deepcopy

from .. import ad_attribution
from .store import Conflict


class References:
    def __init__(self, store, catalog):
        self.store, self.catalog = store, catalog

    def resolve(self, scope, *, ad=None, presentation=None, position=None):
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
        if ad:
            matches = ad_attribution.product_candidates(list(self.catalog.products.values()), ad, scope.store)
            matches = [p for p in matches if p.get('status', 'active') == 'active']
            if len(matches) == 1:
                return dict(status='known', source='ad', product_id=matches[0]['product_id'])
            # Unknown/ambiguous attribution never overwrites a previous selection.
            return dict(status='ambiguous' if matches else 'unknown', source='ad')
        return dict(status='none')

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
