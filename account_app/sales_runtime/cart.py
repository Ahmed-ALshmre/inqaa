"""Structured cart tools. The model proposes changes; local services own truth."""
from copy import deepcopy
import math
import re

from .. import checkout, pricing, product_variants
from .store import Conflict, digest, encode
from .contracts import BusinessChoice,ServiceRequired


class Catalog:
    def __init__(self, store, products):
        self.store = store
        self.products = {str(p['product_id']): deepcopy(p) for p in products
                         if p.get('store_id', 'default') == store}
        for product in self.products.values():
            # Some legacy rows store "60 to 120" without a unit, while their
            # factual description explicitly records the SAME kilogram band.
            # Normalize this read view only; never infer units from the numbers.
            from ..customer_copy import fact
            raw=checkout.normalized(str(product.get('sizes') or ''))
            numeric=re.fullmatch(r'\s*(?:من\s*)?(\d{1,3})\s*(?:الي|الى|لحد|[-–])\s*(\d{1,3})\s*',raw)
            declared=product_variants.size_data(fact(product.get('description')))['weights']
            if (product.get('size_system') in (None,'','legacy') and numeric and len(declared)==1
                    and tuple(map(int,numeric.groups()))==tuple(declared[0])):
                product['_raw_sizes']=product['sizes']
                product['sizes']=f'وزن {numeric[1]} إلى {numeric[2]} كيلو'
            data=product_variants.size_data(product.get('sizes') or '')
            product['selection_mode']=(product.get('size_system') if product.get('size_system') not in (None,'','legacy') else
                'weight' if product_variants.weight_only_ranges(product) else
                'age' if data['years'] and not data['known'] else 'clothing')
        self.revision = digest(self.products)

    def get(self, product_id):
        item = self.products.get(str(product_id))
        if not item or item.get('status', 'active') != 'active':
            raise Conflict('product_not_available_in_store')
        return deepcopy(item)

    def validate(self, line):
        product = self.get(line['product_id'])
        if type(line.get('quantity')) is not int or not 1 <= line['quantity'] <= 20:
            raise ValueError('quantity_must_be_integer_1_to_20')
        system = product.get('size_system') or 'legacy'
        if line.get('size_system') != system:
            raise Conflict('size_system_mismatch')
        if system not in {'none', 'legacy', 'clothing', 'shoe', 'age', 'weight'}:
            raise Conflict('unsupported_size_system')
        if system in {'age', 'weight'} and line.get('size'):
            raise Conflict('typed_measurement_requires_own_field')
        if len(product_variants.colors(line.get('color') or '')) > 1:
            raise Conflict('one_color_per_cart_line')
        for field in ('color', 'size'):
            value = line.get(field) or ''
            if not isinstance(value, str):
                raise ValueError('choice_must_be_text')
            if field == 'size' and system == 'none':
                if value:
                    raise Conflict('sizeless_product_has_size')
                continue
            # Empty choices are permitted in a draft, but never at checkout.
            if value and (field == 'color' or system in {'legacy', 'clothing', 'shoe'}):
                weight_ranges=product_variants.weight_only_ranges(product) if field=='size' and system=='legacy' else []
                if weight_ranges and re.fullmatch(r'\d{1,3}(?:\.\d+)?',value):
                    error='' if any(lo<=float(value)<=hi for lo,hi in weight_ranges) else 'الوزن خارج نطاق هذا المنتج'
                elif (field == 'size' and product_variants.is_free_size_product(product)
                      and not product_variants.size_data(product['sizes'])['known']
                      and not product_variants.FREE_SIZE_RE.fullmatch(checkout.normalized(value))):
                    error = 'الموديل فري سايز وما مسجّل إله هالقياس'
                else:error = product_variants.order_choice_error(product, field, value)
                if error:
                    raise BusinessChoice(error,product_id=product['product_id'],field=field,
                                         available=product.get('colors' if field=='color' else 'sizes',''),requested=value)
        for field, mode, low, high in (
            ('age_months', 'age', 'age_min_months', 'age_max_months'),
            ('weight_kg', 'weight', 'weight_min_kg', 'weight_max_kg'),
        ):
            value = line.get(field)
            if value is None:
                continue
            if system != mode:
                raise Conflict('measurement_type_mismatch')
            if (type(value) not in {int, float} or not math.isfinite(value)
                    or value < 0 or (mode == 'weight' and value == 0)):
                raise ValueError('invalid_measurement')
            if mode == 'age' and type(value) is not int:
                raise ValueError('age_requires_integer_months')
            if product.get(low) is None or product.get(high) is None:
                raise Conflict('measurement_bounds_missing')
            if not float(product[low]) <= value <= float(product[high]):
                raise BusinessChoice('measurement_out_of_range',product_id=product['product_id'],field=field,
                                     available=dict(min=product[low],max=product[high]),requested=value)
        return product


class CartService:
    def __init__(self, store, catalog):
        self.store, self.catalog = store, catalog

    def _scope(self, scope):
        if scope.store != self.catalog.store:
            raise Conflict('catalog_scope_mismatch')

    def change(self, scope, key, version, event_id, *, line_id, values=None, remove=False):
        self._scope(scope)
        if type(remove) is not bool:
            raise ValueError('remove_must_be_boolean')
        if not isinstance(line_id, str) or not line_id:
            raise ValueError('line_id_required')
        values = dict(values or {})
        allowed = {'product_id', 'color', 'size', 'quantity', 'size_system', 'wearer_ref', 'age_months', 'weight_kg'}
        if set(values) - allowed:
            raise ValueError('unsupported_cart_fields')
        command = dict(event_id=event_id, line_id=line_id, values=values, remove=remove)
        def handle(state):
            self.store.evidence(scope, event_id)
            old = next((i for i in state['cart'] if i['line_id'] == line_id), None)
            if state.get('order_id'):
                if old and not remove and all(checkout.normalized(str(v))==checkout.normalized(str(old.get(k,''))) for k,v in values.items()):
                    return dict(cart=deepcopy(state['cart']),unchanged=True)
                raise ServiceRequired('booked_cart_requires_order_change_service')
            if remove:
                if not old:
                    raise Conflict('cart_line_not_found')
                state['cart'].remove(old)
            else:
                if old and 'product_id' in values and str(values['product_id']) != old['product_id']:
                    raise Conflict('new_product_requires_new_line')
                line = dict(old or dict(line_id=line_id, color='', size='', quantity=1), **values)
                line['product_id'] = str(line.get('product_id') or '')
                product = self.catalog.get(line['product_id'])
                # Unit metadata belongs to the catalog, never the language model.
                line['size_system']=product.get('size_system') or 'legacy'
                if values.get('size'):line['size_source_kind']='explicit'
                if line['size_system']=='legacy':
                    age=line.pop('age_months',None)
                    weight=line.pop('weight_kg',None)
                    if age is not None and 'size' not in values:
                        line['size']=f'{age//12} سنوات' if age%12==0 else f'{age} شهر'
                        line['size_source_kind']='age'
                    if (weight is not None and 'size' not in values and product_variants.weight_only_ranges(product)
                            and (not line.get('size') or line.get('size_source_kind')=='weight')):
                        line['size']=format(weight,'g')
                        line['size_source_kind']='weight'
                self.catalog.validate(line)
                if old:
                    state['cart'][state['cart'].index(old)] = line
                else:
                    if len(state['cart']) >= 20:
                        raise Conflict('cart_line_limit')
                    state['cart'].append(line)
            for field, value in (dict(removed=True) if remove else values).items():
                self.store.db.execute('''INSERT INTO sales_v2_evidence
                    (scope,event_id,target,field,value,state_version) VALUES(?,?,?,?,?,?)''',
                    (scope.key, event_id, line_id, field, encode(value), version + 1))
            return dict(cart=deepcopy(state['cart']))
        return self.store.apply(scope, key, version, command, handle)

    def contact(self, scope, key, version, event_id, *, phone=None, address=None):
        self._scope(scope)
        if any(value is not None and not isinstance(value, str) for value in (phone, address)):
            raise ValueError('contact_values_must_be_text')
        command = dict(event_id=event_id, phone=phone, address=address)
        def handle(state, phone=phone, address=address):
            source = self.store.evidence(scope, event_id)['payload'].get('text', '')
            errors = []
            if phone is not None:
                if checkout.phone_number(phone) and checkout.phone_number(phone)==state['contact'].get('phone'):
                    phone=None
            if phone is not None:
                parsed = checkout.phone_number(phone)
                if not parsed:
                    errors.append('contact.phone')
                    state['contact'].pop('phone', None)
                elif parsed != checkout.phone_number(source):
                    raise Conflict('phone_not_in_source_event')
                else:
                    state['contact']['phone'] = parsed
                    state.setdefault('contact_events',{})['phone']=event_id
            if address is not None:
                if address.strip()==state['contact'].get('address'):address=None
            if address is not None:
                if checkout.invalid_shipping_address(address):
                    errors.append('contact.address')
                    state['contact'].pop('address', None)
                    state['contact'].pop('province', None)
                else:
                    if checkout.normalized(address).strip() not in checkout.normalized(source):
                        raise Conflict('address_not_in_source_event')
                    state['contact']['address'] = address.strip()
                    state.setdefault('contact_events',{})['address']=event_id
                    parsed = checkout.contact_fields(address)
                    # A corrected address must not retain an old province.
                    state['contact'].pop('province', None)
                    if parsed.get('province'):
                        state['contact']['province'] = parsed['province']
            return dict(contact=deepcopy(state['contact']), invalid_fields=errors)
        return self.store.apply(scope, key, version, command, handle)

    def readiness(self, scope):
        self._scope(scope)
        snap = self.store.snapshot(scope)
        state, missing = snap['data'], []
        if not state['cart']:
            missing.append('cart')
        for line in state['cart']:
            product = self.catalog.validate(line)
            if product.get('colors') and not line.get('color'):
                missing.append(f"cart.{line['line_id']}.color")
            system = line['size_system']
            if system == 'legacy' and not product.get('sizes'):
                missing.append(f"catalog.{line['product_id']}.size_system")
            if system in {'legacy', 'clothing', 'shoe'} and (product.get('sizes') or system == 'shoe') and not line.get('size'):
                missing.append(f"cart.{line['line_id']}.size")
            if system in {'age', 'weight'}:
                field = 'age_months' if system == 'age' else 'weight_kg'
                if line.get(field) is None:
                    missing.append(f"cart.{line['line_id']}.{field}")
        missing.extend(f'contact.{f}' for f in ('phone', 'address') if not state['contact'].get(f))
        if not state['contact'].get('province'):
            missing.append('contact.province')
        if not state['purchase_event']:
            missing.append('purchase_intent')
        return dict(ready=not missing and not state['conditions'] and state['control_mode'] == 'active',
                    missing_fields=missing, conditions=deepcopy(state['conditions']), version=snap['version'])

    def quote(self, scope, delivery):
        self._scope(scope)
        snap = self.store.snapshot(scope)
        if not snap['data']['cart']:
            raise Conflict('empty_cart')
        for line in snap['data']['cart']:
            self.catalog.validate(line)
        result = pricing.quote(snap['data']['cart'], list(self.catalog.products.values()), delivery)
        result['total'] = result['product_total'] + result['delivery_fee']
        free=all(pricing.terms(self.catalog.get(line['product_id']))[2] for line in snap['data']['cart'])
        if delivery is None and not free:result.update(total=None,delivery_fee=None)
        result['state_version'] = snap['version']
        result['catalog_revision'] = self.catalog.revision
        from .negotiation import adjust
        return adjust(snap['data'],result)

    def purchase(self, scope, key, version, event_id):
        """Only explicit customer purchase wording authorizes this first adapter.

        Broader conversational consent needs its own reviewed interpretation
        contract; a model boolean or a staff message is not a substitute.
        """
        self._scope(scope)
        def handle(state):
            evidence = self.store.evidence(scope, event_id)
            text = evidence['payload'].get('text', '')
            if re.search(r'(?<!\w)(?:اذا|لو|بشرط)(?!\w)', checkout.normalized(text)):
                raise Conflict('purchase_condition_needs_resolution')
            if evidence['actor'] != 'customer' or not checkout.explicit_purchase(text):
                raise Conflict('explicit_customer_purchase_required')
            state['purchase_event'] = event_id
            return dict(purchase_event=event_id)
        return self.store.apply(scope, key, version, dict(event_id=event_id, action='purchase'), handle)
