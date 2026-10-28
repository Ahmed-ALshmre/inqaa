# Resume checkout after an older information question

A customer had chosen a single linked suit, agreed to book, then asked an
unconditional garment-length question. Two days later she supplied an address
and valid phone separately. No local order was recorded; the reply returned to
the old length question. Her subsequent delivery enquiry was routed as a missing
previous order. Review was read-only and retained no personal records.

Two deterministic checkout guards prevented recovery:

1. A numeric answer to the store's size question was retained, but its catalogue
   describes kilograms only. The direct resume path required a supported garment
   size, so it rejected the known answer and fell back to model generation.
2. Even with a valid recovered cart, an old open detail required every contact
   field before allowing the contact-collection path. The first half of split
   contact details therefore returned to the old question instead of collecting
   the missing field.

`resume_known_checkout` now carries an in-range numeric selection in a
weight-only catalogue as `size_type=weight`, retaining the same number as weight.
This does not convert kilograms into a garment size. A mixed catalogue with
actual numbered sizes, an out-of-range weight, or an unknown choice still needs
resolution. Existing product, quantity, colour, consent, stock and pricing
validation remains in force.

After a checkout/contact action, old unconditional information questions no
longer block collection of missing contact fields. The final order validator
still requires a valid phone, province and usable address before saving. A fresh
question or an unresolved explicit purchase condition remains blocking.

## Validation

- Before the change: three positive regressions failed (weight-only checkout,
  split contacts after two days, partial contacts for numbered garment sizes).
- After the change: 53 focused checkout, booking and negotiation tests passed.
- Final full official isolated audit: **1383 passed** in 235.36 seconds.
- New full webhook tests assert one persisted order and one complete confirmation,
  unchanged selected kilograms, missing-contact collection without model calls,
  and no booking for a fresh detail question, explicit length condition, invalid
  weight, mixed size inventory or sold-out product.
- Tests use synthetic people, addresses, numbers and catalogue facts in disposable
  databases. No customer messages or live orders are sent or created.

The fix affects resumed/new checkout processing. It does not retroactively create
an order for someone now reporting a missing delivery; that existing case remains
for staff to verify before any new shipment.
