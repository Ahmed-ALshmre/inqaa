# All-store conversation and checkout reliability — 2026-10-29

## Review scope

Read-only review of the latest 50 conversations combined across stores, with
720 messages and order/review status checked. Distribution: children's store
33, Lamsa 12, Al-Fatena 4, Khuyoot 1; the wholesale store had no conversation in
that time window. Customer transcripts, names, phone numbers and addresses are
not included here or in the regression fixtures.

The concrete checkout defect was a weight-based product losing the customer's
weight and selected colors, inventing a garment size, then requesting all
details again. Other reproduced defects were known catalog photos being
treated as missing, identity questions continuing automated sales, repeated
endearments, and a written child-age range falling into a generic model question.

## Resulting behavior

- Keep kilograms separate from garment sizes. Weight-only catalogs preserve the
  declared weight through color, quantity and contact answers. Numbered-size
  catalogs retain their real inventory requirements; no weight-to-size guess.
- Retain every selected color and its quantity, including different counts,
  attached digits and replacements. Ask only for unresolved counts. Ambiguous,
  zero, unavailable and out-of-range choices cannot silently become an order.
- Keep the latest individual message as the source of selection evidence when
  multiple unanswered messages are aggregated. Earlier choices cannot overwrite
  a later correction by being replayed as a new message.
- Complete a known cart once the actual choices, consent and shipping fields
  are complete. If creation did not execute, request the missing choice/contact
  only. Preserve purchase conditions, refusals, new unanswered product questions,
  pending model choices and delivery receipts as independent guards.
- Read written year endpoints such as “من سنة الى 8 سنوات” as ages. Mixed month
  or kilogram endpoints do not become invented year ranges.
- Send an actual review request silently when a local reply requires staff.
  An empty review response cannot be replaced with the generic model question.
- Recognize “شنو الموديل المطلوب؟” as another known-product re-ask. Reduce
  repeated endearments across replies, respect formal requests/complaints, and
  avoid the duplicated “الحجز الحجز” wording without changing facts.
- Correct the generated delivery phrase “التوصيل بالعالم العادي” to
  “التوصيل بالوضع العادي”, preserving the stated duration and other facts.
- Earlier changes in the same PR keep color-photo browsing on the known catalog
  model, label general albums honestly, and route responder-identity questions
  to a protected staff pause with no acknowledgement or identity claim.

## Validation

- Full isolated offline audit: **1459 passed**, 177.78 seconds. This includes
  existing image routing, gender, reply splitting, checkout, stock and handoff
  regressions. Network connections are blocked and storage is disposable.
- A further focused run of the 27 new regression cases passed after adding the
  mixed-unit age-range guard. The affected child/review/identity/checkout group
  also passed (138 tests).
- The delivery-phrase correction passed a direct wording/fact-preservation
  check and 64 affected delivery, negotiation and checkout regression tests.
- Real-provider acceptance uses synthetic conversations and catalog facts only,
  disposable storage, a bounded OpenRouter HTTP gate and mocked final delivery
  adapters. Main provider/parser, webhook routing, validation, order persistence
  and receipt bookkeeping execute normally; no customer receives a test message.
- Final public-default acceptance: **10/10 dialogues, 38 customer turns** across
  four stores, **13 successful HTTP requests** to `google/gemini-3.8-flash`.
  Seven dialogues reached the provider; the other three correctly used local
  negotiation, unresolved-arrival and silent-identity paths. Provider-reported
  cost for this final run was $0.08491725; no requests were truncated or blocked.
  Six orders were saved only in the disposable database.
- `tools/evaluate_checkout_reliability.py` reads the key from an environment
  variable or hidden terminal input. Its settings allowlist accepts only model
  IDs, temperature and token limits. Production prompts and customer records are
  rejected; prompts come from the public repository defaults.

The acceptance dialogues cover weight/two colors/split contacts; unequal
quantities; replacement without losing quantity; child age; real numbered
sizes; bargaining then refusal; paid delivery after a clear free-delivery
refusal; unresolved arrival condition; silent identity review; and ambiguous
quantity. All normal turns were nonempty, with no model re-ask, duplicate booking
noun, identity statement, or reported failure. Checks include persisted line
quantities/weights/sizes and the actual pending human-review record.

## Operational limits

The children's store had its store-wide AI switch disabled at review time.
Publishing code does not enable that switch. Historic manually acknowledged
bookings need staff reconciliation before a shipment is created; this change
does not replay old customer conversations or create retrospective orders.

Provider acceptance covers synthetic dialogues, not customer-side Meta delivery
or a live recognition benchmark on production customer images. Existing catalog
photo/vision and long-reply routing regressions are covered by the offline suite.
