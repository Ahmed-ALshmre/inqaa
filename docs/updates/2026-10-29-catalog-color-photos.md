# Catalog color photos and silent identity handoff

Read-only review of one reported baraah-kids conversation found that browsing
stopped at a color photo request. No delivery phone/address had been supplied and
no order existed. The selected dress had four working HTTP 200 catalog images,
but no administrator-assigned image colors. The AI's color-specific request was
therefore rejected by the strict image selector. Questions about the assistant's
identity now route straight to staff instead of generating an identity
introduction or continuing product sales. The transfer is silent, with no
acknowledgement sent to the customer.

## Changes

- Recognize a photo request containing color alternatives or a plural follow-up
  as browsing. Do not turn it into a question about purchasing one of each color.
- Follow the last explicit model/color question even when the preceding turn
  described two products, including Arabic name prefixes such as «بفستان».
- Check color availability first. An unavailable color is stated clearly and is
  never represented by another color's photo.
- When all requested available colors have identified photos, send only those
  images. When an exact photo is not identified, explicitly offer the catalog
  model pictures as general model pictures, without claiming their color.
  The low-level exact-color selector remains strict for other call sites.
- For several offered models whose albums exceed four images, send one preview
  per model with an explicit «صورة لكل موديل» caption. This keeps all requested
  models represented within the five-message sales notification limit.
- Real/new photography requests and mixed fabric/price questions retain the full
  response path. Unknown, unavailable and rejected models are not restored.
- Identity questions such as «انتي ذكاء لو بشر؟» create a pending human review,
  cancel automated follow-ups and persist the same protected AI pause as an
  explicit staff request. No text, media or acknowledgement is saved/sent. Product
  links and later messages cannot resume automation; staff must explicitly do so.
- Main-model, opening-greeting, generated follow-up and final automated-delivery
  identity statements are suppressed and routed to real review. No reply claims
  that the automated assistant is a human. The delivery guard checks the main
  reply, reply parts and planned parts, both separately and joined, before any
  transport call. Whitespace/newline splits cannot hide an introduction.
  Questions about AI-generated product photos remain photo questions.

## Validation

Five photo scenarios failed before the initial change. Validation covers the photo
changes, silent staff routing and preservation of prior booking behavior.
The final isolated audit passed **1432 tests in 275.56 seconds**, including 15
photo cases and 15 identity/handoff cases. Command:
`python -m account_app.tests.run_offline_audit`.
The new tests use synthetic conversations, .test images and disposable databases;
actual provider and delivery traffic is blocked. Production inspection was read
only, with credentials/customer records held in memory and no transcript export.
No customer messages or orders were sent/created and no catalog labels were
changed. Live customer model previews were not used.

Identity checks include 32 question phrasings in separate synthetic multi-turn
conversations, 14 identity statements tested in three delivery text locations,
first greetings, generated/stored follow-ups, final contacts, existing bookings,
later replies and product relinking. The latest reported incoming message was
also classified in memory, read only, and correctly selected for staff routing.
Former identity-disclosure expectations were updated to silent staff routing;
the existing checks that reject human impersonation remain enforced.
