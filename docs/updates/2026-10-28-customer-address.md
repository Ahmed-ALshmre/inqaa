# Consistent customer address in women's stores

An unknown customer was treated as male by `owner_sales.surface`: an otherwise
correct question such as «شكد قياس تلبسين؟» became «شكد قياس تلبس؟». The main
model and first-message prompts also received conflicting guidance about names
and neutral wording. Latin names such as Hiba/Heba and the prefix `its.` were
not recognized.

The current customer statement, explicit profile choice, incoming conversation
evidence, and clear first name now determine address before applying the store
default. Lamsa (`default`), al-fatena and khuyoot default to feminine wording as
requested by the owner. This is a wording preference, not a confirmed identity;
inferred names, grammatical hints and store defaults are never written to
`customers.gender`. A male buyer purchasing for his wife remains masculine.
Children's and wholesale stores do not acquire a feminine default from their
merchandise or the child's gender. Men's size questions in women's stores ask
for the requested size without assuming the buyer wears the clothing.

One resolver is used by first replies, model prompts, local factual answers,
the final multi-bubble surface, handoff messages, and AI-generated follow-ups.
Unknown size questions outside women's stores use «شنو القياس المطلوب؟» rather
than treating masculine grammar as neutral. Feminine suffixes are converted
together to avoid mixed output such as «تحب تشوفين».

## Validation

- Read-only live sample: latest 10 conversations per store active since October
  27 (32 available conversations across four stores), at most 100 recent messages
  each. This is a bounded sample, not a complete conversation audit.
- Replayed 16 observed masculine wording occurrences from women's stores.
  Only counters and short, predefined grammar spans were exported; no names,
  contact details, customer IDs, full responses or transcripts were exported.
- Focused offline tests cover explicit corrections, Latin names,
  self-description across turns, recipient/agent exclusions, unchanged price,
  delivery and size facts, and no inferred identity persistence.
- Final official isolated suite: **1375 passed** (155.10 seconds). Assertions
  for the former masculine default were updated to the requested feminine
  wording; their booking, history, price and routing checks remain in place.
- Real OpenRouter: 5 synthetic conversations, 15 turns, 9 HTTP 200 requests,
  5/5 address cases passed; reported cost USD 0.07426725. This run preceded
  the final copy refinements (avoiding automatic endearments for store defaults
  and asking a male buyer for the requested size). A subsequent bounded rerun
  received HTTP 401 for the test credential and stopped further provider calls;
  it is not counted as a passing live run. Final copy is covered offline. Test storage is
  disposable. All non-OpenRouter HTTP destinations are blocked and no customer
  message or order is created.
- `tools/evaluate_address_gender.py` reproduces the bounded live checks using
  catalogue facts/model settings from a supplied backup; no customer records
  from that backup are loaded.

Private local evidence is ignored under `outputs/address-oct28/`. Production
deployment is a separate operation; this document does not claim it has happened.
