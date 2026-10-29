# Complete checkout from an explicit garment size

The reported conversation exposed an Iraqi size expression that was not
recognized. A later size answer could also hit a generic availability response
before checkout recovery, while the kilogram-only recovery path discarded
explicit garment labels and could not complete both chosen colors.

Normalize plural and written variants such as “ثلاث اكسات”, “ثلاثة اكس”,
“اربعة اكسات” and “دبل اكس لارج”. Match multi-X labels before XL so a longer
phrase is not reduced to the wrong size. Existing explicit inventory remains
authoritative; normalizing a label does not declare it available.

Use a valid explicit garment label before an earlier body weight. Retain both
colors and confirmed quantities, including when the label and colors arrive in
one message. Save those lines as actual garment sizes without a fabricated
weight. When recent delivery declarations establish a checkout action, process
the final size before a generic size-availability response. Do not re-ask a color
when several colors are already selected.

Ask for the missing clothing size instead of requiring a numeric kilogram
answer by default. Previously supplied valid kilograms remain usable, with the
existing range validation. A customer's explicit garment label is not a claim
that an out-of-range body weight fits the product. Missing choices, unresolved
quantities, unavailable inventory and purchase conditions still block creation.

Increment the selection evidence version so older cached conversations are
rebuilt from their source messages using the corrected size parser. This does
not replay conversations, resume paused chats or create old shipments.

Validation includes synthetic multi-turn cases for final size after full
contacts, size before split contacts, two colors in one bubble, duplicate-order
prevention, unsupported inventory, unknown quantities, unresolved arrival and
false fit claims. The real-provider evaluator also covers label-first and
label-last dialogues with public repository prompts and isolated storage.

The full isolated offline audit passed **1469 tests** in 189.47 seconds.
The focused size/weight/owner-copy/price group passed **69 tests**. Real
OpenRouter acceptance passed **12/12 dialogues, 46 customer turns**, with
**15 successful HTTP requests** to `google/gemini-3.8-flash`, no truncated
responses and no external deliveries. Label-first and label-last cases both
persisted two correct garment-size lines without requesting a body weight.
The evaluator used public repository prompts and disposable synthetic data.

No customer transcript or contact field is included in this change. Production
inspection was read-only. Existing manually registered orders remain untouched.
