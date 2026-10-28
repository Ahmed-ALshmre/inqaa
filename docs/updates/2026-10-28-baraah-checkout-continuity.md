# Continue children's checkout after delivery details

Read-only production diagnosis covered the named conversation, 20 conversation
histories, and aggregate status for the latest 100 conversations in baraah-kids.
No customer records, identities, contacts or full transcripts were exported.
The named customer's order was eventually created by staff at 17:47 Baghdad,
after the AI had answered age availability and requested details/consent again.
No live orders or customer messages were created during this investigation.

## Behavior

- A known single-item cart can finish when contact details precede a short
  acceptance or the final age/color answer. Recent incoming delivery declarations
  supply the purchase action; an old saved contact profile alone cannot do so.
- Final explicit booking instructions may include the already supported variant.
- A single-product color shortcut no longer combines colors from different
  linked products. Multi-item purchases pass through full cart reconstruction.
- A plain acknowledgement without a cart after completed delivery details gets
  one contextual repair. The repair must retain every selected product and its
  own options. If it still cannot build the cart or ask a focused missing-option
  question, it fails into the existing human review flow instead of sending a
  misleading acknowledgement. Existing legacy flat carts remain supported.
- A single image that the link flow has already identified can carry age and
  delivery details into checkout instead of stopping at an availability reply.
- Existing product, stock, price, options, contact, conditions and duplicate
  validation remain mandatory. Multiple ages cannot become one inferred line.
- New/unresolved pictures and offers to show photos cannot reuse old delivery
  details as permission to buy. Contact evidence expires after 24 hours and is
  bounded to 60 visible messages, stopping at a new incoming attachment/refusal.

## Validation

Four positive synthetic conversations failed before the change and passed after
it. Negative cases cover photo offers, old profiles, unresolved/new pictures,
questions, refusal, shipping conditions, unavailable inventory, alternative
colors, multiple children, and cross-product measurements. Tests use disposable
databases and block actual provider/delivery network calls.

The final isolated audit passed **1402 tests in 154.49 seconds**, including 19 new
checkout continuity cases. Command: `python -m account_app.tests.run_offline_audit`.

The 100-conversation status sample had 25 contacts with all three delivery fields:
20 were marked booked, three were unbooked with AI enabled and two with AI
disabled. Read-only investigation of the three active cases found one multi-item
cart acknowledgement and two unresolved product identities with staff replies.
These counts are a bounded current sample, not a claim that all historical
conversations have been resolved. Product identities that remain unknown still
need review and cannot safely become inferred orders.

No production model previews or live provider tests were run against customer
conversations. The repair retry is tested with synthetic model outputs, including
success with two lines and repeated failure requiring review.
