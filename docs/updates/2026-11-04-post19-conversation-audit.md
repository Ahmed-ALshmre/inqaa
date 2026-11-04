# Follow-up to the November 3 conversation recovery

The next backup contained 173 conversations after the previous deployment. The audit found checkout confirmations looping despite saved contact details, chosen garment ages reverting to body ages, and two-piece questions receiving a single-piece or delivery-only price. Seven automatic booking confirmations in the audited period all had saved orders; a further order was staff-created.

The changes preserve checkout consent across unanswered bubbles and inspection acceptance, normalize catalog weight bands against declared weights, retain an explicit child garment size, and carry recipient and age filters through subsequent catalog pages. Price replies use the requested or current same-product quantity and remove duplicated raw offer labels.

Missing promised photos, after-sales scheduling/additions, and received-quality complaints create silent staff reviews. Clear cancellation requests received while AI is paused create durable pending actions without executing cancellation or enabling AI. Product linking cannot clear those pending actions. Store AI switches remain under staff control.

Regression coverage includes split contact details followed by size 52, weight 85 with inspection acceptance, a younger child explicitly choosing size two, boys' age-seven alternatives, a relative birthday, unavailable ages, stock-dependent future purchases, and staff-action handoffs. Test conversations use synthetic identities and contacts; provider tests run against OpenRouter with Messenger, Telegram, and fulfillment intercepted. Private backup data and generated transcripts are excluded from Git.

Historical manual orders, cancellations, and held-color additions still require staff reconciliation rather than automatic replay into fulfillment. This audit does not measure conversion improvement.
