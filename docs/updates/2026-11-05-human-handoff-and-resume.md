# False handoffs and operator AI recovery

The November 5 snapshot exposed false missing-fact handoffs for recorded double Italian fabric and winter notes. The fabric sanitizer rejected the word `دبل`, while season lookup ignored product notes. Fabric spelling variants and the street name `شارع الكص` also became unknown construction questions. Catalogue discovery requests no longer count as unknown product details.

Operator previews now return the specific missing fields without pausing AI, creating duplicate reviews or incrementing customer-question counts. Repeated staff retries reuse the pending review. An unanswered-product pause has an explicit automatic reason, so a verified product link can recover it without overriding a manual stop or a customer-requested handoff.

Enabling conversation AI queues the saved unanswered input rather than generating a reply inside the HTTP request. The endpoint can return 202 with a job ID; the dashboard polls the result and displays the actual post-generation AI state and specific handoff reason. The stop control remains usable. Completed intentional handoffs can be retried after catalogue correction, while accepted replies, newer input, global stops and uncertain delivery protections remain in force. No synthetic incoming message is inserted by recovery.

A delayed webhook also respects a sticker classification already persisted by the archive worker, preventing the old raw image payload from reopening product review.

Validation: 1,739 Python tests passed, 8 operator-recovery browser scenarios and 10 inbox browser regressions passed, and the dashboard JS checks passed. OpenRouter conversation replay passed 272 checks across 8 conversations/22 incoming turns. On the final source, three customer clothing images, tested with explicit owner permission in synthetic conversations, all resolved to the currently registered P008 and resumed a 40,000 IQD product reply; all 54 checks passed. External customer delivery and fulfillment were intercepted. Private snapshots, images, keys and generated reports are excluded from Git.
