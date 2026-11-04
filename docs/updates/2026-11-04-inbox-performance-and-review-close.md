# Inbox loading and review closure

The inbox queried review and job histories for each customer without sender/status indexes. On the November 4 backup (15,197 customers, 166,634 messages and 52,030 AI jobs), several existing inbox requests exceeded a 45-second local benchmark cutoff. Indexed reads completed in approximately 0.05–0.75 seconds when warm; the first measured cold read with the final schema took 2.82 seconds. These are local snapshot timings, not production latency guarantees.

The migration adds targeted indexes and a per-customer reviewed-message cursor. A completed review acknowledges historical repeated-checkout diagnostics without hiding future messages or new reviews. Orphan color-held orders cannot block unrelated conversations through a SQL NULL comparison. Pending order actions and real color holds still require resolution.

Review closure returns current badges for that customer. The dashboard updates the row directly, retains loaded pages and scroll position, deduplicates clicks, and ignores stale polls started before the action. All explicit and reply-assisted close paths check the response; a late response remains scoped to its original conversation. Successful close and delete actions update their rows without a complete-list fetch.

Regression tests cover review state, new issues after closure, color holds, pending order actions, failed saves, delayed polls, double clicks, switching conversations, modal and main-button closure, and loaded-list retention. Browser acceptance uses disposable synthetic records through a Flask test client with external customer delivery disabled.
