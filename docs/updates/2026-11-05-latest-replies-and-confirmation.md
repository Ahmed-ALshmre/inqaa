# Reply and AI-stop audit — 5 November 2026

The live sample after the preceding reply release contained 87 active conversations and 516 messages, with nine affected histories examined in full. The sample had six saved orders and 260 successful provider requests. Paginated live reads are a bounded review, not a transactionally consistent database export. Private customer records remain outside Git.

Confirmed causes and changes:

- Restore the owner's complete inspection receipt after a successful booking. The previous built-in full receipt was deliberately shortened by `get_order_confirmation_text`; both that legacy text and the short default now resolve to the requested full text. Independently edited store receipts remain configurable. Automatic booking, manual booking and confirmation retry share the same function. Missing contacts and mere inquiries cannot produce a saved-order confirmation.
- Distinguish “كيفية الطلب ... مكانكم وين” from tracking an existing order. Genuine late/missing-order follow-ups retain their human-review routing.
- Answer narrow delivery/payment questions from current store/product policies before unresolved-image and unavailable-product shortcuts. This covers “خدمة توصيل كم” and “شكد توصيل لمحافظات”; compound shopping questions retain the normal pipeline.
- Remember quantity declarations without requiring the word “قطعة”. Rebuild the derived conversation memory using evidence version 10, respect current-product scope and later quantity corrections, and quote the actual basket quantity for “حسابي شكد / جكد صار سعرنع”. An inquiry remains an inquiry, without creating an order.
- Keep a reliable photo product during price negotiation when the buyer uses a generic garment word that also happens to name another SKU. Explicit replacements retain their normal routing.
- Answer a narrow question about recorded fabric directly. Comparisons, requests for actual photography and genuinely missing properties retain their separate processing; no fabric property is invented.
- Complete intentional paused/human-review callbacks successfully rather than reporting a generation failure. The background worker also treats a completed handoff carrying an operator explanation as completed. Explicit service/delivery failures still fail.

The receipt is:

> تم تثبيت الطلب. يرجى فحص الطلب بحضور المندوب والتأكد من الموديل والقياس. إذا لم يطابق الطلب يرجع مع المندوب بدون دفع. أهم شي فحص الطلب قبل دفع المبلغ.

Validation uses disposable databases with customer messaging, fulfillment and external requests intercepted. The new regression module is `account_app/tests/test_latest_ai_review_nov05.py`; the resume browser harness now uses the production worker completion function. Store AI switches and historical conversations/orders are not changed by these fixes or their tests.
