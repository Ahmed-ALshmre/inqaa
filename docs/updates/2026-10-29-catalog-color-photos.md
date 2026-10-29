# Keep color photo requests on the known catalog model

Read-only review of one reported baraah-kids conversation found that browsing
stopped at a color photo request. No delivery phone/address had been supplied and
no order existed. The selected dress had four working HTTP 200 catalog images,
but no administrator-assigned image colors. The AI's color-specific request was
therefore rejected by the strict image selector. A later identity question was
not an explicit request to speak to a person; truthful AI disclosure remains.

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

## Validation

Five new positive cases failed before the initial change. All 123 focused
photo/color/media cases passed before the final preview completeness guard.
The final full isolated audit passed **1417 tests in 162.64 seconds**, including
15 new end-to-end and guard cases. Command:
`python -m account_app.tests.run_offline_audit`.
The new tests use synthetic conversations, .test images and disposable databases;
actual provider and delivery traffic is blocked. Production inspection was read
only, with credentials/customer records held in memory and no transcript export.
No customer messages or orders were sent/created and no catalog labels were
changed. Live customer model previews were not used.
