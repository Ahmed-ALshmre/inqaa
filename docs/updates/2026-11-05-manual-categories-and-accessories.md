# Manual categories, recommendation groups, shoes and bags

`/product-categories` is a store-scoped vocabulary page available to the owner and product-management staff. Categories and groups can exist before their first product. The product editor uses this vocabulary for `productType` and `existingGroupPicker`; existing classifications remain available. An owner can rename or hide entries without discarding their products or their recommendation links.

Each category has a stable identity, an editable display name, a product family and a default size system. Product families provide sizing behavior, while the stable category identity constrains alternatives. A shoe category and a bag category cannot become interchangeable just because both belong to one recommendation group. Existing categories and groups are discovered from catalog records; no bulk rewrite of the live catalog is required. Group renames keep aliases so records saved under the previous name continue to link.

The registry is stored in `catalog_classifications` in the existing SQLite database and is included in full database backups. Products also carry their category display name and family for portable product records. Product writes validate the selected category against the current store's registry and reject inactive assignments for new products, cross-store IDs and client-supplied family/label overrides. Existing products can retain an archived category. Mutations require the product-management permission and a session CSRF token. The read-only live API exposes the classification records and the two new product fields for future reviews.

Shoe and bag sales:

- Shoes ask for the foot/shoe size, using actual current inventory. Body weight, adult clothing labels and a child's age cannot select a shoe size. A half size such as `37.5` is preserved and is accepted only when explicitly listed, without truncating it to `37` or inferring it from an integer range.
- `size_system=none` means no selectable size. Such bags move from the chosen color to missing contact details and booking; they do not request clothing size or body weight. Physical dimensions remain product facts that can be answered from `measurements`.
- Bag browsing and alternatives do not append a clothing-size question. Recommendation filtering still checks category, audience, availability, current sizing evidence when applicable and store scope. Browsing and price inquiries do not create bookings.
- Missing facts retain the silent human-review path. Successful bookings retain the owner's complete inspection receipt.

Usage: choose the store on the categories page, create a category and its size behavior, and add any recommendation group. In Products, select the category under audience/sizing and add the group under alternatives. Products sharing that group become candidates after the eligibility checks. The editor's preview can inspect suggestions without sending anything to a customer.

Validation: `account_app/tests/test_manual_categories_nov05.py` covers registry lifecycle/security, shoe inventory, child-age questions, half sizes, bag selection, booking and alternatives using disposable databases and blocked customer/fulfillment delivery. `tools/check_manual_categories_ui.py` exercises category/group creation, both editor pickers, product saving, actual recommendation preview, archive behavior, store isolation and the mobile layout.
