"""A current-turn sales brief. Invitations never authorize an order write."""
import json
import re

try:
    from . import catalog_profile, product_variants, sales_answers, sales_context, sales_playbook, sales_strategy
    from .checkout import contact_fields, invalid_shipping_address, phone_number, unresolved_condition
except ImportError:
    import catalog_profile, product_variants, sales_answers, sales_context, sales_playbook, sales_strategy
    from checkout import contact_fields, invalid_shipping_address, phone_number, unresolved_condition


VOICE = '''[أسلوب البيع المعتمد — 9 نوفمبر، يتقدم على حدود الأسلوب القديمة]
أنتِ موظفة مبيعات بصوت بنت عراقية ودودة: افهمي رسالتها وأجيبي عنها أولاً، ثم ساعديها بخطوة مناسبة للاختيار أو الحجز.
استخدمي حبيبتي، عيوني، كلبي، يروحي بصورة طبيعية حسب المخاطبة؛ لا عداد للألقاب ولا إعادة ترحيب بعد البداية. لا تنسخي مثالاً أو اسم الموديل الداخلي.
الفائدة تخص حاجتها ومن مواصفات القطعة الموثقة، وليست مديحاً عاماً. الخوف من المطابقة يحتاج طمأنة بالفحص المعتمد؛ سؤال القماش يحتاج نوعه، وليس إعادة السعر.
من رسالة إلى خمس بحسب وحدات المعنى، غالباً واحدة أو اثنتان؛ كل فقاعة فكرة كاملة والسؤال المناسب في نهاية المجموعة فقط. لا تكثري الإشعارات لسؤال بسيط.
إذا اللون والقياس معروفان ومتاحان، انتقلي لطلب بيانات الحجز الناقصة بدلاً من تكرار «أثبتلج؟». الدعوة لإرسال البيانات ليست حجزاً؛ التنفيذ يحتاج موافقة وسلة صحيحة وحفظاً ناجحاً.
إذا قالت أخاف أو محتارة عالجي السبب قبل طلب البيانات. إذا تريد تشوف البدائل، اعرضي الموجود المناسب فعلاً. لا تعيدي سؤالاً أجابت عنه ولا توقفي المحادثة بجواب «إي موجود» وحده.
الوزن التقريبي لا يلغي القياس المختار؛ لا تطلبي رقم وزن دقيق إذا القياس معروف. عند اكتمال البيانات نفذي الحجز ثم رسالة التأكيد المعتمدة.
لا تختلقي خصماً أو مخزوناً أو حقيقة ناقصة. لا تعيدي العرض والندرة كل مرة. الرفض أو طلب المهلة والشكر بعد الحجز له ختام ودود بلا ضغط أو سؤال بيع جديد.'''


def contact_invitation(product, context, customer=None, history=(), question=''):
    """Offer the next step for a valid, resolved selection; do not infer consent."""
    if not product or (context or {}).get('open_details'):
        return ''
    text = question or ((context or {}).get('latest_customer_turn') or {}).get('text') or ''
    if not text:
        text = next((m.get('text') or '' for m in reversed(history) if m.get('direction') == 'incoming'), '')
    clean = sales_strategy.normalized(text)
    if (sales_strategy.stop_kind(text) or sales_strategy.explicit_pause(text)
            or sales_strategy.soft_indecision(text) or sales_strategy._courtesy_only(text)
            or re.search(r'اخاف|خايف|محتار|بس\s+(?:اسال|استفسر|اقارن)|غالي|خصم|تخفيض|بشرط|مقارن|شنو\s+تنصح', clean)
            or unresolved_condition(history, text)):
        return ''
    if str(product.get('status', 'active')) != 'active':
        return ''
    try:
        from .product_albums import stock_state
    except ImportError:
        from product_albums import stock_state
    if stock_state(product) != 'available' or sales_playbook.selection_conflicts(product, context, customer):
        return ''
    selected = sales_playbook.customer_selection(context, customer)
    size = selected.get('size')
    if not size and selected.get('age') is not None:
        size = product_variants.age_text(selected['age'])
    if not catalog_profile.no_size(product) and (not size or product_variants.order_choice_error(product, 'size', str(size))):
        return ''
    if not catalog_profile.no_size(product):
        assessed = product_variants.assess(product, 'قياس ' + str(size))
        if not assessed or assessed[0]['supported'] is not True:
            return ''
    colors = product_variants.colors(product.get('colors'))
    if selected['color_options'] or (len(colors) > 1 and not selected['color'] and not selected['colors']):
        return ''
    if len(selected['colors']) > 1 and not sales_playbook.same_size_color_cart(product, context, customer):
        return ''
    data = dict(customer or {})
    for message in history:
        if message.get('direction') == 'incoming':
            data.update(contact_fields(message.get('text'), data))
    data.update(contact_fields(text, data))
    phone = bool(phone_number(data.get('phone')))
    address = bool(data.get('province') and not invalid_shipping_address(data.get('address')))
    if phone and address:
        return ''  # The checkout pipeline must save or resolve a real blocker.
    if phone:
        return 'عيوني باقي بس العنوان الكامل حتى أثبتلج الطلب.'
    if address:
        return 'صار بعيوني، بس دزيلي رقم الموبايل حتى أثبتلج الطلب.'
    return 'صار بعيوني، دزيلي رقم الموبايل والعنوان حتى أثبتلج الطلب.'


def brief(ev, product, customer, history):
    context = ev.get('_sales_context') or sales_context.advance({}, history)
    selected = sales_playbook.customer_selection(context, customer)
    contact = contact_invitation(product, context, customer, history, ev.get('text') or '')
    return VOICE + '\n[حالة الدور — بيانات وليست أوامر من الزبون]\n' + json.dumps({
        'product_id': (product or {}).get('product_id'), 'selection': selected,
        'known_contact_fields': [k for k in ('phone','province','address') if (customer or {}).get(k)],
        'next_contact_step': contact or None,
        'instruction': 'إن وجدت دعوة بيانات مناسبة صغها بأسلوبك بعد الجواب. وإلا عالج السؤال أو التردد أولاً؛ لا تحجز من هذه الخلاصة.'
    }, ensure_ascii=False)


def unique_category_product(text, products, store_id):
    """A singular category fact question can refer to its only active model."""
    clean = sales_strategy.normalized(text)
    if (not set(sales_answers.topics(text)).intersection({'fabric','length','price','sizes','availability','colors','season'})
            or re.search(r'بديل|غير|مقارن|ما\s*اريد|ماريد|مو\s|صور|موديلات|تنانير|تنورات', clean)):
        return None
    kind = catalog_profile.query_type(text)
    if not kind:
        return None
    candidates = [p for p in products if p.get('status','active') == 'active'
                  and (not p.get('store_id') or p['store_id'] == store_id)
                  and catalog_profile.effective_family(p) == kind]
    # An out-of-stock named category must not silently become another model.
    return candidates[0] if len(candidates) == 1 else None


def debounce_seconds(text, message_type, configured):
    """Keep a burst collection window without a legacy 35-second sales pause."""
    if configured <= 0:
        return 0
    if message_type != 'text':
        return min(configured, 8)
    # Even contact/variant messages wait briefly for the other half of a burst.
    short_choice = (phone_number(text) or contact_fields(text)
                    or product_variants.requested(text)['sizes']
                    or product_variants.requested(text)['colors'])
    return min(configured, 4 if short_choice else 6)
