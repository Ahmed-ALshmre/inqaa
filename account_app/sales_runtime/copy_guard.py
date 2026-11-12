"""Small mechanical checks; semantic sales quality still needs dialogue review."""
import re

from ..checkout import normalized
from .wording import mentions_free_size


def validate_copy(parts,question,action_results=None):
    proof=action_results or {}
    if not isinstance(parts,list) or not 1<=len(parts)<=5:
        raise ValueError('reply.parts: expected 1..5 parts')
    texts=[]
    for part in parts:
        if not isinstance(part,dict) or part.get('type') not in {'text','image'}:
            raise ValueError('reply.part: expected text or image')
        if part['type']=='text':
            if not isinstance(part.get('text'),str) or not part['text'].strip():
                raise ValueError('reply.text: expected nonempty text')
            texts.append(part['text'])
    text=normalized('\n'.join(texts))
    if proof.get('omit_color_list'):
        from ..product_variants import colors
        if len(colors(text)) > 1:
            raise ValueError('reply.relevance: answer the current topic without listing colors again')
    if proof.get('omit_free_size_explanation') and mentions_free_size(text):
        raise ValueError('reply.relevance: the size system was already explained for this item')
    if question is None and re.search(r'[?؟]',text):
        raise ValueError('reply.question: no question is allowed for this turn; answer/acknowledge only')
    asks={
        'color':r'(?:(?:شنو|يا|اي)\s*(?:ال)?لون|(?:تختار\w*|تحدد\w*|تگول\w*|قولي|گولي|دز\w*).{0,25}(?:اللون|لونج))',
        'size':r'(?:(?:شنو|شكد|اي)\s*(?:ال)?(?:قياس|عمر|وزن)|(?:دز\w*|قولي|گولي).{0,20}(?:قياسج|وزنج|عمره))'}
    for field,pattern in asks.items():
        permitted=bool(question and (question.endswith('.'+field) or field=='size' and question.endswith(('.age_months','.weight_kg'))))
        if re.search(pattern,text) and not permitted:raise ValueError('reply.question: do not repeat '+field+' outside its selected step')
    if question and question.startswith('alternative.') and re.search(r'(?:نونو|طفل)\s*ثاني|تحبين\s*قياس',text):
        raise ValueError('reply.alternative: offer a compatible product; do not request a different child or age')
    request=r'(?:دز\w*|ارس\w*|محتاج\w*|نحتاج|تنط\w*|ممكن|بس).{0,55}'
    fields={
        'contact.phone':r'(?:رقم|موبايل|تلفون|تليفون)',
        'contact.address':r'(?:عنوان|محافظه|محافظة)',
    }
    for field,keyword in fields.items():
        if re.search(request+keyword,text):
            permitted=question==field or question=='contact.details' or field=='contact.address' and question=='contact.province'
            if not permitted:
                raise ValueError('reply.question: do not request '+field+'; allowed field is '+str(question))
    booking=(r'تم\s+(?:تثبيت|تسجيل|تاكيد)\s+(?:الطلب|الحجز)|ثبتنالج\s+(?:الطلب|الحجز)|'
             r'ثبتت(?:لج|لك|لچ|لكم)|حجزت(?:لج|لك|لچ)|طلب(?:ج|ك|كم)\s+(?:مثبت|محجوز|تثبت)|'
             r'تثبت\s+(?:طلبج|طلبك|الطلب|الحجز)|'
             r'(?:طلبج|طلبك|حجزج|حجزك)\s+(?:صار\s+)?(?:جاهز|مسجل|تجهز|طلع)|'
             r'سجل(?:ت|نا)(?:لج|لك)?\s*(?:طلبج|الطلب|الحجز)')
    if re.search(booking,text) and not proof.get('order_saved'):
        raise ValueError('reply.booking: do not claim order creation before committed confirmation')
    if re.search(r'(?:طلبج|طلبك|الطلب)\s*(?:تجهز|طلع|انشحن|بالطريق)',text) and not proof.get('order_dispatched'):
        raise ValueError('reply.action: shipping status requires executed fulfillment evidence')
    if re.search(r'(?:يجي|يجيلج|راح\s*يجي).{0,10}مضبوط|يرهم\s*(?:بكل\s*راحه|100)|كلش\s*مناسب',text) and not proof.get('fit_guaranteed'):
        raise ValueError('reply.fit: do not guarantee an inferred fit')
    if re.search(r'(?:نسق|اتواصل|نتواصل|ابلغ|نبلغ|بلغت|نحاول|احرص|خاطب).{0,40}(?:مندوب|شركة\s*التوصيل)',text) and not proof.get('carrier_contacted'):
        raise ValueError('reply.action: carrier coordination requires an executed operation')
    if re.search(r'تم\s*(?:الغاء|تعديل)|الغيت|غيرنا\s*الطلب',text) and not proof.get('order_changed'):
        raise ValueError('reply.action: order change requires a committed operation')
    if re.search(r'خصمتلج|نزلتلج.{0,24}خصم|بعد\s+(?:ال)?خصم', text) and not proof.get('discount_allowed'):
        raise ValueError('reply.discount: no negotiated discount was applied')
    if re.search(r'(?:انا|اني).{0,45}(?:انسان[هة]?|بشر|بنفسي|من\s*المحل)',text):
        raise ValueError('reply.identity: never deny automation or claim human identity')
    if re.search(r'(?<!\w)p\d{3}(?!\w)',text):
        raise ValueError('reply.product: internal product IDs are not customer-facing names')
    image_request=(r'(?:دز\w*|ارس\w*).{0,35}(?:صور|سكرين|اسم\s*الموديل)'
                   r'|(?:ممكن|احتاج|ابعثي).{0,8}(?:صور|سكرين)')
    if re.search(image_request,text) and question!='product':
        raise ValueError('reply.question: do not repeat product identification outside the selected step')
    if re.search(r'مو\s*(?:محدد|موضح|متوفر).{0,15}(?:معلومات|بيانات)|البيانات\s*(?:غير|مو)|منشا\s*الصنع\s*مو\s*محدد',text):
        raise ValueError('reply.missing_fact: genuine missing facts require silent handoff')
    if proof.get('delivery_time')=='من 24 ساعة إلى يومين' and re.search(r'(?:[3-9]|ثلاث[هة]?|اربع[هة]?)\s*(?:ايام|يوم)',text):
        raise ValueError('reply.delivery_time: use the approved 24-hour to two-day window')
