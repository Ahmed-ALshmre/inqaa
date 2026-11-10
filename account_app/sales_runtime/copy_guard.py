"""Small mechanical checks; semantic sales quality still needs dialogue review."""
import re

from ..checkout import normalized


def validate_copy(parts,question):
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
    if question is None and re.search(r'[?؟]',text):
        raise ValueError('reply.question: no question is allowed for this turn; answer/acknowledge only')
    request=r'(?:دز\w*|ارس\w*|محتاج\w*|نحتاج|تنط\w*|ممكن|بس).{0,55}'
    fields={
        'contact.phone':r'(?:رقم|موبايل|تلفون|تليفون)',
        'contact.address':r'(?:عنوان|محافظه|محافظة)',
    }
    for field,keyword in fields.items():
        if re.search(request+keyword,text):
            permitted=question==field or field=='contact.address' and question=='contact.province'
            if not permitted:
                raise ValueError('reply.question: do not request '+field+'; allowed field is '+str(question))
    if re.search(r'تم\s+(?:تثبيت|تسجيل|تاكيد)\s+(?:الطلب|الحجز)|ثبتنالج\s+(?:الطلب|الحجز)',text):
        raise ValueError('reply.booking: do not claim order creation before committed confirmation')
