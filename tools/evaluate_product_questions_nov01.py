"""Isolated real-model acceptance of owner voice, color images and silent gaps.

All buyer messages below are synthetic. No original customer records or backup
utterances are read or sent. Uses the existing disposable webhook/sender harness.
"""
import evaluate_recent_sales as recent
import re

h=recent.h


def scenarios(catalog):
    cases=[]
    def add(identity,store,product,texts,orders=0,**extra):
        cases.append(dict(id=identity,store=store,product=product,purpose=identity,
                          expected_orders=orders,turns=[h.step(t,'') for t in texts],**extra))
    add('voice-velvet-pair','default','P009',[
        'أهلاً، أريد أعرف نوع القماش وهل ثقيل؟',
        'بكم القطعة وبكم قطعتين مع التوصيل لبغداد؟',
        'ممكن أشوف الألوان المتوفرة بالصور؟',
        'أريد ماروني وأزرق قياس XXL قطعة من كل لون، حجزيهم فدوة',
        h.PHONE,recent.ADDRESS,'صار الحجز؟'],1,wanted_quantity=2,wanted_size='XXL',wanted_colors=['ماروني','أزرق'])
    add('voice-kids-written-ages','baraah-kids','P016',[
        'القماش ثخين للشتاء وبيه ليكرا؟',
        'لطفلين أريد قطعتين، شكد يصير السعر والتوصيل لبغداد؟',
        'الأول عمره أربع سنين ونص بس أريدله قياس خمس سنوات والثاني ست سنوات، أكدر أختار لونين؟',
        'حجزيهم عيني، أخضر قياس الخمس سنوات وبرتقالي قياس الست سنوات، قطعة من كل لون',
        h.PHONE,recent.ADDRESS,'تسلمين'],1,wanted_quantity=2,wanted_ages=[5,6],wanted_colors=['أخضر','برتقالي'])
    add('voice-unknown-cut','default','P011',['القصة كلوش لو مستقيمة؟','أنتظر الموظفة تجاوبني'],allow_review=True)
    for case in cases:
        if case.get('allow_review'):
            for turn in case['turns']:turn['allow_handoff']=True
    return cases


def evaluate_turn(case,turn,index,prior,texts,orders,response,m,db,sender):
    checks=original_turn(case,turn,index,prior,texts,orders,response,m,db,sender)
    reply='\n'.join(texts)
    checks['no_missing_data_placeholder']=not re.search(r'مو موضح|تحتاج تأكيد|يحتاج تأكيد|ما أگدر أأكد',reply)
    checks['no_raw_catalog_price_dump']=not re.search(r'عرض\s+\d+\s*قطع\s*[:：]|سعر\s+\d+\s*قطع\s*[:：]',reply)
    if case['id']=='voice-unknown-cut':
        checks.update(silent_handoff=not texts,automation_paused=not m.is_customer_ai_enabled(db,sender),
                      pending_question_recorded=bool(db.execute("SELECT id FROM product_questions WHERE sender_id=? AND status='pending'",(sender,)).fetchone()))
    if case['id']=='voice-velvet-pair' and index==2:
        checks['color_images_sent']=bool(response.get('image_urls'))
    if case['id']=='voice-kids-written-ages' and index==0:
        checks['owner_confirmed_fabric_answered']='ثخين' in reply and 'ليكرا' in reply
    if index==5 and case['id']!='voice-unknown-cut':
        checks['order_created_after_complete_details']=len(orders)==1
    if index==4 and case['id']!='voice-unknown-cut':
        checks['only_address_missing_after_phone']=not re.search(r'دزيلي.{0,15}رقم|شكد العمر|يا عمر|يا لون',reply)
    return checks


original_turn=recent.evaluate_turn

if __name__=='__main__':
    recent.scenarios=scenarios
    recent.evaluate_turn=evaluate_turn
    recent.main()
