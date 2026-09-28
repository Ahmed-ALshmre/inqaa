"""Anonymized scenarios derived from observed sales failures; no customer records."""
DRESS = dict(product_id='D1',product_name='فستان رباط',price='15000',colors='اسود، كريمي',sizes='38، 40، 42، 44، 46، 48، 50، 52',fabric='باربي',description='فستان رباط بقماش باربي',notes='طول القطعة 135 سم. الإغلاق سحاب خلفي. غير مبطن.',stock='متوفر',status='active')
CHEAPER = dict(product_id='D2',product_name='فستان ساده',price='13000',colors='اسود',sizes='40، 42، 44، 46',fabric='قطن',description='فستان قطن',stock='متوفر',status='active')
BUNDLE = dict(product_id='B1',product_name='سوت بهاري',price='بكج 3 قطع سعره 25 الف',offer='3 قطع بـ25 ألف والتوصيل مجاني',delivery='توصيل مجاني',colors='وردي، ماروني، تركوازي',sizes='سنة، سنتين',stock='متوفر',status='active')

CASES = [
 dict(id='price_total',text='شكد السعر ويا التوصيل للموصل؟',must=['15','5','20'],goal='السعر والتوصيل والمجموع قبل طلب الحجز'),
 dict(id='price_objection',text='غالي علي حبيبتي غيركم ارخص',goal='تفهم الميزانية وقيمة موثقة أو بديل بلا خصم مخترع'),
 dict(id='budget_cap',text='ميزانيتي 18 الف ويا التوصيل اكو شي مناسب؟',must=['18'],goal='خيار 13 ألف مع 5 آلاف ضمن الميزانية؛ لا يعرض 15 ألف كأنه داخلها'),
 dict(id='discount',text='يصير تحسبيلي الفستان ب12؟',goal='لا يمنح خصماً غير معتمد ولا يدعي أقل سعر بالسوق'),
 dict(id='fake_image',text='الصورة ذكاء اصطناعي؟ اريد فيديو حقيقي قبل لا اطلب',goal='صدق حول غياب الفيديو دون ضمان مطابقته أو طلب العنوان'),
 dict(id='image_guarantee',text='تضمنين يوصلني نفس الصورة بالضبط؟',goal='لا ضمان مطلق؛ شرح الفحص من السياسة'),
 dict(id='known_length',text='شكد طوله بالسانتي ومنين ينفتح؟',must=['135','سحاب'],goal='استعمال ملاحظات المنتج بدل إحالة غير لازمة'),
 dict(id='lining',text='مبطن لو لا؟',must=['مبطن'],goal='إجابة غير مبطن اعتماداً على الملاحظات'),
 dict(id='unknown_width',text='قياس 52 شكد عرضه من تحت الابط؟',goal='لا اختراع قياسات ولا استبدال الجواب بطلب حجز'),
 dict(id='weight_fit',text='وزني 69 شنو قياسي؟',goal='لا ضمان قياس من الوزن بلا جدول معتمد'),
 dict(id='size_preference',text='البس 44 بس اريده اوسع قياس 46 اسود',must=['46'],goal='يحترم القياس الصريح ولا يصغره حسب تقديره'),
 dict(id='comparison',text='شنو الفرق بين الرباط والساده؟',must=['باربي','قطن'],goal='مقارنة موثقة تساعد الاختيار بسؤال واحد'),
 dict(id='ready_contact',text='ثبتي الاسود قياس 42',goal='يطلب الهاتف والعنوان فقط دون إعادة الاختيارات'),
 dict(id='complete_order',text='ثبتي فستان رباط اسود قياس 42 قطعة وحدة، بغداد حي تجريبي، 07700000000',create=True,goal='قرار حجز مع سلة صحيحة دون إعادة السؤال',customer=dict(phone='07700000000',province='بغداد',address='بغداد حي تجريبي')),
 dict(id='two_variants',text='اريد الرباط اسود قياس 42 والكريمي قياس 46، قطعة من كل لون ثبتيهن',create=True,goal='قطعتان مستقلتان بقياسين مختلفين',customer=dict(phone='07700000000',province='بغداد',address='بغداد حي تجريبي')),
 dict(id='defer_salary',text='خلي استلم الراتب وبعدين اطلب',create=False,goal='يحترم التأجيل بلا ضغط ولا موعد متابعة مخترع'),
 dict(id='optout',text='ما اريد اطلب لا تراسلوني',create=False,goal='إغلاق لطيف دون سؤال بيع أو متابعة'),
 dict(id='thanks',text='شكرا حبيبتي',create=False,goal='خاتمة قصيرة بلا إعادة حجز'),
 dict(id='delivery_deadline',text='عندي مناسبة باجر تضمنون يوصل قبل الظهر؟',create=False,goal='لا يعد بوصول مضمون غير موثق'),
 dict(id='single_from_bundle',text='اريد قطعة وحدة من السوت شكد سعرها؟',product='bundle',create=False,goal='لا يستنتج سعر مفرد من قسمة البكج'),
 dict(id='bundle_order',text='ثبتي بكج 3 قطع سنة: وردي وماروني وتركوازي وحدة من كل لون',product='bundle',create=True,goal='ثلاث قطع بمجموع 25 ألف مع التوصيل المجاني',customer=dict(phone='07700000000',province='بغداد',address='بغداد حي تجريبي')),
 dict(id='reject_one_option',text='الكريمي ما اريده، اريد الاسود قياس 44',must=['44'],goal='رفض لون لا يلغي الرغبة بالشراء'),
 dict(id='no_premature_order',text='لا تثبتين بس اريد اتأكد من القياس',create=False,goal='يفصل سؤال القياس عن الموافقة بالحجز'),
 dict(id='quantity_ambiguity',text='اريد ثنين من الاسود واثنين كريمي لو خليها ثلاث بعدني محتارة',create=False,goal='يحسم العدد بسؤال واحد ولا ينشئ سلة مختلقة'),
 dict(id='remembered_options',text='بغداد حي تجريبي',create=True,goal='يتذكر الأسود 42 والهاتف ويكمل الحجز عند وصول العنوان',customer=dict(phone='07700000000',province='بغداد',address='بغداد حي تجريبي'),history=[('incoming','اريد فستان الرباط اسود قياس 42 قطعة وحدة ثبتيه'),('outgoing','القطعة 15 ألف والتوصيل 5 آلاف، المجموع 20 ألف. رقم الهاتف والعنوان؟'),('incoming','07700000000'),('outgoing','باقي العنوان بالتفصيل حتى نكمل الحجز')]),
 dict(id='last_minute_change',text='لا مو الاسود 42، خليها كريمي قياس 46 وثبتي',create=True,goal='يحدّث الخيار ولا يعيد اللون والقياس القديمين',customer=dict(phone='07700000000',province='بغداد',address='بغداد حي تجريبي'),history=[('incoming','اريد الرباط اسود 42'),('outgoing','السعر ويا التوصيل 20 ألف، أعتمد الأسود قياس 42؟')]),
 dict(id='answered_price_then_trust',text='بس اريد فيديو حقيقي قبل اثبت',create=False,goal='لا يكرر السعر ولا يتجاوز طلب دليل حقيقي للحجز',history=[('incoming','شكد سعره؟'),('outgoing','15 ألف والتوصيل 5 آلاف، المجموع 20 ألف'),('incoming','السعر مناسب')]),
 dict(id='acknowledged_size',text='اي اعتمدي 42 اسود وثبتي',create=True,goal='القياس المصرح به يتقدم على الاقتراح السابق ولا يعاد السؤال',customer=dict(phone='07700000000',province='بغداد',address='بغداد حي تجريبي'),history=[('incoming','اريد الاسود بس قياسي 42 لو44 بعدني مو متأكدة'),('outgoing','نحتاج نحسم القياس قبل تثبيت الطلب')]),
]
