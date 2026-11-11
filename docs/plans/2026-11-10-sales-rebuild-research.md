# بحث مشاريع GitHub لإعادة بناء المحادثة والحجز

تاريخ البحث: 10 نوفمبر 2026. هذه دراسة تصميم؛ لم تُشغّل مشاريع الطرف الثالث، ولم تثبت حزمها، ولم تختبر قدرتها على البيع باللهجة العراقية. القراءة تشمل ملفات توثيق وكود واختبارات محددة أدناه، وليست تدقيقًا كاملًا لكل مستودع. لا توجد نتائج تحويل تجارية مستقلة في هذه الدراسة تبرر وعدًا بنسبة حجز.

الخطة التابعة: [خطة التنفيذ على inqaa](2026-11-10-sales-rebuild-plan.md).

## النتيجة المقترحة

نبني قلبًا جديدًا داخل تطبيق Python الحالي، بذاكرة وسلة وأدوات تجارة مشتركة. نستفيد من NVIDIA في حدود المسؤوليات ومراجع المنتجات، ومن ChatCart في فصل استقبال الرسائل والتدخل البشري عن التنفيذ، ومن LangGraph في تنظيم التنفيذ، ومن τ-bench في قياس النتيجة الفعلية. لا ندمج التطبيقات بكاملها، ولا ننقل شروط الأسواق الأخرى إلى المتاجر العراقية.

## 1. مقارنة المشاريع الأقرب

| المشروع | ما قرأته | التشابه مع inqaa | ما نستفيد منه | الحدود والقرار |
|---|---|---|---|---|
| [NVIDIA Retail Shopping Assistant](https://github.com/NVIDIA-AI-Blueprints/retail-shopping-assistant) | README، وثيقة الهيكلة، كود المحادثات ومراجع المنتجات والسلة | ملابس وأحذية وحقائب، صور، اقتراحات وسلة | فصل الحقيقة التجارية عن قرار الموديل؛ تسجيل ما عُرض؛ تعديل السلة مرة واحدة رغم الإعادة | أفضل مرجع معماري ضمن العينة؛ المخزون الحي غير موصول حسب الوثيقة، والعروض stub. نستفيد من التصميم ولا نستنسخ الخدمات الخمس أو منظومة NVIDIA |
| [ChatCart](https://github.com/Khaled-Ahmmed-Anik/ChatCart) | README، ثلاث وثائق للمحادثة والتخطيط والتقييم، خدمتان واختبار سلوكي | الأقرب وظيفيًا: Messenger وWhatsApp، متاجر، موظفون، سلة وتجهيز | فصل الحدث عن الرد، حفظ المهمة المعلقة، خطة رد معلومة، تدرج الإطلاق | Rails وليس Python، ولهجة بنغالية/إنجليزية. قواعد التصنيف والتأكيد والتحويل لا تُنسخ. لم تظهر رخصة معيارية في بيانات GitHub أو ملف LICENSE في الجذر المقروء؛ نعتمد المفاهيم ونكتب تنفيذنا |
| [Azure Postgres AgenticShop](https://github.com/Azure-Samples/postgres-agentic-shop) | README وملف prompts للمخزون والتخصيص | توصيات حسب التفضيلات ومعلومات المنتج | احترام اللون والميزانية حتى لو لم يوجد تطابق، وفصل صفات المنتج عن تفضيل المشتري | demo يركز على البحث والتخصيص؛ ليس دليلًا على اكتمال حجز Messenger. Azure وLlamaIndex وذاكرة إضافية أكثر مما نحتاج الآن |
| [AWS conversational commerce sample](https://github.com/aws-samples/sample-conversational-commerce-openclaw-eum) | README وخريطة الخدمات | WhatsApp للزبون، Telegram للمالك، متجر وطلبات وتصعيد | كل القنوات تستعمل Store API واحدة، وصلاحيات البائع تختلف عن الزبون | عينة تعتمد خدمات AWS عديدة؛ لا ننقل EKS وOpenClaw وSNS إلى Railway. لم تُراجع أدواتها داخليًا أو تُشغّل |
| [Infobip agentic CRS](https://github.com/infobip/infobip-agentic-crs) | README مع الخوارزمية وحدود المصدر العام | اقتراحات WhatsApp من كتالوج يتغير | تحديث السعر والتوفر دون إعادة بناء تمثيل النص، وتمييز التحديث الجزئي عن snapshot كامل | المصدر العام runnable للمزامنة المصطنعة فقط؛ المحادثة المنشورة موصوفة بمخططات وواجهات مختصرة وليست محركًا مفتوحًا كاملًا. ليس بديلًا جاهزًا |
| [Rasa CALM demo](https://github.com/RasaHQ/rasa-calm-demo) | دليل Flows الرسمي، ترتيب data/flows، order_pizza.yml | جمع بيانات واستئناف خطوات متوقفة | تخطي الحقول الموجودة، وفصل إجراء التنفيذ عن نص السؤال | المثال يفرض confirmation وask_before_filling؛ لا نعتمد ذلك عند اكتمال نية الشراء لدينا. Rasa Pro ليس مجرد إضافة خفيفة إلى Flask |

## 2. أدلة من الكود، وما تعنيه للمشروع

**NVIDIA: المنتج الذي عرضناه يجب أن يبقى مرجعًا قابلًا للحل.**

ملف [product_references.py](https://github.com/NVIDIA-AI-Blueprints/retail-shopping-assistant/blob/main/memory_retriever/src/product_references.py) يحفظ مجموعة العرض وترتيب القطعة والمجموعة وإصدار الكتالوج؛ نتيجة حل المرجع قد تكون resolved أو ambiguous أو not_found. هذا مفيد لـ«الثاني» و«هاي الزرقة». عندنا نربط الترتيب بالصور التي قُبل إرسالها فعليًا، ونبقي حالة التسليم غير المحسومة منفصلة. قياس قطعة معروضة لا ينتقل تلقائيًا لقطعة لاحقة.

**NVIDIA: الاستئناف وإعادة الطلب ليستا تنفيذًا جديدًا.**

ملفا [conversations.py](https://github.com/NVIDIA-AI-Blueprints/retail-shopping-assistant/blob/main/memory_retriever/src/conversations.py) و[main.py للسلة](https://github.com/NVIDIA-AI-Blueprints/retail-shopping-assistant/blob/main/memory_retriever/src/main.py) يتضمنان معاملات، attempt/request identifiers، وفحص تكرار تعديل السلة. نتبنى هذا المبدأ لعدم تكرار الطلب بعد timeout؛ لا نفترض أن checkpoint وحده يضمن عدم تكرار إجراء خارجي.

**ChatCart: سؤال جانبي لا يمسح مهمة الحجز.**

في [ConversationTurnManager](https://github.com/Khaled-Ahmmed-Anik/ChatCart/blob/main/backend/app/services/conversation_turn_manager.rb) حقول للمهمة الحالية والسؤال المعلق والمهمة المتوقفة ومراجع المنتجات. عندنا السؤال «القماش شتوي؟» أثناء إرسال العنوان لا يلغي القياس واللون؛ الإجابة ثم المتابعة تعتمد على نفس السلة.

**ChatCart: أدوات القراءة محصورة بمتجر المحادثة.**

[ConversationToolGateway](https://github.com/Khaled-Ahmmed-Anik/ChatCart/blob/main/backend/app/services/conversation_tool_gateway.rb) يمرر البحث عبر business الخاص بالمحادثة؛ لا يأخذ متجرًا حرًا من الموديل. عندنا نثبت store/channel/sender في سياق الطلب خارج صلاحية الموديل، ونطبق العزل أيضًا على أدوات الكتابة.

**ChatCart: لا يكفي نسخ الاختبارات.**

[اختبار التخطيط](https://github.com/Khaled-Ahmmed-Anik/ChatCart/blob/main/backend/test/services/conversation_planner_quality_benchmark_test.rb) يفحص اختيار المنتج، المقاطعة، استقرار اللغة، تعديل الطلب والتحويل. لكن [توثيق التقييم](https://github.com/Khaled-Ahmmed-Anik/ChatCart/blob/main/docs/conversation-evaluation.md) يوضح أن benchmark التصنيف محلي ولا يستدعي Gemini. لا نعامل أرقامه كاختبارات موديل فعلية أو نسبة حجز؛ نبني تقييمًا عراقيًا متعدد الرسائل يصل إلى قاعدة الطلبات.

**Azure: التخصيص المفيد مرتبط بسبب اختيار الزبون.**

[ملف prompts](https://github.com/Azure-Samples/postgres-agentic-shop/blob/main/backend/src/agents/prompts.py) يطلب مطابقة تفضيلات المستخدم بخصائص المنتج والمخزون. نستفيد من احترام التفضيل، لكن لا ننقل حد «أقل من 10 = ندرة»، ولا قاعدة منع اقتراح البديل؛ كلاهما قرار متجر وتجربة بيع مختلفة.

**Infobip: نتائج البحث ليست نسخة مستقلة من الأسعار.**

[وصف المزامنة](https://github.com/infobip/infobip-agentic-crs/blob/main/README.md) يميز تغييرات السعر/المخزون عن تغييرات الوصف. عندنا نقرأ السعر الحي من CatalogRepository وقت التسعير والحفظ حتى لو استُخدم بحث دلالي لاحقًا. لا نحذف منتجًا بسبب غيابه عن صفحة API جزئية.

## 3. المكتبات والبروتوكولات المساندة

| المرجع | الأدلة المقروءة | القرار المقترح |
|---|---|---|
| [LangGraph](https://github.com/langchain-ai/langgraph) | [واجهة CheckpointSaver](https://github.com/langchain-ai/langgraph/blob/main/libs/checkpoint/langgraph/checkpoint/base/__init__.py)، وثائق persistence، LICENSE | مرشح تنظيم التنفيذ داخل Python. تجربة محدودة أولًا؛ حالة التجارة في قاعدة التطبيق، ولا تُعتبر نسخة graph سلطة ثانية |
| [Pydantic AI](https://github.com/pydantic/pydantic-ai) | [structured output](https://github.com/pydantic/pydantic-ai/blob/main/docs/output.md)، وثائق durable execution، LICENSE | مفيد كبديل لمحرك orchestration؛ لا نجمعه مع LangGraph كمديرين لنفس الدور. استخدام Pydantic للتحقق من العقود قرار منفصل عن تبني Pydantic AI |
| [Langfuse](https://github.com/langfuse/langfuse) | README وLICENSE | تتبع وتقييم اختياري خارج المسار الحرج. تجهيز سجل محلي منقح أولًا؛ انقطاع منصة المراقبة لا يوقف الحجز |
| [Sierra τ-bench](https://github.com/sierra-research/tau2-bench) | README الحديث، [retail tools](https://github.com/sierra-research/tau2-bench/blob/main/src/tau2/domains/retail/tools.py)، LICENSE | نستفيد من بيئة الأدوات والحالة النهائية والمحاكاة، ونكتب مهام عراقية خاصة. مهام خدمة الطلبات ليست benchmark جاهزًا للإقناع أو COD عندنا |
| [UCP](https://github.com/Universal-Commerce-Protocol/ucp) و[Shopify Cart MCP](https://shopify.dev/docs/agents/carts-and-checkout/cart-mcp) | المواصفات والتوثيق الذي روجع في جولة البحث السابقة | مرجع لعقود catalog/cart/checkout. يؤجل الربط البروتوكولي حتى توجد حاجة خارجية فعلية؛ لا يتطلب قلبنا الجديد MCP |

الاختيار المبدئي: Flask الحالي + خدمات تجارة داخلية + عقود Pydantic + LangGraph محدود لتنظيم الدور + مزود موديلات قابل للتبديل + مراقبة اختيارية. لا نحتاج حاليًا Kubernetes أو مجموعة وكلاء أو قاعدة متجهات خارجية للكتالوج الصغير. نضيف البحث الدلالي إذا عجز البحث المفلتر عن benchmark البدائل.

## 4. ما لا يُنقل من المصادر

- لا تُنسخ تعليمات prompts أو قواعد سوق آخر إلى الإنتاج؛ أرقام الهواتف والعناوين وسياسات الدفع والتأكيد عندنا مختلفة.
- لا يُفرض اسم العميل أو إعادة تأكيد الحجز إذا لم تتطلبهما شروط المتجر الحالية؛ المطلوب نية شراء مثبتة وحقول تنفيذ صحيحة.
- لا تُستخدم ردود قوالب واسعة كمسار أول لكل سؤال؛ أكثرية الفهم تكون في العقل، والتحقق البرمجي للحقائق والإجراءات.
- لا يُعتمد اقتراح موديل أو memory summary كحقيقة عن المخزون أو الإرسال أو الموافقة.
- لا يُعاد إرسال ما يحتمل أنه وصل؛ idempotency المحلي لا يمنح ضمان exactly-once عند مزود لا يدعمه.
- لا تُدمج هوية زبون بين Chatwoot وZernio بناءً على الاسم وحده.
- لا تُستورد بيانات أو صور أو تعليمات من المستودعات باعتبارها بيانات المتاجر.

## 5. الرخص وحدود إعادة الاستخدام

قرأت [LICENSE الخاص بـNVIDIA](https://github.com/NVIDIA-AI-Blueprints/retail-shopping-assistant/blob/main/LICENSE) وهو Apache-2.0 في الملف، و[LangGraph](https://github.com/langchain-ai/langgraph/blob/main/LICENSE) و[Pydantic AI](https://github.com/pydantic/pydantic-ai/blob/main/LICENSE) و[τ-bench](https://github.com/sierra-research/tau2-bench/blob/main/LICENSE) بملفات MIT. [Langfuse LICENSE](https://github.com/langfuse/langfuse/blob/main/LICENSE) يستثني أدلة ee من MIT إلى رخصتها الخاصة. عند تبني اعتماد أو مقتطف لاحقًا نسجل الإصدار وملف الرخصة وإشعارات المصدر، ونراجع رخصة أي نموذج أو أصول مستقلة أيضًا.

ChatCart مرجع قراءة وتصميم في هذه الخطة، وليس مصدر نسخ؛ بيانات المستودع رجعت license=null والجذر المقروء لم يتضمن ملف رخصة. بقية عينات Azure/AWS/Infobip/Rasa ليست ضمن قائمة كود سيُنسخ هنا؛ أي إعادة استخدام مستقبلية تستلزم فحص ملفاتها المحددة أولًا.

## 6. سجل قابل لإعادة المراجعة

كل الروابط أعلاه قُرئت عبر موصل GitHub أو التوثيق الرسمي بتاريخ البحث؛ روابط main قابلة للتغير. قبل التنفيذ تثبت نسخ الاعتماديات وتوثق commit للمصدر المستخدم. لا يعني وقت تحديث المستودع نجاحه في الإنتاج.

أمثلة لبصمات blobs التي أعادها فهرس GitHub أثناء القراءة:

| الملف | Git blob SHA |
|---|---|
| NVIDIA memory_retriever/src/conversations.py | c8c97f18ff2ce63e9564fa42a45bde6d6e2a00e4 |
| NVIDIA memory_retriever/src/product_references.py | 1a11f9711cff2d662f401773a1e5121c1f04fd9d |
| NVIDIA memory_retriever/src/main.py | 98476ea6cf9cf4c96ef800847cdf4849d965f7d2 |
| ChatCart README.md | abcf2951c0c9af97991909fceff2d42c6d0e197b |
| Rasa data/flows/order_pizza.yml | 5af413fe7e2ae0063a505df7603f3abb8dc1e034 |
| τ-bench src/tau2/domains/retail/tools.py | 4f862f0325bb2b8e9b7b795bbb8485e154a77327 |

المصدر العام يصلح لتبرير قرار هندسي، لكنه لا يثبت جودة اللهجة العراقية ولا نسبة تحويل تجارية. الحكم النهائي يكون باختبارات مشروعنا ثم تجربة حيّة محدودة مقاسة.
