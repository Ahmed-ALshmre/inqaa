let classificationKind='category',classificationData={categories:[],groups:[]},classificationSaving=false;
const classEl=id=>document.getElementById(id);
const classificationFamilies=JSON.parse(classEl('classificationFamilies').textContent);
const classificationStore=new URLSearchParams(location.search).get('store_id')||'default';
const classificationUrl='/api/products/manage/classifications';
async function classificationRequest(path='',options={}){
  const response=await fetch(classificationUrl+path+'?store_id='+encodeURIComponent(classificationStore),options);
  const data=await response.json();if(!response.ok)throw new Error(data.error||'تعذر حفظ التصنيف');return data;
}
function showClassificationKind(kind){
  classificationKind=kind;classEl('categoriesTab').setAttribute('aria-selected',kind==='category');classEl('groupsTab').setAttribute('aria-selected',kind==='group');
  classEl('categoryFields').hidden=kind!=='category';classEl('classificationHelp').textContent=kind==='category'?'مثل: أحذية رياضية، حقائب يد، دشاديش شتوية.':'مثل: حقائب الدوام، أحذية العيد. أضف نفس المجموعة للمنتجات التي تريد ربط بدائلها.';
  classEl('classificationListTitle').textContent=kind==='category'?'الفئات الموجودة':'المجموعات الموجودة';
  classEl('classificationListHelp').textContent=kind==='category'?'تظهر الفئات في نوع القطعة عند إضافة المنتج.':'تظهر المجموعات حتى قبل إضافة أول منتج لها. يراجع المساعد الفئة والتوفر والمقاس قبل الاقتراح.';
  classEl('classificationSearch').value='';newClassification();renderClassifications();
}
function newClassification(){
  classEl('classificationForm').reset();classEl('classificationId').value='';classEl('classificationActive').checked=true;
  classEl('classificationFamily').value='other';classEl('classificationSize').value='legacy';classEl('classificationFamily').disabled=false;
  classEl('classificationFormTitle').textContent=classificationKind==='category'?'إضافة فئة':'إضافة مجموعة';
  classEl('saveClassification').textContent=classificationKind==='category'?'حفظ الفئة':'حفظ المجموعة';classEl('classificationFormStatus').textContent='';
}
function editClassification(record){
  newClassification();classEl('classificationId').value=record.id;classEl('classificationName').value=record.name;
  classEl('classificationActive').checked=record.active;classEl('classificationFamily').value=record.family||'other';classEl('classificationSize').value=record.size_system||'legacy';
  classEl('classificationFamily').disabled=record.product_count>0||Object.hasOwn(classificationFamilies,record.id);classEl('classificationFormTitle').textContent='تعديل '+record.name;
  classEl('classificationName').focus();classEl('classificationForm').scrollIntoView({behavior:'smooth',block:'center'});
}
function renderClassifications(){
  classEl('categoryCount').textContent=classificationData.categories.filter(r=>r.active).length;classEl('groupCount').textContent=classificationData.groups.filter(r=>r.active).length;
  const query=classEl('classificationSearch').value.trim().toLocaleLowerCase('ar'),target=classEl('classificationList');target.replaceChildren();
  const items=classificationData[classificationKind==='category'?'categories':'groups'].filter(r=>(r.active||classEl('includeInactive').checked)&&r.name.toLocaleLowerCase('ar').includes(query));
  for(const record of items){
    const row=document.createElement('article');row.className='classification-row'+(record.active?'':' taxonomy-inactive');
    const copy=document.createElement('div'),title=document.createElement('h3'),meta=document.createElement('small');title.textContent=record.name;
    meta.textContent=[classificationFamilies[record.family],`${record.product_count} منتج`,record.active?'':'مخفي من الاختيارات الجديدة'].filter(Boolean).join(' · ');copy.append(title,meta);
    const actions=document.createElement('div');actions.className='taxonomy-actions';const edit=document.createElement('button');edit.type='button';edit.className='btn btn-sm btn-outline-primary';edit.textContent='تعديل';edit.addEventListener('click',()=>editClassification(record));actions.append(edit);
    row.append(copy,actions);target.append(row);
  }
  if(!items.length){const empty=document.createElement('p');empty.className='taxonomy-empty';empty.textContent=query?'لا توجد نتائج مطابقة.':'لا توجد عناصر هنا بعد. أضف أول '+(classificationKind==='category'?'فئة.':'مجموعة.');target.append(empty);}
}
classEl('classificationFamily').addEventListener('change',()=>{const family=classEl('classificationFamily').value;classEl('classificationSize').value=family==='shoes'?'shoe':['bags','accessories'].includes(family)?'none':'legacy';});
classEl('classificationForm').addEventListener('submit',async event=>{
  event.preventDefault();if(classificationSaving)return;classificationSaving=true;classEl('saveClassification').disabled=true;
  const status=classEl('classificationFormStatus');status.textContent='جاري الحفظ…';status.dataset.error='false';
  try{
    const id=classEl('classificationId').value,data={kind:classificationKind,name:classEl('classificationName').value.trim(),family:classEl('classificationFamily').value,size_system:classEl('classificationSize').value,active:classEl('classificationActive').checked};
    classificationData=await classificationRequest(id?'/'+classificationKind+'/'+encodeURIComponent(id):'',{method:id?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    newClassification();renderClassifications();status.textContent='تم الحفظ. يظهر الاختيار في بطاقة المنتج.';
  }catch(error){status.textContent=error.message;status.dataset.error='true';}finally{classificationSaving=false;classEl('saveClassification').disabled=false;}
});
document.addEventListener('DOMContentLoaded',async()=>{
  classEl('backToProducts').href='/products?store_id='+encodeURIComponent(classificationStore);newClassification();
  try{classificationData=await classificationRequest();renderClassifications();}catch(error){classEl('classificationStatus').textContent=error.message;}
});
