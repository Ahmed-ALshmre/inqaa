let staffAccounts = [];
async function loadStaff() {
  try {
    const response = await fetch('/api/staff'); const data = await response.json();
    if (!response.ok) throw Error(data.error);
    staffAccounts = data.accounts;
    const list = document.getElementById('staffList'); list.replaceChildren();
    if (!staffAccounts.length) list.textContent = 'لا يوجد موظفون بعد. أضف أول موظف للبدء.';
    for (const account of staffAccounts) {
      const card = document.createElement('article'); card.className = 'admin-panel';
      const name = document.createElement('h2'); name.textContent = account.name;
      const detail = document.createElement('p'); detail.textContent = `${account.username} · ${account.active ? 'فعال' : 'معطل'} · ${account.permissions.length} صلاحيات`;
      const edit = document.createElement('button'); edit.className = 'btn btn-outline-primary'; edit.textContent = 'تعديل الصلاحيات والحساب'; edit.onclick = () => editStaff(account.id);
      card.append(name, detail, edit); list.append(card);
    }
  } catch (error) { document.getElementById('staffStatus').textContent = error.message; }
}
function editStaff(id) {
  const account = staffAccounts.find(a => a.id === id);
  document.getElementById('staffForm').reset();
  for (const [field, value] of Object.entries({staffId:account?.id || '', staffName:account?.name || '', staffUsername:account?.username || ''})) document.getElementById(field).value = value;
  document.getElementById('staffPassword').required = !account;
  document.getElementById('staffActive').checked = account ? Boolean(account.active) : true;
  document.querySelectorAll('[name=permission]').forEach(el => el.checked = account ? account.permissions.includes(el.value) : el.value === 'reply');
  document.getElementById('staffFormStatus').textContent = '';
  document.getElementById('staffDialog').showModal();
}
document.getElementById('staffForm').addEventListener('submit', async event => {
  event.preventDefault(); const button = document.getElementById('staffSave'); button.disabled = true;
  const id = document.getElementById('staffId').value;
  try {
    const response = await fetch('/api/staff' + (id ? '/' + id : ''), {method:id ? 'PUT' : 'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({name:document.getElementById('staffName').value, username:document.getElementById('staffUsername').value, password:document.getElementById('staffPassword').value, active:document.getElementById('staffActive').checked, permissions:[...document.querySelectorAll('[name=permission]:checked')].map(el => el.value)})});
    const data = await response.json(); if (!response.ok) throw Error(data.error);
    document.getElementById('staffDialog').close(); await loadStaff();
  } catch(error) { document.getElementById('staffFormStatus').textContent = error.message; }
  finally { button.disabled = false; }
});
loadStaff();
