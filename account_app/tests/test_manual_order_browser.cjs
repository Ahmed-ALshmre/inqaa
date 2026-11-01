// Synthetic browser exercise: no production requests or customer messages.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const source = fs.readFileSync(path.join(__dirname, '../static/js/dashboard.js'), 'utf8');
const code = source.slice(source.indexOf('let manualOrderRowId'), source.indexOf('async function sendCatalog()'));
(async () => {
  const executablePath = process.env.MANUAL_QA_CHROMIUM || '/usr/bin/chromium';
  const browser = await chromium.launch({headless: true, executablePath, args: ['--no-sandbox']});
  try {
    const page = await browser.newPage();
    await page.setContent('<div id="orderModal"><input id="orderPhone"><input id="orderProvince">' +
      '<input id="orderAddress"><input id="orderPaid" type="checkbox"><textarea id="orderNotes"></textarea>' +
      '<div id="manualOrderItems"></div><button id="submitManualOrderBtn"></button></div>');
    await page.evaluate(() => {
      window.currentSenderId = 'synthetic-customer';
      window.currentCustomer = {name: 'زبونة اختبار', store_id: 'al-fatena'};
      window.allProducts = [];
      window.esc = value => String(value);
      window.calls = []; window.notices = [];
      window.showToast = text => notices.push(text);
      window.bootstrap = {Modal: {getOrCreateInstance: () => ({show() {}}), getInstance: () => ({hide() {}})}};
      window.apiFetch = async (url, opts) => {
        calls.push({url, body: JSON.parse(opts.body)});
        return {json: async () => ({ok: true, order_id: 123, confirmation_sent: calls.length > 1})};
      };
    });
    await page.addScriptTag({content: code});
    await page.evaluate(() => openOrderModal());
    await page.locator('[data-field="product_name"]').fill('قطعة يدوية حسب الصورة');
    await page.locator('[data-field="weight"]').fill('80-90');
    await page.locator('[data-field="color"]').fill('نفس الصورة');
    await page.locator('[data-field="quantity"]').fill('25');
    await page.locator('[data-field="unit_price"]').fill('12000');
    await page.evaluate(() => submitOrder());
    assert.match(await page.locator('#submitManualOrderBtn').textContent(), /إعادة إرسال/);
    await page.evaluate(() => submitOrder());
    const calls = await page.evaluate(() => calls);
    assert.equal(calls.length, 2);
    assert.equal(calls[0].body.items[0].weight, '80-90');
    assert.equal(calls[0].body.items[0].quantity, 25);
    assert.match(calls[0].url, /create_order$/);
    assert.equal(calls[1].url, '/api/orders/123/resend_confirmation?store_id=al-fatena');
    console.log('PASS: manual free entry, missing contacts, weight, quantity, confirmation retry without rebooking');
  } finally { await browser.close(); }
})().catch(error => {console.error(error); process.exitCode = 1;});
