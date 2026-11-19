"""Private-data-free regressions for the owner's November 19 conversation audit."""
import base64
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from PIL import Image, ImageDraw
from account_app import catalog_photo_match
from account_app.sales_runtime import contact
from account_app.sales_runtime.copy_guard import validate_fulfillment
from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.sales_runtime.response import Responses
from account_app.sales_runtime import Conflict, Scope
from account_app.sales_runtime.facts import product_fact, requested_fields
from account_app.sales_runtime.wording import natural_weight_wording
from account_app.tests import test_sales_runtime_linking as linking


class AuditedConversationsTests(unittest.TestCase):
    setUp = linking.LinkingTests.setUp
    tearDown = linking.LinkingTests.tearDown
    event = linking.LinkingTests.event
    version = linking.LinkingTests.version
    run_turn = linking.LinkingTests.run_turn
    select = linking.LinkingTests.select
    prompt = linking.LinkingTests.prompt

    def test_spaced_phone_with_direction_markers_is_a_contact_turn(self):
        for text in ('\u202d0770 123 4567\u202c', '\u20660770 123 4567\u2069',
                     '٠٧٧٠ ١٢٣ ٤٥٦٧', '0770-123-4567'):
            with self.subTest(text=repr(text)):
                self.assertEqual('07701234567', contact.declarations(text, {})['phone'])
                self.assertTrue(contact.contact_only(text))

    def test_selected_item_and_separate_details_book_without_model_consent(self):
        self.select()
        self.run_turn('بغداد حي التجربة قرب المدرسة')
        _, result = self.run_turn('\u202d0770 123 4567\u202c')
        self.assertEqual('booked', result['action'])
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_selected_item_and_combined_details_are_checkout_not_price_inquiry(self):
        self.select()
        self.decide.reset_mock()
        _, result = self.run_turn('دهوك حي التجربة ٠٧٧٠١٢٣٤٥٦٧')
        self.assertEqual('booked', result['action'])
        self.decide.assert_not_called()

    def test_price_only_with_contact_details_does_not_book(self):
        self.select()
        _, result = self.run_turn('قطعتين واصلات شكد؟ رقمي 07701234567 بغداد حي التجربة')
        self.assertEqual('continue', result['action'])
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_chosen_item_is_not_a_saved_order_even_with_copy_guard_disabled(self):
        for text in ('ثبتت الج قياس 54 بالرصاصي.', 'اعتمدت الطلب عيوني.',
                     'تم تثبيت الطلب.', 'طلبج مثبت عيوني.', 'ثبتت الطلب.',
                     'راح نجهزه الج حتى يوصلج.', 'يوصلج نفس الموديل قياس 48 بالأسود.'):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    validate_fulfillment([dict(type='text', text=text)], {})
                validate_fulfillment([dict(type='text', text=text)], dict(order_saved=True))
        validate_fulfillment([dict(type='text', text='باقي بس رقم الموبايل حتى أثبتلج الحجز.')], {})
        validate_fulfillment([dict(type='text', text='بعد ما يتم تثبيت الطلب، التوصيل خلال يومين.')], {})

    def test_repeated_unresolved_photo_followup_gets_review_instead_of_silent_skip(self):
        writer = Mock(return_value=[dict(type='text', text='ممكن اسم القطعة أو رابطها؟')])
        entry = Entrypoint(self.db, lambda _: self.products, lambda *_: 5000, self.decide,
                           writer=writer, media=lambda *_: dict(status='unknown', product_ids=[]))
        args = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                    sender_id=self.scope.sender)
        entry.receive(**args, message_id='first-picture', media_kind='image', media_ref=['https://image.test/a'])
        while part := self.store.claim_reply(self.scope):
            self.store.settle_reply(self.scope, part['id'], 'accepted', 'fixture-receipt')
        result = entry.receive(**args, message_id='still-not-answered', text='دزيتها فوك شوفيها')
        self.assertEqual('human', result['action'])
        self.assertEqual(1, self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])

    def test_inactive_binding_cannot_borrow_another_models_fabric_or_price(self):
        self.products[0].update(status='hidden', fabric='باربي', measurements='الطول 90 سم')
        self.runtime.text_reference = lambda *_args, **_kw: dict(product_id='P1', source='conversation')
        event = self.store.append(self.scope, 'hidden', kind='message', actor='customer',
                                  payload=dict(text='شنو قماشه وطوله؟', media_kind='text'))['id']
        result = self.runtime.turn(self.scope, event)
        self.assertEqual('P1', result['unavailable_product'])
        self.decide.assert_not_called()
        writer = Mock(return_value=[dict(type='text', text='هالقطعة مو متوفرة حالياً عيوني.')])
        reply = Responses(self.store, self.loader, writer).prepare(self.scope, event, result)
        self.assertEqual('queued', reply['action'])
        context = writer.call_args.args[0]
        self.assertFalse(context['unavailable_product']['available'])
        self.assertNotIn('product_facts', context)
        self.assertEqual([], self.store.snapshot(self.scope)['data']['cart'])

    def test_explicit_cancel_still_withdraws_when_it_names_a_color(self):
        self.select()
        self.run_turn('احجزيلي الماروني قياس 44')
        self.run_turn('الغي الطلب الماروني ما اريده')
        self.assertIsNone(self.store.snapshot(self.scope)['data']['purchase_event'])

    def test_rejected_color_with_a_stock_question_does_not_end_the_sale(self):
        self.select()
        self.plan = dict(intent='withdraw')
        result = self.runtime.turn(self.scope, self.event('لا اريد ازرق يا قياس متوفر بالازرق؟'))
        self.assertFalse(result.get('withdrawn'))

    def test_complete_album_is_durable_ordered_and_idempotent(self):
        event = self.event('دزيلي كل الموديلات')
        parts = [dict(type='image', product_id=f'P{i}', url=f'https://catalog.test/{i}') for i in range(12)]
        parts.append(dict(type='text', text='يا واحد عجبج؟'))
        responses = Responses(self.store, self.loader, Mock())
        responses.save(self.scope, 'album-op', 'album-reply', event, self.version(), parts)
        count = self.db.execute('SELECT COUNT(*) FROM sales_v2_outbox').fetchone()[0]
        self.assertEqual(13, count)
        saved_version = self.version()
        responses.enqueue_parts(self.scope, 'album-reply', saved_version, parts)
        self.assertEqual(count, self.db.execute('SELECT COUNT(*) FROM sales_v2_outbox').fetchone()[0])
        sent = []
        while part := self.store.claim_reply(self.scope):
            sent.append(part['payload'])
            self.store.settle_reply(self.scope, part['id'], 'accepted', str(part['id']))
        self.assertEqual(parts, sent)

    def test_album_batch_conflict_rolls_back_the_entire_new_batch(self):
        self.event('صور')
        version = self.version()
        self.store.enqueue_reply(self.scope, 'existing', version, [dict(type='text', text='قديم')])
        with self.assertRaises(Conflict):
            self.store.enqueue_reply_batches(self.scope, version, [
                ('new', [dict(type='text', text='جديد')]),
                ('existing', [dict(type='text', text='تغيير متعارض')])])
        self.assertEqual(0, self.db.execute("SELECT COUNT(*) FROM sales_v2_outbox WHERE reply_key='new'").fetchone()[0])

    def test_thigh_width_does_not_use_chest_width_or_size_range(self):
        question = 'شكد عرض الفخذ؟'
        self.assertIn('thigh_width', requested_fields(question))
        product = dict(measurements='عرض الصدر 55 سم\nالطول 105 سم', sizes='44 إلى 54')
        self.assertIsNone(product_fact(product, 'thigh_width'))
        product['measurements'] += '\nعرض الفخذ 32 سم'
        self.assertEqual('عرض الفخذ 32 سم', product_fact(product, 'thigh_width')['value'])

    def test_spoken_labels_preserve_the_actual_fabric_and_measurement(self):
        self.assertEqual('قماش التنورة دبل إيطالي شتوي، القياسات 44 إلى 54.',
                         natural_weight_wording('قماش التنورة خام دبل إيطالي شتوي، النمر 44 إلى 54.'))

    def test_welcome_after_an_affectionate_prefix_is_not_repeated_mid_conversation(self):
        from account_app.sales_voice import trim_opening_greetings
        current = 'حبيبتي كل الهلا بيج، بانتظارج براحتج حتى نكمل باقي التفاصيل.'
        self.assertEqual('بانتظارج براحتج حتى نكمل باقي التفاصيل.',
                         trim_opening_greetings(current, omit_welcome=True, omit_return_salam=True))
        self.assertEqual(current, trim_opening_greetings(current, omit_welcome=False, omit_return_salam=False))
        self.assertEqual('قياسج متوفر.', trim_opening_greetings('حبيبتي وعليكم السلام والرحمة، قياسج متوفر.',
                         omit_welcome=True, omit_return_salam=True))
        self.assertEqual('حبيبتي قياسج متوفر.', trim_opening_greetings('حبيبتي قياسج متوفر.',
                         omit_welcome=True, omit_return_salam=True))

    def test_accepting_staff_alternative_offer_sends_all_matching_available_models(self):
        for n in range(7):
            self.products.append(dict(self.products[0], product_id=f'S{n}', product_type='skirts'))
        self.products.append(dict(self.products[0], product_id='hidden', product_type='skirts', status='hidden'))
        self.products.append(dict(self.products[0], product_id='sold-out', product_type='skirts', stock='نفذت'))
        self.event('التنورة موجودة؟')
        self.event('هالموديل خلص، أقترحلج موديل مشابه من التنانير؟', actor='staff')
        event, outcome = self.run_turn('اي')
        self.assertTrue(outcome['all_models_requested'])
        writer = Mock(return_value=[dict(type='text', text='هذني الموديلات المتوفرة، يا واحد عجبج عيوني؟')])
        responses = Responses(self.store, self.loader, writer, product_photos=lambda p, colors: [
            dict(type='image', product_id=p['product_id'], url='https://catalog.test/' + p['product_id'])])
        responses.prepare(self.scope, event, outcome)
        queued = [json.loads(row[0]) for row in self.db.execute('SELECT payload FROM sales_v2_outbox ORDER BY id')]
        self.assertEqual({f'S{n}' for n in range(7)}, {part['product_id'] for part in queued if part['type'] == 'image'})
        self.assertEqual('text', queued[-1]['type'])

    def test_twenty_long_journeys_keep_last_choice_contacts_and_exactly_one_order(self):
        questions = ['سلام عليكم', 'القماش شنو؟', 'بيه ليكرا؟', 'صيفي لو شتوي؟',
                     'شلون الفحص؟', 'إذا ما عجبني يرجع؟', 'شوكت يوصلني؟',
                     'كيفية الدفع والتوصيل؟', 'عندكم صور الألوان؟', 'السعر شكد؟',
                     'الطول شكد؟', 'القياسات شنو؟']
        formats = ['07701234567', '٠٧٧٠١٢٣٤٥٦٧', '\u202d0770 123 4567\u202c', '0770-123-4567']
        for n in range(20):
            with self.subTest(journey=n):
                self.scope = Scope('khuyoot', 'chatwoot', 'account-1', f'synthetic-journey-{n}')
                self.select()
                for question in questions[n % 3:] + questions[:n % 3]:
                    self.run_turn(question)
                self.run_turn('بدلي الماروني ازرق', changes=[dict(line_id='one', values=dict(color='أزرق'))])
                self.run_turn('خلي قياس 40', changes=[dict(line_id='one', values=dict(size='40'))])
                phone, address = formats[n % 4], 'بغداد حي التجربة قرب المدرسة'
                if n % 3 == 0:
                    event, outcome = self.run_turn(address + '\n' + phone)
                else:
                    self.run_turn(phone if n % 2 else address)
                    self.run_turn('شلون الفحص؟')
                    event, outcome = self.run_turn(address if n % 2 else phone)
                self.assertEqual('booked', outcome['action'])
                row = self.db.execute('SELECT phone,order_items,total_amount FROM orders WHERE id=?',
                                      (outcome['order_id'],)).fetchone()
                item = json.loads(row[1])[0]
                self.assertEqual(('07701234567', 'أزرق', '40', 17000), (row[0], item['color'], item['size'], row[2]))
                self.runtime.turn(self.scope, event)
                self.assertEqual(n + 1, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])


class ScreenshotCollageTests(unittest.TestCase):
    @staticmethod
    def garment(seed=0):
        image = Image.new('RGB', (320, 320), (223, 206, 181))
        draw = ImageDraw.Draw(image)
        draw.polygon([(140, 30), (210, 40), (300, 290), (15, 270)], fill=(25 + seed, 30, 45))
        for x in range(70, 260, 27):
            draw.line([(172, 50), (x, 275)], fill=(62, 65 + seed, 78), width=3)
        draw.rectangle((132, 28, 214, 42), fill=(150, 82, 42))
        draw.ellipse((176, 29, 195, 45), outline=(240, 190, 55), width=3)
        return image

    def test_same_model_in_two_column_post_matches_without_assuming_one_color(self):
        with tempfile.TemporaryDirectory() as directory:
            references = []
            screen = Image.new('RGB', (640, 1280), 'white')
            for n, color in enumerate(('اسود', 'نيلي', 'رصاصي', 'جوزي')):
                photo = self.garment(n * 25)
                path = Path(directory) / f'{n}.png'
                photo.save(path)
                references.append(('P1', color, str(path)))
                screen.paste(photo, ((n % 2) * 320, 240 + (n // 2) * 320))
            data = io.BytesIO()
            screen.save(data, format='JPEG', quality=92)
            source = 'data:image/jpeg;base64,' + base64.b64encode(data.getvalue()).decode()
            self.assertEqual(('P1', ''), catalog_photo_match.match_inline_photo(source, references))
            self.assertIsNone(catalog_photo_match.match_inline_photo(source, [*references, ('P2', '', references[0][2])]))
