"""Legacy /manychat/send uses the same truthful delivery semantics as chat."""
import unittest
from unittest.mock import patch

import requests

from account_app.tests import test_delivery_acceptance_oct24 as acceptance_fixtures


class ManychatEndpointOct24Tests(unittest.TestCase):
    setUp = acceptance_fixtures.DeliveryAcceptanceOct24Tests.setUp
    tearDown = acceptance_fixtures.DeliveryAcceptanceOct24Tests.tearDown
    incoming = acceptance_fixtures.DeliveryAcceptanceOct24Tests.incoming
    saved_outgoing = acceptance_fixtures.DeliveryAcceptanceOct24Tests.saved_outgoing
    assert_question_pending = acceptance_fixtures.DeliveryAcceptanceOct24Tests.assert_question_pending
    response = staticmethod(acceptance_fixtures.DeliveryAcceptanceOct24Tests.response)
    sent_messages = staticmethod(acceptance_fixtures.DeliveryAcceptanceOct24Tests.sent_messages)

    def send(self, *, image=True):
        with patch.object(self.m, 'API_SECRET_KEY', 'synthetic-endpoint-key'):
            return self.client.post('/manychat/send', json={
                'subscriber_id': self.sender, 'text': 'الفستان سعره 16000 د.ع.',
                'image_url': 'https://images.test/legacy-endpoint.jpg' if image else ''},
                headers={'X-API-Key': 'synthetic-endpoint-key'})

    def test_text_rejection_stops_images_and_never_marks_the_question_answered(self):
        self.incoming('شكد سعره ودزي صورته؟')
        with patch.object(self.m.requests, 'post', return_value=self.response(400)) as post:
            response = self.send()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json['ok'])
        self.assertFalse(response.json['image_ok'])
        self.assertEqual(post.call_count, 1)
        self.assertEqual([message['type'] for message in self.sent_messages(post)], ['text'])
        self.assertEqual(len(self.saved_outgoing()), 1)
        self.assertEqual(self.saved_outgoing()[0]['metadata']['_delivery_status'], 'rejected')
        self.assertFalse(any(row['direction'] == 'outgoing' for row in self.m.load_history(self.db, self.sender)))
        self.assert_question_pending('شكد سعره ودزي صورته؟')

    def test_timeout_does_not_send_images_or_pretend_the_reply_was_accepted(self):
        self.incoming('شكد السعر؟')
        with patch.object(self.m.requests, 'post', side_effect=requests.ReadTimeout) as post:
            response = self.send()
        self.assertFalse(response.json['ok'])
        self.assertFalse(response.json['image_ok'])
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.saved_outgoing()[0]['metadata']['_delivery_status'], 'unconfirmed')
        self.assert_question_pending('شكد السعر؟')

    def test_image_failure_is_reported_as_partial_and_does_not_attach_an_unaccepted_image_to_text(self):
        self.incoming('دزيلي السعر والصورة')
        with patch.object(self.m.requests, 'post', side_effect=[self.response(), self.response(503)]) as post:
            response = self.send()
        self.assertFalse(response.json['ok'])
        self.assertFalse(response.json['image_ok'])
        self.assertEqual(post.call_count, 2)
        rows = self.saved_outgoing()
        self.assertEqual([row['message_type'] for row in rows], ['text', 'image'])
        self.assertFalse(rows[0]['image_url'])
        self.assertEqual([row['metadata']['_delivery_status'] for row in rows], ['accepted', 'unconfirmed'])
        self.assertTrue(all(row['metadata']['_partial_delivery_reply'] for row in rows))
        visible = self.m.load_history(self.db, self.sender)
        self.assertEqual([row['message_type'] for row in visible if row['direction'] == 'outgoing'], ['text'])
        self.assert_question_pending('دزيلي السعر والصورة')

    def test_success_saves_text_and_image_as_independently_accepted_rows(self):
        self.incoming('دزيلي السعر والصورة')
        with patch.object(self.m.requests, 'post', return_value=self.response()) as post:
            response = self.send()
        self.assertTrue(response.json['ok'])
        self.assertTrue(response.json['image_ok'])
        self.assertEqual(post.call_count, 2)
        rows = self.saved_outgoing()
        self.assertEqual([row['message_type'] for row in rows], ['text', 'image'])
        self.assertTrue(all(row['metadata']['_delivery_status'] == 'accepted' for row in rows))
        self.assertFalse(any(row['metadata']['_partial_delivery_reply'] for row in rows))

    def test_missing_key_is_a_known_rejection_and_no_provider_call_is_attempted(self):
        self.incoming('شكد السعر؟')
        with patch.object(self.m, 'manychat_api_key_for_page', return_value=''), \
             patch.object(self.m.requests, 'post') as post:
            response = self.send(image=False)
        self.assertFalse(response.json['ok'])
        post.assert_not_called()
        self.assertEqual(self.saved_outgoing()[0]['metadata']['_delivery_status'], 'rejected')
