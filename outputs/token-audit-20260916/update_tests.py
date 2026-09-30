from pathlib import Path
p=Path('account_app/tests/test_checkout_regressions.py');s=p.read_text(encoding='utf-8')
s=s.replace('test_new_image_ignores_old_ad_and_uses_product_image_fallback','test_new_image_ignores_old_ad_without_second_visual_call')
s=s.replace('        vision.assert_called_once()\n        self.assertEqual(product["product_id"], "F2")\n        self.assertEqual(method, "image_recognition")','        vision.assert_not_called()\n        self.assertIsNone(product)\n        self.assertIsNone(method)',1)
s=s.replace('            self.assertEqual(ai.call_args.args[5]["product_id"], selected["product_id"])','''            if fallback:
                ai.assert_not_called()
                vision.assert_not_called()
                review.assert_called_once()
                return
            self.assertEqual(ai.call_args.args[5]["product_id"], selected["product_id"])''',1)
s=s.replace('test_photo_same_model_fallback_continues','test_photo_same_model_failure_requires_review').replace('test_photo_different_model_fallback_replaces_old_binding','test_photo_different_model_failure_requires_review')
s=s.replace('test_image_second_attempt_success_continues_without_review','test_image_failure_does_not_attempt_second_recognition').replace('test_image_two_failures_then_silent_handoff','test_image_one_failure_then_silent_handoff')
s=s.replace('self.assertEqual(match.call_count, 2)','self.assertEqual(match.call_count, 1)')
s=s.replace('        self.assertEqual(product["product_id"], "F2")\n        self.assertEqual(len(result["images"][0]["attempts"]), 2)','        self.assertIsNone(product)\n        self.assertEqual(len(result["images"][0]["attempts"]), 1)')
p.write_text(s,encoding='utf-8')
p=Path('account_app/tests/test_conversation_context.py');s=p.read_text(encoding='utf-8').replace('self.assertEqual(match.call_count,2)','self.assertEqual(match.call_count,1)').replace('self.assertEqual(match.call_count,3)','self.assertEqual(match.call_count,2)')
s=s.replace('self.assertEqual(match.call_args.args[0], "https://images.test/async.jpg")','self.assertTrue(match.call_args.args[0].startswith("data:image/png;base64,"))')
p.write_text(s,encoding='utf-8')
p=Path('account_app/tests/test_messaging_media.py');s=p.read_text(encoding='utf-8').replace('test_catalog_retries_stop_on_success_and_never_use_product_photos','test_catalog_attempts_once_and_never_uses_fallback_photos').replace('([miss, hit], 2), ([miss, miss, miss], 3)','([miss, hit], 1), ([miss, miss, miss], 1)');p.write_text(s,encoding='utf-8')
