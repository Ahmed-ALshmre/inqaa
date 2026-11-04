"""Inbox closure acknowledges old diagnostics and returns narrow authoritative state."""
from unittest import TestCase
from account_app.tests import test_close_human_attention as fixtures


class InboxReviewTests(TestCase):
    setUp=fixtures.CloseHumanAttentionTests.setUp
    tearDown=fixtures.CloseHumanAttentionTests.tearDown
    def test_close_returns_badges_for_target_without_loading_the_full_inbox(self):
        result=self.client.post('/api/conversations/old/mark_reviewed',json={'resume_ai':False})
        self.assertEqual(result.status_code,200)
        self.assertEqual(result.json['sender_id'],'old')
        state=result.json['attention']
        self.assertEqual(state['pending_reviews_count'],0)
        self.assertEqual(state['problem_count'],0)
        self.assertEqual(state['system_issue_count'],0)
        self.assertTrue(self.db.execute("SELECT 1 FROM human_reviews WHERE sender_id='other' AND status='pending'").fetchone())

    def test_orphan_held_order_does_not_block_every_other_review(self):
        self.db.execute("INSERT INTO orders(sender_id,status) VALUES(NULL,?)",(self.m.COLOR_REVIEW_STATUS,))
        self.db.commit()
        response=self.client.post('/api/conversations/old/mark_reviewed')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['closed_reviews'],1)

    def test_acknowledged_repeated_prompt_disappears_but_new_issues_reappear(self):
        def repeat():
            self.db.execute("INSERT INTO messages(sender_id,direction,text,created_at) VALUES('old','outgoing','باقي نحدد لون القطعة',?)",(self.m.now_baghdad_iso(),))
        for _ in range(3):repeat()
        self.db.commit()
        self.assertEqual(self.client.get('/api/conversations?status=system_issues').json['conversations'][0]['system_issue_count'],1)
        response=self.client.post('/api/conversations/old/mark_reviewed',json={'resume_ai':False})
        self.assertEqual(response.json['attention']['system_issue_count'],0)
        self.assertEqual(self.client.get('/api/conversations?status=system_issues').json['conversations'],[])
        for _ in range(3):repeat()
        self.m.create_human_review(self.db,{'sender_id':'old'},'provider_http_500',notify_telegram=False)
        self.db.commit()
        state=self.client.get('/api/conversations?status=system_issues').json['conversations'][0]
        self.assertGreaterEqual(state['system_issue_count'],2)
        self.assertEqual(state['pending_reviews_count'],1)

    def test_protected_color_hold_and_pending_actions_remain_visible(self):
        order_id=self.db.execute("INSERT INTO orders(sender_id,status) VALUES('old',?)",(self.m.COLOR_REVIEW_STATUS,)).lastrowid
        self.db.commit()
        response=self.client.post('/api/conversations/old/mark_reviewed')
        self.assertEqual(response.status_code,409)
        self.assertEqual(response.json['held_order_ids'],[order_id])
        self.assertTrue(self.m.has_pending_human_review(self.db,'old'))

    @property
    def m(self):
        from account_app import app
        return app
