import unittest

from order_simulation import (
    FencedOrderStore,
    OrderEvent,
    OrderProcessor,
)


class OrderProcessorTests(unittest.TestCase):
    def test_out_of_order_then_missing_version_and_duplicate(self):
        processor = OrderProcessor()
        self.assertIn("PENDING", processor.receive(OrderEvent("o", 2, "SHIPPED")))
        self.assertEqual(processor.snapshot("o").version, 0)
        self.assertIn("APPLIED v1:PAID, v2:SHIPPED", processor.receive(OrderEvent("o", 1, "PAID")))
        self.assertIn("IGNORED", processor.receive(OrderEvent("o", 2, "SHIPPED")))
        state = processor.snapshot("o")
        self.assertEqual(state.applied_versions, [1, 2])
        self.assertEqual(state.status, "SHIPPED")

    def test_missing_event_stays_pending(self):
        processor = OrderProcessor()
        processor.receive(OrderEvent("o", 3, "SHIPPED"))
        state = processor.snapshot("o")
        self.assertEqual((state.version, state.status), (0, "NEW"))
        self.assertEqual(state.pending_versions, {3})


class FencingTests(unittest.TestCase):
    def test_newer_worker_wins_and_older_worker_is_rejected(self):
        store = FencedOrderStore()
        old_worker = store.register_worker("old")
        new_worker = store.register_worker("new")
        self.assertIn("ACCEPTED", store.update(new_worker, "o", "PAID"))
        self.assertIn("REJECTED", store.update(old_worker, "o", "SHIPPED"))
        self.assertEqual(store.status_by_order["o"], "PAID")


if __name__ == "__main__":
    unittest.main()
