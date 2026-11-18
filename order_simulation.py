"""Distributed order-processing simulation.

The simulation models two common safeguards:
1. Versioned events are buffered until the next contiguous version arrives.
2. Worker writes require the latest fencing token.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set


@dataclass(frozen=True)
class OrderEvent:
    order_id: str
    version: int
    status: str


@dataclass
class OrderState:
    order_id: str
    version: int = 0
    status: str = "NEW"
    pending_versions: Set[int] = field(default_factory=set)
    applied_versions: List[int] = field(default_factory=list)


class OrderProcessor:
    """Apply each order's events exactly once and in version order."""

    def __init__(self) -> None:
        self.orders: Dict[str, OrderState] = {}
        self._pending: Dict[str, Dict[int, OrderEvent]] = {}

    def receive(self, event: OrderEvent) -> str:
        state = self.orders.setdefault(event.order_id, OrderState(event.order_id))
        pending = self._pending.setdefault(event.order_id, {})

        if event.version <= state.version or event.version in pending:
            return (
                f"IGNORED duplicate/stale event: order={event.order_id} "
                f"version={event.version} (current={state.version})"
            )

        pending[event.version] = event
        self._refresh_pending(state, pending)

        applied = []
        while state.version + 1 in pending:
            next_event = pending.pop(state.version + 1)
            state.version = next_event.version
            state.status = next_event.status
            state.applied_versions.append(next_event.version)
            applied.append(f"v{next_event.version}:{next_event.status}")

        self._refresh_pending(state, pending)
        if applied:
            return (
                f"APPLIED {', '.join(applied)}: order={event.order_id} "
                f"state=v{state.version} {state.status}"
            )
        missing = state.version + 1
        return (
            f"PENDING out-of-order event: order={event.order_id} "
            f"received=v{event.version}; waiting for v{missing}"
        )

    @staticmethod
    def _refresh_pending(state: OrderState, pending: Dict[int, OrderEvent]) -> None:
        state.pending_versions = set(pending)

    def snapshot(self, order_id: str) -> OrderState:
        return self.orders[order_id]


@dataclass
class Worker:
    name: str
    fencing_token: int


class FencedOrderStore:
    """Reject writes from workers whose lease/token is no longer current."""

    def __init__(self) -> None:
        self.latest_token = 0
        self.status_by_order: Dict[str, str] = {}

    def register_worker(self, name: str) -> Worker:
        self.latest_token += 1
        return Worker(name, self.latest_token)

    def update(self, worker: Worker, order_id: str, status: str) -> str:
        if worker.fencing_token != self.latest_token:
            return (
                f"REJECTED stale worker={worker.name} token={worker.fencing_token}; "
                f"latest_token={self.latest_token}"
            )
        self.status_by_order[order_id] = status
        return (
            f"ACCEPTED worker={worker.name} token={worker.fencing_token}: "
            f"order={order_id} status={status}"
        )


def run_demo() -> List[str]:
    lines = ["=== Versioned event ordering ==="]
    processor = OrderProcessor()
    order_id = "order-100"
    events = [
        OrderEvent(order_id, 2, "SHIPPED"),
        OrderEvent(order_id, 1, "PAID"),
        OrderEvent(order_id, 2, "SHIPPED"),
    ]
    for event in events:
        lines.append(
            f"RECEIVED order={event.order_id} version={event.version} "
            f"status={event.status}"
        )
        lines.append(processor.receive(event))
    state = processor.snapshot(order_id)
    lines.append(
        f"FINAL order={order_id} version={state.version} status={state.status} "
        f"applied_versions={state.applied_versions}"
    )

    lines.append("=== Missing event ===")
    missing = OrderProcessor()
    missing_event = OrderEvent("order-200", 3, "SHIPPED")
    lines.append(missing.receive(missing_event))
    missing_state = missing.snapshot("order-200")
    lines.append(
        f"FINAL order=order-200 version={missing_state.version} "
        f"status={missing_state.status} pending_versions="
        f"{sorted(missing_state.pending_versions)}"
    )

    lines.append("=== Stale worker fencing ===")
    store = FencedOrderStore()
    worker_a = store.register_worker("worker-A")
    worker_b = store.register_worker("worker-B")
    lines.append(store.update(worker_b, "order-300", "PAID"))
    lines.append(store.update(worker_a, "order-300", "SHIPPED"))
    lines.append(
        f"FINAL order=order-300 status={store.status_by_order['order-300']} "
        f"latest_token={store.latest_token}"
    )
    return lines


if __name__ == "__main__":
    print("\n".join(run_demo()))
