# Distributed Order-Processing Simulation

This repository implements a small Python simulation of a distributed order-processing system. It demonstrates versioned events arriving late, out of order, and more than once, as well as stale workers being blocked by fencing tokens.

## Run it

Requires Python 3.9 or newer. No third-party packages are needed.

```powershell
python order_simulation.py
python -m unittest -v
```

## Implementation

- `OrderEvent` contains the required `order_id`, `version`, and `status` fields.
- `OrderProcessor` buffers future versions and applies only the next contiguous version. It ignores events at or below the applied version, so duplicates cannot change state.
- `FencedOrderStore` issues monotonically increasing fencing tokens. Only the worker with the latest token may write.
- `test_order_simulation.py` verifies ordering, missing-event behavior, duplicate handling, and stale-worker rejection.

## Evidence: terminal output

The following output was produced by running `python order_simulation.py` in this repository:

```text
=== Versioned event ordering ===
RECEIVED order=order-100 version=2 status=SHIPPED
PENDING out-of-order event: order=order-100 received=v2; waiting for v1
RECEIVED order=order-100 version=1 status=PAID
APPLIED v1:PAID, v2:SHIPPED: order=order-100 state=v2 SHIPPED
RECEIVED order=order-100 version=2 status=SHIPPED
IGNORED duplicate/stale event: order=order-100 version=2 (current=2)
FINAL order=order-100 version=2 status=SHIPPED applied_versions=[1, 2]
=== Missing event ===
PENDING out-of-order event: order=order-200 received=v3; waiting for v1
FINAL order=order-200 version=0 status=NEW pending_versions=[3]
=== Stale worker fencing ===
ACCEPTED worker=worker-B token=2: order=order-300 status=PAID
REJECTED stale worker=worker-A token=1; latest_token=2
FINAL order=order-300 status=PAID latest_token=2
```

This demonstrates the required delivery sequence `version 2`, `version 1`, `version 2 again`: version 2 is held, version 1 unlocks versions 1 and 2 in order, and the repeated version 2 is ignored. The missing-event test leaves version 3 pending while the order remains at version 0 / `NEW`. The fencing test accepts worker B's newer token and rejects worker A's older token, leaving the order at `PAID`.

The automated verification completed successfully:

```text
Ran 3 tests in ...
OK
```

## Reflection

Timestamps alone are not enough because distributed machines can have clocks that drift or are synchronized imperfectly. Network delay can cause an event with an earlier timestamp to arrive after one with a later timestamp. Timestamps also do not reliably represent causality when events are created concurrently. Version numbers or logical clocks, combined with deduplication and a policy for gaps, provide ordering information tied to the order's state transitions rather than to wall-clock time.

