# Phase 0 — v0.7 → v0.8

Scope agreed by all three reviewers: N1 · N3 · N2 · supervisory thread-failure chaos.

## N1 — one-shot runtime capability + single-use boot proof
`execution.py`
- `bind_runtime_capability` refuses rebinding unconditionally. A closed gate is no
  longer a rebinding window; it is exactly when a hostile component would try.
- `release_runtime_capability(token)` lets only the current holder hand the engine
  back, so sequential runtime ownership still works.
- `issue_boot_proof(token, unresolved, quarantined)` mints a private `_BootProof`
  object, only for a clean reconciliation, only for the current epoch.
- `grant_boot_authority(token, boot_proof)` now needs both. The proof is
  identity-checked (unforgeable by construction), single-use, and cleared on revoke.

## N3 — closed supervision ring, structural gate
`runtime.py` · `runtime_watchdog.py` · `protection_supervisor.py` · `lease_supervisor.py`
- `TradingCoreRuntime.ready` is a computed property: latched boot result AND every
  supervisory loop healthy AND witness not degraded. A passive reader sees the truth
  with no submit() needed.
- `RuntimeSafetyWatchdog._run` wrapped: the watchdog can no longer die silently, and
  it now also polices the lease supervisor.
- `ProtectionSupervisor` watches the watchdog (`peer_health`). Mutual supervision, so
  no single supervisory thread is a single point of failure.
- `LeaseSupervisor` exposes renewal progress and a `healthy` property that checks the
  real invariant (is authority still valid?) rather than the progress proxy alone.
- **`engine.gate_open`**: the gate consults a live health probe synchronously.
  Found by the new chaos harness against an earlier v0.8 patch: when every supervisory
  thread is dead, nobody is left to call `revoke_boot_authority`, so a latch-only gate
  stayed open while `ready` correctly read False. Fixed structurally, not with a
  fourth watcher — per the hardening-treadmill rule.

## N2 — witness height
`audit.py` · `audit_anchor.py`
- The published witness carries `height`. A witness recorded above the current local
  height means history was truncated: a truncated prefix is itself a valid chain, so
  head-hash comparison alone cannot see it.
- Enforced in `_publish_best_effort`, `sync_anchor`, and `verify(verify_anchor=True)`.
- A height-less witness against a non-empty local chain is treated as a downgrade
  attempt, not a legacy record.
- Limit unchanged and still documented: an attacker who owns both the log and the
  witness is outside what an unkeyed chain can detect. Production needs a signed or
  WORM witness in an independent trust domain.

## Item 4 — `scripts/supervisor_kill_chaos_1000.py`
Randomly kills or stalls one of the three supervisory loops (lease / protection /
watchdog), including a "kill all but one" mode. Asserts readiness degrades within a
bounded window **with no submit() and no manual verify_once()** — that manual call is
what hid N3 in the v0.7 suites. Also asserts the execution gate itself shuts.

## Results

| suite | result |
|---|---|
| `pytest tests/` | 79 passed (71 pre-existing + 8 self-attack) |
| `chaos_1000` | 1000 runs / 0 failures |
| `restart_chaos_1000` | 1000 / 0 |
| `multi_position_chaos_1000` | 1000 / 0 |
| `protection_chaos_1000_fast` | 1000 / 0 |
| `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.299s, mean 0.073s, budget 1.5s |
| v0.7 attack replay (12 attacks) | 0/12 succeed (was 3/12) |

## Evidence status

`tests/test_v08_self_attack.py` is written by the builder and carries **lower
evidentiary weight** per REVIEW_PROTOCOL.md §5. Independent regression tests for
N1/N2/N3 are owed by Gemini and ChatGPT and are not present in this package.

## Known open items carried into review

- `exchange.cancel_protection_by_client_id` still has no caller in the engine.
- `RateGovernorTimeout` is raised but never caught by name; a timed-out safety call
  is indistinguishable from an ordinary protection failure in the audit trail.
- `fail_emergency_exit` remains an unused fault injector: emergency-exit failure is
  still unexercised under chaos.
- `_last_cycle_started_monotonic` is written and never read.

## No-Secrets / No-Live-Authority audit (v0.8)

Rule adopted from v0.8 on: every review package must be fully runnable and completely
incapable of reaching real money.

Automated pre-delivery scan, run on this package:

```
grep -rniE "api[_-]?key|api[_-]?secret|secret|password|credential|binance\.com|wss://|https://" src/ scripts/ tests/ config/
  -> 0 matches
grep -rnE "^\s*(import|from)\s+(requests|urllib|http|socket|websocket|aiohttp|ccxt|binance)" src/ scripts/ tests/
  -> 0 matches
```

- Exchange implementations present: `SimulatedExchange`, `PersistentSimulatedExchange`.
- No live adapter exists in the tree at all. Nothing to disable — there is no path.
- `config/risk-policy.example.json` carries `"environment": "DEMO_ONLY"`.
- No network library is imported anywhere in src/, scripts/ or tests/.

Any future patch that adds a network call, an endpoint, a credential read, or a new
dependency must be flagged here explicitly and is a blocking review item.

---

# v0.8.1 — response to ChatGPT Fast Gate NO-GO

## CG-1 — shared SQLite connection across threads — **CONFIRMED, FIXED**

ChatGPT's root-cause hypothesis was correct. Reproduced independently at the
persistence layer, with no runtime involved (6 writer threads + 2 reader threads on
one `PersistentSimulatedExchange`):

```
781 x OperationalError: cannot start a transaction within a transaction
348 x READER InterfaceError: bad parameter or other API misuse
  3 x InterfaceError: bad parameter or other API misuse
```

`sqlite3.InterfaceError: bad parameter or other API misuse` raised from
`persistent_exchange.py:201 protection_details_by_client_id` — exactly the error
ChatGPT observed, surfacing as `PROTECTION_REVERIFY_QUERY_FAILED:InterfaceError`.

**Honest scope note:** the *runtime-level* symptom is genuinely flaky. 100 iterations
of 8 concurrent submitters with a 1ms supervisor interval did not reproduce it here.
The persistence-layer test reproduces it deterministically and is the reliable
detector. ChatGPT saw the runtime-level manifestation; I saw the cause.

### Fix — structural, per REVIEW_PROTOCOL §11

Option 1 of ChatGPT's list: one connection per thread. Chosen over a global exchange
lock because it removes the shared mutable resource rather than guarding it, and it
preserves the rule that no long-lived transaction is held across external I/O — each
thread's transaction is its own.

- **new** `src/shata_trader/db.py` — `ThreadLocalSqlite` / `SharedMemorySqlite`
- `persistent_exchange.py` — `conn` is now a per-thread property; visibility-lag
  counter guarded
- `events.py` — `OrderEventStore` had the same unguarded shared connection and is
  reachable from any `ingest_exchange_event` caller; now per-thread, with the
  multi-statement ingest transaction under a store lock
- `exchange.py` — in-memory `SimulatedExchange` had no lock at all; read-modify-write
  on balances and dicts is now atomic

### Regression

`tests/test_v081_concurrency_regression.py` — 3 tests:
100 iterations of 8 concurrent submitters with a live 1ms-interval supervisor;
a direct cross-thread attack on the exchange persistence layer; a ResourceWarning check.

Verified to **fail on pre-patch code** (`InterfaceError: 3, OperationalError: 909`)
and pass after. BUILDER-WRITTEN — lower evidentiary weight; the independent
regression for CG-1 is still owed.

## B-4 — losing readiness mid-trade discarded the trade result — **HIGH, FIXED**

Found by the full matrix after the CG-1 fix (`chaos_1000` run 965).

`submit()` raised `RuntimeNotReady('Runtime safety authority was lost during trade')`
*after* `engine.process` had already completed. The side effect had happened, but the
exception discarded `sm` — hiding a real, protected position from the only caller able
to act on it. Now `submit()` always returns `sm` once a side effect has occurred, and
degrades readiness / revokes the gate separately.

## B-5 — cold boot could raise instead of failing closed — **MEDIUM, FIXED**

`chaos_1000` run 770: `BootGateClosed: Cannot issue a boot proof without a valid lease`
escaped from `start()` when the lease lapsed between acquire and the end of
reconciliation. `start()` now returns an unresolved `BootReport` tagged
`AUTHORITY_LOST_DURING_BOOT`, matching the existing `WAITING_FOR_LEASE` behaviour.
Boot must never raise at the caller.

## Re-acceptance evidence

| gate item | result |
|---|---|
| 1. full suite | **82 passed** (79 + 3 new concurrency regressions) |
| 2. `chaos_1000` | 1000 / 0 |
| 3. `restart_chaos_1000` | 1000 / 0 |
| 4. `multi_position_chaos_1000` | 1000 / 0 |
| 5. `protection_chaos_1000_fast` | 1000 / 0 |
| 6. `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.306s, budget 1.5s |
| 7. new concurrency regression | 3/3, repeated clean passes |
| 8. ResourceWarning check | `-W error::ResourceWarning` → 0 warnings |
| v0.7 attack replay | 0/12 succeed |
| no-secrets scan | 1 match, a code comment in `audit_anchor.py` ("secret key"); 0 network imports |

---

# v0.8.2 — response to ChatGPT CG-2

## CG-2 — supervisor completion-age conflated with stall — **CONFIRMED, FIXED**

ChatGPT's analysis was exactly right. Reproduced 3/3 on this tree with the packaged
reproducer, before any change:

```
FIRST_NOT_READY_SECONDS: 0.502     RUNTIME_READY: False   ENGINE_GATE_OPEN: False
PROTECTION_SUPERVISOR_ALIVE: True  PROTECTION_SUPERVISOR_LAST_ERROR: None
WATCHDOG_LAST_ERROR: PROTECTION_SUPERVISOR_STALLED:0.555772s
DURABLE_STATES: PROTECTED x 8      ACTIVE_PROTECTIONS: 8   LEDGER_ERRORS: []
```

Eight physically protected positions, a live supervisor making progress, declared stalled.

### Fix — items A–D as specified

**A/B. Progress liveness separated from cycle completion.**
`ProtectionSupervisor._last_progress_monotonic` now advances after **every record**, not
per cycle. `progress_age_seconds()` is the liveness signal, and the watchdog uses it.
Its bound is one query, so it is independent of N.

**C. Freshness enforced independently, with two bounds instead of one.**
- `max_verification_age_seconds` — SOFT per-position target. Exceeding it raises
  `PROTECTION_FRESHNESS_DEGRADED` in the audit trail, carrying position count, cycle
  duration and both bounds. It does **not** close the gate and does **not** claim a stall.
- `freshness_ceiling_seconds` (default 10x soft) — HARD bound. Beyond it the system
  genuinely cannot police what it holds, and the gate closes with
  `PROTECTION_FRESHNESS_CEILING_EXCEEDED`.

The deadline was **not** loosened to hide the failure. One number carrying two different
meanings was split into two numbers with one meaning each. Query *uncertainty*
(`result is None`) still expires at the soft target — that is the v0.6 H-2 contract and
it is unchanged.

Cycles now run **oldest-verified first**, so freshness degrades uniformly instead of
starving one record.

**D. Post-trade validation is O(1).**
`submit()` calls the new `verify_one(intent_id)` instead of a full portfolio
`verify_once()`. Background stays O(N); per-submit cost no longer grows with position
count.

### Verification

```
CG-2 reproducer, post-fix:   NOT REPRODUCED, 3/3
                             RUNTIME_READY True, WATCHDOG_LAST_ERROR None,
                             ACTIVE_PROTECTIONS 8, freshness_degraded reported
N3 frozen-query attack:      still closes ready + gate, watchdog still reports STALLED
supervisor_kill_chaos_1000:  1000 / 0, detection max 0.298s vs 1.5s budget
```

## B-6 — supervisory threads outlived their runtime — **HIGH, FIXED**

Surfaced by the full matrix while fixing CG-2: `chaos_1000` failed 1–2 runs in 1000,
but only when the machine was already loaded. Root cause measured, not guessed:

```
threads at start: 1
after 25 runtimes (no stop()):  76      -> 3 threads leaked per runtime
after 50:                       151
after 100:                      226
```

`chaos_1000` never calls `rt.stop()`. By run ~900 roughly 2,700 supervisory threads are
live, the scheduler starves, and a 3-second lease lapses mid-trade — surfacing as
`UNCAUGHT:StaleEpoch`. v0.8 made this 50% worse by adding a third supervisory thread.

Two defects, both fixed in the product rather than in the harness:

1. **Reference cycle.** `bind_health_probe(lambda: self.ready)` captured the runtime
   strongly; the engine held the probe, the supervisors held the engine, and the live
   threads held the supervisors. A dropped runtime could never be collected. The probe
   now holds a weak reference and reads unhealthy once the runtime is gone.
2. **No lifetime tie.** `weakref.finalize` now stops all three supervisors when the
   runtime is collected, so a caller who forgets `stop()` does not leak threads.

```
after 25 runtimes (still no stop()):  4
after 50:                             7
after 100:                            10
```

`chaos_1000` now passes 1000/0 twice consecutively **without any change to the harness**.

## B-7 — StaleEpoch could escape `process()` — **MEDIUM, FIXED**

Authority can lapse anywhere in `process()`, not only around the dispatch window that
already handled it. `process()` now wraps `_process()`, revokes the gate, audits
`EXECUTION_AUTHORITY_LOST`, and raises a typed `BootGateClosed` instead of letting a raw
`StaleEpoch` traceback escape the public entry point.

## Re-acceptance evidence

| gate item | result |
|---|---|
| full suite | **90 passed** (71 pre-existing + 8 self-attack + 3 concurrency + 8 CG-2/B-6) |
| `chaos_1000` | 1000 / 0 (twice consecutively) |
| `restart_chaos_1000` | 1000 / 0 |
| `multi_position_chaos_1000` | 1000 / 0 |
| `protection_chaos_1000_fast` | 1000 / 0 |
| `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.298s, budget 1.5s |
| CG-2 reproducer | NOT REPRODUCED 3/3 |
| v0.7 attack replay | 0/12 succeed |
| raw SQLite cross-thread attack | clean |
| ResourceWarning (`-W error`) | 0 |
| no-secrets / no-live-authority | 0 matches, 0 network imports |

## Open question handed to the reviewers

While shaping the B-5 regression I hit something I did **not** resolve and am not
claiming is safe:

- `start()` constructs a **fresh** `ColdBootCoordinator` (`runtime.py:143`), so a
  reference to `rt.boot` taken before `start()` is silently discarded. Harmless for
  correctness here, but it makes `rt.boot` misleading to any caller or test that holds it.
- More important: **is there any path where a running engine loses and silently
  re-acquires authority without a cold boot?** `acquire_authority()` resets state and
  re-acquires; if it can run mid-life while the gate is open, a leader could reassert
  authority without reconciling. I could not construct that path, but I could not rule
  it out either. Worth an attack.

All builder tests in `tests/test_v082_cg2_and_thread_lifetime.py` are BUILDER-WRITTEN and
carry lower evidentiary weight. Independent regressions for CG-2, B-6 and B-7 are owed.

---

# v0.8.3 — response to ChatGPT CG-2/D follow-up

ChatGPT read the v0.8.2 patch and found item D incomplete. Both claims verified before
changing anything.

## D-2 — `verify_one()` was serialised behind the full background cycle — **CONFIRMED, FIXED**

`verify_one()` took `_cycle_lock`, the same lock `verify_once()` holds for an entire
O(N) portfolio cycle. So post-trade work was O(1) but post-trade **wall clock** stayed
tied to portfolio size — exactly the coupling item D was meant to remove.

Measured, 10–15 positions at 0.05s/query:

```
before:  submit  max=1.008s  mean=0.757s     (high variance = waiting on the lock)
after:   submit  max=0.515s  mean=0.513s     (variance collapsed; this is the trade itself)

verify_one() with a 0.60s background cycle in flight:
before:  blocked for the remainder of the cycle
after:   max=0.051s  mean=0.051s   == exactly one query
```

`verify_one()` no longer takes `_cycle_lock`. Verifying one record concurrently with the
background cycle is safe: the ledger has its own locking and a duplicate verification of
the same record is idempotent.

## D-3 — `healthy` scanned the ledger on every gated call — **CONFIRMED, FIXED**

`healthy` called `oldest_verification_age_seconds()` → `ledger.protected_records()`, an
O(N) read. `healthy` is reached from the engine health probe on **every** gated call, so
every `process()` paid an O(N) ledger scan and contended with the background cycle.

The background cycle now records the true oldest age; the hot path uses
`cached_oldest_verification_age_seconds()`, which extrapolates that measurement forward
by elapsed time. Extrapolating forward can only **over**-estimate age, so the error is
always fail-safe. The watchdog ceiling check uses the cached value too.

```
before:  50 readiness checks -> 50 ledger scans
after:   50 readiness checks ->  0 ledger scans
         200 x rt.ready: 0.015s -> 0.001s
```

## Regressions added

Three, in `tests/test_v082_cg2_and_thread_lifetime.py`, all verified to **fail on
pre-patch v0.8.2**:

- `test_post_trade_check_is_not_serialised_behind_the_background_cycle`
- `test_readiness_check_does_not_scan_the_ledger`
- `test_stall_inside_ledger_read_still_closes_gate` — ChatGPT's own follow-up point:
  since `healthy` no longer reads the ledger, prove a freeze **inside**
  `protected_records()` is still caught. It is, by progress liveness.

## Re-acceptance evidence

| gate item | result |
|---|---|
| full suite | **93 passed** |
| `chaos_1000` | 1000 / 0 |
| `restart_chaos_1000` | 1000 / 0 |
| `multi_position_chaos_1000` | 1000 / 0 |
| `protection_chaos_1000_fast` | 1000 / 0 |
| `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.299s, budget 1.5s |
| CG-2 reproducer | NOT REPRODUCED 3/3 |
| v0.7 attack replay | 0/12 succeed |
| no-secrets / no-live-authority | clean |

BUILDER-WRITTEN tests, lower evidentiary weight. Independent regressions still owed.

---

# v0.8.4 — response to ChatGPT CG-4

## CG-4 — foreground traffic masked a frozen background supervisor — **CONFIRMED, FIXED**

**This defect was introduced by my own D-2 patch.** ChatGPT caught it on review of
v0.8.3 before it ever reached a chaos run.

When `verify_one()` stopped taking `_cycle_lock`, it kept updating
`_last_progress_monotonic` — the very counter the watchdog reads as the supervisor
liveness signal. So a steady stream of `submit()` calls kept the signal fresh while the
background thread was frozen inside a single call. Reproduced with ChatGPT's script,
run verbatim:

```
supervisor thread frozen for >1.2s inside protected_records()
  rt.ready            = True
  engine.gate_open    = True
  watchdog last_error = None
  progress_age        = 0.050s   (bound 0.3s)   <- refreshed by the FOREGROUND path
```

The v0.8.3 test `test_stall_inside_ledger_read_still_closes_gate` passed only because it
did no foreground work during the freeze. ChatGPT's exact words: *"the current test does
not detect this because it waits without running foreground checks."*

### Fix — items 1–5 as specified

1–3. **`_background_progress_monotonic`, advanced only by `verify_once()`** — at cycle
start and after each record. `progress_age_seconds()` reads it alone and is deliberately
blind to the foreground path. **Liveness of a supervisor can only be evidenced by that
supervisor doing work.**

5. **Striped per-record locks.** Removing `_cycle_lock` in D-2 also allowed the
background cycle and the foreground path to verify the *same* record simultaneously,
where one side may write `UNKNOWN`. 64 striped `RLock`s serialise per-record
verification while keeping foreground cost bounded by one query, not by portfolio size
— O(1) in memory, no cleanup, no reintroduction of the D-2 coupling.

### Verification

```
CG-4 reproducer, post-fix:
  rt.ready            = False
  engine.gate_open    = False
  watchdog last_error = PROTECTION_SUPERVISOR_STALLED:0.356357s
  progress_age        = 1.219s   (bound 0.3s)

D-2 still holds:  verify_one() with a 0.60s cycle in flight = 0.052s == one query
CG-2 still holds: NOT REPRODUCED
```

### Regressions added (4, 6)

Both verified to **fail on v0.8.3**:

- `test_foreground_traffic_cannot_mask_a_frozen_supervisor` — ChatGPT's script verbatim
- `test_same_record_is_never_verified_concurrently` — 4 hammering threads against the
  background cycle, asserting zero overlapping verifications of one record and that the
  durable state is not knocked to `UNKNOWN`

## Re-acceptance evidence

| gate item | result |
|---|---|
| full suite | **95 passed** |
| `chaos_1000` | 1000 / 0 |
| `restart_chaos_1000` | 1000 / 0 |
| `multi_position_chaos_1000` | 1000 / 0 |
| `protection_chaos_1000_fast` | 1000 / 0 |
| `supervisor_kill_chaos_1000` | 1000 / 0 · detection max 0.298s, budget 1.5s |
| CG-2 reproducer | NOT REPRODUCED |
| CG-4 reproducer | NOT REPRODUCED |
| D-2 latency | 0.052s == one query |
| v0.7 attack replay | 0/12 succeed |
| no-secrets / no-live-authority | clean |

## Note on the pattern

Three of the last four findings (B-6, CG-4, and the earlier gate-open latch) were
**introduced by the fix for the previous finding**. This is the hardening treadmill named
in REVIEW_PROTOCOL §11, and it is the strongest argument for the two rules that caught
them: the builder does not write the proof for his own patch, and every patch that adds
machinery must answer "what watches this?"

CG-4 in particular was caught by review, not by 5,000 chaos runs — because the harnesses
did no foreground work during the injected freeze. Chaos coverage is not review.
