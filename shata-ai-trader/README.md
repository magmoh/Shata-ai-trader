# SHATA AI TRADER — Phase 0 v0.8.4

Simulation only. No exchange credentials, no network imports, no live trading path.

```bash
python3 -m unittest discover -s tests    # expect: Ran 95 tests ... OK
python3 scripts/supervisor_kill_chaos_1000.py
python3 scripts/chaos_1000.py
```

See `docs/PHASE0_v0.8.4_CHANGES.md` for the full finding history and
`docs/REVIEW_PROTOCOL_v1.1.md` for the multi-model review rules.
