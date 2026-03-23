# Polymarket Sure-Thing Scanner (Phase 1)

Implemented now:
- `scanner.py`: Gamma + CLOB scanner for high-probability YES opportunities
- Stale `/book` guard via `/price` cross-check
- JSON outputs under `state/`
- Lightweight dashboard at `state/dashboard.html`

## Run

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-surething-scanner
python3 scanner.py
```

## Open dashboard

```bash
cd /home/kai/.openclaw/workspace/ops/polymarket-surething-scanner/state
python3 -m http.server 8788
```

Then visit: http://localhost:8788/dashboard.html
