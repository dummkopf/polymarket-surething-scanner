## [ERR-20260307-001] systemd-inhibit-user-service

**Logged**: 2026-03-07T23:12:00+08:00
**Priority**: medium
**Status**: resolved
**Area**: infra

### Summary
Attempt to block sleep with user-level `systemd-inhibit` service failed with `Access denied`.

### Error
```
systemd-inhibit: Failed to inhibit: Access denied
```

### Context
- Tried to create `openclaw-keepawake.service` under user systemd.
- Environment appears to deny user-session inhibition (likely policy/host restrictions).
- Goal was preventing host sleep while OpenClaw gateway runs.

### Suggested Fix
Use self-heal watchdog timer instead of sleep inhibitor when inhibition is not permitted.

### Metadata
- Reproducible: yes
- Related Files: ~/.config/systemd/user/openclaw-watchdog.service
- See Also: LRN-20260307-002

### Resolution
- **Resolved**: 2026-03-07T23:11:00+08:00
- **Notes**: Disabled failed inhibitor service; deployed `openclaw-watchdog.timer` (5-minute health check + auto restart) as fallback resilience layer.

---
