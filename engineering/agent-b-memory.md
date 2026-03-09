# Agent B Memory (Code & Review)

## Current objectives
- Implement Agent A requirements with tests and safe defaults.
- Keep runtime stable (monitor + dashboard + runner).
- Ensure secrets never enter git history.

## Quality checklist
- No `.env` / credentials / state artifacts tracked
- CLI options documented in README
- Monitor script supports strategy params
- Commit messages reflect intent and scope

## Notes (append-only)
- 2026-03-09: Implemented city cap + edge-decay auto-close + summary fields.
