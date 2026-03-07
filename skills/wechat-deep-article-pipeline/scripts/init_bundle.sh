#!/usr/bin/env bash
set -euo pipefail

slug="${1:-deep-article}"
date_str="$(date +%F)"
out_dir="${2:-output/${date_str}-${slug}}"

mkdir -p "$out_dir"

cat >"$out_dir/master.md" <<'EOF'
# [主标题]

> [一句话承诺]

## Why this matters now

## Signal 1

## Signal 2

## Signal 3

## Structural interpretation

## Practical framework

## 7-day action plan

## Risks and boundaries

## CTA

## Sources

1. [Title](URL)
2. [Title](URL)
3. [Title](URL)
EOF

cat >"$out_dir/wechat.md" <<'EOF'
# [公众号标题]

[副标题]

[正文]

[行动清单]

[CTA]
EOF

cat >"$out_dir/xiaohongshu.md" <<'EOF'
[开头钩子]

[3-5个核心点]

[行动建议]

[CTA]

#标签A #标签B #标签C
EOF

cat >"$out_dir/titles-hooks.md" <<'EOF'
## Titles (12)
1.
2.
3.
4.
5.
6.
7.
8.
9.
10.
11.
12.

## Hooks (6)
1.
2.
3.
4.
5.
6.
EOF

cat >"$out_dir/comment-ops.md" <<'EOF'
## First comment (3)
1.
2.
3.

## Reply templates (5)
1.
2.
3.
4.
5.
EOF

cat >"$out_dir/image-plan.md" <<'EOF'
## Cover
- Main text:
- Subtitle:
- Color direction:
- Visual motif:

## Body images (2-4)
1) Placement:
   Purpose:
   Prompt:
2) Placement:
   Purpose:
   Prompt:
EOF

cat >"$out_dir/sources.md" <<'EOF'
## Source log
1.
2.
3.
EOF

echo "Initialized article bundle at: $out_dir"
ls -1 "$out_dir"
