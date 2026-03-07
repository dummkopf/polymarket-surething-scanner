---
name: wechat-deep-article-pipeline
description: "End-to-end workflow for weekly deep analysis content: research, synthesis, writing, layout, image planning, and multi-platform packaging from one Markdown master draft. Use when the user asks for a deep article, weekly longform analysis, WeChat/Xiaohongshu one-source multi-publish, or wants 90% automation with final human review before publish."
---

# wechat-deep-article-pipeline

Produce high-quality, research-backed weekly deep articles with a consistent voice and reusable output pack.

## Non-negotiable quality bar

Always enforce these rules:

1. Use **at least 3 credible sources**; cite links in a source list.
2. Write **2–4 lines per paragraph**; avoid long unbroken blocks.
3. Use **conclusion first, then explanation** at paragraph level.
4. Include an **action checklist** (what to do next).
5. Include **spread hooks + CTA** (save/comment/follow/DM/lead action).
6. Run **fact check + tone check** before handoff.
7. Output in **Markdown master draft** first, then repurpose.
8. Provide image plan: **1 cover + 2–4 body images** (prefer chart/flow diagrams).

For detailed checks, read `references/quality-gates.md`.
For writing tone and rhythm, read `references/style-guide.md`.
For exact deliverable format, read `references/output-templates.md`.

## Default operating mode (90/10)

Operate in semi-automation mode by default:

- Assistant handles 90%: research, synthesis, drafting, formatting, image planning, packaging.
- User keeps 10%: final review and publish click.

Never skip final human review for live posting.

## Workflow

Use this sequence every time.

### Step 1) Lock objective and angle

Define:

- Audience segment (who this is for)
- Primary goal (growth / conversion / authority / lead gen)
- Weekly angle (one core thesis, one key tension)

If unclear, propose one default direction and proceed.

### Step 2) Collect and filter sources

Collect from high-signal sources first (industry reports, primary posts, official docs, reputable analysis).

Minimum source quality requirement:

- 3+ credible sources that directly support claims
- At least one source with concrete data / measurements
- No single-source narrative

### Step 3) Build argument map

Before drafting, create a concise argument map:

- Core thesis
- 3–5 supporting claims
- Evidence link for each claim
- Counterpoint or boundary condition

### Step 4) Draft Markdown master article

Write the full deep article in Markdown with:

- Strong title and 1-sentence lead promise
- Structured sections with clear subheadings
- Action checklist section
- Risk / caveat section
- CTA section

Apply the style constraints from `references/style-guide.md`.

### Step 5) Package multi-platform outputs

From one master draft, generate:

1. WeChat longform version (layout-ready)
2. Xiaohongshu compressed version (600–900 chars)
3. Title pack (12 options) + hook pack (6 options)
4. First comment + reply templates
5. Hashtag / topic set

### Step 6) Prepare layout and image plan

Create:

- Cover concept (title + subtitle + brand color direction)
- 2–4 body image prompts (chart/flow priority)
- Suggested image insertion points in the article

### Step 7) Pre-publish QA

Run two checks before handoff:

- Fact check: claim ↔ evidence consistency, link validity
- Tone check: consistency, clarity, non-hype, no overclaim

Use checklist in `references/quality-gates.md`.

### Step 8) Handoff package

Return a complete package with paths and clear publish instructions.

If output files are needed in workspace, initialize with:

```bash
bash scripts/init_bundle.sh "topic-slug"
```

## Required deliverables

Always provide all of the following (unless user explicitly opts out):

- Markdown master draft
- WeChat-ready version
- Xiaohongshu compressed version
- 12 titles + 6 opening hooks
- First comment + 5 reply templates
- Source list (with URLs)
- Image plan (cover + 2–4 body images)
- A/B angle suggestion for next iteration

## Iteration rule (run-to-rule)

After each published piece, capture what worked and update stable rules:

- Style decisions (voice, length, rhythm)
- Packaging decisions (format, CTA shape)
- Distribution decisions (title/hook patterns)

After 5 runs, promote repeated wins into default behavior for future drafts.
