# Gibran UI demo script

A ~60-second walkthrough that hits the value props in order. Designed
for a screen recording you can clip up for Instagram / embed in a blog
post.

## Setup (one-time, off-camera)

```bash
# Project root
gibran init --sample
gibran sync
gibran check

# Frontend build
cd frontend && npm install && npm run build && cd ..

# Start the UI
gibran ui
```

The CLI prints the URL (e.g. `http://127.0.0.1:54321`) and opens the
browser. You're at the IdentitySetup modal.

## The 60-second script

### Scene 1 — "What is this?" (5s)

- **Identity setup modal**
- Type: `user: adam`, `role: analyst_west`, `attrs: region=west`
- Click Continue → land in **Workbench**.

### Scene 2 — "Ask a question in English" (15s)

- The Workbench shows the Examples panel underneath the text box —
  click **"top 5 region by gross revenue"**.
- The text appears in the input. Hit cmd+enter (or click Run).
- The **Compiled preview** pane updates instantly:
  - Pattern: `top_n_by_metric`
  - Matched text: highlighted
  - **The full SQL** including `WHERE "region" = 'west'`
- The **Result** pane shows: `west, 100.0`

> Narration: "I asked in English, gibran matched a pattern, compiled
> SQL — and notice that WHERE clause got AND-ed in. That's the
> governance row-filter."

### Scene 3 — "What governance is actually doing" (20s)

- Click **Policy Visualizer** in the sidebar.
- Left pane: `analyst_west`. Right pane: `admin (break-glass)`.
- Both show the orders table but the **attributes** row on the left
  shows `region=west`, on the right shows nothing.
- Scroll down on the left to show the column list with `public`
  badges. Scroll right to show the admin sees the same columns but
  without the row-filter scoping.

> Narration: "Here's what each role can actually see — column by
> column. Same source, two roles. The row-filter for analyst_west is
> hidden in the policy table, but you can prove it's there by going
> back to the Workbench…"

### Scene 4 — "Re-run as admin" (10s)

- Click the **change identity** button at the bottom-left.
- Set `user: admin_user`, `role: admin`, `attrs: (blank)`. Continue.
- Back in Workbench, click the same example: "top 5 region by gross
  revenue".
- Result now shows **3 rows**: `north 300, east 200, west 100`.
- The SQL preview no longer has the `WHERE region = 'west'` clause.

> Narration: "Same code, same question, different identity — and the
> emitted SQL changes. That's not a permissions layer bolted on after
> the query, it's woven into the compilation."

### Scene 5 — "Every query is logged" (10s)

- Click **Audit Log** in the sidebar.
- Both queries are there. Each row shows: timestamp, user, role,
  status, row count, duration.
- Click a row to expand. Shows the prompt + the actual SQL that ran.
- The admin row is **red-bordered** (break-glass marker).

> Narration: "Everything's audited. Break-glass access is marked
> visually. The audit log itself is queryable — admins see all,
> analysts see only their own."

### End

> "Single-source-of-truth governance over a natural-language interface
> — without an LLM in the emission path. It's gibran. Code's on
> GitHub, link in bio."

## Things to call out in the post

- **"No LLM in the SQL path"** — the NL is pattern-template matching.
  Means you get a hard correctness guarantee: gibran never invents
  metric or column names. If it can't match, it says so.
- **"Governance is structural, not bolted on"** — the row filter
  literally compiles into the SQL string. There's no way to query
  around it because there's no separate "filter layer" to bypass.
- **"Everything's free + MIT"** — `pip install gibran[ui]` and you're
  off. Postgres / Snowflake / BigQuery adapters exist too (free, not
  yet verified end-to-end without paid accounts).

## What to NOT show in the demo

These work but aren't visually compelling enough to warrant the
60-second budget:

- The **Catalog Browser** is nice but it's a tree view, not a wow
  moment. Skip unless you have a 90-second slot.
- The **Source Health** view is empty until you run `gibran check`,
  and the demo flow doesn't have a reason to surface a failing rule.
- The **API docs** at `/api/docs` are FastAPI's auto-generated Swagger
  UI — useful for devs, boring for video.

## Recording tips

- **Window size**: 1280×800 looks good on Instagram square (the
  sidebar uses 220px; the main pane gets ~1060px which fits a wide
  result table comfortably).
- **Cursor**: enable "show cursor" + click-highlight in your screen
  recorder. The cmd+enter shortcut is invisible otherwise.
- **Dark mode**: the UI respects `prefers-color-scheme`. Toggle your
  OS dark mode to match your video's vibe — both look polished.
- **Audio**: the script above runs ~60s of narration. Record clean,
  edit out filler. The compiled-SQL panel is where you'll want to
  pause to let viewers read.
