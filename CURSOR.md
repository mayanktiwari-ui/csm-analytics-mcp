# Cursor Agent Instructions — CSM Analytics Dashboard

> Paste this into Cursor chat to get the dashboard running locally:
> **"Read CURSOR.md and set up the CSM analytics dashboard for me"**

---

## What to build

A local Node.js analytics dashboard for the Customer Success team at HighLevel. It reads live data from Snowflake via an internal MCP proxy and serves a full dashboard at `http://localhost:3000`. No npm packages, no bundler — just `node server.js`.

---

## Step 1 — Get the MCP API key

Ask Mayank for the `MCP_KEY`. It looks like `sk-mcp-orch-...`.

---

## Step 2 — Set it as an environment variable

```bash
export MCP_KEY="sk-mcp-orch-your-key-here"
```

Or create a `.env` file (already gitignored):
```
MCP_KEY=sk-mcp-orch-your-key-here
```

If using `.env`, run the server with:
```bash
node -e "require('fs').readFileSync('.env','utf8').split('\n').forEach(l=>{const[k,...v]=l.split('=');if(k)process.env[k.trim()]=v.join('=').trim()});require('./server.js')"
```

---

## Step 3 — Start the server

```bash
node server.js
```

Open `http://localhost:3000`. **First load takes 3–4 minutes** — it paginates through ~27,000 CSM tickets from Snowflake. You'll see a loading spinner. Once loaded, data is cached for 10 minutes.

---

## Step 4 — Verify it's working

Open the browser console or check terminal output. You should see:
```
Server running at http://localhost:3000
Fetching all pages from MCP...
  Page offset=0: got=300
  Page offset=300: got=300
  ...
After dedup: 27XXX unique tickets
CSAT rows fetched: ...
```

---

## Snowflake views this dashboard depends on

All views live in `HIGHLEVEL_ANALYSIS_DB.HIGHLEVEL_ANALYSIS` and are accessible via the MCP proxy:

| View | Used for |
|---|---|
| `RPT_CUSTOMER_SUCCESS_METRICS_VW` | All ticket/call data — main dataset |
| `FACT_CSAT_RESPONSE_VW` | Booked CSAT survey scores |
| `RPT_CUSTOMER_360_VW` | Customer counts per CSM |

---

## How the MCP proxy works

`server.js` makes JSON-RPC calls to the MCP endpoint:

```js
POST https://snowflake-mcp-583040996742.us-central1.run.app/mcp
Headers: { "x-api-key": MCP_KEY, "Accept": "application/json" }
Body: { "jsonrpc": "2.0", "method": "tools/call", "params": { "name": "run_query", "arguments": { "sql": "..." } } }
```

The MCP proxy runs the SQL against Snowflake and returns rows as a JSON array. **Email columns are redacted** by the proxy (`**REDACTED**`) — use name-based joins instead.

---

## Architecture

```
Browser (localhost:3000)
    │
    ▼
server.js  (Node.js HTTP server, no frameworks)
    │
    ├── GET /         → serves inline HTML/CSS/JS dashboard
    ├── GET /api/data → fetches + caches all ticket rows, CSAT, C360
    └── POST /mcp     → proxies raw MCP calls (for debugging)
         │
         ▼
    Snowflake MCP proxy (GCP Cloud Run)
         │
         ▼
    Snowflake (HIGHLEVEL_ANALYSIS_DB)
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `401 Unauthorized` from MCP | Wrong or missing `MCP_KEY` |
| Dashboard loads but shows 0 rows | MCP endpoint unreachable — check VPN/network |
| Only 471 rows loaded | MCP payload truncation — `PAGE_SIZE` is already set to 300 to avoid this |
| Data looks stale | Click **Refresh** button (top right) or add `?refresh=1` to `/api/data` |
| `EADDRINUSE` error | Port 3000 in use — kill existing process: `pkill -f "node server.js"` |
