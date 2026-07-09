# CSM Analytics Dashboard (Local MCP Replica)

A self-contained Node.js dashboard that pulls live data from Snowflake via the internal MCP proxy. No build step, no npm packages — just `node server.js`.

---

## Prerequisites

- **Node.js 18+** — [download here](https://nodejs.org)
- **MCP API key** — get from Mayank
- **MCP endpoint access** — the server must be able to reach `https://snowflake-mcp-583040996742.us-central1.run.app`

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/mayanktiwari-ui/csm-analytics-mcp.git
cd csm-analytics-mcp

# 2. Set your MCP key
export MCP_KEY="your-mcp-api-key-here"

# 3. Run
node server.js

# 4. Open in browser
open http://localhost:3000
```

---

## Snowflake Data Sources

The dashboard reads from these views/tables (all in `HIGHLEVEL_ANALYSIS_DB.HIGHLEVEL_ANALYSIS`):

| View | Purpose |
|---|---|
| `RPT_CUSTOMER_SUCCESS_METRICS_VW` | Main ticket data — all CSM calls, statuses, agent info |
| `FACT_CSAT_RESPONSE_VW` | Booked CSAT survey responses per ticket |
| `RPT_CUSTOMER_360_VW` | Customer assignments per CSM (static counts) |

### Key filters applied by the dashboard
- `agent_impl_role IN ('CSM I', 'CSM II', 'Frontline')` — CSM roles only
- `ticket_type LIKE '[CSM]:%'` — CSM ticket types only (excludes `[IMP]:`, `L1:`, etc.)
- `created_at_central IS NOT NULL`

---

## Dashboard Tabs

| Tab | What it shows |
|---|---|
| **Team KPIs** | Weekly/daily/monthly KPIs by manager — bookings, OOH, CSAT, no-show, rescheduled |
| **CSM Productivity** | Per-agent breakdown with shift, date, and manager filters |
| **Product Analytics** | Product call booked vs completed breakdown |
| **Call Volume Heatmap** | Hour-of-day call volume by day of week |
| **Tickets Info** | Raw drill-down table — click any metric cell to open |

---

## KPI Definitions

| KPI | Definition |
|---|---|
| **# Total Tickets** | All unique `[CSM]:` tickets in the period |
| **Total New Bookings** | Tickets with type in `$497 OB, Frontline OB, Branded Mobile App, ENT, Ads Manager, AI Employee, AAS, Whitelabel, Whatsapp, Wordpress, Affiliate, LC Email` |
| **Total Successful Engagements** | Completed OOH (`[CSM]: OOH`, `[CSM]: Q&A`, not left early) + Completed bookings |
| **OOH Calls** | `[CSM]: OOH` + `[CSM]: Q&A` where `call_status = Complete` and agent did not leave early |
| **$497 OB Completed** | `[CSM]: $497 OB` + `[CSM]: Frontline OB` where `call_status = Complete` |
| **Product Calls** | Completed calls excluding `$497 OB`, `Frontline OB`, `OOH`, `Q&A`, `Proactive Outreach` |
| **No-show / Rescheduled / Cancelled** | `call_status` = `No Show` / `Rescheduled` / `Cancelled` |
| **No-Status** | Tickets with blank `call_status` |
| **Proactive Outreach** | `[CSM]: Proactive Outreach` tickets (total count only) |
| **Total Booked CSAT** | Tickets with a CSAT survey response (from `FACT_CSAT_RESPONSE_VW` where `survey_channel = 'csm_booked'`) |
| **Avg. Booked CSAT** | Average `raw_value` of answered CSAT surveys ÷ 10 (scored out of 10) |

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `MCP_KEY` | *(required)* | API key for the Snowflake MCP proxy |
| `MCP_URL` | `https://snowflake-mcp-583040996742.us-central1.run.app/mcp` | MCP endpoint |
| `PORT` | `3000` | Local port to serve the dashboard on |

Set via environment variables:
```bash
export MCP_KEY="sk-mcp-orch-..."
node server.js
```

---

## Performance Notes

- **First load**: ~3–4 minutes — fetches all CSM tickets in pages of 300 rows (stable `ORDER BY ticket_id`)
- **Cache TTL**: 10 minutes — click **Refresh** button to force reload
- **Data volume**: ~27,000–38,000 CSM tickets depending on date range
- Pagination is deduplicated server-side to prevent double-counting
