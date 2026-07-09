# CSM Analytics Dashboard (Local MCP Replica)

A local Node.js dashboard that pulls data from Snowflake via the internal MCP proxy and renders a full CSM analytics UI at `localhost:3000`.

## Requirements

- Node.js 18+
- Access to the Snowflake MCP endpoint and API key (get from Mayank)

## Setup

1. Clone the repo:
   ```bash
   git clone https://github.com/mayanktiwari-ui/csm-analytics-mcp.git
   cd csm-analytics-mcp
   ```

2. Open `server.js` and set your MCP key on line 11:
   ```js
   const MCP_KEY = "your-mcp-api-key-here";
   ```

3. Start the server:
   ```bash
   node server.js
   # or
   npm start
   ```

4. Open [http://localhost:3000](http://localhost:3000)

## What it does

- Fetches all CSM tickets from `RPT_CUSTOMER_SUCCESS_METRICS_VW` via the MCP proxy (paginated, deduplicated)
- Fetches CSAT data from `FACT_CSAT_RESPONSE_VW`
- Renders a multi-tab dashboard with filters, drill-downs, and KPIs:
  - **Team KPIs** — weekly/daily/monthly metrics by manager
  - **CSM Productivity** — per-agent breakdown with shift and date filters
  - **Product Analytics** — product call breakdown
  - **Call Volume Heatmap** — hour-of-day heatmap
  - **Tickets Info** — raw ticket drill-down table

## Data refresh

Data is cached for 10 minutes. Click the **Refresh** button in the top-right to force a reload. First load takes ~3-4 minutes as it paginates through all CSM tickets.
