/**
 * Local replica dashboard — queries Snowflake via MCP.
 * Run: node server.js
 * Open: http://localhost:3000
 */

const http = require("http");
const https = require("https");

const MCP_URL = "https://snowflake-mcp-583040996742.us-central1.run.app/mcp";
const MCP_KEY = "sk-mcp-orch-6c806c200615c5f6222e3738f7e118d4";
const PORT = 3000;

function mcpCall(method, params) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ jsonrpc: "2.0", id: 1, method, params });
    const url = new URL(MCP_URL);
    const options = {
      hostname: url.hostname, path: url.pathname, method: "POST",
      headers: {
        "Content-Type": "application/json", "Accept": "application/json",
        "x-api-key": MCP_KEY, "Content-Length": Buffer.byteLength(body),
      },
    };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        try { const p = JSON.parse(data); if (p.error) return reject(new Error(p.error.message)); resolve(p.result); }
        catch (e) { reject(e); }
      });
    });
    req.on("error", reject);
    req.write(body); req.end();
  });
}

async function runQuery(sql) {
  const result = await mcpCall("tools/call", { name: "run_query", arguments: { sql } });
  const text = result.content?.[0]?.text || "{}";
  if (text.startsWith("Error")) throw new Error(text);
  const parsed = JSON.parse(text);
  const cols = (parsed.columns || []).map(c => c.name);
  const rows = (parsed.rows || []).map(row =>
    Object.fromEntries(cols.map((col, i) => [col, row[i]]))
  );
  // rowCount = how many rows the query matched (may be > rows.length due to MCP payload cap)
  return { rows, rowCount: parsed.rowCount };
}

// ── New data layer: HIGHLEVEL_ANALYSIS.RPT_CUSTOMER_SUCCESS_METRICS_VW ──────
// Key changes from developer (2026-06-26):
//   • View: RPT_CUSTOMER_SUCCESS_METRICS_VW (replaces CUST_SUCCESS_TICKETS_ENRICHED_VW)
//   • Date: created_at_central (real timestamp, no epoch conversion needed)
//   • Week grain: created_week_central (pre-computed Monday week start in Central time)
//   • Call outcome: call_status (not CUSTOM_CF_S_CALL_TYPE / call_type)
//   • left_zoom_early: now BOOLEAN — use = FALSE, not = 'false'
//   • Agent name: agent_name (not staffing_full_name)
//   • Staffing: agent_impl_role / agent_impl_manager / agent_impl_shift
//   • CSAT: FACT_CSAT_RESPONSE_VW WHERE survey_channel = 'csm_booked', AVG(TRY_TO_DOUBLE(raw_value))

const PAGE_SIZE = 500;

function makePageSQL(offset) {
  return `
SELECT
  ticket_id                     AS ID,
  created_at_central            AS CREATED_AT_RAW,
  created_week_central          AS WEEK_START,
  created_hour_of_day           AS HOUR_OF_DAY,
  ticket_type                   AS TYPE,
  call_status                   AS CALL_STATUS,
  left_zoom_early               AS LEFT_ZOOM_EARLY,
  call_source                   AS CALL_SOURCE2,
  company_plan_level            AS COMPANY_PLAN_LEVEL,
  agency_country                AS COMPANY_COUNTRY,
  ticket_relationship_number    AS RELATIONSHIP_NUMBER,
  agent_email                   AS CUSTOM_EMAIL,
  agent_name                    AS FULL_NAME,
  agent_impl_manager            AS MANAGER,
  agent_impl_role               AS ROLE,
  agent_impl_shift              AS SHIFT
FROM RPT_CUSTOMER_SUCCESS_METRICS_VW
WHERE agent_impl_role IN ('CSM I', 'CSM II', 'Frontline')
  AND created_at_central IS NOT NULL
ORDER BY created_at_central ASC
LIMIT ${PAGE_SIZE} OFFSET ${offset}
`;
}

let cachedData = null, cachedCsat = null, cacheTime = 0;
let csatByTicket = {}; // module-level: ticketId -> scorePct (0-100), available after first load
const CACHE_TTL = 10 * 60 * 1000;

async function getCsatData() {
  // One row per ticket — take the latest response per ticket_id
  // raw_value=0 = unanswered; raw_value>0 = answered
  const sql = `
SELECT
  CAST(c.ticket_id AS VARCHAR)  AS TICKET_ID,
  MAX(c.agent_name)             AS AGENT_NAME,
  MAX(c.raw_value)              AS RAW_VALUE,
  MAX(c.csat_score_pct)         AS CSAT_SCORE_PCT,
  MAX(m.created_week_central)   AS WEEK_START,
  MAX(m.agent_name)             AS TICKET_AGENT_NAME,
  MAX(m.agent_impl_manager)     AS MANAGER
FROM FACT_CSAT_RESPONSE_VW c
JOIN RPT_CUSTOMER_SUCCESS_METRICS_VW m
  ON CAST(c.ticket_id AS VARCHAR) = CAST(m.ticket_id AS VARCHAR)
WHERE c.survey_channel = 'csm_booked'
  AND c.ticket_id IS NOT NULL
GROUP BY c.ticket_id
LIMIT 10000
`;
  try {
    const result = await runQuery(sql);
    return result.rows || [];
  } catch (e) {
    console.warn("CSAT fetch failed:", e.message);
    return [];
  }
}

async function getData(force = false) {
  if (!force && cachedData && Date.now() - cacheTime < CACHE_TTL) return cachedData;
  console.log("Fetching all pages from MCP (new view: RPT_CUSTOMER_SUCCESS_METRICS_VW)…");
  let allRows = [];
  let offset = 0;
  const MAX_PAGES = 100; // 100 × 500 = 50k rows, covers all ~37k CSM tickets
  while (offset < MAX_PAGES * PAGE_SIZE) {
    const result = await runQuery(makePageSQL(offset));
    const rows = result.rows || [];
    const declared = result.rowCount || 0;
    console.log(`  Page offset=${offset}: declared=${declared} got=${rows.length}`);
    allRows = allRows.concat(rows);
    if (declared < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  // created_at_central may come back as epoch seconds or YYYY-MM-DD string — handle both
  // created_week_central comes back as YYYY-MM-DD string already
  allRows.forEach(r => {
    const toDate = (raw) => {
      if (!raw) return '';
      const s = String(raw);
      if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
      if (/^\d{10}/.test(s)) {
        const d = new Date(parseFloat(s) * 1000);
        return isNaN(d.getTime()) ? '' : d.toISOString().slice(0, 10);
      }
      return s.slice(0, 10);
    };
    r.CREATED_AT_RAW = toDate(r.CREATED_AT_RAW);
    r.WEEK_START     = toDate(r.WEEK_START);
    // left_zoom_early is BOOLEAN from new view — normalise to JS boolean
    r.LEFT_ZOOM_EARLY = r.LEFT_ZOOM_EARLY === true || String(r.LEFT_ZOOM_EARLY).toLowerCase() === 'true';
  });
  // Fetch CSAT and attach to rows by ticket_id
  console.log("Fetching CSAT data…");
  const csatRows = await getCsatData();
  // Build csatByTicket: ticketId -> { scorePct, weekStart, manager }
  // Also build csatByWeek for direct Snowflake-accurate weekly counts
  csatByTicket = {};
  csatRows.forEach(row => {
    const tid = String(row.TICKET_ID).trim();
    const rv  = parseFloat(row.RAW_VALUE || 0);
    const pct = parseFloat(row.CSAT_SCORE_PCT || 0);
    if (!csatByTicket[tid]) {
      csatByTicket[tid] = {
        scorePct : rv > 0 ? pct : null,  // null = unanswered
        weekStart: String(row.WEEK_START||'').slice(0,10),
        manager  : row.MANAGER || '',
        agent    : row.TICKET_AGENT_NAME || row.AGENT_NAME || '',
      };
    }
  });
  // Attach to role-filtered allRows for per-agent breakdown
  allRows.forEach(r => {
    const tid   = String(r.ID).trim();
    const entry = csatByTicket[tid];
    r.HAS_SURVEY    = entry != null;
    r.BOOKED_CSAT   = entry ? entry.scorePct : null;
    r.HAS_CSAT_RESP = entry != null && entry.scorePct != null;
  });
  console.log(`CSAT rows fetched: ${csatRows.length}, unique tickets: ${Object.keys(csatByTicket).length}, answered: ${Object.values(csatByTicket).filter(e=>e.scorePct!=null).length}`);
  cachedData = allRows;
  cacheTime = Date.now();
  console.log(`Total loaded: ${cachedData.length} rows, CSAT matched: ${Object.keys(csatByTicket).length}`);
  return cachedData;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  if (url.pathname === "/api/data") {
    try {
      const rows = await getData(url.searchParams.get("refresh") === "1");
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ rows, fetchedAt: new Date(cacheTime).toISOString() }));
    } catch (e) {
      console.error(e);
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }
  res.writeHead(200, {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Pragma": "no-cache"
  });
  res.end(getDashboardHTML());
});

server.listen(PORT, () => console.log(`\n✅ Dashboard at http://localhost:${PORT}\n`));

function getDashboardHTML() { return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CSM Analytics (Local MCP)</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f7fafc;color:#2d3748;font-size:13px}
.header{background:#2b6cb0;color:#fff;padding:10px 18px;display:flex;align-items:center;justify-content:space-between}
.header h1{font-size:1rem;font-weight:700}
.header small{font-size:0.72rem;opacity:0.8}
.badge{background:#e53e3e;color:#fff;font-size:0.65rem;padding:2px 7px;border-radius:10px;margin-left:8px}
.refresh-btn{background:#3182ce;color:#fff;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:0.78rem;font-weight:600}
.refresh-btn:hover{background:#2b6cb0}.refresh-btn:disabled{background:#90cdf4;cursor:not-allowed}
.tab-nav{background:#fff;border-bottom:1px solid #e2e8f0;display:flex;gap:0;padding:0 16px;overflow-x:auto}
.tab-btn{padding:9px 16px;border:none;border-bottom:2px solid transparent;background:none;font-size:0.8rem;font-weight:500;color:#718096;cursor:pointer;white-space:nowrap}
.tab-btn.active{color:#3182ce;border-bottom-color:#3182ce;font-weight:600}
.tab-btn:hover:not(.active){color:#2d3748}
.content{padding:14px 18px}
.panel{display:none}.panel.active{display:block}
.kpi-row{display:flex;gap:12px;margin-bottom:14px;flex-wrap:wrap}
.kpi-card{flex:1;min-width:160px;padding:14px 18px;border-radius:10px;color:#fff}
.kpi-card:nth-child(1){background:linear-gradient(135deg,#667eea,#764ba2)}
.kpi-card:nth-child(2){background:linear-gradient(135deg,#48bb78,#38a169)}
.kpi-card:nth-child(3){background:linear-gradient(135deg,#10b981,#059669)}
.kpi-card:nth-child(4){background:linear-gradient(135deg,#f59e0b,#d97706)}
.kpi-val{font-size:1.8rem;font-weight:700;line-height:1.1}
.kpi-lbl{font-size:0.7rem;font-weight:500;opacity:0.9}
.filter-row{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px;background:#fff;padding:10px 14px;border-radius:8px;border:1px solid #e2e8f0;align-items:flex-end}
.filter-group{display:flex;flex-direction:column;gap:3px;min-width:120px;position:relative}
.filter-group label{font-size:0.68rem;font-weight:600;color:#718096;text-transform:uppercase}
.filter-group select{padding:4px 8px;border:1px solid #e2e8f0;border-radius:5px;font-size:0.78rem;background:#fff}
/* Checkbox dropdown */
.ms-wrap{position:relative;min-width:130px}
.ms-btn{width:100%;padding:4px 26px 4px 8px;border:1px solid #e2e8f0;border-radius:5px;font-size:0.78rem;background:#fff;cursor:pointer;text-align:left;position:relative;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:#2d3748}
.ms-btn::after{content:'▾';position:absolute;right:7px;top:50%;transform:translateY(-50%);color:#718096;font-size:0.7rem}
.ms-btn.open{border-color:#3182ce;outline:none}
.ms-dropdown{display:none;position:absolute;top:calc(100% + 2px);left:0;min-width:100%;max-width:260px;background:#fff;border:1px solid #e2e8f0;border-radius:6px;z-index:999;max-height:240px;overflow-y:auto}
.ms-dropdown.open{display:block}
.ms-dropdown label{display:flex;align-items:center;gap:7px;padding:5px 10px;font-size:0.78rem;cursor:pointer;white-space:nowrap;color:#2d3748}
.ms-dropdown label:hover{background:#ebf8ff}
.ms-dropdown label input{margin:0;cursor:pointer}
.ms-sep{height:1px;background:#e2e8f0;margin:2px 0}
.ms-search{width:100%;padding:4px 7px;font-size:0.75rem;border:1px solid #cbd5e0;border-radius:4px;margin-bottom:4px;outline:none;box-sizing:border-box}
.ms-search:focus{border-color:#667eea}
.ms-list{max-height:200px;overflow-y:auto}
/* Relative date picker */
.rdp-wrap{position:relative;min-width:150px}
.rdp-btn{width:100%;padding:4px 26px 4px 8px;border:1px solid #e2e8f0;border-radius:5px;font-size:0.78rem;background:#fff;cursor:pointer;text-align:left;position:relative;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:#2d3748}
.rdp-btn::after{content:'▾';position:absolute;right:7px;top:50%;transform:translateY(-50%);color:#718096;font-size:0.7rem}
.rdp-btn.open{border-color:#3182ce}
.rdp-popup{display:none;position:absolute;top:calc(100% + 4px);left:0;width:340px;background:#fff;border:1px solid #e2e8f0;border-radius:8px;z-index:1000;padding:0;overflow:hidden}
.rdp-popup.open{display:block}
.rdp-tabs{display:flex;border-bottom:1px solid #e2e8f0}
.rdp-tab{flex:1;padding:7px 4px;border:none;background:none;font-size:0.75rem;font-weight:500;color:#718096;cursor:pointer;border-bottom:2px solid transparent}
.rdp-tab.active{color:#3182ce;border-bottom-color:#3182ce;font-weight:600}
.rdp-body{padding:14px}
.rdp-options{display:flex;flex-direction:column;gap:8px}
.rdp-option{display:flex;align-items:center;gap:10px;font-size:0.8rem;cursor:pointer}
.rdp-option input[type=radio]{cursor:pointer;accent-color:#3182ce}
.rdp-inline{display:flex;align-items:center;gap:6px;font-size:0.8rem}
.rdp-inline input[type=number]{width:52px;padding:3px 6px;border:1px solid #e2e8f0;border-radius:4px;font-size:0.8rem;text-align:center}
.rdp-range{display:flex;flex-direction:column;gap:8px}
.rdp-range-row{display:flex;align-items:center;gap:8px;font-size:0.8rem}
.rdp-range-row input[type=date]{padding:3px 6px;border:1px solid #e2e8f0;border-radius:4px;font-size:0.78rem}
.rdp-footer{padding:8px 14px;background:#f7fafc;border-top:1px solid #e2e8f0;font-size:0.72rem;color:#718096;display:flex;justify-content:space-between;align-items:center}
.rdp-apply{padding:4px 14px;background:#3182ce;color:#fff;border:none;border-radius:5px;font-size:0.75rem;cursor:pointer;font-weight:600}
.rdp-apply:hover{background:#2b6cb0}
.table-wrap{overflow-x:auto;border-radius:8px;border:1px solid #e2e8f0;background:#fff;max-height:72vh;overflow-y:auto}
table{width:100%;border-collapse:collapse;font-size:0.75rem}
th{background:#f7fafc;padding:7px 10px;text-align:left;border-bottom:2px solid #e2e8f0;white-space:nowrap;font-weight:600;color:#4a5568;position:sticky;top:0;z-index:1}
td{padding:6px 10px;border-bottom:1px solid #f0f0f0;white-space:nowrap}
tr:hover td{background:#f7fafc}
.status-bar{background:#ebf8ff;border:1px solid #bee3f8;border-radius:6px;padding:6px 12px;margin-bottom:10px;font-size:0.78rem;color:#2b6cb0;display:flex;justify-content:space-between;align-items:center}
.empty-note{text-align:center;color:#a0aec0;padding:24px;font-size:0.85rem}
.loading{text-align:center;padding:40px;color:#718096}
.error-note{color:#e53e3e;padding:12px;background:#fff5f5;border-radius:6px;border:1px solid #feb2b2}
.panel-row{display:flex;gap:12px;align-items:flex-start}
.table-panel{flex:1;min-width:0}
.defs-panel{width:220px;flex-shrink:0;background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:12px;font-size:0.72rem;color:#4a5568;line-height:1.6}
.defs-panel h4{font-size:0.78rem;font-weight:700;margin-bottom:6px;color:#2d3748}
.section-title{font-size:0.9rem;font-weight:700;color:#2d3748;margin-bottom:10px;margin-top:4px}
/* Heatmap */
.hm-table{border-collapse:collapse;font-size:0.72rem}
.hm-table th{background:#f7fafc;padding:4px 7px;border:1px solid #e2e8f0;font-weight:600;text-align:center;white-space:nowrap}
.hm-table td{padding:4px 7px;border:1px solid #e2e8f0;text-align:center;min-width:30px}
.hm-total{font-weight:700;background:#ebf8ff!important}
/* Drill-down clickable cells */
.drill{cursor:pointer;color:#2b6cb0;text-decoration:underline dotted}
.drill:hover{background:#ebf8ff!important;color:#1a4a8a}
</style>
</head>
<body>
<div class="header">
  <div><h1>CSM Analytics <span class="badge">LOCAL · MCP</span></h1><small>Weekly team &amp; agent KPIs — MCP replica</small></div>
  <button class="refresh-btn" id="refreshBtn" onclick="loadData(true)">↻ Refresh</button>
</div>
<div class="tab-nav">
  <button class="tab-btn active" onclick="switchTab('teamkpis',this)">Team KPIs</button>
  <button class="tab-btn" onclick="switchTab('productivity',this)">CSM Productivity</button>
  <button class="tab-btn" onclick="switchTab('product',this)">Product Analytics</button>
  <button class="tab-btn" onclick="switchTab('heatmap',this)">Call Volume Heat Map</button>
  <button class="tab-btn" onclick="switchTab('tickets',this)">Tickets Info</button>
</div>
<div class="content">
  <div id="statusBar" class="status-bar" style="display:none">
    <span id="statusMsg">Loading…</span>
    <span id="fetchedAt" style="color:#718096;font-size:0.72rem"></span>
  </div>

  <!-- Team KPIs -->
  <div id="panel-teamkpis" class="panel active">
    <div class="filter-row">
      <div class="filter-group"><label>Manager</label><div class="ms-wrap" id="ms-tk-manager"></div></div>
      <div class="filter-group"><label>Role</label><div class="ms-wrap" id="ms-tk-role"></div></div>
      <div class="filter-group"><label>CSM</label><div class="ms-wrap" id="ms-tk-csm"></div></div>
      <div class="filter-group"><label>Date Level</label><select id="tk-datelevel" onchange="rdpRebuild('tk');renderAll()"><option value="Day">Day</option><option value="Week">Week</option><option value="Month">Month</option><option value="Year">Year</option></select></div>
      <div class="filter-group"><label>Date</label><div class="rdp-wrap" id="rdp-tk"></div></div>
      <div class="filter-group" style="justify-content:flex-end"><button onclick="clearFilters('tk')" style="padding:5px 12px;font-size:0.75rem;border:1px solid #e2e8f0;border-radius:5px;background:#fff;cursor:pointer;margin-top:14px">Clear</button></div>
    </div>
    <div id="tk-kpis" class="kpi-row"></div>
    <div class="table-wrap" id="tk-table"><div class="loading">Loading…</div></div>
  </div>

  <!-- CSM Productivity -->
  <div id="panel-productivity" class="panel">
    <div class="filter-row">
      <div class="filter-group"><label>Manager</label><div class="ms-wrap" id="ms-cp-manager"></div></div>
      <div class="filter-group"><label>Shift</label><div class="ms-wrap" id="ms-cp-shift"></div></div>
      <div class="filter-group"><label>CSM</label><div class="ms-wrap" id="ms-cp-csm"></div></div>
      <div class="filter-group"><label>Role</label><div class="ms-wrap" id="ms-cp-role"></div></div>
      <div class="filter-group"><label>Date Level</label><select id="cp-datelevel" onchange="rdpRebuild('cp');renderAll()"><option value="Day">Day</option><option value="Week">Week</option><option value="Month">Month</option><option value="Year">Year</option></select></div>
      <div class="filter-group"><label>Date</label><div class="rdp-wrap" id="rdp-cp"></div></div>
      <div class="filter-group" style="justify-content:flex-end"><button onclick="clearFilters('cp')" style="padding:5px 12px;font-size:0.75rem;border:1px solid #e2e8f0;border-radius:5px;background:#fff;cursor:pointer;margin-top:14px">Clear</button></div>
    </div>
    <div id="cp-kpis" class="kpi-row"></div>
    <div class="panel-row">
      <div class="defs-panel"><h4>KPI Definitions</h4><ol style="padding-left:14px">
        <li># Total Tickets: All tickets by CSM Team.</li>
        <li>Total New Bookings: Booking type tickets.</li>
        <li>Total Successful Engagements: OOH Completed + All Booking Types Completed.</li>
        <li>Completed OOH Calls: OOH type + not Left Zoom Early.</li>
        <li>Completed Product Calls: Product type + Complete status.</li>
        <li>Total $497 OB: OB types + Complete.</li>
        <li>No-show / Rescheduled / Cancelled: % of New Bookings.</li>
        <li>Booked CSAT: Average CSAT on booked completed calls.</li>
      </ol></div>
      <div class="table-panel"><div class="table-wrap" id="cp-table"><div class="loading">Loading…</div></div></div>
    </div>
  </div>

  <!-- Product Analytics -->
  <div id="panel-product" class="panel">
    <div class="filter-row">
      <div class="filter-group"><label>Manager</label><div class="ms-wrap" id="ms-pa-manager"></div></div>
      <div class="filter-group"><label>CSM</label><div class="ms-wrap" id="ms-pa-csm"></div></div>
      <div class="filter-group"><label>Role</label><div class="ms-wrap" id="ms-pa-role"></div></div>
      <div class="filter-group"><label>Date Level</label><select id="pa-datelevel" onchange="rdpRebuild('pa');renderAll()"><option value="Day">Day</option><option value="Week">Week</option><option value="Month">Month</option><option value="Year">Year</option></select></div>
      <div class="filter-group"><label>Date</label><div class="rdp-wrap" id="rdp-pa"></div></div>
      <div class="filter-group" style="justify-content:flex-end"><button onclick="clearFilters('pa')" style="padding:5px 12px;font-size:0.75rem;border:1px solid #e2e8f0;border-radius:5px;background:#fff;cursor:pointer;margin-top:14px">Clear</button></div>
    </div>
    <div id="pa-kpis" class="kpi-row"></div>
    <div class="section-title">Team Product Call Stats</div>
    <div class="table-wrap" id="pa-team-table"><div class="loading">Loading…</div></div>
    <div class="section-title" style="margin-top:16px">CSM Product Calls</div>
    <div class="table-wrap" id="pa-csm-table"></div>
  </div>

  <!-- Heatmap -->
  <div id="panel-heatmap" class="panel">
    <div class="filter-row">
      <div class="filter-group"><label>Manager</label><div class="ms-wrap" id="ms-hm-manager"></div></div>
      <div class="filter-group"><label>CSM</label><div class="ms-wrap" id="ms-hm-csm"></div></div>
      <div class="filter-group"><label>Call Type</label><div class="ms-wrap" id="ms-hm-type"></div></div>
      <div class="filter-group"><label>Date Level</label><select id="hm-datelevel" onchange="rdpRebuild('hm');renderAll()"><option value="Day">Day</option><option value="Week">Week</option><option value="Month">Month</option><option value="Year">Year</option></select></div>
      <div class="filter-group"><label>Date</label><div class="rdp-wrap" id="rdp-hm"></div></div>
      <div class="filter-group" style="justify-content:flex-end"><button onclick="clearFilters('hm')" style="padding:5px 12px;font-size:0.75rem;border:1px solid #e2e8f0;border-radius:5px;background:#fff;cursor:pointer;margin-top:14px">Clear</button></div>
    </div>
    <div class="section-title">Call Volume by Period × Hour (CST)</div>
    <div style="overflow-x:auto" id="hm-table"><div class="loading">Loading…</div></div>
  </div>

  <!-- Tickets Info -->
  <div id="panel-tickets" class="panel">
    <div class="filter-row">
      <div class="filter-group"><label>Manager</label><div class="ms-wrap" id="ms-ti-manager"></div></div>
      <div class="filter-group"><label>CSM</label><div class="ms-wrap" id="ms-ti-csm"></div></div>
      <div class="filter-group"><label>Type</label><div class="ms-wrap" id="ms-ti-type"></div></div>
      <div class="filter-group"><label>Call Status</label><div class="ms-wrap" id="ms-ti-status"></div></div>
      <div class="filter-group"><label>Date Level</label><select id="ti-datelevel" onchange="rdpRebuild('ti');renderAll()"><option value="Day">Day</option><option value="Week">Week</option><option value="Month">Month</option><option value="Year">Year</option></select></div>
      <div class="filter-group"><label>Date</label><div class="rdp-wrap" id="rdp-ti"></div></div>
      <div class="filter-group" style="justify-content:flex-end"><button onclick="clearFilters('ti')" style="padding:5px 12px;font-size:0.75rem;border:1px solid #e2e8f0;border-radius:5px;background:#fff;cursor:pointer;margin-top:14px">Clear</button></div>
    </div>
    <div id="ti-kpis" class="kpi-row"></div>
    <div class="table-wrap" id="ti-table"><div class="loading">Loading…</div></div>
  </div>
</div>

<script>
const BOOKING_TYPES=new Set(["[CSM]: Branded Mobile App","[CSM]: AI Employee","[CSM]: $497 OB","[CSM]: AAS","[CSM]: Whitelabel Mobile App","[CSM]: Whatsapp","[CSM]: Wordpress","[CSM]: Affiliate","[CSM]: LC Email","[CSM]: ENT","[CSM]: Ads Manager"]);
const PRODUCT_CALL_TYPES=new Set(["[CSM]: Branded Mobile App","[CSM]: AI Employee","[CSM]: Whitelabel Mobile App","[CSM]: Whatsapp","[CSM]: Wordpress","[CSM]: Affiliate","[CSM]: LC Email","[CSM]: ENT","[CSM]: Ads Manager"]);
const OB_497_TYPES=new Set(["[CSM]: $497 OB","[CSM]: AAS","[CSM]: Frontline OB","[IMP]: Implementation Call","[IMP]: AAS"]);
const OOH_TYPES=new Set(["[IMP]: OOH Call","[CSM]: OOH"]);
const TOTAL_COMPLETED_BOOKING_TYPES=new Set(["[CSM]: Branded Mobile App","[CSM]: AI Employee","[CSM]: $497 OB","[CSM]: AAS","[CSM]: Whitelabel Mobile App","[CSM]: Whatsapp","[CSM]: Wordpress","[CSM]: Affiliate","[CSM]: LC Email","[CSM]: ENT","[CSM]: Ads Manager","[CSM]: Frontline OB","[IMP]: Implementation Call","[IMP]: AAS"]);
const COMPLETED_PRODUCT_EXCLUDED=new Set(["[IMP]: OOH Call","[CSM]: OOH","[CSM]: Frontline OB","[IMP]: Implementation Call","[IMP]: AAS","[CSM]: Affiliate","[CSM]: AAS","Customer Success","[CSM]: Ads Manager","[PS] L1 - Frontline","L1 - Frontline","[CSM]: AAS Ticket ID","[CSM]: $497 OB"]);
const PRODUCT_LINES=[
  ["Branded Mobile App","[CSM]: Branded Mobile App"],
  ["AI Employee","[CSM]: AI Employee"],
  ["Whitelabel Mobile App","[CSM]: Whitelabel Mobile App"],
  ["Whatsapp","[CSM]: Whatsapp"],
  ["Wordpress","[CSM]: Wordpress"],
  ["LC Email","[CSM]: LC Email"],
  ["ENT","[CSM]: ENT"],
  ["Ads Manager","[CSM]: Ads Manager"],
];

let allRows=[];

// ── Relative Date Picker ──────────────────────────────────────────────────────
// State per prefix: { mode:'last'|'this'|'range', n:8, unit:'Week', from:'', to:'' }
const rdpState={};

function rdpInit(prefix){
  if(!rdpState[prefix]) rdpState[prefix]={mode:'last',n:8,unit:'Week',from:'',to:''};
}

function rdpBtnLabel(prefix){
  rdpInit(prefix);
  const s=rdpState[prefix];
  if(s.mode==='all') return 'All Dates';
  if(s.mode==='last') return \`Last \${s.n} \${s.unit}s\`;
  if(s.mode==='this') return \`This \${s.unit}\`;
  if(s.mode==='prev') return \`Previous \${s.unit}\`;
  if(s.mode==='range'&&s.from&&s.to) return \`\${s.from} → \${s.to}\`;
  return 'All Dates';
}

function rdpBuild(prefix){
  rdpInit(prefix);
  const wrap=document.getElementById('rdp-'+prefix);
  if(!wrap)return;
  const level=sel(prefix+'-datelevel')||'Week';
  rdpState[prefix].unit=level;
  const s=rdpState[prefix];
  const unitLabel=level;
  wrap.innerHTML=\`
    <button class="rdp-btn" id="rdpb-\${prefix}" onclick="rdpToggle('\${prefix}')" type="button">\${rdpBtnLabel(prefix)}</button>
    <div class="rdp-popup" id="rdpp-\${prefix}">
      <div class="rdp-tabs">
        <button class="rdp-tab\${s.mode!=='range'?' active':''}" onclick="rdpTabSwitch('\${prefix}','relative')">Relative</button>
        <button class="rdp-tab\${s.mode==='range'?' active':''}" onclick="rdpTabSwitch('\${prefix}','range')">Date Range</button>
      </div>
      <div id="rdp-rel-\${prefix}" class="rdp-body" style="\${s.mode==='range'?'display:none':''}">
        <div class="rdp-options">
          <label class="rdp-option"><input type="radio" name="rdpm-\${prefix}" value="all" \${s.mode==='all'?'checked':''} onchange="rdpModeChange('\${prefix}',this)"> All Dates</label>
          <label class="rdp-option"><input type="radio" name="rdpm-\${prefix}" value="prev" \${s.mode==='prev'?'checked':''} onchange="rdpModeChange('\${prefix}',this)"> Previous \${unitLabel}</label>
          <label class="rdp-option"><input type="radio" name="rdpm-\${prefix}" value="this" \${s.mode==='this'?'checked':''} onchange="rdpModeChange('\${prefix}',this)"> This \${unitLabel}</label>
          <div class="rdp-option">
            <input type="radio" name="rdpm-\${prefix}" value="last" \${s.mode==='last'?'checked':''} onchange="rdpModeChange('\${prefix}',this)">
            <div class="rdp-inline">Last <input type="number" id="rdpn-\${prefix}" value="\${s.n}" min="1" max="200" style="width:52px" onchange="rdpNChange('\${prefix}',this)"> \${unitLabel}s</div>
          </div>
        </div>
      </div>
      <div id="rdp-rng-\${prefix}" class="rdp-body" style="\${s.mode!=='range'?'display:none':''}">
        <div class="rdp-range">
          <div class="rdp-range-row"><span style="width:36px;color:#718096">From</span><input type="date" id="rdpf-\${prefix}" value="\${s.from}" onchange="rdpRangeChange('\${prefix}')"></div>
          <div class="rdp-range-row"><span style="width:36px;color:#718096">To</span><input type="date" id="rdpt-\${prefix}" value="\${s.to}" onchange="rdpRangeChange('\${prefix}')"></div>
        </div>
      </div>
      <div class="rdp-footer">
        <span id="rdp-label-\${prefix}" style="font-style:italic">\${rdpBtnLabel(prefix)}</span>
        <button class="rdp-apply" onclick="rdpApply('\${prefix}')">Apply</button>
      </div>
    </div>\`;
}

function rdpToggle(prefix){
  const popup=document.getElementById('rdpp-'+prefix);
  const btn=document.getElementById('rdpb-'+prefix);
  const isOpen=popup.classList.contains('open');
  document.querySelectorAll('.rdp-popup.open').forEach(p=>p.classList.remove('open'));
  document.querySelectorAll('.rdp-btn.open').forEach(b=>b.classList.remove('open'));
  if(!isOpen){popup.classList.add('open');btn.classList.add('open');}
}

function rdpTabSwitch(prefix,tab){
  document.getElementById('rdp-rel-'+prefix).style.display=tab==='relative'?'':'none';
  document.getElementById('rdp-rng-'+prefix).style.display=tab==='range'?'':'none';
  document.querySelectorAll(\`#rdpp-\${prefix} .rdp-tab\`).forEach((t,i)=>{
    t.classList.toggle('active',(tab==='relative'&&i===0)||(tab==='range'&&i===1));
  });
  if(tab==='range') rdpState[prefix].mode='range';
}

function rdpModeChange(prefix,radio){
  rdpState[prefix].mode=radio.value;
  const lbl=document.getElementById('rdp-label-'+prefix);
  if(lbl)lbl.textContent=rdpBtnLabel(prefix);
}

function rdpNChange(prefix,inp){
  rdpState[prefix].n=parseInt(inp.value)||8;
  const lbl=document.getElementById('rdp-label-'+prefix);
  if(lbl)lbl.textContent=rdpBtnLabel(prefix);
}

function rdpRangeChange(prefix){
  rdpState[prefix].from=document.getElementById('rdpf-'+prefix)?.value||'';
  rdpState[prefix].to=document.getElementById('rdpt-'+prefix)?.value||'';
  rdpState[prefix].mode='range';
  const lbl=document.getElementById('rdp-label-'+prefix);
  if(lbl)lbl.textContent=rdpBtnLabel(prefix);
}

function rdpApply(prefix){
  // sync n from input in case user typed without triggering onchange
  const ni=document.getElementById('rdpn-'+prefix);
  if(ni) rdpState[prefix].n=parseInt(ni.value)||8;
  document.getElementById('rdpp-'+prefix)?.classList.remove('open');
  document.getElementById('rdpb-'+prefix)?.classList.remove('open');
  document.getElementById('rdpb-'+prefix).textContent=rdpBtnLabel(prefix);
  renderAll();
}

function rdpRebuild(prefix){
  const level=sel(prefix+'-datelevel')||'Week';
  if(rdpState[prefix]) rdpState[prefix].unit=level;
  rdpBuild(prefix);
}

// Compute the set of period keys that pass the relative date filter
function rdpActivePeriods(prefix,allPeriods){
  rdpInit(prefix);
  const s=rdpState[prefix];
  const sorted=[...allPeriods].sort();
  if(s.mode==='all') return new Set(sorted);
  if(s.mode==='last'){
    const recent=sorted.slice(-Math.max(1,s.n));
    return new Set(recent);
  }
  if(s.mode==='this'){
    const today=new Date();
    const level=s.unit;
    const thisPeriod=truncDate(today.toISOString().slice(0,10),level);
    return new Set(sorted.filter(p=>p===thisPeriod));
  }
  if(s.mode==='prev'){
    const today=new Date();
    const level=s.unit;
    // go back one period
    const offset=level==='Day'?1:level==='Week'?7:level==='Month'?31:365;
    const prev=new Date(today-offset*86400000);
    const prevPeriod=truncDate(prev.toISOString().slice(0,10),level);
    return new Set(sorted.filter(p=>p===prevPeriod));
  }
  if(s.mode==='range'&&s.from&&s.to){
    return new Set(sorted.filter(p=>p>=s.from&&p<=s.to));
  }
  // default: last 8
  return new Set(sorted.slice(-8));
}

// close on outside click
document.addEventListener('click',e=>{
  if(!e.target.closest('.rdp-wrap')&&!e.target.closest('.ms-wrap')){
    document.querySelectorAll('.rdp-popup.open').forEach(p=>p.classList.remove('open'));
    document.querySelectorAll('.rdp-btn.open').forEach(b=>b.classList.remove('open'));
    document.querySelectorAll('.ms-dropdown.open').forEach(d=>d.classList.remove('open'));
    document.querySelectorAll('.ms-btn.open').forEach(b=>b.classList.remove('open'));
  }
});

// ── Checkbox multi-select dropdown ────────────────────────────────────────────
// State: msState[id] = Set of selected values
const msState={};

function msGetSelected(id){return msState[id]||new Set();}

function msBuildDropdown(containerId,vals,label){
  const wrap=document.getElementById('ms-'+containerId);
  if(!wrap)return;
  const prev=msState[containerId]||new Set();
  const btnId='msb-'+containerId;
  const ddId='msd-'+containerId;
  const btnLabel=prev.size===0?label:(prev.size===1?[...prev][0]:prev.size+' selected');
  wrap.innerHTML=\`
    <button class="ms-btn\${prev.size>0?' active':''}" id="\${btnId}" onclick="msToggle('\${containerId}')" type="button">\${btnLabel}</button>
    <div class="ms-dropdown" id="\${ddId}">
      <input class="ms-search" type="text" placeholder="Search…" oninput="msFilter('\${containerId}',this.value)" onclick="event.stopPropagation()">
      <label style="font-weight:600;color:#4a5568;margin-top:2px"><input type="checkbox" onchange="msSelectAll('\${containerId}',this)" \${prev.size===0?'checked':''}> All</label>
      <div class="ms-sep"></div>
      <div class="ms-list" id="msl-\${containerId}">
        \${vals.map(v=>\`<label><input type="checkbox" value="\${v}" onchange="msChange('\${containerId}')" \${prev.has(v)?'checked':''}> \${v}</label>\`).join('')}
      </div>
    </div>\`;
}

function msFilter(id,q){
  const list=document.getElementById('msl-'+id);
  if(!list)return;
  const term=q.trim().toLowerCase();
  list.querySelectorAll('label').forEach(lbl=>{
    const txt=lbl.textContent.toLowerCase();
    lbl.style.display=(!term||txt.includes(term))?'':'none';
  });
}

function msToggle(id){
  const dd=document.getElementById('msd-'+id);
  const btn=document.getElementById('msb-'+id);
  const isOpen=dd.classList.contains('open');
  document.querySelectorAll('.ms-dropdown.open').forEach(d=>d.classList.remove('open'));
  document.querySelectorAll('.ms-btn.open').forEach(b=>b.classList.remove('open'));
  if(!isOpen){
    dd.classList.add('open');btn.classList.add('open');
    // clear search and show all options
    const srch=dd.querySelector('.ms-search');
    if(srch){srch.value='';msFilter(id,'');setTimeout(()=>srch.focus(),50);}
  }
}

function msSelectAll(id,cb){
  const dd=document.getElementById('msd-'+id);
  dd.querySelectorAll('input[type=checkbox][value]').forEach(c=>c.checked=false);
  msState[id]=new Set();
  msRefreshBtn(id);
  renderAll();
}

function msChange(id){
  const dd=document.getElementById('msd-'+id);
  const checked=new Set([...dd.querySelectorAll('input[value]:checked')].map(c=>c.value));
  msState[id]=checked;
  // uncheck "All" if anything selected
  const allCb=dd.querySelector('input:not([value])');
  if(allCb)allCb.checked=checked.size===0;
  msRefreshBtn(id);
  renderAll();
}

function msRefreshBtn(id){
  const btn=document.getElementById('msb-'+id);
  if(!btn)return;
  const s=msState[id]||new Set();
  const label=btn.dataset.label||id;
  btn.textContent=s.size===0?label:(s.size===1?[...s][0]:s.size+' selected');
  btn.classList.toggle('active',s.size>0);
}

// close dropdowns on outside click
document.addEventListener('click',e=>{
  if(!e.target.closest('.ms-wrap')){
    document.querySelectorAll('.ms-dropdown.open').forEach(d=>d.classList.remove('open'));
    document.querySelectorAll('.ms-btn.open').forEach(b=>b.classList.remove('open'));
  }
});

// Single-select value (for non-multi selects like Date Level)
const sel=id=>{const e=document.getElementById(id);return e?e.value:''};

// selMulti now reads from msState
function selMulti(id){return msGetSelected(id);}

function setOptions(id,vals,labelText){
  // For native <select> (single)
  const el=document.getElementById(id);
  if(el&&el.tagName==='SELECT'){
    const cur=el.value;
    el.innerHTML='<option value="">All</option>'+vals.map(v=>\`<option\${v===cur?' selected':''}>\${v}</option>\`).join('');
    return;
  }
  // For ms-wrap (checkbox dropdown)
  msBuildDropdown(id,vals,labelText||'All');
  const btn=document.getElementById('msb-'+id);
  if(btn)btn.dataset.label=labelText||'All';
}

function clearFilters(prefix){
  ['manager','role','csm','shift','type','status'].forEach(f=>{
    const k=prefix+'-'+f;
    if(msState[k])msState[k]=new Set();
  });
  // Reset date picker to default Last 8
  rdpState[prefix]={mode:'last',n:8,unit:sel(prefix+'-datelevel')||'Week',from:'',to:''};
  populateFilters();
  renderAll();
}

function switchTab(name,btn){
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('panel-'+name).classList.add('active');
  if(btn)btn.classList.add('active');
}

function uniq(arr){return[...new Set(arr.filter(Boolean))].sort();}

function toDateStr(raw){
  if(!raw)return'';
  return String(raw).trim().slice(0,10);
}

function truncDate(raw,level){
  const s=toDateStr(raw);if(!s)return'';
  const d=new Date(s+'T00:00:00');if(isNaN(d))return'';
  if(level==='Day')return s.slice(0,10);
  if(level==='Week'){const day=d.getDay();const sun=new Date(d);sun.setDate(d.getDate()-day);return sun.toISOString().slice(0,10);}
  if(level==='Month')return s.slice(0,7)+'-01';
  if(level==='Year')return s.slice(0,4)+'-01-01';
  return s;
}

function fmtDate(str,level){
  if(!str)return'';
  const d=new Date(str+'T00:00:00');if(isNaN(d))return str;
  if(level==='Day')return d.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'2-digit'});
  if(level==='Week')return d.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'2-digit'});
  if(level==='Month')return d.toLocaleDateString('en-US',{month:'short',year:'numeric'});
  if(level==='Year')return String(d.getFullYear());
  return d.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'2-digit'});
}

function kpiCards(id,items){
  document.getElementById(id).innerHTML=items.map(([v,l])=>
    \`<div class="kpi-card"><div class="kpi-val">\${v}</div><div class="kpi-lbl">\${l}</div></div>\`
  ).join('');
}

function uniqIds(rows,pred){const s=new Set();rows.forEach(r=>{if(pred(r))s.add(r.ID);});return s.size;}

// call_status = Complete / No Show / Rescheduled / Cancelled (from RPT_CUSTOMER_SUCCESS_METRICS_VW)
// left_zoom_early is now a JS boolean (normalised server-side)
function callSt(r){return r.CALL_STATUS||'';}
function leftedEarly(r){return r.LEFT_ZOOM_EARLY===true;}

function filterBase(rows,prefix){
  const level=sel(prefix+'-datelevel')||'Week';
  const mgrs=selMulti(prefix+'-manager');

  // Use relative date picker if available, otherwise show all
  const allPeriods=new Set(rows.map(r=>getPeriodKey(r,level)).filter(Boolean));
  const activeDates=document.getElementById('rdp-'+prefix)
    ? rdpActivePeriods(prefix,allPeriods)
    : allPeriods;

  return rows.filter(r=>{
    if(mgrs.size>0&&!mgrs.has(r.MANAGER))return false;
    if(!activeDates.has(getPeriodKey(r,level)))return false;
    return true;
  });
}

// Use WEEK_START (pre-computed by Snowflake as created_week_central) when level=Week
// for all other levels fall back to truncating CREATED_AT_RAW
function getCreatedAt(r){return r.CREATED_AT_RAW||r.CREATED_AT_NEW||'';}
function getPeriodKey(r,level){
  if(level==='Week'&&r.WEEK_START) return r.WEEK_START;
  return truncDate(getCreatedAt(r),level);
}

function populateDateOptions(prefix,rows){
  const level=sel(prefix+'-datelevel')||'Week';
  const allDates=[...new Set(rows.map(r=>getPeriodKey(r,level)).filter(Boolean))].sort().reverse().slice(0,80);
  // For ms-wrap date dropdowns, build with formatted labels
  const wrap=document.getElementById('ms-'+prefix+'-date');
  if(wrap){
    const prev=msState[prefix+'-date']||new Set();
    const btnId='msb-'+prefix+'-date';
    const ddId='msd-'+prefix+'-date';
    const btnLabel=prev.size===0?'All Dates':(prev.size===1?fmtDate([...prev][0],level):prev.size+' dates');
    wrap.innerHTML=\`
      <button class="ms-btn" id="\${btnId}" onclick="msToggle('\${prefix}-date')" type="button">\${btnLabel}</button>
      <div class="ms-dropdown" id="\${ddId}">
        <label style="font-weight:600;color:#4a5568"><input type="checkbox" onchange="msSelectAll('\${prefix}-date',this)" \${prev.size===0?'checked':''}> All Dates</label>
        <div class="ms-sep"></div>
        \${allDates.map(d=>\`<label><input type="checkbox" value="\${d}" onchange="msChange('\${prefix}-date')" \${prev.has(d)?'checked':''}> \${fmtDate(d,level)}</label>\`).join('')}
      </div>\`;
    const btn=document.getElementById(btnId);
    if(btn)btn.dataset.label='All Dates';
    return;
  }
  // fallback for native select
  const el=document.getElementById(prefix+'-date');if(!el)return;
  const cur=el.value;
  el.innerHTML='<option value="">All</option>'+allDates.map(d=>\`<option value="\${d}"\${d===cur?' selected':''}>\${fmtDate(d,level)}</option>\`).join('');
}

function populateFilters(){
  const rows=allRows;
  // Relative date pickers for all tabs
  ['tk','cp','pa','hm','ti'].forEach(p=>rdpBuild(p));
  // Team KPIs
  setOptions('tk-manager',uniq(rows.map(r=>r.MANAGER)),'All Managers');
  setOptions('tk-role',uniq(rows.map(r=>r.ROLE)),'All Roles');
  setOptions('tk-csm',uniq(rows.map(r=>r.FULL_NAME)),'All CSMs');
  // CSM Productivity
  setOptions('cp-manager',uniq(rows.map(r=>r.MANAGER)),'All Managers');
  setOptions('cp-csm',uniq(rows.map(r=>r.FULL_NAME||r.FRESHDESK_AGENT_NAME)),'All CSMs');
  setOptions('cp-shift',uniq(rows.map(r=>r.SHIFT||'Null')),'All Shifts');
  setOptions('cp-role',uniq(rows.map(r=>r.ROLE)),'All Roles');
  // Product Analytics
  setOptions('pa-manager',uniq(rows.map(r=>r.MANAGER)),'All Managers');
  setOptions('pa-csm',uniq(rows.map(r=>r.FULL_NAME)),'All CSMs');
  setOptions('pa-role',uniq(rows.map(r=>r.ROLE)),'All Roles');
  // Heatmap
  setOptions('hm-manager',uniq(rows.map(r=>r.MANAGER)),'All Managers');
  setOptions('hm-csm',uniq(rows.map(r=>r.FULL_NAME)),'All CSMs');
  // Heatmap call type: only relevant CSM/IMP types
  const HM_TYPES=[
    '[CSM]: Branded Mobile App','[CSM]: AI Employee','[CSM]: $497 OB','[CSM]: AAS',
    '[CSM]: Whitelabel Mobile App','[CSM]: Whatsapp','[CSM]: Wordpress','[CSM]: Affiliate',
    '[CSM]: LC Email','[CSM]: ENT','[CSM]: Ads Manager','[CSM]: Frontline OB',
    '[CSM]: OOH','[IMP]: OOH Call','[IMP]: Implementation Call','[IMP]: AAS',
  ].filter(t=>rows.some(r=>r.TYPE===t));
  setOptions('hm-type',HM_TYPES,'All Types');
  // Tickets Info
  setOptions('ti-manager',uniq(rows.map(r=>r.MANAGER)),'All Managers');
  setOptions('ti-csm',uniq(rows.map(r=>r.FULL_NAME||r.FRESHDESK_AGENT_NAME)),'All CSMs');
  const TI_TYPES=[
    '[CSM]: Branded Mobile App','[CSM]: AI Employee','[CSM]: $497 OB','[CSM]: AAS',
    '[CSM]: Whitelabel Mobile App','[CSM]: Whatsapp','[CSM]: Wordpress','[CSM]: Affiliate',
    '[CSM]: LC Email','[CSM]: ENT','[CSM]: Ads Manager','[CSM]: Frontline OB',
    '[CSM]: OOH','[IMP]: OOH Call','[IMP]: Implementation Call','[IMP]: AAS',
  ].filter(t=>rows.some(r=>r.TYPE===t));
  setOptions('ti-type',TI_TYPES,'All Types');
  setOptions('ti-status',uniq(rows.map(r=>callSt(r)||'No Status')),'All Statuses');
}


// ── CSAT helpers ──────────────────────────────────────────────────────────────
function csatMetrics(rows){
  const total=uniqIds(rows,r=>r.HAS_SURVEY);
  const satisfied=uniqIds(rows,r=>r.HAS_SURVEY&&r.CSAT_RAW>=102);
  const pct=total>0?(satisfied/total*100).toFixed(1)+'%':'-';
  const surveyPct=rows.length>0?(total/uniqIds(rows,()=>true)*100).toFixed(1)+'%':'-';
  return{total,satisfied,pct,surveyPct};
}
// ── Team KPIs ─────────────────────────────────────────────────────────────────
function renderTeamKpis(){
  const level=sel('tk-datelevel')||'Week';
  const roles=selMulti('tk-role');
  const csms=selMulti('tk-csm');
  const rows=filterBase(allRows,'tk')
    .filter(r=>roles.size>0?roles.has(r.ROLE):true)
    .filter(r=>csms.size>0?csms.has(r.FULL_NAME):true);

  const tt=uniqIds(rows,()=>true);
  const bk=uniqIds(rows,r=>BOOKING_TYPES.has(r.TYPE));
  const ooh=uniqIds(rows,r=>OOH_TYPES.has(r.TYPE)&&!leftedEarly(r)&&callSt(r)==='Complete');
  const tcb=uniqIds(rows,r=>TOTAL_COMPLETED_BOOKING_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
  const succ=ooh+tcb;
  kpiCards('tk-kpis',[
    [tt.toLocaleString(),'# Total Tickets'],
    [bk.toLocaleString(),'Total New Bookings'],
    [succ.toLocaleString(),'Total Successful Engagements'],
    [tt>0?(succ/tt*100).toFixed(1)+'%':'-','Total Completed %'],
  ]);

  const selectedDates=selMulti('tk-date');
  const seen=new Set();
  rows.forEach(r=>{const t=getPeriodKey(r,level);if(t)seen.add(t);});
  const periods=[...seen].sort().reverse().slice(0,70);
  if(!periods.length){document.getElementById('tk-table').innerHTML='<p class="empty-note">No data.</p>';return;}

  const metrics=[
    ['# Total Tickets',      g=>uniqIds(g,()=>true)],
    ['Total New Bookings',   g=>uniqIds(g,r=>BOOKING_TYPES.has(r.TYPE))],
    ['Total Successful Engagements', g=>{
      const o=uniqIds(g,r=>OOH_TYPES.has(r.TYPE)&&!leftedEarly(r)&&callSt(r)==='Complete');
      const c=uniqIds(g,r=>TOTAL_COMPLETED_BOOKING_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
      return o+c;
    }],
    ['Completed OOH Calls',  g=>uniqIds(g,r=>OOH_TYPES.has(r.TYPE)&&!leftedEarly(r)&&callSt(r)==='Complete')],
    ['Completed Product Calls', g=>uniqIds(g,r=>callSt(r)==='Complete'&&!COMPLETED_PRODUCT_EXCLUDED.has(r.TYPE))],
    ['Total $497 OB (Booked + Frontline) Completed', g=>uniqIds(g,r=>OB_497_TYPES.has(r.TYPE)&&callSt(r)==='Complete')],
    ['Total Completed %',    g=>{
      const t=uniqIds(g,()=>true);if(!t)return'-';
      const o=uniqIds(g,r=>OOH_TYPES.has(r.TYPE)&&!leftedEarly(r)&&callSt(r)==='Complete');
      const c=uniqIds(g,r=>TOTAL_COMPLETED_BOOKING_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
      return((o+c)/t*100).toFixed(2)+'%';
    }],
    ['Total Rescheduled',    g=>uniqIds(g,r=>callSt(r)==='Rescheduled')],
    ['Total Rescheduled %',  g=>{const b=uniqIds(g,r=>BOOKING_TYPES.has(r.TYPE));if(!b)return'-';return(uniqIds(g,r=>callSt(r)==='Rescheduled')/b*100).toFixed(2)+'%';}],
    ['Total Cancelled',      g=>uniqIds(g,r=>callSt(r)==='Cancelled')],
    ['Total Cancelled %',    g=>{const b=uniqIds(g,r=>BOOKING_TYPES.has(r.TYPE));if(!b)return'-';return(uniqIds(g,r=>callSt(r)==='Cancelled')/b*100).toFixed(2)+'%';}],
    ['Total No-show',        g=>uniqIds(g,r=>callSt(r)==='No Show')],
    ['Total No-show %',      g=>{const b=uniqIds(g,r=>BOOKING_TYPES.has(r.TYPE));if(!b)return'-';return(uniqIds(g,r=>callSt(r)==='No Show')/b*100).toFixed(2)+'%';}],
    ['No-Status',            g=>uniqIds(g,r=>!callSt(r).trim())],
    ['Total Booked CSAT',    g=>uniqIds(g,r=>r.HAS_SURVEY)],
    ['Avg. Booked CSAT',     g=>{const rs=g.filter(r=>r.HAS_CSAT_RESP);if(!rs.length)return'-';return(rs.reduce((s,r)=>s+(r.BOOKED_CSAT||0),0)/rs.length/10).toFixed(1);}],
  ];

  const byPeriod={};
  periods.forEach(p=>{
    const g=rows.filter(r=>getPeriodKey(r,level)===p);
    byPeriod[p]=metrics.map(([,fn])=>fn(g));
  });

  let html='<table><thead><tr><th>Metric</th>'+periods.map(p=>\`<th>\${fmtDate(p,level)}</th>\`).join('')+'</tr></thead><tbody>';
  const tkStatusByIdx={7:['Rescheduled'],9:['Cancelled'],11:['No Show']};
  const tkDrillRows=new Set([0,1,2,3,4,5,7,9,11,13,14,16]);
  metrics.forEach(([name],i)=>{
    html+=\`<tr><td style="font-weight:600;background:#f7fafc;white-space:nowrap">\${name}</td>\`+
      periods.map(p=>{
        const v=byPeriod[p][i];
        const isDrill=tkDrillRows.has(i)&&v&&v!=='-'&&v!=='0'&&v!==0&&!String(v).includes('%');
        if(!isDrill)return \`<td>\${v}</td>\`;
        const esc=JSON.stringify([...selMulti('tk-manager')]).replace(/"/g,"'");
        const cesc=JSON.stringify([...selMulti('tk-csm')]).replace(/"/g,"'");
        let extra='';
        if(tkStatusByIdx[i]) extra=\`,statuses:['\`+tkStatusByIdx[i][0]+\`']\`;
        else if(i===13)      extra=',noStatus:true';
        return \`<td class="drill" onclick="drillToTickets({srcPrefix:'tk',period:'\${p}',periodLevel:'\${level}',mgrList:\${esc},csmList:\${cesc}\${extra}})" title="Open Tickets Info">\${v}</td>\`;
      }).join('')+'</tr>';
  });
  html+='</tbody></table>';
  document.getElementById('tk-table').innerHTML=html;
}

// ── CSM Productivity ──────────────────────────────────────────────────────────
function renderProductivity(){
  const level=sel('cp-datelevel')||'Week';
  let rows=filterBase(allRows,'cp');
  const csms=selMulti('cp-csm'),shifts=selMulti('cp-shift'),roles=selMulti('cp-role');
  if(csms.size>0)rows=rows.filter(r=>csms.has(r.FULL_NAME||r.FRESHDESK_AGENT_NAME));
  if(shifts.size>0)rows=rows.filter(r=>shifts.has(r.SHIFT||'Null'));
  if(roles.size>0)rows=rows.filter(r=>roles.has(r.ROLE));

  const tt=uniqIds(rows,()=>true);
  const bk=uniqIds(rows,r=>BOOKING_TYPES.has(r.TYPE));
  const ooh=uniqIds(rows,r=>OOH_TYPES.has(r.TYPE)&&!leftedEarly(r)&&callSt(r)==='Complete');
  const tcb=uniqIds(rows,r=>TOTAL_COMPLETED_BOOKING_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
  const succ=ooh+tcb;
  kpiCards('cp-kpis',[
    [tt.toLocaleString(),'# Total Tickets'],
    [succ.toLocaleString(),'Total Successful Engagements'],
    [bk.toLocaleString(),'Total New Bookings'],
    [tt>0?(succ/tt*100).toFixed(1)+'%':'-','Total Completed %'],
  ]);

  const agentMap={};
  rows.forEach(r=>{
    const agent=r.FULL_NAME||r.FRESHDESK_AGENT_NAME||'Unknown';
    const mgr=r.MANAGER||'';const sh=r.SHIFT||'Null';
    const dl=getPeriodKey(r,level);
    const key=mgr+'||'+agent+'||'+dl+'||'+sh;
    if(!agentMap[key])agentMap[key]={mgr,agent,dl,sh,rows:[]};
    agentMap[key].rows.push(r);
  });

  const COLS=['MANAGER','Agent Name','Date','Shift','# Total Tickets','Total Successful','Total New Bookings','$497 OB Completed','Product Calls','OOH Calls','No-show','No-show %','Rescheduled','Rescheduled %','Cancelled','Cancelled %','No-Status','Total Booked CSAT','Avg. Booked CSAT'];
  const tableRows=Object.values(agentMap).sort((a,b)=>(a.mgr+a.agent+a.dl).localeCompare(b.mgr+b.agent+b.dl));
  if(!tableRows.length){document.getElementById('cp-table').innerHTML='<p class="empty-note">No data.</p>';return;}

  let html='<table><thead><tr>'+COLS.map(c=>\`<th>\${c}</th>\`).join('')+'</tr></thead><tbody>';
  tableRows.forEach(({mgr,agent,dl,sh,rows:g})=>{
    const gtt=uniqIds(g,()=>true);
    const gbk=uniqIds(g,r=>BOOKING_TYPES.has(r.TYPE));
    const gooh=uniqIds(g,r=>OOH_TYPES.has(r.TYPE)&&!leftedEarly(r)&&callSt(r)==='Complete');
    const gtcb=uniqIds(g,r=>TOTAL_COMPLETED_BOOKING_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
    const gsucc=gooh+gtcb;
    const gcp=uniqIds(g,r=>callSt(r)==='Complete'&&!COMPLETED_PRODUCT_EXCLUDED.has(r.TYPE));
    const gob=uniqIds(g,r=>OB_497_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
    const gns=uniqIds(g,r=>callSt(r)==='No Show');
    const grs=uniqIds(g,r=>callSt(r)==='Rescheduled');
    const gca=uniqIds(g,r=>callSt(r)==='Cancelled');
    const gnost=uniqIds(g,r=>!callSt(r).trim());
    // CSAT
    const csatRows=g.filter(r=>r.BOOKED_CSAT!=null&&parseFloat(r.BOOKED_CSAT)>0);
    const totalCsat=new Set(csatRows.map(r=>r.ID)).size;
    const avgCsat=csatRows.length?(csatRows.reduce((s,r)=>s+parseFloat(r.BOOKED_CSAT),0)/csatRows.length).toFixed(2):'';
    const gTotalCsat=new Set(g.filter(r=>r.HAS_SURVEY).map(r=>r.ID)).size;  // surveys sent
    const csatAnswered=g.filter(r=>r.HAS_CSAT_RESP);
    const gAvgCsat=csatAnswered.length>0?(csatAnswered.reduce((s,r)=>s+(r.BOOKED_CSAT||0),0)/csatAnswered.length/10).toFixed(1):'-';
    const vals=[mgr,agent,fmtDate(dl,level),sh,gtt,gsucc,gbk,gob,gcp,gooh,
      gns,gbk?(gns/gbk*100).toFixed(1)+'%':'-',
      grs,gbk?(grs/gbk*100).toFixed(1)+'%':'-',
      gca,gbk?(gca/gbk*100).toFixed(1)+'%':'-',
      gnost,gTotalCsat,gAvgCsat];
    // Indices 4+ are ticket counts → make clickable
    const drillIdxs=new Set([4,5,6,7,8,9,10,12,14,16]);
    const statusByIdx={10:['No Show'],12:['Rescheduled'],14:['Cancelled']};
    html+='<tr>'+vals.map((v,idx)=>{
      const isNum=drillIdxs.has(idx)&&v&&v!=='-'&&v!==0&&v!=='0'&&!String(v).includes('%')&&idx<=16;
      if(!isNum)return \`<td>\${v??''}</td>\`;
      const spec={srcPrefix:'cp',period:dl,periodLevel:level,csm:agent,manager:mgr};
      if(statusByIdx[idx])spec.statuses=statusByIdx[idx];
      if(idx===16)spec.noStatus=true;
      return \`<td class="drill" onclick="drillToTickets(\${JSON.stringify(spec).replace(/"/g,\"'\")})" title="Open Tickets Info">\${v}</td>\`;
    }).join('')+'</tr>';
  });
  html+='</tbody></table>';
  document.getElementById('cp-table').innerHTML=html;
}

// ── Product Analytics ─────────────────────────────────────────────────────────
function renderProductAnalytics(){
  const level=sel('pa-datelevel')||'Week';
  const paCsms=selMulti('pa-csm'), paRoles=selMulti('pa-role');
  const rows=filterBase(allRows,'pa')
    .filter(r=>paCsms.size>0?paCsms.has(r.FULL_NAME):true)
    .filter(r=>paRoles.size>0?paRoles.has(r.ROLE):true);

  const tt=uniqIds(rows,()=>true);
  const bookedProd=uniqIds(rows,r=>PRODUCT_CALL_TYPES.has(r.TYPE));
  const compProd=uniqIds(rows,r=>PRODUCT_CALL_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
  kpiCards('pa-kpis',[
    [tt.toLocaleString(),'# Total Tickets'],
    [bookedProd.toLocaleString(),'Booked Product Calls'],
    [compProd.toLocaleString(),'Completed Product Calls'],
    [bookedProd>0?(compProd/bookedProd*100).toFixed(1)+'%':'-','Product Completion %'],
  ]);

  const seen=new Set();
  rows.forEach(r=>{const t=getPeriodKey(r,level);if(t)seen.add(t);});
  const periods=[...seen].sort().reverse().slice(0,70);
  if(!periods.length){
    document.getElementById('pa-team-table').innerHTML='<p class="empty-note">No data.</p>';
    return;
  }

  // ── Team Product Stats table (metric rows × period cols) ──────────────────
  const teamMetrics=[
    ['# Total Tickets',          g=>uniqIds(g,()=>true)],
    ['Total Successful Engagements', g=>{
      const o=uniqIds(g,r=>OOH_TYPES.has(r.TYPE)&&!leftedEarly(r));
      const c=uniqIds(g,r=>TOTAL_COMPLETED_BOOKING_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
      return o+c;
    }],
    ['Total New Bookings',        g=>uniqIds(g,r=>BOOKING_TYPES.has(r.TYPE))],
    ['Booked Prod Calls',         g=>uniqIds(g,r=>PRODUCT_CALL_TYPES.has(r.TYPE))],
    ['% Product Calls',           g=>{const t=uniqIds(g,()=>true);if(!t)return'-';return(uniqIds(g,r=>PRODUCT_CALL_TYPES.has(r.TYPE))/t*100).toFixed(2)+'%';}],
    ['Completed Prod Calls',      g=>uniqIds(g,r=>PRODUCT_CALL_TYPES.has(r.TYPE)&&callSt(r)==='Complete')],
    ['Completed Prod Calls %',    g=>{const b=uniqIds(g,r=>PRODUCT_CALL_TYPES.has(r.TYPE));if(!b)return'-';return(uniqIds(g,r=>PRODUCT_CALL_TYPES.has(r.TYPE)&&callSt(r)==='Complete')/b*100).toFixed(2)+'%';}],
    ['Total No-show',             g=>uniqIds(g,r=>callSt(r)==='No Show')],
    ['Total No-show %',           g=>{const b=uniqIds(g,r=>BOOKING_TYPES.has(r.TYPE));if(!b)return'-';return(uniqIds(g,r=>callSt(r)==='No Show')/b*100).toFixed(2)+'%';}],
    ...PRODUCT_LINES.map(([label,type])=>[\`\${label}\`,g=>uniqIds(g,r=>r.TYPE===type)]),
    ...PRODUCT_LINES.map(([label,type])=>[\`\${label} Completed %\`,g=>{const b=uniqIds(g,r=>r.TYPE===type);if(!b)return'-';return(uniqIds(g,r=>r.TYPE===type&&callSt(r)==='Complete')/b*100).toFixed(2)+'%';}]),
  ];

  const byP={};
  periods.forEach(p=>{
    const g=rows.filter(r=>getPeriodKey(r,level)===p);
    byP[p]=teamMetrics.map(([,fn])=>fn(g));
  });

  let html='<table><thead><tr><th></th>'+periods.map(p=>\`<th>\${fmtDate(p,level)}</th>\`).join('')+'</tr></thead><tbody>';
  teamMetrics.forEach(([name],i)=>{
    html+=\`<tr><td style="font-weight:600;background:#f7fafc;white-space:nowrap">\${name}</td>\`+
      periods.map(p=>\`<td>\${byP[p][i]}</td>\`).join('')+'</tr>';
  });
  html+='</tbody></table>';
  document.getElementById('pa-team-table').innerHTML=html;

  // ── CSM Product Calls table ───────────────────────────────────────────────
  const agentMap={};
  rows.forEach(r=>{
    const agent=r.FULL_NAME||r.FRESHDESK_AGENT_NAME||'Unknown';
    const mgr=r.MANAGER||'';
    const dl=getPeriodKey(r,level);
    const key=mgr+'||'+agent+'||'+dl;
    if(!agentMap[key])agentMap[key]={mgr,agent,dl,rows:[]};
    agentMap[key].rows.push(r);
  });
  const CCOLS=['MANAGER','Agent Name','Day of Period','# Total Tickets','Total Successful Eng.','Total New Bookings','Booked Prod Calls','% Product Calls','Completed Prod Calls','Completed Prod %','Total No-show'];
  const aRows=Object.values(agentMap).sort((a,b)=>(a.mgr+a.agent+a.dl).localeCompare(b.mgr+b.agent+b.dl));
  let chtml='<table><thead><tr>'+CCOLS.map(c=>\`<th>\${c}</th>\`).join('')+'</tr></thead><tbody>';
  aRows.forEach(({mgr,agent,dl,rows:g})=>{
    const gtt=uniqIds(g,()=>true);
    const gbk=uniqIds(g,r=>BOOKING_TYPES.has(r.TYPE));
    const gprod=uniqIds(g,r=>PRODUCT_CALL_TYPES.has(r.TYPE));
    const gcomp=uniqIds(g,r=>PRODUCT_CALL_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
    const gooh=uniqIds(g,r=>OOH_TYPES.has(r.TYPE)&&!leftedEarly(r)&&callSt(r)==='Complete');
    const gtcb=uniqIds(g,r=>TOTAL_COMPLETED_BOOKING_TYPES.has(r.TYPE)&&callSt(r)==='Complete');
    const gns=uniqIds(g,r=>callSt(r)==='No Show');
    const pctProd=gtt>0?(gprod/gtt*100).toFixed(2)+'%':'-';
    const pctComp=gprod>0?(gcomp/gprod*100).toFixed(2)+'%':'-';
    const vals=[mgr,agent,fmtDate(dl,level),gtt,gooh+gtcb,gbk,gprod,pctProd,gcomp,pctComp,gns];
    chtml+='<tr>'+vals.map(v=>\`<td>\${v??''}</td>\`).join('')+'</tr>';
  });
  chtml+='</tbody></table>';
  document.getElementById('pa-csm-table').innerHTML=chtml;
}

// ── Call Volume Heatmap ───────────────────────────────────────────────────────
// Layout matches original: rows = periods, columns = hours 0-23, last col = Total
function renderHeatmap(){
  const level=sel('hm-datelevel')||'Week';
  const hmCsms=selMulti('hm-csm'), hmTypes=selMulti('hm-type');
  const rows=filterBase(allRows,'hm')
    .filter(r=>hmCsms.size>0?hmCsms.has(r.FULL_NAME):true)
    .filter(r=>hmTypes.size>0?hmTypes.has(r.TYPE):true);

  const seen=new Set();
  rows.forEach(r=>{const t=getPeriodKey(r,level);if(t)seen.add(t);});
  const periods=[...seen].sort().reverse().slice(0,70);
  const hours=Array.from({length:24},(_,i)=>i);

  if(!periods.length){document.getElementById('hm-table').innerHTML='<p class="empty-note">No data.</p>';return;}

  // counts[period][hour]
  const counts={};
  periods.forEach(p=>{counts[p]={};hours.forEach(h=>{counts[p][h]=0;});});
  rows.forEach(r=>{
    const h=parseInt(r.HOUR_OF_DAY||0);
    const p=getPeriodKey(r,level);
    if(counts[p]&&counts[p][h]!==undefined)counts[p][h]++;
  });

  // row totals
  const rowTotals={};
  periods.forEach(p=>{rowTotals[p]=hours.reduce((s,h)=>s+counts[p][h],0);});

  // max for colour scale
  let maxVal=1;
  periods.forEach(p=>hours.forEach(h=>{if(counts[p][h]>maxVal)maxVal=counts[p][h];}));

  const heatColor=v=>{
    if(!v)return'#f7fafc';
    const pct=v/maxVal;
    const r=Math.round(255-(255-56)*pct);
    const g=Math.round(255-(255-161)*pct);
    const b=Math.round(255-(255-105)*pct);
    return\`rgb(\${r},\${g},\${b})\`;
  };

  const hLabel=h=>h===0?'0':String(h);

  let html='<table class="hm-table"><thead><tr><th></th>'+hours.map(h=>\`<th>\${hLabel(h)}</th>\`).join('')+'<th class="hm-total">Total</th></tr></thead><tbody>';
  periods.forEach(p=>{
    html+=\`<tr><td style="font-weight:600;background:#f7fafc;white-space:nowrap">\${fmtDate(p,level)}</td>\`;
    hours.forEach(h=>{
      const v=counts[p][h];
      html+=\`<td style="background:\${heatColor(v)}">\${v||''}</td>\`;
    });
    html+=\`<td class="hm-total">\${rowTotals[p]}</td></tr>\`;
  });
  html+='</tbody></table>';
  document.getElementById('hm-table').innerHTML=html;
}

// ── Tickets Info ──────────────────────────────────────────────────────────────
function renderTickets(){
  const level=sel('ti-datelevel')||'Week';
  let rows=filterBase(allRows,'ti');
  const csms=selMulti('ti-csm'),types=selMulti('ti-type'),statuses=selMulti('ti-status');
  if(csms.size>0)rows=rows.filter(r=>csms.has(r.FULL_NAME||r.FRESHDESK_AGENT_NAME));
  if(types.size>0)rows=rows.filter(r=>types.has(r.TYPE));
  if(statuses.size>0)rows=rows.filter(r=>{
    const st=callSt(r);
    const label=st&&st.trim()?st:'No Status';
    return statuses.has(label);
  });

  const tt=uniqIds(rows,()=>true);
  const bk=uniqIds(rows,r=>BOOKING_TYPES.has(r.TYPE));
  const succ=uniqIds(rows,r=>callSt(r)==='Complete');
  kpiCards('ti-kpis',[
    [tt.toLocaleString(),'# Total Tickets'],
    [succ.toLocaleString(),'Total Completed'],
    [bk.toLocaleString(),'Total New Bookings'],
    [rows.length.toLocaleString(),'Rows Shown'],
  ]);

  if(!rows.length){document.getElementById('ti-table').innerHTML='<p class="empty-note">No data.</p>';return;}

  const LIMIT=2500;
  const COLS=['Ticket ID','Agent Name','Manager','Role','Type','Call Status','Date','Relationship #','Email','Call Source','Plan Level','Country'];
  let html='<table><thead><tr>'+COLS.map(c=>\`<th>\${c}</th>\`).join('')+'</tr></thead><tbody>';
  rows.slice(0,LIMIT).forEach(r=>{
    const dt=toDateStr(r.CREATED_AT_RAW||r.CREATED_AT_NEW);
    html+='<tr>'+
      \`<td>\${r.ID||''}</td>\`+
      \`<td>\${r.FULL_NAME||r.FRESHDESK_AGENT_NAME||''}</td>\`+
      \`<td>\${r.MANAGER||''}</td>\`+
      \`<td>\${r.ROLE||''}</td>\`+
      \`<td>\${r.TYPE||''}</td>\`+
      \`<td>\${callSt(r)}</td>\`+
      \`<td>\${dt}</td>\`+
      \`<td>\${r.CUSTOM_CF_RELATIONSHIP_NUMBER||''}</td>\`+
      \`<td>\${r.CUSTOM_EMAIL||''}</td>\`+
      \`<td>\${r.CALL_SOURCE2||''}</td>\`+
      \`<td>\${r.COMPANY_PLAN_LEVEL||r.PLAN_LEVEL||''}</td>\`+
      \`<td>\${r.COMPANY_COUNTRY||''}</td>\`+
      '</tr>';
  });
  if(rows.length>LIMIT)html+=\`<tr><td colspan="12" style="text-align:center;color:#718096;padding:8px">Showing \${LIMIT} of \${rows.length} rows</td></tr>\`;
  html+='</tbody></table>';
  document.getElementById('ti-table').innerHTML=html;
}

// ── Drill-down: click a cell → open Tickets Info with matching filters ────────
// drillSpec: { manager?, csm?, types?:[], statuses?:[], period?, periodLevel? }
function drillToTickets(spec){
  // Reset all Tickets Info filters
  ['ti-manager','ti-csm','ti-type','ti-status'].forEach(k=>msState[k]=new Set());

  // Manager: prefer mgrList (Team KPIs), fallback to single manager (CP/PA)
  if(spec.mgrList&&spec.mgrList.length) msState['ti-manager']=new Set(spec.mgrList);
  else if(spec.manager)                 msState['ti-manager']=new Set([spec.manager]);

  // CSM: prefer csmList, fallback to single csm
  if(spec.csmList&&spec.csmList.length) msState['ti-csm']=new Set(spec.csmList);
  else if(spec.csm)                     msState['ti-csm']=new Set([spec.csm]);

  // Types
  if(spec.types&&spec.types.length) msState['ti-type']=new Set(spec.types);

  // Statuses — 'No Status' sentinel maps to the empty-string callSt rows
  if(spec.noStatus){
    msState['ti-status']=new Set(['No Status']);
  } else if(spec.statuses&&spec.statuses.length){
    msState['ti-status']=new Set(spec.statuses);
  }

  // Date: set rdp to single-period range so only that period shows
  const level=spec.periodLevel||'Week';
  document.getElementById('ti-datelevel').value=level;
  if(spec.period){
    rdpState['ti']={mode:'range',n:8,unit:level,from:spec.period,to:spec.period};
  } else {
    // Mirror source tab date state
    const src=spec.srcPrefix||'tk';
    rdpState['ti']={...(rdpState[src]||{mode:'last',n:8,unit:'Week',from:'',to:''}),unit:level};
  }

  // Switch to Tickets tab
  const btn=document.querySelector('[onclick*="tickets"]');
  switchTab('tickets',btn);
  populateFilters();
  renderTickets();
}

function renderAll(){
  if(!allRows.length)return;
  populateFilters();
  renderTeamKpis();
  renderProductivity();
  renderProductAnalytics();
  renderHeatmap();
  renderTickets();
}

async function loadData(force=false){
  const btn=document.getElementById('refreshBtn');
  const bar=document.getElementById('statusBar');
  const msg=document.getElementById('statusMsg');
  const fat=document.getElementById('fetchedAt');
  btn.disabled=true;btn.textContent='↻ Loading…';
  bar.style.display='flex';msg.textContent='Fetching all rows from Snowflake via MCP…';
  try{
    const resp=await fetch(force?'/api/data?refresh=1':'/api/data');
    if(!resp.ok)throw new Error(await resp.text());
    const json=await resp.json();
    if(json.error)throw new Error(json.error);
    allRows=(json.rows||[]).map(r=>{
      const out={};Object.keys(r).forEach(k=>out[k.toUpperCase()]=r[k]);return out;
    });
    fat.textContent='Fetched at '+new Date(json.fetchedAt).toLocaleTimeString()+' · '+allRows.length+' rows';
    msg.textContent='Loaded '+allRows.length+' rows OK.';
    renderAll();
  }catch(e){
    msg.innerHTML='<span class="error-note">Error: '+e.message+'</span>';
  }finally{
    btn.disabled=false;btn.textContent='↻ Refresh';
  }
}

window.addEventListener('load',()=>loadData(false));
</script>
</body>
</html>`;}
