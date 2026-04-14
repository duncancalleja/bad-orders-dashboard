from __future__ import annotations

import argparse
import datetime as dt
import html as html_lib
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd


def _ensure_dbx_on_path() -> None:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    dbx_dir = os.path.join(repo_root, "databricks-setup")
    if dbx_dir not in sys.path:
        sys.path.insert(0, dbx_dir)


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Expected YYYY-MM-DD.") from e


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (dt.datetime, dt.date)):
        # Keep sortable format for client-side filtering/sorting.
        if isinstance(value, dt.datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return value.isoformat()
    # pandas / numpy scalars
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    # Decimal or other numeric types
    try:
        import decimal

        if isinstance(value, decimal.Decimal):
            return float(value)
    except Exception:
        pass
    return str(value)


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    # Replace NaN with None to keep JSON clean
    clean = df.where(pd.notnull(df), None)
    out: list[dict[str, Any]] = []
    for rec in clean.to_dict(orient="records"):
        out.append({k: _to_jsonable(v) for k, v in rec.items()})
    return out


def _month_label(month_yyyy_mm: str) -> str:
    d = dt.date.fromisoformat(month_yyyy_mm + "-01")
    return d.strftime("%b %Y")


def _months_between(start_date: dt.date, end_date_inclusive: dt.date) -> list[str]:
    start_month = dt.date(start_date.year, start_date.month, 1)
    end_month = dt.date(end_date_inclusive.year, end_date_inclusive.month, 1)
    out: list[str] = []
    cur = start_month
    while cur <= end_month:
        out.append(cur.strftime("%Y-%m"))
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)
    return out


def _slug(value: str) -> str:
    s = value.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "dashboard"


def _default_output_path(
    *,
    country_code: str,
    start_date: dt.date,
    end_date_inclusive: dt.date,
    year: Optional[int],
    vendor_names: Optional[list[str]],
    vendor_ids: Optional[list[int]],
) -> str:
    docs_dir = os.path.expanduser("~/Documents")
    preferred_dir = os.path.join(docs_dir, "Bad orders")
    base_dir = preferred_dir if os.path.isdir(preferred_dir) else docs_dir
    scope = "all_accounts"
    if vendor_names:
        scope = _slug("_".join(vendor_names[:3]))
        if len(vendor_names) > 3:
            scope += f"_plus_{len(vendor_names) - 3}"
    elif vendor_ids:
        scope = "vendor_ids_" + "_".join(str(int(x)) for x in vendor_ids[:3])
        if len(vendor_ids) > 3:
            scope += f"_plus_{len(vendor_ids) - 3}"

    if year is not None:
        filename = f"{country_code.upper()}_bad_orders_{year}_{scope}.html"
    else:
        filename = f"{country_code.upper()}_bad_orders_{start_date.isoformat()}_{end_date_inclusive.isoformat()}_{scope}.html"
    return os.path.join(base_dir, filename)


def _safe_rate(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def _build_trend_records(
    *,
    months: list[str],
    placed_by_month: dict[str, int],
    rejected_by_month: dict[str, int],
    dnr_by_month: dict[str, int],
    bad_by_month: dict[str, int],
    late15_by_month: dict[str, int],
    missing_by_month: dict[str, int],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in months:
        placed = int(placed_by_month.get(m, 0) or 0)
        rejected = int(rejected_by_month.get(m, 0) or 0)
        dnr = int(dnr_by_month.get(m, 0) or 0)
        bad = int(bad_by_month.get(m, 0) or 0)
        late15 = int(late15_by_month.get(m, 0) or 0)
        missing = int(missing_by_month.get(m, 0) or 0)
        out.append(
            {
                "month": m,
                "bad_rate": _safe_rate(bad, placed),
                "not_delivered_rate": _safe_rate(rejected + dnr, placed),
                "late_15_rate": _safe_rate(late15, placed),
                "missing_wrong_rate": _safe_rate(missing, placed),
            }
        )
    return out


@dataclass(frozen=True)
class Filters:
    country_code: str
    start_date: dt.date
    end_date_inclusive: dt.date
    vendor_ids: Optional[list[int]] = None
    vendor_names: Optional[list[str]] = None

    @property
    def end_date_exclusive(self) -> dt.date:
        return self.end_date_inclusive + dt.timedelta(days=1)


def _vendor_filter_sql(filters: Filters, table_alias: str = "d") -> str:
    t = table_alias
    if filters.vendor_ids:
        # Keep IN list sizes reasonable; vendor_id is int.
        ids = ",".join(str(int(x)) for x in filters.vendor_ids)
        return f" AND {t}.vendor_id IN ({ids})\n"
    if filters.vendor_names:
        names = ",".join(_sql_quote(x) for x in filters.vendor_names)
        return f" AND TRIM({t}.vendor_name) IN ({names})\n"
    return ""


def _from_order_with_provider_sql() -> str:
    """Join orders to merchant dimension for AM owner (account manager name)."""
    return (
        "FROM ng_delivery_spark.dim_order_delivery d\n"
        "LEFT JOIN ng_delivery_spark.dim_provider_v2 p ON d.provider_id = p.provider_id\n"
    )


def _base_where_sql(filters: Filters, table_alias: str = "d") -> str:
    t = table_alias
    return (
        "WHERE 1=1\n"
        f"  AND {t}.country_code = {_sql_quote(filters.country_code.lower())}\n"
        f"  AND {t}.order_created_date_local >= {_sql_quote(filters.start_date.isoformat())}\n"
        f"  AND {t}.order_created_date_local < {_sql_quote(filters.end_date_exclusive.isoformat())}\n"
        f"  AND {t}.order_state IN ('delivered','failed','rejected')\n"
        + _vendor_filter_sql(filters, table_alias)
    )


def _bad_orders_where_sql(filters: Filters) -> str:
    return (
        _base_where_sql(filters)
        + "  AND d.is_bad_order = true\n"
        + "  AND d.bad_order_actor_at_fault = 'provider'\n"
    )


def _am_owner_sql() -> str:
    """Account manager (person) from merchant master."""
    return "COALESCE(NULLIF(TRIM(p.account_manager_name), ''), 'Unknown')"


# Malta Food: preferred dropdown order (must match dim_provider_v2.account_manager_name).
_MT_AM_OWNER_PRIMARY_ORDER: tuple[str, ...] = (
    "Yousef Moungad",
    "Alena Tokareva",
    "Gulcin Erguven",
    "Duncan Calleja",
)


def _am_owner_dropdown_list(am_values: set[str], country_code: str) -> list[str]:
    """Order AM owner options: primary roster first, then remaining names A–Z, then Unknown."""
    cc = country_code.lower()
    primary = _MT_AM_OWNER_PRIMARY_ORDER if cc == "mt" else ()
    seen: set[str] = set()
    out: list[str] = []
    for name in primary:
        if name in am_values and name not in seen:
            out.append(name)
            seen.add(name)
    rest = sorted((x for x in am_values if x not in seen and x != "Unknown"), key=lambda s: str(s).lower())
    out.extend(rest)
    if "Unknown" in am_values:
        out.append("Unknown")
    return out


def _reason_sql() -> str:
    # Keep logic aligned with existing Dr Juice dashboard expectations.
    return """
CASE bad_order_type
  WHEN 'failed_order_provider_rejected' THEN COALESCE(failed_order_parent_reason, manually_failed_order_reason, failed_order_reason, bad_order_main_reason, 'unknown')
  WHEN 'failed_order_after_provider_accepted' THEN COALESCE(manually_failed_order_reason, failed_order_parent_reason, failed_order_reason, bad_order_main_reason, 'unknown')
  WHEN 'late_delivery_order_15min' THEN COALESCE(late_delivery_actor_at_fault_reason, bad_order_main_reason, 'unknown')
  WHEN 'missing_or_wrong_item_cs_ticket' THEN COALESCE(missing_or_wrong_items_cs_ticket_types, bad_order_main_reason, 'unknown')
  WHEN 'order_quality_cs_ticket' THEN COALESCE(order_quality_cs_ticket_types, bad_order_main_reason, 'unknown')
  WHEN 'timing_quality_cs_ticket' THEN COALESCE(timing_quality_cs_ticket_types, bad_order_main_reason, 'unknown')
  ELSE COALESCE(bad_order_main_reason, 'unknown')
END
""".strip()


def _query_accounts_sql(filters: Filters) -> str:
    return (
        "SELECT\n"
        "  COALESCE(NULLIF(TRIM(d.vendor_name), ''), 'Unknown') AS cohort,\n"
        "  COALESCE(NULLIF(TRIM(d.vendor_name), ''), 'Unknown') AS vendor_name\n"
        + _from_order_with_provider_sql()
        + _base_where_sql(filters)
        + "GROUP BY COALESCE(NULLIF(TRIM(d.vendor_name), ''), 'Unknown')\n"
        + "ORDER BY cohort\n"
    )


def _query_detail_rows_sql(filters: Filters) -> str:
    reason = _reason_sql()
    # Qualify columns with alias `d.` for join. `failed_order_reason` is a suffix of
    # `manually_failed_order_reason`, so we must not substring-replace in the wrong order.
    _ph = "__MANUALLY_FAILED_ORDER_REASON__"
    reason_d = reason.replace("manually_failed_order_reason", _ph)
    reason_d = reason_d.replace("bad_order_type", "d.bad_order_type")
    for col in (
        "failed_order_parent_reason",
        "failed_order_reason",
        "bad_order_main_reason",
        "late_delivery_actor_at_fault_reason",
        "missing_or_wrong_items_cs_ticket_types",
        "order_quality_cs_ticket_types",
        "timing_quality_cs_ticket_types",
    ):
        reason_d = reason_d.replace(col, f"d.{col}")
    reason_d = reason_d.replace(_ph, "d.manually_failed_order_reason")
    return (
        "SELECT\n"
        "  date_format(d.order_created_ts_local, 'yyyy-MM-dd HH:mm:ss') AS time,\n"
        "  date_format(d.order_created_date_local, 'yyyy-MM') AS month,\n"
        "  d.order_reference_id AS order_ref,\n"
        "  d.provider_name AS provider,\n"
        "  COALESCE(NULLIF(TRIM(d.vendor_name), ''), 'Unknown') AS cohort,\n"
        f"  {_am_owner_sql()} AS am_owner,\n"
        "  d.bad_order_type AS type,\n"
        f"  {reason_d} AS reason\n"
        + _from_order_with_provider_sql()
        + _bad_orders_where_sql(filters)
        + "ORDER BY d.order_created_ts_local DESC\n"
    )


def _query_kpi_by_provider_sql(filters: Filters) -> str:
    return (
        "WITH base AS (\n"
        "  SELECT\n"
        "    d.provider_name AS provider,\n"
        "    COALESCE(NULLIF(TRIM(d.vendor_name), ''), 'Unknown') AS cohort,\n"
        f"    {_am_owner_sql()} AS am_owner,\n"
        "    COUNT(*) AS placed_orders,\n"
        "    SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' THEN 1 ELSE 0 END) AS bad_orders,\n"
        "    SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' AND d.bad_order_type = 'failed_order_provider_rejected' THEN 1 ELSE 0 END) AS rejected_orders,\n"
        "    SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' AND d.bad_order_type = 'failed_order_after_provider_accepted' THEN 1 ELSE 0 END) AS dnr_orders,\n"
        "    SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' AND d.bad_order_type = 'late_delivery_order_15min' THEN 1 ELSE 0 END) AS late_15_orders,\n"
        "    SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' AND d.bad_order_type = 'missing_or_wrong_item_cs_ticket' THEN 1 ELSE 0 END) AS missing_wrong_orders,\n"
        "    SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' AND d.bad_order_type = 'order_quality_cs_ticket' THEN 1 ELSE 0 END) AS quality_ticket_orders,\n"
        "    SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' AND d.bad_order_type = 'timing_quality_cs_ticket' THEN 1 ELSE 0 END) AS timing_ticket_orders\n"
        + _from_order_with_provider_sql()
        + _base_where_sql(filters)
        + "  GROUP BY d.provider_name, COALESCE(NULLIF(TRIM(d.vendor_name), ''), 'Unknown'), "
        + _am_owner_sql()
        + "\n"
        ")\n"
        "SELECT\n"
        "  provider,\n"
        "  cohort,\n"
        "  am_owner,\n"
        "  placed_orders,\n"
        "  bad_orders,\n"
        "  CASE WHEN placed_orders > 0 THEN bad_orders / placed_orders ELSE NULL END AS bad_rate,\n"
        "  CASE WHEN placed_orders > 0 THEN rejected_orders / placed_orders ELSE NULL END AS rejected_rate,\n"
        "  CASE WHEN placed_orders > 0 THEN (rejected_orders + dnr_orders) / placed_orders ELSE NULL END AS not_delivered_rate,\n"
        "  CASE WHEN placed_orders > 0 THEN dnr_orders / placed_orders ELSE NULL END AS failed_after_accept_rate,\n"
        "  CASE WHEN placed_orders > 0 THEN quality_ticket_orders / placed_orders ELSE NULL END AS quality_ticket_rate,\n"
        "  CASE WHEN placed_orders > 0 THEN timing_ticket_orders / placed_orders ELSE NULL END AS timing_ticket_rate,\n"
        "  CASE WHEN placed_orders > 0 THEN late_15_orders / placed_orders ELSE NULL END AS late_15_rate,\n"
        "  CASE WHEN placed_orders > 0 THEN missing_wrong_orders / placed_orders ELSE NULL END AS missing_wrong_rate\n"
        "FROM base\n"
        "ORDER BY bad_orders DESC, placed_orders DESC\n"
    )


def _query_rejection_data_sql(filters: Filters) -> str:
    return (
        "SELECT\n"
        "  d.provider_name AS provider,\n"
        "  COALESCE(NULLIF(TRIM(d.vendor_name), ''), 'Unknown') AS cohort,\n"
        f"  {_am_owner_sql()} AS am_owner,\n"
        "  date_format(d.order_created_date_local, 'yyyy-MM') AS month,\n"
        "  COUNT(*) AS placed_orders,\n"
        "  SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' AND d.bad_order_type = 'failed_order_provider_rejected' THEN 1 ELSE 0 END) AS rejected,\n"
        "  SUM(CASE WHEN d.is_bad_order = true AND d.bad_order_actor_at_fault = 'provider' AND d.bad_order_type = 'failed_order_after_provider_accepted' THEN 1 ELSE 0 END) AS dnr\n"
        + _from_order_with_provider_sql()
        + _base_where_sql(filters)
        + "GROUP BY d.provider_name, COALESCE(NULLIF(TRIM(d.vendor_name), ''), 'Unknown'), "
        + _am_owner_sql()
        + ", date_format(d.order_created_date_local, 'yyyy-MM')\n"
        + "ORDER BY month DESC, rejected DESC\n"
    )


def _chart_script_tag() -> str:
    """Prefer vendored Chart.js (no CDN) for internal / Apps Script hosting."""
    vendor = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor", "chart.umd.min.js")
    if os.path.isfile(vendor):
        with open(vendor, "r", encoding="utf-8") as f:
            body = f.read().replace("</script>", "<\\/script>")
        return f"<script>\n{body}\n</script>"
    return '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>'


def _html_template(
    title: str,
    account_options_html: str,
    am_options_html: str,
    month_options_html: str,
    data_json: str,
) -> str:
    # Self-contained HTML; Chart.js from vendor/ when present (otherwise CDN fallback).
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  {_chart_script_tag()}
  <style>
    :root {{
      --bolt-green: #2A9C64; --bolt-green-dark: #1e7a4d; --bolt-bg: #ffffff;
      --bolt-card: #ffffff; --bolt-border: #e0e6e0; --bolt-muted: #607d6b;
      --bolt-text: #1a1a1a; --red: #e53935; --orange: #fb8c00;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bolt-bg); color: var(--bolt-text); }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 0 1rem 2rem; }}
    header {{ background: linear-gradient(135deg, #2A9C64 0%, #1e7a4d 100%); color: #fff; padding: 1.4rem 1.6rem; border-radius: 0 0 16px 16px; }}
    header h1 {{ font-size: 1.4rem; font-weight: 700; }}
    header .sub {{ margin-top: .25rem; opacity: .9; font-size: .85rem; }}
    .filters {{ display: flex; gap: .75rem; align-items: center; margin: 1rem 0; flex-wrap: wrap; }}
    .filters select {{ padding: .4rem .7rem; border-radius: 8px; border: 1px solid var(--bolt-border); font-size: .85rem; }}
    .tabs {{ display: flex; gap: .35rem; flex-wrap: wrap; margin-bottom: 1rem; }}
    .tab {{ padding: .45rem 1rem; border: none; background: #eef3ee; border-radius: 8px; cursor: pointer; font-size: .82rem; font-weight: 600; color: var(--bolt-muted); }}
    .tab.active {{ background: var(--bolt-green); color: #fff; }}
    .panel {{ background: var(--bolt-card); border: 1px solid var(--bolt-border); border-radius: 12px; padding: 1rem 1.2rem; margin-bottom: 1rem; }}
    .panel h2 {{ font-size: .95rem; font-weight: 700; margin-bottom: .75rem; color: var(--bolt-green-dark); }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr)); gap: .75rem; margin-bottom: 1rem; }}
    .kpi {{ background: var(--bolt-card); border: 1px solid var(--bolt-border); border-radius: 10px; padding: .75rem 1rem; text-align: center; }}
    .kpi h3 {{ font-size: .72rem; text-transform: uppercase; color: var(--bolt-muted); letter-spacing: .5px; margin-bottom: .3rem; }}
    .kpi .val {{ font-size: 1.35rem; font-weight: 700; color: var(--bolt-text); }}
    .kpi .val.bad {{ color: var(--red); }}
    .kpi .val.warn {{ color: var(--orange); }}
    .grid2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }}
    @media (max-width: 900px) {{ .grid2 {{ grid-template-columns: 1fr; }} }}
    canvas {{ max-height: 340px; }}
    table.data {{ width: 100%; border-collapse: collapse; font-size: .82rem; }}
    table.data.compact {{ width: auto; }}
    table.data th, table.data td {{ border-bottom: 1px solid var(--bolt-border); padding: .4rem .6rem; text-align: right; white-space: nowrap; }}
    table.data th:first-child, table.data td:first-child {{ text-align: left; }}
    table.data th {{ background: #fafcfa; color: var(--bolt-muted); font-weight: 600; }}
    tr.highlight td {{ background: #fff3f3; }}
    .section-hidden {{ display: none !important; }}
    .leak-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: .75rem; }}
    .leak-block {{ background: #fafcfa; border-radius: 8px; padding: .65rem .75rem; }}
    .leak-block h4 {{ margin: 0 0 .35rem; font-size: .85rem; font-weight: 600; }}
    .leak-block .leak-total {{ color: var(--bolt-muted); font-weight: 600; }}
    .leak-block table.data {{ font-size: .78rem; }}
    .leak-block table.data th, .leak-block table.data td {{ padding: .25rem .4rem; }}
  </style>
</head>
<body>
  <header>
    <h1>{title}</h1>
    <div class="sub">Provider-at-fault bad orders — orders from `dim_order_delivery`; AM owner from `dim_provider_v2.account_manager_name`</div>
  </header>
  <div class="wrap">
    <div class="filters">
      <label>Brand:
        <select id="brandSel">
          {account_options_html}
        </select>
      </label>
      <label>AM owner:
        <select id="amSel">
          {am_options_html}
        </select>
      </label>
      <label>Month:
        <select id="monthSel">
          {month_options_html}
        </select>
      </label>
    </div>
    <div class="tabs" id="mainTabs">
      <button type="button" class="tab active" data-tab="tab-overview">Overview</button>
      <button type="button" class="tab" data-tab="tab-rejected">Rejected &amp; failed orders</button>
      <button type="button" class="tab" data-tab="tab-reasons">Reasons &amp; leakages</button>
      <button type="button" class="tab" data-tab="tab-cases">Recent cases</button>
    </div>

    <div id="tab-overview" class="tab-panel">
      <div class="kpi-grid" id="kpiOverview"></div>
      <div class="grid2">
        <div class="panel"><h2>Bad orders by type (trend)</h2><p class="sub" style="font-size:0.8rem;color:var(--bolt-muted);margin:-0.25rem 0 0.5rem">Hover a segment: count and % of bad orders in that month.</p><canvas id="chTypeTrend"></canvas></div>
        <div class="panel"><h2>Bad order rates (trend)</h2><canvas id="chSegTrend"></canvas></div>
      </div>
      <div class="grid2">
        <div class="panel"><h2>Bad orders by type</h2><p class="sub" style="font-size:0.8rem;color:var(--bolt-muted);margin:-0.25rem 0 0.5rem">Labels show count and % of all bad orders in the current filters (Brand / AM owner / Month).</p><canvas id="chTypeBar"></canvas></div>
        <div class="panel"><h2>Bad orders by provider</h2><canvas id="chProvBar"></canvas></div>
      </div>
      <div class="panel"><h2>Provider KPIs</h2><div style="overflow:auto" id="tableKpi"></div></div>
    </div>

    <div id="tab-rejected" class="tab-panel section-hidden">
      <div class="kpi-grid" id="kpiRejected"></div>
      <div class="panel"><h2>Rejected &amp; DNR orders by provider</h2><div style="overflow:auto" id="tableRejected"></div></div>
      <div class="panel"><h2>Failed / rejected reasons breakdown</h2><canvas id="chFailedReasons"></canvas></div>
    </div>

    <div id="tab-reasons" class="tab-panel section-hidden">
      <div class="grid2">
        <div class="panel"><h2>By main reason</h2><canvas id="chReasonBar"></canvas></div>
        <div class="panel"><h2>Bad order type split</h2><canvas id="chTypePie"></canvas></div>
      </div>
      <div class="panel"><h2>Reasons by provider (key leakages)</h2><div id="tableLeakage"></div></div>
    </div>

    <div id="tab-cases" class="tab-panel section-hidden">
      <div class="panel"><h2>Recent bad order cases</h2><div style="overflow:auto;max-height:600px" id="tableRecent"></div></div>
    </div>
  </div>

  <script type="application/json" id="data-json">{data_json}</script>
  <script>
  const DATA = JSON.parse(document.getElementById("data-json").textContent);
  const MONTHS = DATA.months;
  const ML = DATA.month_labels;
  const HIDE = DATA.hide_reasons;

  function fmtPct(n, d=1) {{ if (n == null || isNaN(n)) return "—"; return (n * 100).toFixed(d) + "%"; }}
  function fmtNum(n) {{ if (n == null || isNaN(n)) return "—"; return new Intl.NumberFormat("en-GB").format(n); }}
  function humanize(s) {{ return s ? String(s).replace(/_/g, " ") : ""; }}
  function activeMonths() {{ const v = document.getElementById("monthSel").value; return v === "all" ? MONTHS : [v]; }}
  function activeBrand() {{ return document.getElementById("brandSel").value; }}
  function activeAm() {{ return document.getElementById("amSel").value; }}
  function rowAm(r) {{ return r.am_owner != null ? String(r.am_owner) : "Unknown"; }}
  function matchesAm(r) {{ const a = activeAm(); return a === "all" || rowAm(r) === a; }}

  function filteredRows() {{
    const ms = activeMonths();
    const b = activeBrand();
    return DATA.detail_rows.filter(r => ms.includes(r.month) && (b === "all" || r.cohort === b) && matchesAm(r));
  }}

  /** Rate trend rows for selected brand + AM owner (computed client-side from raw rows). */
  function buildSegmentTrendRows(ms) {{
    const b = activeBrand();
    const a = activeAm();
    const rej = DATA.rejection_data.filter(r => (b === "all" || r.cohort === b) && (a === "all" || rowAm(r) === a));
    const det = DATA.detail_rows.filter(r => (b === "all" || r.cohort === b) && (a === "all" || rowAm(r) === a));
    return ms.map(m => {{
      const rmonth = rej.filter(x => x.month === m);
      const placed = rmonth.reduce((s, x) => s + (Number(x.placed_orders) || 0), 0);
      const rejected = rmonth.reduce((s, x) => s + (Number(x.rejected) || 0), 0);
      const dnr = rmonth.reduce((s, x) => s + (Number(x.dnr) || 0), 0);
      const dm = det.filter(x => x.month === m);
      const bad = dm.length;
      const late15 = dm.filter(x => x.type === "late_delivery_order_15min").length;
      const missing = dm.filter(x => x.type === "missing_or_wrong_item_cs_ticket").length;
      return {{
        month: m,
        bad_rate: placed ? bad / placed : null,
        not_delivered_rate: placed ? (rejected + dnr) / placed : null,
        late_15_rate: placed ? late15 / placed : null,
        missing_wrong_rate: placed ? missing / placed : null,
      }};
    }});
  }}

  let charts = {{}};
  function destroyChart(id) {{ if (charts[id]) {{ charts[id].destroy(); delete charts[id]; }} }}

  const C = {{
    green: "#2A9C64", dark: "#1e7a4d", red: "#e53935", orange: "#fb8c00",
    muted: "#90a4ae", blue: "#1e88e5",
    palette: ["#2A9C64","#e53935","#fb8c00","#1e88e5","#8e24aa","#00897b","#c0ca33","#6d4c41","#78909c","#ec407a","#26a69a","#5c6bc0","#ff7043"]
  }};

  function typeCountShare(n, total) {{
    if (!total) return fmtNum(n);
    return fmtNum(n) + " · " + fmtPct(n / total);
  }}

  function renderOverviewKpis() {{
    const rows = filteredRows();
    const types = {{}};
    rows.forEach(r => {{ types[r.type] = (types[r.type] || 0) + 1; }});
    const el = document.getElementById("kpiOverview");
    const total = rows.length;
    const items = [
      {{ label: "Total bad orders", val: fmtNum(total), cls: total > 1000 ? "bad" : (total > 300 ? "warn" : "") }},
      {{ label: "Rejected / failed", val: typeCountShare(types["failed_order_provider_rejected"] || 0, total) }},
      {{ label: "Failed after accepted", val: typeCountShare(types["failed_order_after_provider_accepted"] || 0, total) }},
      {{ label: "Late delivery 15min+", val: typeCountShare(types["late_delivery_order_15min"] || 0, total) }},
      {{ label: "Missing / wrong item", val: typeCountShare(types["missing_or_wrong_item_cs_ticket"] || 0, total) }},
      {{ label: "Quality tickets", val: typeCountShare(types["order_quality_cs_ticket"] || 0, total) }},
    ];
    el.innerHTML = items.map(x =>
      '<div class="kpi"><h3>' + x.label + '</h3><div class="val' + (x.cls ? ' ' + x.cls : '') + '">' + x.val + '</div></div>'
    ).join("");
  }}

  function drawOverviewCharts() {{
    const ms = activeMonths();
    const b = activeBrand();
    const allRows = DATA.detail_rows.filter(r => (b === "all" || r.cohort === b) && matchesAm(r));
    const allTypes = [...new Set(allRows.map(r => r.type))];

    // Type trend
    const typeData = ms.map(m => {{
      const mRows = allRows.filter(r => r.month === m);
      const entry = {{ month: m }};
      allTypes.forEach(t => {{ entry[t] = mRows.filter(r => r.type === t).length; }});
      return entry;
    }});
    destroyChart("typeTrend");
    charts.typeTrend = new Chart(document.getElementById("chTypeTrend"), {{
      type: "bar",
      data: {{
        labels: ms.map(m => ML[m] || m),
        datasets: allTypes.map((t, i) => ({{
          label: humanize(t), data: typeData.map(r => r[t] || 0),
          backgroundColor: C.palette[i % C.palette.length] + "cc"
        }}))
      }},
      options: {{
        plugins: {{
          legend: {{ position: "bottom", labels: {{ font: {{ size: 10 }} }} }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                const t = allTypes[ctx.datasetIndex];
                const v = ctx.parsed.y != null ? ctx.parsed.y : ctx.parsed;
                const month = ms[ctx.dataIndex];
                const monthTotal = allRows.filter(r => r.month === month).length;
                const pct = monthTotal ? (100 * v / monthTotal) : 0;
                return humanize(t) + ": " + fmtNum(v) + " (" + pct.toFixed(1) + "% of bad orders in that month)";
              }}
            }}
          }}
        }},
        scales: {{ x: {{ stacked: true }}, y: {{ stacked: true, beginAtZero: true }} }}
      }}
    }});

    // Rate trend (recomputed for brand + AM owner filters)
    const seg = buildSegmentTrendRows(ms);
    destroyChart("segTrend");
    charts.segTrend = new Chart(document.getElementById("chSegTrend"), {{
      type: "line",
      data: {{
        labels: seg.map(r => ML[r.month] || r.month),
        datasets: [
          {{ label: "At-fault bad order %", data: seg.map(x => (x.bad_rate || 0) * 100), borderColor: C.red, tension: .25 }},
          {{ label: "Not delivered %", data: seg.map(x => (x.not_delivered_rate || 0) * 100), borderColor: C.orange, tension: .25 }},
          {{ label: "Late 15min+ %", data: seg.map(x => (x.late_15_rate || 0) * 100), borderColor: C.muted, tension: .25 }},
          {{ label: "Missing/wrong %", data: seg.map(x => (x.missing_wrong_rate || 0) * 100), borderColor: C.blue, tension: .25 }},
        ]
      }},
      options: {{ plugins: {{ legend: {{ position: "bottom", labels: {{ font: {{ size: 10 }} }} }} }} }}
    }});

    // Type bar (count + % of all bad orders in current filters)
    const rows = filteredRows();
    const totalBad = rows.length;
    const typeCounts = {{}};
    rows.forEach(r => {{ typeCounts[r.type] = (typeCounts[r.type] || 0) + 1; }});
    const sortedTypes = Object.entries(typeCounts).sort((a,b) => b[1] - a[1]);
    destroyChart("typeBar");
    charts.typeBar = new Chart(document.getElementById("chTypeBar"), {{
      type: "bar",
      data: {{
        labels: sortedTypes.map(x => humanize(x[0])),
        datasets: [{{ label: "Bad orders", data: sortedTypes.map(x => x[1]), backgroundColor: C.green + "99" }}]
      }},
      options: {{
        indexAxis: "y",
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                const v = ctx.parsed.x != null ? ctx.parsed.x : ctx.parsed;
                const pct = totalBad ? (100 * v / totalBad) : 0;
                return fmtNum(v) + " (" + pct.toFixed(1) + "% of filtered bad orders)";
              }}
            }}
          }}
        }},
        layout: {{ padding: {{ right: 100 }} }},
        scales: {{ x: {{ beginAtZero: true }} }}
      }},
      plugins: [{{
        id: "badorders-typebar-pct",
        afterDatasetsDraw(chart) {{
          const ctx = chart.ctx;
          chart.data.datasets[0].data.forEach((val, i) => {{
            const meta = chart.getDatasetMeta(0).data[i];
            if (!meta) return;
            const pct = totalBad ? (100 * val / totalBad) : 0;
            ctx.save();
            ctx.fillStyle = "#333"; ctx.font = "bold 11px sans-serif";
            ctx.textAlign = "left"; ctx.textBaseline = "middle";
            ctx.fillText(fmtNum(val) + " (" + pct.toFixed(1) + "%)", meta.x + 6, meta.y);
            ctx.restore();
          }});
        }}
      }}]
    }});

    // Provider bar (bad count + % of placed orders in same month/brand/AM filters)
    const provCounts = {{}};
    rows.forEach(r => {{ provCounts[r.provider] = (provCounts[r.provider] || 0) + 1; }});
    const placedByProv = {{}};
    DATA.rejection_data.filter(r => ms.includes(r.month) && (b === "all" || r.cohort === b) && matchesAm(r)).forEach(r => {{
      const p = r.provider;
      placedByProv[p] = (placedByProv[p] || 0) + (Number(r.placed_orders) || 0);
    }});
    const sortedProvs = Object.entries(provCounts).filter(x => x[1] > 0).sort((a,b) => b[1] - a[1]).slice(0, 25);
    destroyChart("provBar");
    charts.provBar = new Chart(document.getElementById("chProvBar"), {{
      type: "bar",
      data: {{ labels: sortedProvs.map(x => x[0]), datasets: [{{ label: "Bad orders", data: sortedProvs.map(x => x[1]), backgroundColor: C.red + "88" }}] }},
      options: {{
        indexAxis: "y",
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                const name = sortedProvs[ctx.dataIndex][0];
                const bad = sortedProvs[ctx.dataIndex][1];
                const placed = placedByProv[name] || 0;
                const pct = placed ? (100 * bad / placed) : null;
                const pctStr = pct != null ? pct.toFixed(1) + "% of total orders" : "—";
                return fmtNum(bad) + " bad · " + pctStr;
              }}
            }}
          }}
        }},
        layout: {{ padding: {{ right: 140 }} }},
        scales: {{ x: {{ beginAtZero: true }} }}
      }},
      plugins: [{{
        id: "badorders-provbar-count",
        afterDatasetsDraw(chart) {{
          const ctx = chart.ctx;
          chart.data.datasets[0].data.forEach((val, i) => {{
            const meta = chart.getDatasetMeta(0).data[i];
            if (!meta) return;
            const name = sortedProvs[i][0];
            const placed = placedByProv[name] || 0;
            const pct = placed ? (100 * val / placed) : null;
            const suffix = pct != null ? " (" + pct.toFixed(1) + "% of orders)" : "";
            ctx.save();
            ctx.fillStyle = "#333"; ctx.font = "bold 11px sans-serif";
            ctx.textAlign = "left"; ctx.textBaseline = "middle";
            ctx.fillText(fmtNum(val) + suffix, meta.x + 6, meta.y);
            ctx.restore();
          }});
        }}
      }}]
    }});
  }}

  function renderKpiTable() {{
    const b = activeBrand();
    const rows = DATA.kpi_by_provider.filter(r => (b === "all" || r.cohort === b) && matchesAm(r));
    let html = "<table class='data'><thead><tr><th>Provider</th><th>AM owner</th><th>Placed</th><th>Bad orders</th><th>Bad rate</th><th>Rejected %</th><th>Not deliv %</th><th>Late 15+ %</th><th>Missing/wrong %</th></tr></thead><tbody>";
    rows.forEach(r => {{
      const cls = (r.bad_rate || 0) > 0.05 ? " class='highlight'" : "";
      html += "<tr" + cls + "><td>" + r.provider + "</td><td>" + humanize(rowAm(r)) + "</td><td>" + fmtNum(r.placed_orders) + "</td><td>" + fmtNum(r.bad_orders) + "</td><td>" + fmtPct(r.bad_rate) + "</td><td>" + fmtPct(r.rejected_rate) + "</td><td>" + fmtPct(r.not_delivered_rate) + "</td><td>" + fmtPct(r.late_15_rate) + "</td><td>" + fmtPct(r.missing_wrong_rate) + "</td></tr>";
    }});
    html += "</tbody></table>";
    document.getElementById("tableKpi").innerHTML = html;
  }}

  function renderRejectedKpis() {{
    const ms = activeMonths();
    const b = activeBrand();
    const rej = DATA.rejection_data.filter(r => ms.includes(r.month) && (b === "all" || r.cohort === b) && matchesAm(r));
    let totalRej = 0, totalDnr = 0, totalPlaced = 0;
    rej.forEach(r => {{ totalRej += r.rejected || 0; totalDnr += r.dnr || 0; totalPlaced += r.placed_orders || 0; }});
    const el = document.getElementById("kpiRejected");
    el.innerHTML = [
      {{ label: "Total rejected orders", val: fmtNum(totalRej) }},
      {{ label: "Total DNR orders", val: fmtNum(totalDnr) }},
      {{ label: "Rejection rate", val: totalPlaced ? fmtPct(totalRej / totalPlaced) : "—" }},
      {{ label: "DNR rate", val: totalPlaced ? fmtPct(totalDnr / totalPlaced) : "—" }},
    ].map(x => '<div class="kpi"><h3>' + x.label + '</h3><div class="val">' + x.val + '</div></div>').join("");
  }}

  function renderRejectedTable() {{
    const ms = activeMonths();
    const b = activeBrand();
    const rej = DATA.rejection_data.filter(r => ms.includes(r.month) && (b === "all" || r.cohort === b) && matchesAm(r));
    const byProv = {{}};
    rej.forEach(r => {{
      if (!byProv[r.provider]) byProv[r.provider] = {{ placed: 0, rejected: 0, dnr: 0 }};
      byProv[r.provider].placed += r.placed_orders || 0;
      byProv[r.provider].rejected += r.rejected || 0;
      byProv[r.provider].dnr += r.dnr || 0;
    }});
    const rows = Object.entries(byProv).sort((a,b) => b[1].rejected - a[1].rejected).slice(0, 50);
    let html = "<table class='data compact'><thead><tr><th>Provider</th><th>Placed</th><th>Rejected</th><th>Reject %</th><th>DNR</th><th>DNR %</th></tr></thead><tbody>";
    rows.forEach(([name, r]) => {{
      const rp = r.placed ? fmtPct(r.rejected / r.placed) : "—";
      const dp = r.placed ? fmtPct(r.dnr / r.placed) : "—";
      const cls = r.placed && (r.rejected / r.placed) > 0.05 ? " class='highlight'" : "";
      html += "<tr" + cls + "><td>" + name + "</td><td>" + fmtNum(r.placed) + "</td><td>" + fmtNum(r.rejected) + "</td><td>" + rp + "</td><td>" + fmtNum(r.dnr) + "</td><td>" + dp + "</td></tr>";
    }});
    html += "</tbody></table>";
    document.getElementById("tableRejected").innerHTML = html;
  }}

  function drawFailedReasons() {{
    const rows = filteredRows().filter(r => r.type === "failed_order_provider_rejected" || r.type === "failed_order_after_provider_accepted");
    const counts = {{}};
    rows.forEach(r => {{ counts[r.reason] = (counts[r.reason] || 0) + 1; }});
    const sorted = Object.entries(counts).sort((a,b) => b[1] - a[1]).slice(0, 25);
    destroyChart("failedReasons");
    charts.failedReasons = new Chart(document.getElementById("chFailedReasons"), {{
      type: "bar",
      data: {{ labels: sorted.map(x => humanize(x[0])), datasets: [{{ label: "Count", data: sorted.map(x => x[1]), backgroundColor: C.orange + "99" }}] }},
      options: {{ indexAxis: "y", plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ beginAtZero: true }} }} }}
    }});
  }}

  function drawReasonCharts() {{
    const rows = filteredRows();
    const reasonCounts = {{}};
    rows.forEach(r => {{ if (!HIDE.includes(r.reason)) reasonCounts[r.reason] = (reasonCounts[r.reason] || 0) + 1; }});
    const sorted = Object.entries(reasonCounts).sort((a,b) => b[1] - a[1]).slice(0, 15);
    destroyChart("reasonBar");
    charts.reasonBar = new Chart(document.getElementById("chReasonBar"), {{
      type: "bar",
      data: {{ labels: sorted.map(x => humanize(x[0])), datasets: [{{ label: "Count", data: sorted.map(x => x[1]), backgroundColor: C.muted + "99" }}] }},
      options: {{ indexAxis: "y", plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ beginAtZero: true }} }} }}
    }});

    const typeCounts = {{}};
    rows.forEach(r => {{ typeCounts[r.type] = (typeCounts[r.type] || 0) + 1; }});
    const tSorted = Object.entries(typeCounts).sort((a,b) => b[1] - a[1]);
    const badTotal = rows.length;
    destroyChart("typePie");
    charts.typePie = new Chart(document.getElementById("chTypePie"), {{
      type: "doughnut",
      data: {{ labels: tSorted.map(x => humanize(x[0])), datasets: [{{ data: tSorted.map(x => x[1]), backgroundColor: C.palette.slice(0, tSorted.length) }}] }},
      options: {{
        plugins: {{
          legend: {{ position: "bottom", labels: {{ font: {{ size: 10 }} }} }},
          tooltip: {{
            callbacks: {{
              label: function(ctx) {{
                const v = ctx.parsed;
                const pct = badTotal ? (100 * v / badTotal) : 0;
                return fmtNum(v) + " (" + pct.toFixed(1) + "% of filtered bad orders)";
              }}
            }}
          }}
        }}
      }}
    }});
  }}

  function renderLeakage() {{
    const el = document.getElementById("tableLeakage");
    const rows = filteredRows();
    const reasonProv = {{}};
    rows.forEach(r => {{
      if (HIDE.includes(r.reason)) return;
      if (!reasonProv[r.reason]) reasonProv[r.reason] = {{}};
      reasonProv[r.reason][r.provider] = (reasonProv[r.reason][r.provider] || 0) + 1;
    }});
    const blocks = Object.entries(reasonProv)
      .map(([reason, provs]) => ({{ reason, total: Object.values(provs).reduce((a,b) => a+b, 0), by_provider: Object.entries(provs).sort((a,b) => b[1] - a[1]).slice(0, 10).map(([n,c]) => ({{name:n, count:c}})) }}))
      .sort((a,b) => b.total - a.total)
      .slice(0, 30);
    if (!blocks.length) {{ el.innerHTML = "<p style='color:var(--bolt-muted)'>No data.</p>"; return; }}
    let html = "<div class='leak-grid'>";
    blocks.forEach(block => {{
      html += "<div class='leak-block'><h4>" + humanize(block.reason) + " <span class='leak-total'>(" + fmtNum(block.total) + ")</span></h4>";
      html += "<table class='data compact'><thead><tr><th>Provider</th><th>#</th></tr></thead><tbody>";
      block.by_provider.forEach(x => {{ html += "<tr><td>" + x.name + "</td><td>" + fmtNum(x.count) + "</td></tr>"; }});
      html += "</tbody></table></div>";
    }});
    html += "</div>";
    el.innerHTML = html;
  }}

  function renderRecent() {{
    const limit = DATA.recent_limit || 100;
    const rows = filteredRows().slice(0, limit);
    let html = "<table class='data'><thead><tr><th>Time</th><th>Ref</th><th>Provider</th><th>AM owner</th><th>Type</th><th>Reason</th></tr></thead><tbody>";
    rows.forEach(r => {{
      html += "<tr><td>" + r.time + "</td><td>" + r.order_ref + "</td><td>" + r.provider + "</td><td>" + humanize(rowAm(r)) + "</td><td>" + humanize(r.type) + "</td><td>" + humanize(r.reason) + "</td></tr>";
    }});
    html += "</tbody></table>";
    document.getElementById("tableRecent").innerHTML = html;
  }}

  function refresh() {{
    renderOverviewKpis();
    drawOverviewCharts();
    renderKpiTable();
    renderRejectedKpis();
    renderRejectedTable();
    drawFailedReasons();
    drawReasonCharts();
    renderLeakage();
    renderRecent();
  }}

  document.getElementById("monthSel").addEventListener("change", refresh);
  document.getElementById("brandSel").addEventListener("change", refresh);
  document.getElementById("amSel").addEventListener("change", refresh);

  document.querySelectorAll("#mainTabs .tab").forEach(btn => {{
    btn.addEventListener("click", () => {{
      document.querySelectorAll("#mainTabs .tab").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      document.querySelectorAll(".tab-panel").forEach(p => p.classList.add("section-hidden"));
      document.getElementById(btn.getAttribute("data-tab")).classList.remove("section-hidden");
    }});
  }});

  refresh();
  </script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a self-contained Bad Orders HTML dashboard from Databricks (provider-at-fault)."
    )
    parser.add_argument("--country-code", default="mt", help="Country code, e.g. mt, pl, ro (default: mt)")
    parser.add_argument(
        "--year",
        type=int,
        help="Convenience: build from Jan 1 of this year to today (if current year), otherwise through Dec 31",
    )
    parser.add_argument("--start-date", type=_parse_date, help="Start date (YYYY-MM-DD), inclusive")
    parser.add_argument("--end-date", type=_parse_date, help="End date (YYYY-MM-DD), inclusive")
    parser.add_argument("--lookback-days", type=int, default=90, help="If dates not provided, use this lookback (default: 90)")
    parser.add_argument("--vendor-ids", type=int, nargs="*", help="Optional vendor_id(s) to include")
    parser.add_argument("--vendor-names", nargs="*", help="Optional vendor_name(s) to include (exact match)")
    parser.add_argument(
        "--hide-reasons",
        nargs="*",
        default=[
            "manually_failed_by_cs",
            "too_many_orders",
            "closed",
            "provider_preparation_overestimate_seconds",
        ],
        help="Reasons to hide in reason charts (default: common non-actionable reasons)",
    )
    parser.add_argument("--recent-limit", type=int, default=100, help="How many rows to show in Recent tab (default: 100)")
    parser.add_argument(
        "--output",
        default=None,
        help="Output HTML path (default: auto under ~/Documents)",
    )

    args = parser.parse_args()

    today = dt.date.today()
    if args.start_date and args.end_date:
        start = args.start_date
        end = args.end_date
        year_for_naming: Optional[int] = None
    elif args.year:
        start = dt.date(int(args.year), 1, 1)
        year_end = dt.date(int(args.year), 12, 31)
        end = min(today, year_end) if int(args.year) == today.year else year_end
        year_for_naming = int(args.year)
    else:
        end = today
        start = today - dt.timedelta(days=int(args.lookback_days))
        year_for_naming = None

    filters = Filters(
        country_code=args.country_code,
        start_date=start,
        end_date_inclusive=end,
        vendor_ids=list(args.vendor_ids) if args.vendor_ids else None,
        vendor_names=list(args.vendor_names) if args.vendor_names else None,
    )

    _ensure_dbx_on_path()
    from dbx import DBX  # type: ignore

    with DBX() as dbx:
        accounts_df = dbx.query(_query_accounts_sql(filters))
        detail_df = dbx.query(_query_detail_rows_sql(filters))
        kpi_df = dbx.query(_query_kpi_by_provider_sql(filters))
        rejection_df = dbx.query(_query_rejection_data_sql(filters))

    for _df in (detail_df, rejection_df, kpi_df):
        if not _df.empty and "am_owner" not in _df.columns:
            _df["am_owner"] = "Unknown"

    accounts = _records(accounts_df)
    detail_rows = _records(detail_df)
    kpi_by_provider = _records(kpi_df)
    rejection_data = _records(rejection_df)

    months = _months_between(filters.start_date, filters.end_date_inclusive)
    month_labels = {m: _month_label(m) for m in months}

    account_options_html = "<option value=\"all\">All brands</option>" + "".join(
        f"<option value=\"{html_lib.escape(str(a.get('cohort') or ''), quote=True)}\">{html_lib.escape(str(a.get('vendor_name') or ''))}</option>"
        for a in accounts
    )
    month_options_html = "<option value=\"all\">All months</option>" + "".join(
        f"<option value=\"{m}\">{month_labels.get(m, m)}</option>" for m in months
    )

    am_values: set[str] = set()
    if not kpi_df.empty and "am_owner" in kpi_df.columns:
        am_values.update(str(x) for x in kpi_df["am_owner"].fillna("Unknown").unique())
    elif not rejection_df.empty and "am_owner" in rejection_df.columns:
        am_values.update(str(x) for x in rejection_df["am_owner"].fillna("Unknown").unique())
    if filters.country_code.lower() == "mt":
        am_values.update(_MT_AM_OWNER_PRIMARY_ORDER)
    am_list = _am_owner_dropdown_list(am_values, filters.country_code)
    am_options_html = "<option value=\"all\">All AM owners</option>" + "".join(
        f"<option value=\"{html_lib.escape(x, quote=True)}\">{html_lib.escape(x)}</option>" for x in am_list
    )

    # Precompute segment trends overall and by cohort for fast client-side rendering.
    # Detail DF only contains provider-at-fault bad orders, one row per bad order.
    if detail_df.empty:
        bad_by_month: dict[str, int] = {}
        late15_by_month: dict[str, int] = {}
        missing_by_month: dict[str, int] = {}
    else:
        bad_by_month = detail_df.groupby("month").size().to_dict()
        late15_by_month = (
            detail_df[detail_df["type"] == "late_delivery_order_15min"].groupby("month").size().to_dict()
        )
        missing_by_month = (
            detail_df[detail_df["type"] == "missing_or_wrong_item_cs_ticket"].groupby("month").size().to_dict()
        )

    if rejection_df.empty:
        placed_by_month: dict[str, int] = {}
        rejected_by_month: dict[str, int] = {}
        dnr_by_month: dict[str, int] = {}
    else:
        placed_by_month = rejection_df.groupby("month")["placed_orders"].sum().to_dict()
        rejected_by_month = rejection_df.groupby("month")["rejected"].sum().to_dict()
        dnr_by_month = rejection_df.groupby("month")["dnr"].sum().to_dict()

    segment_trend = _build_trend_records(
        months=months,
        placed_by_month=placed_by_month,
        rejected_by_month=rejected_by_month,
        dnr_by_month=dnr_by_month,
        bad_by_month=bad_by_month,
        late15_by_month=late15_by_month,
        missing_by_month=missing_by_month,
    )

    cohorts = sorted({str(a["cohort"]) for a in accounts if a.get("cohort")})
    segment_trend_by_cohort: dict[str, list[dict[str, Any]]] = {}
    if cohorts and not rejection_df.empty:
        placed_cm = rejection_df.groupby(["cohort", "month"])["placed_orders"].sum()
        rejected_cm = rejection_df.groupby(["cohort", "month"])["rejected"].sum()
        dnr_cm = rejection_df.groupby(["cohort", "month"])["dnr"].sum()
    else:
        placed_cm = pd.Series(dtype="float64")
        rejected_cm = pd.Series(dtype="float64")
        dnr_cm = pd.Series(dtype="float64")

    if cohorts and not detail_df.empty:
        bad_cm = detail_df.groupby(["cohort", "month"]).size()
        late15_cm = detail_df[detail_df["type"] == "late_delivery_order_15min"].groupby(["cohort", "month"]).size()
        missing_cm = detail_df[detail_df["type"] == "missing_or_wrong_item_cs_ticket"].groupby(["cohort", "month"]).size()
    else:
        bad_cm = pd.Series(dtype="float64")
        late15_cm = pd.Series(dtype="float64")
        missing_cm = pd.Series(dtype="float64")

    for cohort in cohorts:
        def _xs(series: pd.Series) -> dict[str, int]:
            if series.empty:
                return {}
            try:
                s = series.xs(cohort, level=0, drop_level=True)
            except Exception:
                return {}
            return {str(k): int(v) for k, v in s.to_dict().items()}

        segment_trend_by_cohort[cohort] = _build_trend_records(
            months=months,
            placed_by_month=_xs(placed_cm),
            rejected_by_month=_xs(rejected_cm),
            dnr_by_month=_xs(dnr_cm),
            bad_by_month=_xs(bad_cm),
            late15_by_month=_xs(late15_cm),
            missing_by_month=_xs(missing_cm),
        )

    recent = detail_rows[: int(args.recent_limit)]

    title_bits: list[str] = []
    if filters.vendor_names:
        title_bits = filters.vendor_names[:]
    elif accounts and len(accounts) <= 3:
        title_bits = [a["vendor_name"] for a in accounts]
    if title_bits:
        title = " & ".join(title_bits) + " — Bad Orders Dashboard"
    else:
        title = f"{filters.country_code.upper()} — Bad Orders Dashboard"

    data = {
        "months": months,
        "month_labels": month_labels,
        "hide_reasons": list(args.hide_reasons),
        "recent_limit": int(args.recent_limit),
        "source_file": (
            f"Databricks: ng_delivery_spark.dim_order_delivery | country={filters.country_code.lower()} | "
            f"{filters.start_date.isoformat()}..{filters.end_date_inclusive.isoformat()} | actor_at_fault=provider"
        ),
        "detail_rows": detail_rows,
        "segment_trend": segment_trend,
        "segment_trend_by_cohort": segment_trend_by_cohort,
        "kpi_by_provider": kpi_by_provider,
        "rejection_data": rejection_data,
        "recent": recent,
    }

    data_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    html = _html_template(
        title=title,
        account_options_html=account_options_html,
        am_options_html=am_options_html,
        month_options_html=month_options_html,
        data_json=data_json,
    )

    output_arg = args.output
    if not output_arg:
        output_arg = _default_output_path(
            country_code=filters.country_code,
            start_date=filters.start_date,
            end_date_inclusive=filters.end_date_inclusive,
            year=year_for_naming,
            vendor_names=filters.vendor_names,
            vendor_ids=filters.vendor_ids,
        )
    out_path = os.path.abspath(os.path.expanduser(output_arg))
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote: {out_path}")
    print(f"Detail rows (bad orders): {len(detail_rows)}")
    print(f"Accounts in scope: {len(accounts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

