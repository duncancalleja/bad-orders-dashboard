# Bad Orders Dashboard (Databricks → self-contained HTML)

This repo includes a generator script, `build_bad_orders_dashboard.py`, that builds a **single HTML file** (Chart.js-based) similar to your `Dr_Juice_Bad_Orders_Dashboard.html`, but sourcing data directly from Databricks via `databricks-setup/dbx.py`.

It uses:

- Databricks table: `ng_delivery_spark.dim_order_delivery`
- Scope: **provider-at-fault bad orders** (`is_bad_order = true` and `bad_order_actor_at_fault = 'provider'`)

## Prereqs

- Python 3
- Databricks auth:
  - `~/.databricks_token` (preferred), or
  - `DATABRICKS_TOKEN` env var, or
  - OAuth fallback (browser login)

## Usage

### Build for all brands in a country

This creates **one** dashboard with a **Brand** dropdown for all brands in scope.

```bash
python3 build_bad_orders_dashboard.py \
  --country-code mt \
  --year 2026
```

By default, the output is written under:

- `~/Documents/Bad orders/` (if that folder exists), otherwise
- `~/Documents/`

### Build for specific accounts

Filter by exact `vendor_name`:

```bash
python3 build_bad_orders_dashboard.py \
  --country-code mt \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --vendor-names "Dr Juice" "WOM" \
  --output "/Users/dc/Documents/Dr_Juice_WOM_Bad_Orders_Dashboard.html"
```

Or filter by `vendor_id`:

```bash
python3 build_bad_orders_dashboard.py \
  --country-code mt \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --vendor-ids 1113 17272 \
  --output "/Users/dc/Documents/Bad_Orders_Dashboard.html"
```

### Optional: hide noisy reasons in “Reasons” charts

```bash
python3 build_bad_orders_dashboard.py \
  --country-code mt \
  --year 2026 \
  --hide-reasons manually_failed_by_cs too_many_orders closed provider_preparation_overestimate_seconds \
  --output "/Users/dc/Documents/MT_Bad_Orders_Dashboard.html"
```

## Output

The output is a shareable, self-contained HTML file:

- Filters: **Brand** and **Month**
- Tabs: Overview, Rejected & failed, Reasons & leakages, Recent cases

### Brand grouping

The **Brand** dropdown is built from `vendor_name` (trimmed) and **aggregates across multiple `vendor_id`s** when the same brand name appears under more than one vendor.

## Automatic weekly refresh (macOS)

This is set up via a LaunchAgent that runs every **Monday at 09:15** and overwrites the latest dashboard.

### Where to open the dashboard

- **Open this**: `~/Documents/MT_bad_orders_2026_all_accounts.html`

That path is a **symlink** to the real output file under:

- `~/Library/Application Support/bad-orders-dashboard/MT_bad_orders_2026_all_accounts.html`

This avoids macOS background-job privacy restrictions that prevent `launchd` jobs from writing directly into `~/Documents/`.

### LaunchAgent file

- `~/Library/LaunchAgents/com.bolt.badorders.mt.2026.plist`

### Logs

- `~/Library/Application Support/bad-orders-dashboard/MT_bad_orders_2026_all_accounts.update.log`
- `~/Library/Application Support/bad-orders-dashboard/MT_bad_orders_2026_all_accounts.update.err.log`

## Hosting for Bolt employees (GitHub)

See `GITHUB-HOSTING.md`.


