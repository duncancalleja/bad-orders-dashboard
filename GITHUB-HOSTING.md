# Hosting this dashboard on GitHub (for Bolt employees)

This repo includes a GitHub Actions workflow (`.github/workflows/publish-pages.yml`) that:

- runs **every Monday** (and on-demand),
- generates `site/index.html`,
- deploys it to **GitHub Pages**.

## Safe-by-default behavior

When `PUBLISH_LIVE_DASHBOARD` is **not** set to `true`, the workflow publishes a **safe public landing page** that contains **no production data**.

This is the recommended mode for a **public** repo / public Pages site.

Even if `PUBLISH_LIVE_DASHBOARD=true`, the workflow includes a guard that **refuses to publish the live dashboard when the repo visibility is public**.

## Critical: GitHub Pages visibility (public vs private)

If Bolt is on **GitHub Enterprise Cloud**, you can publish the Pages site **privately** so it’s only accessible to people with **read access to the repo** ([GitHub Docs: “Changing the visibility of your GitHub Pages site”](https://docs.github.com/en/pages/getting-started-with-github-pages/changing-the-visibility-of-your-github-pages-site)).

If you **cannot** publish Pages privately, then a Pages site may be publicly reachable on the internet — in that case, **do not publish raw order-level data** via Pages. Instead, either:

- host the code in GitHub and have users run it locally, or
- deploy via an internal hosting solution (or use a self-hosted runner + internal web hosting).

## One-time setup steps (recommended)

1. **Create a new repo** in the Bolt GitHub org.
2. Push this code to the repo (make sure you are not committing any tokens).
3. Enable Pages:
   - Repo **Settings** → **Pages**
   - Set **Build and deployment** to **GitHub Actions**
4. Trigger the workflow:
   - Repo → **Actions** → “Publish Bad Orders Dashboard” → **Run workflow**

After the first deploy, GitHub will show a **Visit site** link in Settings → Pages.

## Enabling the LIVE dashboard (only when internal/private)

Only do this once the repo/Pages site is internal/private.

1. Add a **Repository Variable**:
   - **Name**: `PUBLISH_LIVE_DASHBOARD`
   - **Value**: `true`
2. Add a **Repository Secret**:
   - **Name**: `DATABRICKS_TOKEN`
   - **Value**: a Databricks SQL Warehouse token (prefer a service account / least-privilege token)

## What gets published

Current workflow publishes a Malta dashboard:

- `--country-code mt`
- `--year 2026` (YTD)

If you want multiple countries/years, we can convert the workflow to a matrix build and publish multiple pages.

## Troubleshooting

- **Databricks blocks GitHub-hosted runners**: If the warehouse is IP-allowlisted, GitHub’s hosted runners may not be able to connect. Use a **self-hosted runner** inside Bolt’s network, or adjust allowlisting.
- **Token expiry**: If builds start failing, rotate `DATABRICKS_TOKEN` in repo secrets.

