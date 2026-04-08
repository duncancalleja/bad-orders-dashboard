/**
 * Bad Orders Dashboard — Google Apps Script (internal web app)
 *
 * Official docs (what you still do in the browser):
 * - Apps Script home / new project: https://script.google.com/home/start
 * - Web apps overview: https://developers.google.com/apps-script/guides/web
 * - Deploy a web app: https://developers.google.com/apps-script/concepts/deployments#deploy_a_web_app
 * - HtmlService: https://developers.google.com/apps-script/reference/html/html-service
 * - DriveApp.getFileById: https://developers.google.com/apps-script/reference/drive/drive-app#getFileById(String)
 * - Find a Google Drive file ID (support): https://support.google.com/drive/answer/2423485
 *
 * What this script does
 * - Serves your static dashboard HTML from a file in Google Drive (works for large files).
 * - Use HTML built from this repo with vendored Chart.js (no CDN) — see build_bad_orders_dashboard.py + vendor/chart.umd.min.js.
 *
 * One-time setup
 * 1) Build the HTML locally:
 *      python3 build_bad_orders_dashboard.py --country-code mt --year 2026 --output bad-orders.html
 *    (Or copy docs/index.html after rebuilding.)
 * 2) Upload bad-orders.html to Google Drive (same Google account as Apps Script).
 * 3) Open the file in Drive; copy the file ID from the URL:
 *      https://drive.google.com/file/d/THIS_IS_THE_FILE_ID/view
 * 4) Set DASHBOARD_FILE_ID below.
 * 5) script.google.com → New project → paste this script → Save.
 * 6) Deploy → New deployment → Select type: Web app
 *      Execute as: Me
 *      Who has access: Anyone within <your organisation> (tighten while testing)
 * 7) Open the Web app URL and share that link internally.
 *
 * Updating data
 * - Replace the Drive file contents or change DASHBOARD_FILE_ID. No redeploy needed for Drive-only changes.
 */
var DASHBOARD_FILE_ID = '';

function doGet() {
  if (!DASHBOARD_FILE_ID) {
    return HtmlService.createHtmlOutput(
      '<p>Set <code>DASHBOARD_FILE_ID</code> in <code>Code.gs</code> to your Google Drive file ID.</p>'
    ).setTitle('Bad Orders Dashboard');
  }
  try {
    var file = DriveApp.getFileById(DASHBOARD_FILE_ID);
    var html = file.getBlob().getDataAsString('UTF-8');
    return HtmlService.createHtmlOutput(html).setTitle('Bad Orders Dashboard');
  } catch (e) {
    return HtmlService.createHtmlOutput(
      '<p>Could not load the dashboard from Drive. Check the file ID and that this script runs as a user who can open that file.</p><pre>'
        + String(e)
        + '</pre>'
    ).setTitle('Bad Orders Dashboard — error');
  }
}
