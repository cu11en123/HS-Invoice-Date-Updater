/**
 * update-invoice-dates.js
 * Node 18+ standalone script
 *
 * Behavior:
 *  - Finds draft invoices with invoice date BEFORE today (UTC midnight)
 *  - Sets invoice date to today (UTC midnight)
 *  - Updates due date so the term (due - original invoice date) is preserved (in whole days)
 *  - If estimated_invoice_date_field is empty, set it to the original invoice date (UTC midnight)
 *
 * Config:
 *  - TEST_MODE = true (keeps behavior same as requested)
 *  - Provide HubSpot API key in env: HS
 *
 * Usage:
 *   HS="pat_xxx" node update-invoice-dates.js
 *
 * Note: This script expects Node 18+ (global fetch available).
 */

const TEST_MODE = false;            // keep as true per your request
const TEST_INVOICE_LIMIT = 1;      // how many to process in test mode
const HUBSPOT_BASE = 'https://api.hubapi.com';
const SEARCH_URL = `${HUBSPOT_BASE}/crm/v3/objects/invoices/search`;
const UPDATE_URL = `${HUBSPOT_BASE}/crm/v3/objects/invoices/batch/update`;
const MAX_BATCH = 100;             // HubSpot supports up to 100 per batch

// Read HubSpot key from env var named "HS"
const HUBSPOT_KEY = process.env.HS;
if (!HUBSPOT_KEY) {
  console.error('ERROR: HubSpot API key not provided. Set environment variable HS (e.g. HS="pat_xxx").');
  process.exit(1);
}

const HEADERS = {
  Authorization: `Bearer ${HUBSPOT_KEY}`,
  Accept: 'application/json',
  'Content-Type': 'application/json',
};

// ---------- Utilities ----------

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve));
}

async function makeRequest(url, options = {}, attempt = 1) {
  try {
    const res = await fetch(url, options);

    if (res.status === 429) {
      // rate limited
      const retryAfter = res.headers.get('retry-after');
      const delay = retryAfter ? parseInt(retryAfter, 10) * 1000 : Math.min(60000, 1000 * Math.pow(2, attempt));
      console.warn(`429 -> rate limited. Waiting ${delay}ms before retry #${attempt}`);
      await sleep(delay);
      if (attempt < 6) return makeRequest(url, options, attempt + 1);
      throw new Error(`Rate limited after ${attempt} attempts`);
    }

    if (!res.ok) {
      // include helpful snippet
      const text = await res.text().catch(() => '');
      const snippet = text ? (text.length > 800 ? text.slice(0, 800) + '...' : text) : '';
      throw new Error(`HTTP ${res.status} ${res.statusText} - ${snippet}`);
    }

    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) {
      return res.json();
    }
    return res.text();
  } catch (err) {
    if (attempt < 4 && !err.message.includes('Rate limited')) {
      const wait = 1000 * Math.pow(2, attempt);
      console.warn(`Network/error: ${err.message}. Retrying in ${wait}ms (attempt ${attempt})`);
      await sleep(wait);
      return makeRequest(url, options, attempt + 1);
    }
    throw err;
  }
}

function utcMidnightMsForDate(d = new Date()) {
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

function utcMidnightIsoForDate(d = new Date()) {
  const ms = utcMidnightMsForDate(d);
  return new Date(ms).toISOString();
}

function parseHubspotDate(value) {
  if (value === undefined || value === null || value === '') return null;
  const s = String(value).trim();
  // numeric epoch ms
  if (/^\d{8,}$/.test(s)) {
    const n = Number(s);
    if (!Number.isNaN(n)) return new Date(n);
  }
  // ISO-ish
  const parsed = new Date(s);
  if (!isNaN(parsed.getTime())) return parsed;
  return null;
}

function toUtcMidnightMs(d) {
  const date = new Date(d);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

// ---------- Main flow ----------

async function run() {
  console.log('--- Starting invoice date refresh script ---');
  console.log(`TEST_MODE: ${TEST_MODE ? 'ON' : 'OFF'}`);

  // Get today's UTC midnight for setting new invoice dates
  const todayUtcMidnightMs = utcMidnightMsForDate(new Date());
  const todayUtcMidnightIso = new Date(todayUtcMidnightMs).toISOString();
  console.log(`Today (UTC midnight): ${todayUtcMidnightIso}`);

  // FIXED: Use today's UTC midnight as the cutoff for the search
  // This ensures we get invoices with dates before today at 00:00 UTC
  // regardless of what time the script actually runs
  const searchCutoffMs = todayUtcMidnightMs;
  console.log(`Search cutoff: ${new Date(searchCutoffMs).toISOString()} (invoices before this will be updated)`);

  // ------------- Search for draft invoices with invoice date before today -------------
  const found = [];
  let after = undefined;
  const pageSize = TEST_MODE ? Math.min(100, TEST_INVOICE_LIMIT) : 100;

  do {
    const body = {
      filterGroups: [
        {
          filters: [
            { propertyName: 'hs_invoice_status', operator: 'EQ', value: 'draft' },
            { propertyName: 'hs_invoice_date', operator: 'LT', value: String(searchCutoffMs) },
          ],
        },
      ],
      properties: ['hs_invoice_date', 'hs_due_date', 'estimated_invoice_date_field', 'hs_number'],
      limit: pageSize,
    };
    if (after) body.after = after;

    console.log(`Searching invoices${after ? ` (after=${after})` : ''} ...`);
    const payload = JSON.stringify(body);

    const searchRes = await makeRequest(SEARCH_URL, {
      method: 'POST',
      headers: HEADERS,
      body: payload,
    });

    if (Array.isArray(searchRes.results)) {
      found.push(...searchRes.results);
    }

    after = searchRes.paging?.next?.after;

    if (TEST_MODE && found.length >= TEST_INVOICE_LIMIT) {
      found.length = TEST_INVOICE_LIMIT;
      break;
    }

    if (found.length >= 10000) {
      console.warn('Hit HubSpot search limit of 10,000 results, stopping early.');
      break;
    }

    if (after) await sleep(300);
  } while (after);

  console.log(`Found ${found.length} draft invoice(s) with invoice date < today (UTC midnight).`);

  if (found.length === 0) {
    console.log('No invoices to update. Exiting.');
    return {
      updated: 0,
      total: 0,
      dateSet: todayUtcMidnightIso.slice(0, 10),
      errors: undefined,
    };
  }

  // ------------- Build update payloads -------------
  const updates = found.map((inv) => {
    const id = inv.id;
    const props = inv.properties || {};

    const origInvRaw = props.hs_invoice_date || null;
    const origDueRaw = props.hs_due_date || null;

    const origInvDate = parseHubspotDate(origInvRaw) || new Date(todayUtcMidnightMs);
    const origDueDate = parseHubspotDate(origDueRaw) || new Date(toUtcMidnightMs(origInvDate) + 24 * 3600 * 1000);

    const origInvMidMs = toUtcMidnightMs(origInvDate);
    const origDueMidMs = toUtcMidnightMs(origDueDate);

    const termMs = Math.max(origDueMidMs - origInvMidMs, 0);
    const termDays = Math.round(termMs / (24 * 3600 * 1000)); // integer days

    const newDueMs = todayUtcMidnightMs + termDays * 24 * 3600 * 1000;
    const newDueIso = new Date(newDueMs).toISOString();

    const newInvoiceIso = new Date(todayUtcMidnightMs).toISOString();

    const estimatedFilled = props.estimated_invoice_date_field && String(props.estimated_invoice_date_field).trim() !== '';

    const newProps = {
      hs_invoice_date: newInvoiceIso,
      hs_due_date: newDueIso,
    };

    if (!estimatedFilled) {
      const estIso = new Date(origInvMidMs).toISOString();
      newProps.estimated_invoice_date_field = estIso;
    }

    return {
      id,
      properties: newProps,
    };
  });

  console.log(`Prepared ${updates.length} invoice update payload(s).`);

  // ------------- Batch update -------------
  const errors = [];
  let successCount = 0;
  const batchSize = Math.min(MAX_BATCH, TEST_MODE ? updates.length : MAX_BATCH);

  for (let i = 0; i < updates.length; i += batchSize) {
    const chunk = updates.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    console.log(`Updating batch ${batchNum} (${chunk.length} invoice(s))...`);

    try {
      const body = JSON.stringify({ inputs: chunk });
      const res = await makeRequest(UPDATE_URL, {
        method: 'POST',
        headers: HEADERS,
        body,
      });

      if (res && Array.isArray(res.results)) {
        const batchErrors = res.results.filter((r) => r.status === 'error' || r.status === 'failed');
        const batchSuccess = res.results.filter((r) => r.status === 'success' || r.status === 'updated');
        if (batchErrors.length > 0) {
          console.warn(`Batch ${batchNum} had ${batchErrors.length} errors.`);
          batchErrors.forEach((be) => errors.push({ batch: batchNum, item: be }));
        }
        successCount += batchSuccess.length;
        console.log(`Batch ${batchNum} => success: ${batchSuccess.length}, errors: ${batchErrors.length}`);
      } else {
        successCount += chunk.length;
        console.log(`Batch ${batchNum} updated (no detailed results returned).`);
      }
    } catch (err) {
      console.error(`Error updating batch ${batchNum}: ${err.message || err}`);
      errors.push({
        batch: batchNum,
        error: err.message || String(err),
        invoiceIds: chunk.map((c) => c.id),
      });
    }

    if (i + batchSize < updates.length) await sleep(800);
  }

  const summary = {
    updated: successCount,
    total: updates.length,
    dateSet: new Date(todayUtcMidnightMs).toISOString().slice(0, 10),
    errors: errors.length ? errors : undefined,
    successRate: updates.length ? `${Math.round((successCount / updates.length) * 100)}%` : '0%',
  };

  console.log('--- Final Summary ---');
  console.log(JSON.stringify(summary, null, 2));
  return summary;
}

// Run script when invoked directly
run()
  .then(() => {
    console.log('Done.');
    process.exit(0);
  })
  .catch((err) => {
    console.error('Script failed:', err && err.message ? err.message : err);
    process.exit(2);
  });
