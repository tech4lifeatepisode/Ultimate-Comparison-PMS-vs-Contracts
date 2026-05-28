/**
 * HTTP server for Render Web Service (binds to PORT).
 * Contract extraction: /extract, /extract-all
 * Contract blinding: /blind, /blind-all
 */
import http from 'http';
import { runExtractFromSupabaseStorage, isEnvTruthy } from './extract-from-storage.mjs';
import { runExtractFromSupabaseStorageUntilDone } from './extract-from-storage-batch.mjs';
import { runNcOcrRerunAllFolders } from './extract-nc-ocr-rerun-all-folders.mjs';
import {
  handleBlindRequest,
  scheduleAutoBlindOnBoot,
  blindHealthLines,
} from './blinding-server.mjs';

const port = Number(process.env.PORT) || 3000;

let extractionRunning = false;

function checkExtractAuth(req) {
  const secret = process.env.EXTRACT_TRIGGER_SECRET;
  const provided = req.headers['x-extract-secret'];
  if (secret && provided !== secret) {
    return false;
  }
  return true;
}

async function safeRunExtract(label) {
  if (extractionRunning) {
    console.warn(`[${label}] Extraction already running, skip.`);
    return { ok: false, skipped: true, message: 'already_running' };
  }
  extractionRunning = true;
  try {
    const r = await runExtractFromSupabaseStorage();
    console.log(`[${label}] Extraction finished:`, r);
    return { ok: true, ...r };
  } catch (e) {
    console.error(`[${label}] Extraction failed:`, e);
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  } finally {
    extractionRunning = false;
  }
}

async function safeRunExtractAll(label) {
  if (extractionRunning) {
    console.warn(`[${label}] Extraction already running, skip.`);
    return { ok: false, skipped: true, message: 'already_running' };
  }
  extractionRunning = true;
  try {
    const r = await runExtractFromSupabaseStorageUntilDone();
    console.log(`[${label}] Full batch run finished:`, r);
    return r;
  } catch (e) {
    console.error(`[${label}] Full batch run failed:`, e);
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  } finally {
    extractionRunning = false;
  }
}

async function safeRunNcOcrRerunAll(label) {
  if (extractionRunning) {
    console.warn(`[${label}] Extraction already running, skip.`);
    return { ok: false, skipped: true, message: 'already_running' };
  }
  extractionRunning = true;
  try {
    const r = await runNcOcrRerunAllFolders();
    console.log(`[${label}] OCR rerun (all folders) finished:`, r);
    return r;
  } catch (e) {
    console.error(`[${label}] OCR rerun failed:`, e);
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  } finally {
    extractionRunning = false;
  }
}

function handleExtractRequest(req, res, mode) {
  if (!checkExtractAuth(req)) {
    res.writeHead(401, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: false, error: 'unauthorized' }));
    return;
  }
  if (!process.env.OPENAI_API_KEY) {
    res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: false, error: 'OPENAI_API_KEY not set' }));
    return;
  }

  const message =
    mode === 'nc-rerun-all'
      ? 'OCR rerun (filtered NCs across To Fill 1, To Fill 2, Fill 3) started in background'
      : mode === 'all'
        ? 'full extraction (all pending batches) started in background'
        : 'extraction started in background';

  const modeKey =
    mode === 'nc-rerun-all' ? 'extract-nc-rerun-all' : mode === 'all' ? 'extract-all' : 'extract';

  res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ ok: true, accepted: true, message, mode: modeKey }));

  setImmediate(() => {
    if (mode === 'nc-rerun-all') {
      safeRunNcOcrRerunAll('http').catch((e) => console.error(e));
    } else if (mode === 'all') {
      safeRunExtractAll('http').catch((e) => console.error(e));
    } else {
      safeRunExtract('http').catch((e) => console.error(e));
    }
  });
}

function extractionHealthLines() {
  const table = process.env.EXTRACTION_TABLE || 'contract_extractions';
  const ncFilter = process.env.EXTRACTION_NC_FILTER?.trim();
  return (
    'Contract extraction:\n' +
    `  EXTRACTION_TABLE=${table}\n` +
    (ncFilter ? `  EXTRACTION_NC_FILTER=${ncFilter.slice(0, 80)}${ncFilter.length > 80 ? '…' : ''}\n` : '') +
    '  POST/GET /extract — one batch (MAX_EXTRACTION_FILES).\n' +
    '  POST/GET /extract-all — all pending in SUPABASE_STORAGE_FOLDER.\n' +
    '  POST/GET /extract-nc-rerun-all — OCR rerun for filtered NCs across To Fill 1/2/Fill 3.\n' +
    '  AUTO_EXTRACT_FROM_STORAGE=true: on boot, extract-all for one folder.\n' +
    '  AUTO_NC_OCR_RERUN_FROM_STORAGE=true: on boot, extract-nc-rerun-all (takes priority over blinding).\n' +
    '  Optional header: X-Extract-Secret: <EXTRACT_TRIGGER_SECRET>\n'
  );
}

const server = http.createServer(async (req, res) => {
  const url = req.url?.split('?')[0] || '/';

  if (url === '/' || url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(
      'Contract extraction + blinding service OK.\n\n' +
        extractionHealthLines() +
        '\n' +
        blindHealthLines(),
    );
    return;
  }

  if (url === '/extract' && (req.method === 'POST' || req.method === 'GET')) {
    handleExtractRequest(req, res, 'single');
    return;
  }

  if (url === '/extract-all' && (req.method === 'POST' || req.method === 'GET')) {
    handleExtractRequest(req, res, 'all');
    return;
  }

  if (url === '/extract-nc-rerun-all' && (req.method === 'POST' || req.method === 'GET')) {
    handleExtractRequest(req, res, 'nc-rerun-all');
    return;
  }

  if (url === '/blind' && (req.method === 'POST' || req.method === 'GET')) {
    handleBlindRequest(req, res, 'single');
    return;
  }

  if (url === '/blind-all' && (req.method === 'POST' || req.method === 'GET')) {
    handleBlindRequest(req, res, 'all');
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found\n');
});

server.listen(port, '0.0.0.0', () => {
  console.log(`Listening on 0.0.0.0:${port}`);

  const autoNcRerun = isEnvTruthy('AUTO_NC_OCR_RERUN_FROM_STORAGE');
  const autoExtract = isEnvTruthy('AUTO_EXTRACT_FROM_STORAGE');

  if (autoNcRerun) {
    console.log(
      'AUTO_NC_OCR_RERUN_FROM_STORAGE: scheduling OCR rerun across To Fill 1, To Fill 2, Fill 3 NC_1250-NC_1470...',
    );
    setImmediate(() => {
      safeRunNcOcrRerunAll('startup').catch((e) => console.error(e));
    });
  } else if (autoExtract) {
    console.log('AUTO_EXTRACT_FROM_STORAGE: scheduling full batch extraction (all pending files)...');
    setImmediate(() => {
      safeRunExtractAll('startup').catch((e) => console.error(e));
    });
  }

  // Do not auto-blind on the same boot when extraction is scheduled (they compete for OpenAI/CPU).
  if (autoNcRerun || autoExtract) {
    if (isEnvTruthy('AUTO_BLIND_FROM_STORAGE')) {
      console.warn(
        'AUTO_BLIND_FROM_STORAGE is set but skipped on this boot because extraction auto-run is active. ' +
          'Set AUTO_BLIND_FROM_STORAGE=false while running OCR rerun, or trigger /blind-all manually later.',
      );
    }
  } else {
    scheduleAutoBlindOnBoot();
  }
});
