/**
 * HTTP server for Render Web Service (binds to PORT).
 * Optional: AUTO_EXTRACT_FROM_STORAGE=true runs full batch loop (all pending files) after listen.
 * GET/POST /extract — one batch (MAX_EXTRACTION_FILES). X-Extract-Secret if EXTRACT_TRIGGER_SECRET set.
 * GET/POST /extract-all — loop until folder done (same auth).
 */
import http from 'http';
import { runExtractFromSupabaseStorage, isEnvTruthy } from './extract-from-storage.mjs';
import { runExtractFromSupabaseStorageUntilDone } from './extract-from-storage-batch.mjs';

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
    mode === 'all'
      ? 'full extraction (all pending batches) started in background'
      : 'extraction started in background';

  res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ ok: true, accepted: true, message, mode: mode === 'all' ? 'extract-all' : 'extract' }));

  setImmediate(() => {
    if (mode === 'all') {
      safeRunExtractAll('http').catch((e) => console.error(e));
    } else {
      safeRunExtract('http').catch((e) => console.error(e));
    }
  });
}

const server = http.createServer(async (req, res) => {
  const url = req.url?.split('?')[0] || '/';

  if (url === '/' || url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(
      'Contract extraction service OK.\n' +
        'AUTO_EXTRACT_FROM_STORAGE=true: on boot, runs all batches until the folder is done.\n' +
        'POST/GET /extract — one batch (MAX_EXTRACTION_FILES). POST/GET /extract-all — until done.\n' +
        'Optional header: X-Extract-Secret: <EXTRACT_TRIGGER_SECRET>\n',
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

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found\n');
});

server.listen(port, '0.0.0.0', () => {
  console.log(`Listening on 0.0.0.0:${port}`);

  if (isEnvTruthy('AUTO_EXTRACT_FROM_STORAGE')) {
    console.log('AUTO_EXTRACT_FROM_STORAGE: scheduling full batch extraction (all pending files)...');
    setImmediate(() => {
      safeRunExtractAll('startup').catch((e) => console.error(e));
    });
  }
});
