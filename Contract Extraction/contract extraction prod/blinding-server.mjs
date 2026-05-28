/**
 * Extend the contract extraction Render server with blinding endpoints.
 * Same machine: https://contract-extraction-joyy.onrender.com
 */
import { runBlindFromSupabaseStorage, isEnvTruthy } from '../../Blinding_Contracts/Blinding_prod/blind-from-storage.mjs';
import { runBlindFromSupabaseStorageUntilDone } from '../../Blinding_Contracts/Blinding_prod/blind-from-storage-batch.mjs';
import { runAllBlindingFromSupabaseStorageUntilDone } from '../../Blinding_Contracts/Blinding_prod/blind-all-from-storage.mjs';

let blindingRunning = false;

function getBlindSecret() {
  return process.env.BLIND_TRIGGER_SECRET || process.env.EXTRACT_TRIGGER_SECRET;
}

function checkBlindAuth(req) {
  const secret = getBlindSecret();
  const provided = req.headers['x-blind-secret'] || req.headers['x-extract-secret'];
  if (secret && provided !== secret) return false;
  return true;
}

export async function safeRunBlind(label) {
  if (blindingRunning) {
    console.warn(`[${label}] Blinding already running, skip.`);
    return { ok: false, skipped: true, message: 'already_running' };
  }
  blindingRunning = true;
  try {
    const r = await runBlindFromSupabaseStorage();
    console.log(`[${label}] Blinding batch finished:`, r);
    return { ok: true, ...r };
  } catch (e) {
    console.error(`[${label}] Blinding failed:`, e);
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  } finally {
    blindingRunning = false;
  }
}

export async function safeRunBlindAll(label) {
  if (blindingRunning) {
    console.warn(`[${label}] Blinding already running, skip.`);
    return { ok: false, skipped: true, message: 'already_running' };
  }
  blindingRunning = true;
  try {
    const r = await runAllBlindingFromSupabaseStorageUntilDone();
    console.log(`[${label}] Full blinding run finished:`, r);
    return r;
  } catch (e) {
    console.error(`[${label}] Full blinding run failed:`, e);
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  } finally {
    blindingRunning = false;
  }
}

export function handleBlindRequest(req, res, mode) {
  if (!checkBlindAuth(req)) {
    res.writeHead(401, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: false, error: 'unauthorized' }));
    return;
  }

  const message =
    mode === 'all'
      ? 'full blinding (all pending batches) started in background'
      : 'blinding batch started in background';

  res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ ok: true, accepted: true, message, mode: mode === 'all' ? 'blind-all' : 'blind' }));

  setImmediate(() => {
    if (mode === 'all') {
      safeRunBlindAll('http').catch((e) => console.error(e));
    } else {
      safeRunBlind('http').catch((e) => console.error(e));
    }
  });
}

export function scheduleAutoBlindOnBoot() {
  if (isEnvTruthy('AUTO_BLIND_FROM_STORAGE')) {
    console.log('AUTO_BLIND_FROM_STORAGE: scheduling full blinding run...');
    setImmediate(() => {
      safeRunBlindAll('startup').catch((e) => console.error(e));
    });
  }
}

export function blindHealthLines() {
  return (
    'POST/GET /blind — one blinding batch (MAX_BLINDING_FILES per source folder).\n' +
    'POST/GET /blind-all — blind all pending PDFs + office docs (Fill X Blinded + Blinded Missing).\n' +
    'AUTO_BLIND_FROM_STORAGE=true: on boot, runs PDF and office blinding until done.\n' +
    'Optional headers: X-Blind-Secret or X-Extract-Secret: <BLIND_TRIGGER_SECRET or EXTRACT_TRIGGER_SECRET>\n'
  );
}
