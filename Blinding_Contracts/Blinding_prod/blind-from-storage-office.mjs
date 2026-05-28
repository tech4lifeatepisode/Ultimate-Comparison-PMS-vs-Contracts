/**
 * Blind non-PDF contracts (Word / OpenDocument) from Supabase Storage.
 * Converts to PDF, applies redaction, uploads blinded PDF to destination folders.
 * Skips files already present in Blinded Missing (or legacy blinded folders).
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import { blindPdfBuffer } from './blind-core.mjs';
import { convertOfficeDocumentToPdf, destroyDocumentConverter } from './document-convert.mjs';
import {
  getOfficeBlindingSources,
  getOfficeBlindingDestination,
  outputPdfPathForOfficeSource,
  possibleOfficeBlindedOutputPaths,
  getLegacyOfficeOutputDestinationsBySource,
} from './storage-folders.mjs';
import {
  collectOfficeObjectPaths,
  collectAllFilePaths,
  downloadObject,
  uploadPdfObject,
  isEnvTruthy,
  getSupabaseKey,
} from './storage-utils.mjs';
import {
  markBlindingProcessing,
  markBlindingSuccess,
  markBlindingError,
  recordBlindingErrorRow,
} from './supabase-output.mjs';

const __filename = fileURLToPath(import.meta.url);

export { isEnvTruthy };

/**
 * @typedef {{ sourceFolder: string, destFolder: string, sourcePath: string, outputPath: string }} BlindJob
 */

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {string} destFolder
 */
async function fetchExistingBlindedPaths(supabase, bucket, destFolder) {
  const paths = await collectAllFilePaths(supabase, bucket, destFolder);
  return new Set(paths);
}

/**
 * @returns {Promise<{ jobs: BlindJob[], allAlreadyBlinded: boolean }>}
 */
async function collectPendingOfficeJobs(supabase, bucket) {
  const sources = getOfficeBlindingSources();
  const destFolder = getOfficeBlindingDestination();

  const maxRaw = process.env.MAX_BLINDING_FILES;
  const maxPerFolder =
    maxRaw != null && String(maxRaw).trim() !== '' ? Number(maxRaw) : null;

  const existingInDest = await fetchExistingBlindedPaths(supabase, bucket, destFolder);
  const legacyDests = getLegacyOfficeOutputDestinationsBySource();
  /** @type {Map<string, Set<string>>} */
  const existingInLegacy = new Map();
  for (const legacyFolder of new Set(legacyDests.values())) {
    existingInLegacy.set(legacyFolder, await fetchExistingBlindedPaths(supabase, bucket, legacyFolder));
  }

  /** @type {BlindJob[]} */
  const jobs = [];
  let totalListed = 0;

  for (const source of sources) {
    const paths = [...new Set(await collectOfficeObjectPaths(supabase, bucket, source))].sort();
    totalListed += paths.length;

    const pending = paths.filter((sourcePath) => {
      const outputs = possibleOfficeBlindedOutputPaths(sourcePath, source);
      if (outputs.some((p) => existingInDest.has(p))) return false;
      const legacyFolder = legacyDests.get(source);
      if (legacyFolder) {
        const legacySet = existingInLegacy.get(legacyFolder);
        const legacyOutputs = outputs.filter((p) => p.startsWith(`${legacyFolder}/`));
        if (legacyOutputs.some((p) => legacySet?.has(p))) return false;
      }
      return true;
    });

    if (isEnvTruthy('SKIP_ALREADY_BLINDED')) {
      console.log(
        `[${source}] ${paths.length} office file(s), ${paths.length - pending.length} already blinded, ${pending.length} pending → ${destFolder}.`,
      );
    } else {
      console.log(`[${source}] ${paths.length} office file(s) to process → ${destFolder}.`);
    }

    if (maxPerFolder != null && Number.isFinite(maxPerFolder) && maxPerFolder > 0 && pending.length > maxPerFolder) {
      console.log(`  Limiting to ${maxPerFolder} file(s) (MAX_BLINDING_FILES).`);
      pending = pending.slice(0, maxPerFolder);
    }

    for (const sourcePath of pending) {
      jobs.push({
        sourceFolder: source,
        destFolder,
        sourcePath,
        outputPath: outputPdfPathForOfficeSource(sourcePath, source, destFolder),
      });
    }
  }

  if (totalListed > 0 && jobs.length === 0 && isEnvTruthy('SKIP_ALREADY_BLINDED')) {
    return { jobs: [], allAlreadyBlinded: true };
  }

  return { jobs, allAlreadyBlinded: false };
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {BlindJob} job
 */
async function processOfficeBlindJob(supabase, bucket, job) {
  const baseName = path.basename(job.sourcePath);
  console.log(`Blinding (office): ${job.sourcePath} → ${job.outputPath}`);

  await markBlindingProcessing(supabase, {
    sourceFolder: job.sourceFolder,
    sourceFileName: job.sourcePath,
  });

  try {
    const fileBuffer = await downloadObject(supabase, bucket, job.sourcePath);
    const pdfBuffer = await convertOfficeDocumentToPdf(fileBuffer, baseName);
    const { buffer, redactionRegions, pagesRasterized } = await blindPdfBuffer(pdfBuffer, baseName);
    await uploadPdfObject(supabase, bucket, job.outputPath, buffer);
    await markBlindingSuccess(supabase, job.sourcePath, {
      outputFileName: job.outputPath,
      redactionRegions,
      pagesRasterized,
    });
    console.log(`  OK: uploaded ${job.outputPath}`);
    return { ok: true };
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    console.error(`  FAILED: ${message}`);
    try {
      await markBlindingError(supabase, job.sourcePath, message);
    } catch {
      await recordBlindingErrorRow(supabase, job.sourceFolder, job.sourcePath, message);
    }
    return { ok: false, error: message };
  }
}

export async function runBlindOfficeFromSupabaseStorage() {
  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  const bucket = process.env.SUPABASE_STORAGE_BUCKET;

  if (!url || !key) {
    throw new Error('SUPABASE_URL and a Supabase key are required for storage blinding.');
  }
  if (!bucket) {
    throw new Error('SUPABASE_STORAGE_BUCKET is required.');
  }

  const supabase = createClient(url, key);
  console.log(`Contract blinding (office / non-PDF) — bucket "${bucket}" → ${getOfficeBlindingDestination()}`);

  const { jobs, allAlreadyBlinded } = await collectPendingOfficeJobs(supabase, bucket);
  if (allAlreadyBlinded) {
    console.log('All office documents in configured source folders are already blinded.');
    return { processed: 0, failed: 0, allAlreadyBlinded: true };
  }
  if (jobs.length === 0) {
    console.log('No pending office documents found in source folders.');
    return { processed: 0, failed: 0 };
  }

  console.log(`Processing ${jobs.length} office file(s) this batch...`);
  let processed = 0;
  let failed = 0;

  try {
    for (const job of jobs) {
      const r = await processOfficeBlindJob(supabase, bucket, job);
      if (r.ok) processed++;
      else failed++;
    }
  } finally {
    await destroyDocumentConverter();
  }

  return { processed, failed, pendingThisBatch: jobs.length };
}

const isDirectRun =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isDirectRun) {
  runBlindOfficeFromSupabaseStorage()
    .then((r) => {
      console.log('Done.', r);
      process.exit(r.failed > 0 ? 1 : 0);
    })
    .catch(async (e) => {
      await destroyDocumentConverter().catch(() => {});
      console.error(e);
      process.exit(1);
    });
}
