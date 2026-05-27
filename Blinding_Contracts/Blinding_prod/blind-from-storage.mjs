/**
 * Download PDFs from Supabase Storage source folders, blind, upload to blinded folders, track in DB.
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { blindPdfBuffer } from './blind-core.mjs';
import { getBlindingFolderPairs, outputPathForSource } from './storage-folders.mjs';
import {
  collectPdfObjectPaths,
  downloadObject,
  uploadPdfObject,
  isEnvTruthy,
  getSupabaseKey,
} from './storage-utils.mjs';
import {
  fetchSuccessfullyBlindedPaths,
  markBlindingProcessing,
  markBlindingSuccess,
  markBlindingError,
  recordBlindingErrorRow,
} from './supabase-output.mjs';
import { createClient } from '@supabase/supabase-js';

const __filename = fileURLToPath(import.meta.url);

export { isEnvTruthy };

/**
 * @typedef {{ sourceFolder: string, destFolder: string, sourcePath: string, outputPath: string }} BlindJob
 */

/**
 * @returns {Promise<{ jobs: BlindJob[], allAlreadyBlinded: boolean }>}
 */
async function collectPendingJobs(supabase, bucket) {
  const pairs = getBlindingFolderPairs();
  const skipDone = isEnvTruthy('SKIP_ALREADY_BLINDED');
  const done = skipDone ? await fetchSuccessfullyBlindedPaths(supabase) : new Set();

  const maxRaw = process.env.MAX_BLINDING_FILES;
  const maxPerFolder =
    maxRaw != null && String(maxRaw).trim() !== '' ? Number(maxRaw) : null;

  /** @type {BlindJob[]} */
  const jobs = [];
  let totalListed = 0;

  for (const { source, destination } of pairs) {
    const paths = [...new Set(await collectPdfObjectPaths(supabase, bucket, source))].sort();
    totalListed += paths.length;
    let pending = paths.filter((p) => !done.has(p));

    if (skipDone) {
      console.log(
        `[${source}] ${paths.length} PDF(s), ${paths.length - pending.length} already blinded, ${pending.length} pending.`,
      );
    } else {
      console.log(`[${source}] ${paths.length} PDF(s) to process.`);
    }

    if (maxPerFolder != null && Number.isFinite(maxPerFolder) && maxPerFolder > 0 && pending.length > maxPerFolder) {
      console.log(`  Limiting to ${maxPerFolder} file(s) (MAX_BLINDING_FILES).`);
      pending = pending.slice(0, maxPerFolder);
    }

    for (const sourcePath of pending) {
      jobs.push({
        sourceFolder: source,
        destFolder: destination,
        sourcePath,
        outputPath: outputPathForSource(sourcePath, source, destination),
      });
    }
  }

  if (totalListed > 0 && jobs.length === 0 && skipDone) {
    return { jobs: [], allAlreadyBlinded: true };
  }

  return { jobs, allAlreadyBlinded: false };
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {BlindJob} job
 */
async function processBlindJob(supabase, bucket, job) {
  const baseName = path.basename(job.sourcePath);
  console.log(`Blinding: ${job.sourcePath} → ${job.outputPath}`);

  await markBlindingProcessing(supabase, {
    sourceFolder: job.sourceFolder,
    sourceFileName: job.sourcePath,
  });

  try {
    const pdfBuffer = await downloadObject(supabase, bucket, job.sourcePath);
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

export async function runBlindFromSupabaseStorage() {
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
  console.log(`Contract blinding — bucket "${bucket}"`);

  const { jobs, allAlreadyBlinded } = await collectPendingJobs(supabase, bucket);
  if (allAlreadyBlinded) {
    console.log('All PDFs in configured source folders are already blinded.');
    return { processed: 0, failed: 0, allAlreadyBlinded: true };
  }
  if (jobs.length === 0) {
    console.log('No pending PDFs found in source folders.');
    return { processed: 0, failed: 0 };
  }

  console.log(`Processing ${jobs.length} file(s) this batch...`);
  let processed = 0;
  let failed = 0;

  for (const job of jobs) {
    const r = await processBlindJob(supabase, bucket, job);
    if (r.ok) processed++;
    else failed++;
  }

  return { processed, failed, pendingThisBatch: jobs.length };
}

const isDirectRun =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isDirectRun) {
  runBlindFromSupabaseStorage()
    .then((r) => {
      console.log('Done.', r);
      process.exit(r.failed > 0 ? 1 : 0);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
