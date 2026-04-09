/**
 * Repeatedly runs storage extraction until no pending files remain.
 * Requires SKIP_ALREADY_EXTRACTED=true (set below if unset) so each batch advances.
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { runExtractFromSupabaseStorage } from './extract-from-storage.mjs';

const __filename = fileURLToPath(import.meta.url);

if (process.env.SKIP_ALREADY_EXTRACTED !== 'false') {
  process.env.SKIP_ALREADY_EXTRACTED = 'true';
}

/**
 * Loops runExtractFromSupabaseStorage() until all storage paths are extracted or a hard stop.
 * @returns {Promise<{ ok: boolean, totalProcessed: number, rounds: number, done: boolean, error?: string }>}
 */
export async function runExtractFromSupabaseStorageUntilDone() {
  const folder = (process.env.SUPABASE_STORAGE_FOLDER || 'To Fill 2').replace(/\\/g, '/');
  const max = process.env.MAX_EXTRACTION_FILES || '(unlimited)';
  console.log(
    '\n=== Full batch run: starting at batch 1 (first files in sorted order) ===\n' +
      `Folder: "${folder}" · up to ${max} file(s) per round · SKIP_ALREADY_EXTRACTED=${process.env.SKIP_ALREADY_EXTRACTED}\n` +
      'If contract_extractions is empty, nothing is skipped and processing begins from the first path.\n',
  );

  let total = 0;
  let round = 0;
  for (;;) {
    round += 1;
    console.log(`\n--- Batch ${round} ---\n`);
    const r = await runExtractFromSupabaseStorage();
    total += r.processed || 0;

    if (r.allAlreadyExtracted) {
      console.log(`\nAll files in folder are extracted. Session total new rows: ${total}.`);
      return { ok: true, totalProcessed: total, rounds: round, done: true };
    }
    if ((r.processed || 0) > 0) {
      continue;
    }
    if (r.downloadsAllFailed) {
      console.error('\nStopping: batch had download failures for all files. Fix Storage/network and re-run.');
      return { ok: false, totalProcessed: total, rounds: round, done: false, error: 'downloads_all_failed' };
    }
    console.log('\nNo work done this round; stopping.');
    return { ok: true, totalProcessed: total, rounds: round, done: true };
  }
}

const isDirectRun =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isDirectRun) {
  runExtractFromSupabaseStorageUntilDone()
    .then((r) => {
      console.log('Done.', r);
      process.exit(r.ok ? 0 : 1);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
