/**
 * Repeatedly runs office-document blinding until no pending non-PDF files remain.
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { runBlindOfficeFromSupabaseStorage } from './blind-from-storage-office.mjs';

const __filename = fileURLToPath(import.meta.url);

if (process.env.SKIP_ALREADY_BLINDED !== 'false') {
  process.env.SKIP_ALREADY_BLINDED = 'true';
}

export async function runBlindOfficeFromSupabaseStorageUntilDone() {
  const pairs = process.env.BLINDING_FOLDER_PAIRS_JSON ? '(custom JSON)' : 'To Fill 1/2/Fill 3 NC_1250-NC_1470';
  const max = process.env.MAX_BLINDING_FILES || '(unlimited per folder)';
  console.log(
    `\n=== Full office blinding run (non-PDF) ===\n` +
      `Folders: ${pairs} · up to ${max} file(s) per folder per round · SKIP_ALREADY_BLINDED=${process.env.SKIP_ALREADY_BLINDED}\n`,
  );

  let totalProcessed = 0;
  let totalFailed = 0;
  let round = 0;

  for (;;) {
    round += 1;
    console.log(`\n--- Office blinding batch ${round} ---\n`);
    const r = await runBlindOfficeFromSupabaseStorage();
    totalProcessed += r.processed || 0;
    totalFailed += r.failed || 0;

    if (r.allAlreadyBlinded) {
      console.log(
        `\nAll office documents in configured source folders are blinded. Processed this session: ${totalProcessed}, failed: ${totalFailed}.`,
      );
      return { ok: totalFailed === 0, totalProcessed, totalFailed, rounds: round, done: true };
    }

    if ((r.processed || 0) > 0 || (r.failed || 0) > 0) {
      if ((r.processed || 0) === 0 && (r.failed || 0) > 0) {
        console.log('\nBatch had failures but no successes; stopping to avoid a tight error loop.');
        return { ok: false, totalProcessed, totalFailed, rounds: round, done: false, error: 'batch_all_failed' };
      }
      if ((r.processed || 0) > 0) continue;
    }

    console.log('\nNo work done this round; stopping.');
    return { ok: true, totalProcessed, totalFailed, rounds: round, done: true };
  }
}

const isDirectRun =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isDirectRun) {
  runBlindOfficeFromSupabaseStorageUntilDone()
    .then((r) => {
      console.log('Done.', r);
      process.exit(r.ok ? 0 : 1);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
