/**
 * Full blinding run: PDFs → Fill X Blinded, office docs → Blinded Missing.
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { runBlindFromSupabaseStorageUntilDone } from './blind-from-storage-batch.mjs';
import { runBlindOfficeFromSupabaseStorageUntilDone } from './blind-from-storage-office-batch.mjs';
import { getOfficeBlindingDestination } from './storage-folders.mjs';

const __filename = fileURLToPath(import.meta.url);

if (process.env.SKIP_ALREADY_BLINDED !== 'false') {
  process.env.SKIP_ALREADY_BLINDED = 'true';
}

export async function runAllBlindingFromSupabaseStorageUntilDone() {
  console.log('\n========== Phase 1: PDF blinding (Fill X Blinded) ==========\n');
  const pdf = await runBlindFromSupabaseStorageUntilDone();

  console.log(`\n========== Phase 2: Office blinding (${getOfficeBlindingDestination()}) ==========\n`);
  const office = await runBlindOfficeFromSupabaseStorageUntilDone();

  const totalProcessed = (pdf.totalProcessed || 0) + (office.totalProcessed || 0);
  const totalFailed = (pdf.totalFailed || 0) + (office.totalFailed || 0);

  return {
    ok: pdf.ok && office.ok,
    totalProcessed,
    totalFailed,
    pdf,
    office,
    done: pdf.done && office.done,
  };
}

const isDirectRun =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isDirectRun) {
  runAllBlindingFromSupabaseStorageUntilDone()
    .then((r) => {
      console.log('All blinding done.', r);
      process.exit(r.ok ? 0 : 1);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
