/**
 * OCR rerun: extract only the configured NC list across To Fill 1, To Fill 2, and Fill 3.
 * Requires supabase-schema-ocr-rerun.sql applied and EXTRACTION_TABLE=contract_extractions_ocr_rerun.
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  DEFAULT_OCR_RERUN_FOLDERS,
  DEFAULT_OCR_RERUN_NCS,
  getExtractionTableName,
  parseNcFilterSet,
} from './extraction-config.mjs';
import { runExtractFromSupabaseStorageUntilDone } from './extract-from-storage-batch.mjs';

const __filename = fileURLToPath(import.meta.url);

function foldersFromEnv() {
  const raw = process.env.EXTRACTION_FOLDER_LIST?.trim();
  if (!raw) return DEFAULT_OCR_RERUN_FOLDERS;
  return raw
    .split(/[\n,;]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export async function runNcOcrRerunAllFolders() {
  if (!process.env.EXTRACTION_NC_FILTER?.trim()) {
    process.env.EXTRACTION_NC_FILTER = DEFAULT_OCR_RERUN_NCS;
  }
  if (!process.env.EXTRACTION_TABLE?.trim()) {
    process.env.EXTRACTION_TABLE = 'contract_extractions_ocr_rerun';
  }
  process.env.SKIP_ALREADY_EXTRACTED = 'true';

  const table = getExtractionTableName();
  const ncFilter = parseNcFilterSet(process.env.EXTRACTION_NC_FILTER);
  const folders = foldersFromEnv();

  console.log(`OCR rerun table: ${table}`);
  console.log(`NC filter (${ncFilter?.size ?? 0}): ${[...(ncFilter || [])].sort((a, b) => Number(a) - Number(b)).join(', ')}`);
  console.log(`Folders: ${folders.join(' | ')}\n`);

  /** @type {{ folder: string, ok: boolean, totalProcessed: number, rounds: number, done: boolean, error?: string }[]} */
  const results = [];

  for (const folder of folders) {
    process.env.SUPABASE_STORAGE_FOLDER = folder;
    console.log(`\n========== Folder: "${folder}" ==========\n`);
    const r = await runExtractFromSupabaseStorageUntilDone();
    results.push({ folder, ...r });
  }

  const total = results.reduce((n, r) => n + (r.totalProcessed || 0), 0);
  console.log('\n========== OCR rerun summary ==========');
  for (const r of results) {
    console.log(
      `  ${r.folder}: processed=${r.totalProcessed} rounds=${r.rounds} ok=${r.ok}${r.error ? ` error=${r.error}` : ''}`,
    );
  }
  console.log(`  Total new rows: ${total}`);
  return { ok: results.every((r) => r.ok), totalProcessed: total, folders: results };
}

const isDirectRun =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isDirectRun) {
  runNcOcrRerunAllFolders()
    .then((r) => {
      console.log('Done.', r);
      process.exit(r.ok ? 0 : 1);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
