/**
 * OCR rerun: extract only the configured NC list across To Fill 1, To Fill 2, and Fill 3.
 * Optionally scans blinded fallback folders for NCs missing from source folders (e.g. NC_3).
 * Requires supabase-schema-ocr-rerun.sql applied and EXTRACTION_TABLE=contract_extractions_ocr_rerun.
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import {
  DEFAULT_OCR_RERUN_FALLBACK_FOLDERS,
  DEFAULT_OCR_RERUN_FOLDERS,
  DEFAULT_OCR_RERUN_NCS,
  foldersListFromEnv,
  getExtractionTableName,
  logNcCoverage,
  parseNcFilterSet,
} from './extraction-config.mjs';
import {
  fetchExtractedNcSet,
  isEnvTruthy,
  mapNcPathsInFolders,
} from './extract-from-storage.mjs';
import { runExtractFromSupabaseStorageUntilDone } from './extract-from-storage-batch.mjs';

const __filename = fileURLToPath(import.meta.url);

function getSupabaseKey() {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY
  );
}

function fallbackFoldersFromEnv() {
  if (process.env.EXTRACTION_FALLBACK_FOLDERS?.trim() === 'none') return [];
  return foldersListFromEnv(process.env.EXTRACTION_FALLBACK_FOLDERS, DEFAULT_OCR_RERUN_FALLBACK_FOLDERS);
}

function useFallbackFolders() {
  if (process.env.EXTRACTION_USE_FALLBACK_FOLDERS === 'false') return false;
  return isEnvTruthy('EXTRACTION_USE_FALLBACK_FOLDERS') || process.env.EXTRACTION_USE_FALLBACK_FOLDERS == null;
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
  const primaryFolders = foldersListFromEnv(process.env.EXTRACTION_FOLDER_LIST, DEFAULT_OCR_RERUN_FOLDERS);
  const fallbackFolders = fallbackFoldersFromEnv();
  const runFallback = useFallbackFolders() && fallbackFolders.length > 0;

  console.log(`OCR rerun table: ${table}`);
  console.log(
    `NC filter (${ncFilter?.size ?? 0}): ${[...(ncFilter || [])].sort((a, b) => Number(a) - Number(b)).join(', ')}`,
  );
  console.log(`Primary folders: ${primaryFolders.join(' | ')}`);
  if (runFallback) {
    console.log(`Fallback folders: ${fallbackFolders.join(' | ')}`);
  }

  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  const bucket = process.env.SUPABASE_STORAGE_BUCKET;
  if (url && key && bucket && ncFilter) {
    const supabase = createClient(url, key);
    const primaryMap = await mapNcPathsInFolders(supabase, bucket, primaryFolders, ncFilter);
    logNcCoverage('Storage coverage (primary folders)', ncFilter, primaryMap);
    if (runFallback) {
      const fallbackMap = await mapNcPathsInFolders(supabase, bucket, fallbackFolders, ncFilter);
      const missingPrimary = [...ncFilter].filter((nc) => !primaryMap.has(nc));
      const fallbackHits = missingPrimary.filter((nc) => fallbackMap.has(nc));
      if (fallbackHits.length > 0) {
        console.log(
          `\nFallback only (not in primary): ${fallbackHits.join(', ')} — will try blinded copies after primary pass.`,
        );
        for (const nc of fallbackHits) {
          console.log(`  NC_${nc}: ${fallbackMap.get(nc)?.[0]}`);
        }
      }
    }
    console.log('');
  }

  /** @type {{ folder: string, ok: boolean, totalProcessed: number, rounds: number, done: boolean, error?: string, phase?: string }[]} */
  const results = [];

  for (const folder of primaryFolders) {
    process.env.SUPABASE_STORAGE_FOLDER = folder;
    console.log(`\n========== Primary folder: "${folder}" ==========\n`);
    const r = await runExtractFromSupabaseStorageUntilDone();
    results.push({ folder, phase: 'primary', ...r });
  }

  if (runFallback && url && key && bucket && ncFilter) {
    const supabase = createClient(url, key);
    const extracted = await fetchExtractedNcSet(supabase, table);
    const stillMissing = [...ncFilter].filter((nc) => !extracted.has(nc)).sort((a, b) => Number(a) - Number(b));
    const fallbackMap = await mapNcPathsInFolders(supabase, bucket, fallbackFolders, ncFilter);
    const fallbackToRun = stillMissing.filter((nc) => fallbackMap.has(nc));

    if (fallbackToRun.length > 0) {
      console.log(
        `\n========== Fallback pass (${fallbackToRun.length} NC(s) only in blinded folders) ==========`,
      );
      console.warn(
        'WARNING: Fallback uses blinded PDFs/DOCX copies — redactions may reduce extraction quality. Prefer re-uploading originals to To Fill 1/2.',
      );
      for (const nc of fallbackToRun) {
        console.log(`  NC_${nc}: ${fallbackMap.get(nc)?.[0]}`);
      }

      const foldersWithPending = new Set();
      for (const nc of fallbackToRun) {
        for (const p of fallbackMap.get(nc) || []) {
          foldersWithPending.add(p.split('/')[0]);
        }
      }

      for (const folder of fallbackFolders) {
        if (!foldersWithPending.has(folder)) continue;
        process.env.SUPABASE_STORAGE_FOLDER = folder;
        console.log(`\n========== Fallback folder: "${folder}" ==========\n`);
        const r = await runExtractFromSupabaseStorageUntilDone();
        results.push({ folder, phase: 'fallback', ...r });
      }
    }
  }

  const total = results.reduce((n, r) => n + (r.totalProcessed || 0), 0);
  console.log('\n========== OCR rerun summary ==========');
  for (const r of results) {
    console.log(
      `  [${r.phase || 'primary'}] ${r.folder}: processed=${r.totalProcessed} rounds=${r.rounds} ok=${r.ok}${r.error ? ` error=${r.error}` : ''}`,
    );
  }
  console.log(`  Total new rows: ${total}`);

  if (url && key && bucket && ncFilter) {
    const supabase = createClient(url, key);
    const extracted = await fetchExtractedNcSet(supabase, table);
    const allMaps = await mapNcPathsInFolders(
      supabase,
      bucket,
      [...primaryFolders, ...(runFallback ? fallbackFolders : [])],
      ncFilter,
    );
    const stillMissing = [...ncFilter].filter((nc) => !extracted.has(nc)).sort((a, b) => Number(a) - Number(b));
    console.log(`\nExtracted in ${table}: ${extracted.size}/${ncFilter.size} NC(s)`);
    if (stillMissing.length > 0) {
      console.log(`Still missing (${stillMissing.length}): ${stillMissing.join(', ')}`);
      for (const nc of stillMissing) {
        const paths = allMaps.get(nc);
        if (paths?.length) {
          console.log(`  NC_${nc}: file exists but not extracted — ${paths[0]}`);
        } else {
          console.log(`  NC_${nc}: NOT IN STORAGE — upload original contract to To Fill 1/2 or Fill 3`);
        }
      }
    }
  }

  return {
    ok: results.every((r) => r.ok),
    totalProcessed: total,
    folders: results,
  };
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
