/**
 * Copy office blinded PDFs from legacy Fill X Blinded folders into Blinded Missing.
 * Updates contract_blindings.output_file_name. Optionally removes legacy copies.
 */
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';
import {
  getOfficeBlindingSources,
  getOfficeBlindingDestination,
  getLegacyOfficeOutputDestinationsBySource,
  outputPdfPathForOfficeSource,
  possibleOfficeBlindedOutputPaths,
} from './storage-folders.mjs';
import {
  collectOfficeObjectPaths,
  collectAllFilePaths,
  downloadObject,
  uploadPdfObject,
  getSupabaseKey,
} from './storage-utils.mjs';

const __filename = fileURLToPath(import.meta.url);
const REMOVE_LEGACY = process.env.MIGRATE_REMOVE_LEGACY !== 'false';

async function updateOutputPath(supabase, sourceFileName, outputFileName) {
  const now = new Date().toISOString();
  const { error } = await supabase
    .from('contract_blindings')
    .update({ output_file_name: outputFileName, updated_at: now })
    .eq('source_file_name', sourceFileName);
  if (error) throw new Error(`Supabase update output path: ${error.message}`);
}

export async function migrateOfficeBlindedToBlindedMissing() {
  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  const bucket = process.env.SUPABASE_STORAGE_BUCKET;
  if (!url || !key || !bucket) {
    throw new Error('SUPABASE_URL, Supabase key, and SUPABASE_STORAGE_BUCKET are required.');
  }

  const supabase = createClient(url, key);
  const destFolder = getOfficeBlindingDestination();
  const legacyDests = getLegacyOfficeOutputDestinationsBySource();
  const existingInDest = new Set(await collectAllFilePaths(supabase, bucket, destFolder));

  /** @type {Map<string, Set<string>>} */
  const existingInLegacy = new Map();
  for (const legacyFolder of new Set(legacyDests.values())) {
    existingInLegacy.set(legacyFolder, new Set(await collectAllFilePaths(supabase, bucket, legacyFolder)));
  }

  let copied = 0;
  let skipped = 0;
  let missing = 0;
  let removed = 0;

  for (const source of getOfficeBlindingSources()) {
    const legacyFolder = legacyDests.get(source);
    if (!legacyFolder) continue;
    const legacySet = existingInLegacy.get(legacyFolder);
    const paths = await collectOfficeObjectPaths(supabase, bucket, source);

    for (const sourcePath of paths) {
      const newPath = outputPdfPathForOfficeSource(sourcePath, source, destFolder);
      if (existingInDest.has(newPath)) {
        skipped++;
        continue;
      }

      const candidates = possibleOfficeBlindedOutputPaths(sourcePath, source).filter((p) =>
        p.startsWith(`${legacyFolder}/`),
      );
      const legacyPath = candidates.find((p) => legacySet?.has(p));
      if (!legacyPath) {
        missing++;
        console.log(`  No legacy blinded PDF for ${sourcePath}`);
        continue;
      }

      const pdfBuffer = await downloadObject(supabase, bucket, legacyPath);
      await uploadPdfObject(supabase, bucket, newPath, pdfBuffer);
      existingInDest.add(newPath);
      await updateOutputPath(supabase, sourcePath, newPath);
      copied++;
      console.log(`  ${legacyPath} → ${newPath}`);

      if (REMOVE_LEGACY) {
        const { error } = await supabase.storage.from(bucket).remove([legacyPath]);
        if (error) console.warn(`  Could not remove legacy ${legacyPath}: ${error.message}`);
        else removed++;
      }
    }
  }

  return { copied, skipped, missing, removed, destFolder };
}

const isDirectRun =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isDirectRun) {
  migrateOfficeBlindedToBlindedMissing()
    .then((r) => {
      console.log('Migration done.', r);
      process.exit(0);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
