/**
 * Compare Supabase Storage contract paths to contract_extractions.file_name.
 * Reports pending paths, duplicate rows per file_name, and DB rows whose path is not in Storage.
 */
import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { collectContractObjectPaths } from './extract-from-storage.mjs';

function getSupabaseKey() {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY
  );
}

/** @returns {Promise<Map<string, number>>} file_name -> row count */
async function fetchFileNameCounts(supabase) {
  const counts = new Map();
  const page = 1000;
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from('contract_extractions')
      .select('file_name')
      .not('file_name', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + page - 1);
    if (error) throw new Error(`Supabase: ${error.message}`);
    if (!data?.length) break;
    for (const row of data) {
      const fn = row.file_name;
      if (!fn) continue;
      counts.set(fn, (counts.get(fn) || 0) + 1);
    }
    if (data.length < page) break;
    from += page;
  }
  return counts;
}

async function main() {
  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  const bucket = process.env.SUPABASE_STORAGE_BUCKET;
  const folder = (process.env.SUPABASE_STORAGE_FOLDER || 'To Fill 1')
    .replace(/\\/g, '/')
    .replace(/^\/+|\/+$/g, '');

  if (!url || !key) {
    console.error('Set SUPABASE_URL and a Supabase key in .env');
    process.exit(1);
  }
  if (!bucket) {
    console.error('Set SUPABASE_STORAGE_BUCKET');
    process.exit(1);
  }

  const supabase = createClient(url, key);

  console.log(`Listing contract files in bucket "${bucket}" under "${folder}"...`);
  const paths = await collectContractObjectPaths(supabase, bucket, folder);
  const storageSet = new Set(paths);
  const storageCount = paths.length;

  console.log(`Fetching contract_extractions file_name counts...`);
  const fileNameCounts = await fetchFileNameCounts(supabase);

  let totalRows = 0;
  let uniqueFileNamesInDb = 0;
  let fileNamesWithDuplicates = 0;
  let extraDuplicateRows = 0;
  for (const [, n] of fileNameCounts) {
    totalRows += n;
    uniqueFileNamesInDb += 1;
    if (n > 1) {
      fileNamesWithDuplicates += 1;
      extraDuplicateRows += n - 1;
    }
  }

  const pending = [];
  for (const p of storageSet) {
    const n = fileNameCounts.get(p) || 0;
    if (n === 0) pending.push(p);
  }

  const inDbNotInStorage = [];
  for (const fn of fileNameCounts.keys()) {
    if (!storageSet.has(fn)) inDbNotInStorage.push(fn);
  }

  console.log('\n--- Summary ---');
  console.log(`Storage: ${storageCount} contract file path(s) under "${folder}".`);
  console.log(`Database: ${totalRows} row(s), ${uniqueFileNamesInDb} distinct file_name value(s).`);
  console.log(
    `Duplicates: ${fileNamesWithDuplicates} file_name(s) appear more than once; ${extraDuplicateRows} extra row(s) (re-runs of the same path).`,
  );
  console.log(`Pending extraction: ${pending.length} path(s) in Storage with no matching row (by file_name).`);

  if (pending.length > 0) {
    console.log('\nPending paths (first 50):');
    pending.sort().slice(0, 50).forEach((p) => console.log(`  - ${p}`));
    if (pending.length > 50) console.log(`  ... and ${pending.length - 50} more`);
  }

  if (inDbNotInStorage.length > 0) {
    console.log(
      `\nDB rows whose file_name is not in current Storage list: ${inDbNotInStorage.length} (e.g. moved/deleted files or old folder). First 20:`,
    );
    inDbNotInStorage.sort().slice(0, 20).forEach((p) => console.log(`  - ${p}`));
    if (inDbNotInStorage.length > 20) console.log(`  ... and ${inDbNotInStorage.length - 20} more`);
  }

  if (fileNamesWithDuplicates > 0) {
    console.log('\nfile_name values with more than one row (path: count):');
    const dups = [...fileNameCounts.entries()].filter(([, n]) => n > 1).sort((a, b) => b[1] - a[1]);
    dups.slice(0, 30).forEach(([fn, n]) => console.log(`  ${n}x  ${fn}`));
    if (dups.length > 30) console.log(`  ... and ${dups.length - 30} more distinct duplicated paths`);
  }

  const ok = pending.length === 0;
  console.log(`\n${ok ? 'OK: No pending Storage paths left to extract.' : 'Action needed: pending paths remain.'}`);
  process.exit(ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
