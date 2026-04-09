import { createClient } from '@supabase/supabase-js';
import fs from 'fs/promises';
import path from 'path';

/**
 * @param {object} row
 */
function rowToRecord(row) {
  const finalRent = row.final_rent ?? null;
  return {
    nc: row.nc ?? null,
    contract_title: row.contract_title ?? null,
    name: row.name ?? null,
    id_type: row.id_type ?? null,
    id_number: row.id_number ?? null,
    number_of_people: row.number_of_people ?? null,
    unit_type: row.unit_type ?? null,
    check_in_date: row.check_in_date ?? null,
    check_out_date: row.check_out_date ?? null,
    // Legacy column: dashboards that still read "rent" see the same total as final_rent (CSV v2).
    rent: finalRent ?? row.rent ?? null,
    base_rent: row.base_rent ?? null,
    extra_name: row.extra_name ?? null,
    extra_price: row.extra_price ?? null,
    discount_type: row.discount_type ?? null,
    price_after_discount: row.price_after_discount ?? null,
    discount_price: row.discount_price ?? null,
    final_rent: finalRent,
    deposit: row.deposit ?? null,
    deposit_wording: row.deposit_wording ?? null,
    deposit_source: row.deposit_source ?? null,
    file_name: row.file_name ?? null,
    model_used: row.model_used ?? null,
    error: row.error ?? null,
  };
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {object[]} records
 */
async function insertContractRows(supabase, records) {
  if (records.length === 0) return;
  const { data, error } = await supabase
    .from('contract_extractions')
    .insert(records)
    .select('id, nc, contract_title, base_rent, final_rent, rent, file_name');

  if (error) {
    const hint =
      /column|does not exist|schema/i.test(error.message || '')
        ? ' Run supabase-schema.sql (or supabase-migration-csv-v2-headers.sql) in Supabase SQL Editor so all v2 columns exist.'
        : '';
    throw new Error(`Supabase insert: ${error.message}${error.details ? ` — ${error.details}` : ''}${hint}`);
  }

  const first = data && data[0];
  if (first) {
    console.log(
      `Supabase: row shape check — id=${first.id} contract_title=${JSON.stringify(first.contract_title)} base_rent=${JSON.stringify(first.base_rent)} final_rent=${JSON.stringify(first.final_rent)} rent=${JSON.stringify(first.rent)}`,
    );
  }
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} csvPath
 * @param {string} bucket
 * @param {string} folderPrefix e.g. "To Fill 2"
 */
async function uploadCsvToStorage(supabase, csvPath, bucket, folderPrefix) {
  const buf = await fs.readFile(csvPath);
  const base = path.basename(csvPath);
  const prefix = folderPrefix.replace(/\\/g, '/').replace(/\/+$/, '');
  const objectPath = `${prefix}/${base}`;
  const { error } = await supabase.storage.from(bucket).upload(objectPath, buf, {
    contentType: 'text/csv; charset=utf-8',
    upsert: true,
  });
  if (error) throw new Error(`Supabase storage upload: ${error.message}`);
  return objectPath;
}

/**
 * @param {object} options
 * @param {string} options.csvPath
 * @param {object[]} options.dataRows Plain objects with file_name, error, etc. per CSV row
 */
export async function syncExtractionsToSupabase({ csvPath, dataRows }) {
  const url = process.env.SUPABASE_URL;
  const key =
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY;

  if (!url || !key) {
    console.warn(
      'Supabase: skipped (set SUPABASE_URL and SUPABASE_ANON_KEY or SUPABASE_SERVICE_ROLE_KEY in .env).',
    );
    return;
  }

  const supabase = createClient(url, key);
  const bucket = process.env.SUPABASE_STORAGE_BUCKET;
  const folder = process.env.SUPABASE_STORAGE_FOLDER || 'To Fill 2';

  const records = dataRows.map(rowToRecord);
  await insertContractRows(supabase, records);
  console.log(`Supabase: inserted ${records.length} row(s) into contract_extractions.`);

  if (bucket) {
    const objectPath = await uploadCsvToStorage(supabase, csvPath, bucket, folder);
    console.log(`Supabase Storage: uploaded CSV to bucket "${bucket}" at "${objectPath}".`);
  } else {
    console.warn(
      'Supabase Storage: skipped (set SUPABASE_STORAGE_BUCKET to your bucket name in Dashboard > Storage).',
    );
  }
}
