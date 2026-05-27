/**
 * Supabase persistence for contract blinding runs.
 */
import { createClient } from '@supabase/supabase-js';
import { getSupabaseKey } from './storage-utils.mjs';

const TABLE = 'contract_blindings';

/**
 * @returns {import('@supabase/supabase-js').SupabaseClient}
 */
export function createSupabaseClient() {
  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  if (!url || !key) {
    throw new Error('SUPABASE_URL and a Supabase key are required.');
  }
  return createClient(url, key);
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 */
export async function fetchSuccessfullyBlindedPaths(supabase) {
  const set = new Set();
  const page = 1000;
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from(TABLE)
      .select('source_file_name')
      .eq('status', 'success')
      .not('source_file_name', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + page - 1);
    if (error) throw new Error(`Supabase read blinded paths: ${error.message}`);
    if (!data?.length) break;
    for (const row of data) {
      if (row.source_file_name) set.add(row.source_file_name);
    }
    if (data.length < page) break;
    from += page;
  }
  return set;
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {{ sourceFolder: string, sourceFileName: string }} params
 */
export async function markBlindingProcessing(supabase, { sourceFolder, sourceFileName }) {
  const now = new Date().toISOString();
  const { data, error } = await supabase
    .from(TABLE)
    .upsert(
      {
        source_folder: sourceFolder,
        source_file_name: sourceFileName,
        status: 'processing',
        error: null,
        updated_at: now,
      },
      { onConflict: 'source_file_name' },
    )
    .select('id')
    .single();
  if (error) throw new Error(`Supabase mark processing: ${error.message}`);
  return data.id;
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} sourceFileName
 * @param {{ outputFileName: string, redactionRegions: number, pagesRasterized: number }} result
 */
export async function markBlindingSuccess(supabase, sourceFileName, result) {
  const now = new Date().toISOString();
  const { error } = await supabase
    .from(TABLE)
    .update({
      status: 'success',
      output_file_name: result.outputFileName,
      redaction_regions: result.redactionRegions,
      pages_rasterized: result.pagesRasterized,
      error: null,
      completed_at: now,
      updated_at: now,
    })
    .eq('source_file_name', sourceFileName);
  if (error) throw new Error(`Supabase mark success: ${error.message}`);
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} sourceFileName
 * @param {string} message
 */
export async function markBlindingError(supabase, sourceFileName, message) {
  const now = new Date().toISOString();
  const { error } = await supabase
    .from(TABLE)
    .update({
      status: 'error',
      error: message.slice(0, 4000),
      completed_at: now,
      updated_at: now,
    })
    .eq('source_file_name', sourceFileName);
  if (error) throw new Error(`Supabase mark error: ${error.message}`);
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} sourceFolder
 * @param {string} sourceFileName
 * @param {string} message
 */
export async function recordBlindingErrorRow(supabase, sourceFolder, sourceFileName, message) {
  const now = new Date().toISOString();
  const { error } = await supabase.from(TABLE).upsert(
    {
      source_folder: sourceFolder,
      source_file_name: sourceFileName,
      status: 'error',
      error: message.slice(0, 4000),
      completed_at: now,
      updated_at: now,
    },
    { onConflict: 'source_file_name' },
  );
  if (error) throw new Error(`Supabase record error: ${error.message}`);
}
