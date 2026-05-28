/**
 * Extract local PDFs → contract_extractions_ocr_rerun via OpenAI (sandbox .env) + Supabase (prod .env).
 */
import dotenv from '../Contract Extraction/contract extraction prod/node_modules/dotenv/lib/main.js';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import OpenAI from '../Contract Extraction/contract extraction prod/node_modules/openai/index.mjs';
import { createClient } from '../Contract Extraction/contract extraction prod/node_modules/@supabase/supabase-js/dist/index.mjs';
import { runExtractionPipeline } from '../Contract Extraction/contract extraction prod/extract-contracts.mjs';
import { isContractFileName } from '../Contract Extraction/contract extraction prod/contract-file-types.mjs';
import { extractNcFromStoragePath, normalizeNcNumber } from '../Contract Extraction/contract extraction prod/extraction-config.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LOCAL_PREFIX = 'Local Extraction and Blinding';

dotenv.config({ path: path.resolve(__dirname, '../Contract Extraction/contract extraction sandbox/.env') });
dotenv.config({ path: path.resolve(__dirname, '../Contract Extraction/contract extraction prod/.env') });

process.env.EXTRACTION_TABLE = 'contract_extractions_ocr_rerun';

function getSupabaseKey() {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY
  );
}

async function loadLocalEntries() {
  const entries = await fs.readdir(__dirname, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && isContractFileName(e.name) && !e.name.endsWith('.mjs'))
    .map((e) => e.name)
    .sort();

  /** @type {{ name: string, buffer: Buffer, storagePath: string }[]} */
  const out = [];
  for (const name of files) {
    const buf = await fs.readFile(path.join(__dirname, name));
    out.push({
      name,
      buffer: buf,
      storagePath: `${LOCAL_PREFIX}/${name}`,
    });
  }
  return out;
}

async function fetchExistingFileNames(tableName) {
  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  if (!url || !key) return new Set();

  const supabase = createClient(url, key);
  const set = new Set();
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from(tableName)
      .select('file_name')
      .like('file_name', `${LOCAL_PREFIX}/%`)
      .range(from, from + 999);
    if (error) throw new Error(error.message);
    if (!data?.length) break;
    for (const row of data) {
      if (row.file_name) set.add(row.file_name);
    }
    if (data.length < 1000) break;
    from += 1000;
  }
  return set;
}

export async function runLocalExtractToOcrRerun() {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY missing — set in contract extraction sandbox/.env');
  }
  if (!process.env.SUPABASE_URL || !getSupabaseKey()) {
    throw new Error('Supabase credentials missing — set in contract extraction prod/.env');
  }

  const all = await loadLocalEntries();
  if (all.length === 0) {
    console.log('No contract PDFs found in Local Extraction and Blinding.');
    return { processed: 0, skipped: 0 };
  }

  const existing = await fetchExistingFileNames('contract_extractions_ocr_rerun');
  const pending = all.filter((e) => !existing.has(e.storagePath));
  const skipped = all.length - pending.length;

  console.log(`Local contracts: ${all.length}, already in ocr_rerun: ${skipped}, to extract: ${pending.length}`);
  for (const e of all) {
    const nc = extractNcFromStoragePath(e.storagePath) || normalizeNcNumber(e.name);
    console.log(`  NC_${nc || '?'} — ${e.name}${existing.has(e.storagePath) ? ' (skip)' : ''}`);
  }

  if (pending.length === 0) {
    console.log('Nothing to extract.');
    return { processed: 0, skipped };
  }

  const client = new OpenAI({ apiKey });
  await runExtractionPipeline(client, pending);
  console.log(`Extracted ${pending.length} local file(s). Rows synced to contract_extractions_ocr_rerun by pipeline.`);
  return { processed: pending.length, skipped };
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));

if (isDirectRun) {
  runLocalExtractToOcrRerun()
    .then((r) => {
      console.log('Done.', r);
      process.exit(0);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
