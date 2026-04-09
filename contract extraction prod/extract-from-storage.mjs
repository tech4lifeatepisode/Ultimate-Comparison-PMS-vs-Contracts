/**
 * List contract files under SUPABASE_STORAGE_FOLDER, download from Storage, run extraction + DB sync.
 */
import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';
import path from 'path';
import { fileURLToPath } from 'url';
import { runExtractionPipeline } from './extract-contracts.mjs';
import { CONTRACT_FILE_EXTENSIONS } from './contract-file-types.mjs';

const __filename = fileURLToPath(import.meta.url);

const SUPPORTED_EXT = CONTRACT_FILE_EXTENSIONS;

function getSupabaseKey() {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY
  );
}

/** Accepts true/1/yes/on (case-insensitive). Render and .env sometimes use True or 1. */
export function isEnvTruthy(name) {
  const v = process.env[name];
  if (v == null) return false;
  const s = String(v).trim();
  if (s === '') return false;
  return /^(1|true|yes|on)$/i.test(s);
}

/**
 * @param {string} name
 */
function isContractFileName(name) {
  return SUPPORTED_EXT.has(path.extname(name).toLowerCase());
}

/**
 * List all objects/folders under prefix (Storage API returns max `limit` per call).
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {string} prefix folder path, no leading/trailing slashes
 */
async function listAllStorageItems(supabase, bucket, prefix) {
  const limit = 1000;
  const all = [];
  let offset = 0;
  for (;;) {
    const { data, error } = await supabase.storage.from(bucket).list(prefix || '', {
      limit,
      offset,
      sortBy: { column: 'name', order: 'asc' },
    });
    if (error) throw new Error(`Storage list "${prefix || '/'}": ${error.message}`);
    const chunk = data || [];
    all.push(...chunk);
    if (chunk.length < limit) break;
    offset += limit;
  }
  return all;
}

/**
 * Recursively collect storage object paths (relative to bucket root) for supported extensions.
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {string} prefix folder path, no leading/trailing slashes
 */
export async function collectContractObjectPaths(supabase, bucket, prefix) {
  const data = await listAllStorageItems(supabase, bucket, prefix);

  const out = [];
  for (const item of data || []) {
    const rel = prefix ? `${prefix}/${item.name}` : item.name;

    if (isContractFileName(item.name)) {
      out.push(rel);
      continue;
    }

    const meta = item.metadata;
    const fileSize =
      meta && typeof meta.size === 'number' ? meta.size : meta && meta.size != null ? Number(meta.size) : null;
    if (fileSize != null && !Number.isNaN(fileSize)) {
      continue;
    }

    const childPrefix = prefix ? `${prefix}/${item.name}` : item.name;
    try {
      const nested = await collectContractObjectPaths(supabase, bucket, childPrefix);
      out.push(...nested);
    } catch (e) {
      console.warn(`Skipping storage path "${rel}":`, e instanceof Error ? e.message : e);
    }
  }
  return out;
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {string} objectPath
 */
async function downloadObject(supabase, bucket, objectPath) {
  const maxAttempts = Number(process.env.STORAGE_DOWNLOAD_RETRIES) || 4;
  let lastMsg = '';
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const { data, error } = await supabase.storage.from(bucket).download(objectPath);
    if (!error && data) {
      return Buffer.from(await data.arrayBuffer());
    }
    lastMsg = error?.message || 'unknown error';
    if (attempt < maxAttempts) {
      await new Promise((r) => setTimeout(r, 500 * attempt * attempt));
    }
  }
  throw new Error(`Download "${objectPath}": ${lastMsg}`);
}

/**
 * When Storage list is empty (bucket name, RLS), optionally fetch public object URLs.
 * Same shape as …/object/public/Contracts/<SUPABASE_STORAGE_FOLDER>/file.docx
 * @param {string} urlText newline- or comma-separated
 */
async function entriesFromPublicUrls(urlText) {
  const urls = urlText
    .split(/[\n,]+/)
    .map((s) => s.trim())
    .filter(Boolean);
  /** @type {{ name: string, buffer: Buffer, storagePath?: string }[]} */
  const out = [];
  for (const u of urls) {
    const res = await fetch(u);
    if (!res.ok) {
      throw new Error(`GET ${u} → HTTP ${res.status}`);
    }
    const buf = Buffer.from(await res.arrayBuffer());
    let pathname;
    try {
      pathname = new URL(u).pathname;
    } catch {
      throw new Error(`Invalid URL: ${u}`);
    }
    const lastSeg = decodeURIComponent(pathname.split('/').pop() || 'document');
    out.push({ name: lastSeg, buffer: buf, storagePath: u });
  }
  return out;
}

/**
 * Paths already stored in contract_extractions.file_name (full storage path, e.g. To Fill 2/x.pdf).
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 */
async function fetchAlreadyExtractedStoragePaths(supabase) {
  const set = new Set();
  const page = 1000;
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from('contract_extractions')
      .select('file_name')
      .not('file_name', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + page - 1);
    if (error) throw new Error(`Supabase read file_name: ${error.message}`);
    if (!data?.length) break;
    for (const row of data) {
      if (row.file_name) set.add(row.file_name);
    }
    if (data.length < page) break;
    from += page;
  }
  return set;
}

export async function runExtractFromSupabaseStorage() {
  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  const bucket = process.env.SUPABASE_STORAGE_BUCKET;
  const folder =
    (process.env.SUPABASE_STORAGE_FOLDER || 'To Fill 2').replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');

  if (!url || !key) {
    throw new Error('SUPABASE_URL and a Supabase key are required for storage extraction.');
  }
  if (!bucket) {
    throw new Error('SUPABASE_STORAGE_BUCKET is required.');
  }

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY is required.');
  }

  const supabase = createClient(url, key);
  console.log(`Listing contracts in bucket "${bucket}" under "${folder || '(root)'}"...`);

  const paths = await collectContractObjectPaths(supabase, bucket, folder);
  let sorted = [...new Set(paths)].sort();

  if (isEnvTruthy('SKIP_ALREADY_EXTRACTED')) {
    const done = await fetchAlreadyExtractedStoragePaths(supabase);
    const n = sorted.length;
    sorted = sorted.filter((p) => !done.has(p));
    console.log(
      `SKIP_ALREADY_EXTRACTED: ${n - sorted.length} already in contract_extractions, ${sorted.length} not yet extracted.`,
    );
  }

  const maxRaw = process.env.MAX_EXTRACTION_FILES;
  const maxFiles = maxRaw != null && String(maxRaw).trim() !== '' ? Number(maxRaw) : null;
  const pendingBeforeSlice = sorted.length;
  if (maxFiles != null && Number.isFinite(maxFiles) && maxFiles > 0 && sorted.length > maxFiles) {
    console.log(`Limiting to ${maxFiles} of ${sorted.length} file(s) (MAX_EXTRACTION_FILES).`);
    sorted = sorted.slice(0, maxFiles);
  }

  /** @type {{ name: string, buffer: Buffer, storagePath?: string }[]} */
  let entries = [];

  if (sorted.length > 0) {
    console.log(`Found ${sorted.length} file(s) via Storage API. Downloading...`);
    for (const objectPath of sorted) {
      try {
        const buf = await downloadObject(supabase, bucket, objectPath);
        const baseName = path.basename(objectPath);
        entries.push({ name: baseName, buffer: buf, storagePath: objectPath });
      } catch (e) {
        console.error(
          `[download failed] ${objectPath}:`,
          e instanceof Error ? e.message : e,
        );
      }
    }
  } else if (paths.length > 0) {
    console.log('No files left to extract in this folder (all listed objects already in contract_extractions).');
    return { processed: 0, allAlreadyExtracted: true, pendingBeforeSlice: 0 };
  } else {
    const publicUrls = process.env.CONTRACT_PUBLIC_URLS || '';
    if (publicUrls.trim()) {
      console.log('Storage list empty; using CONTRACT_PUBLIC_URLS (public URLs).');
      entries = await entriesFromPublicUrls(publicUrls);
    } else {
      console.log(
        'No contract files (pdf/docx/images) found. Set SUPABASE_STORAGE_BUCKET to match public URLs (e.g. Contracts), check SUPABASE_STORAGE_FOLDER, or set CONTRACT_PUBLIC_URLS with newline-separated public object URLs.',
      );
      return { processed: 0 };
    }
  }

  if (entries.length === 0) {
    if (sorted.length > 0) {
      console.error('All storage downloads failed (0 files ready). Check network or Storage; see logs above.');
      return { processed: 0, downloadsAllFailed: true, pendingBeforeSlice };
    }
    return { processed: 0, pendingBeforeSlice };
  }

  console.log(`Extracting ${entries.length} file(s)...`);

  const client = new OpenAI({ apiKey });
  await runExtractionPipeline(client, entries);

  return { processed: entries.length, pendingBeforeSlice };
}

const isDirectRun =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isDirectRun) {
  runExtractFromSupabaseStorage()
    .then((r) => {
      console.log('Done.', r);
      process.exit(0);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
