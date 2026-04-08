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

/**
 * @param {string} name
 */
function isContractFileName(name) {
  return SUPPORTED_EXT.has(path.extname(name).toLowerCase());
}

/**
 * Recursively collect storage object paths (relative to bucket root) for supported extensions.
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {string} prefix folder path, no leading/trailing slashes
 */
async function collectContractObjectPaths(supabase, bucket, prefix) {
  const { data, error } = await supabase.storage.from(bucket).list(prefix || '', {
    limit: 1000,
    sortBy: { column: 'name', order: 'asc' },
  });
  if (error) throw new Error(`Storage list "${prefix || '/'}": ${error.message}`);

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
  const { data, error } = await supabase.storage.from(bucket).download(objectPath);
  if (error) throw new Error(`Download "${objectPath}": ${error.message}`);
  const buf = Buffer.from(await data.arrayBuffer());
  return buf;
}

/**
 * When Storage list is empty (bucket name, RLS), optionally fetch public object URLs.
 * Same shape as …/object/public/Contracts/HouseMonk%20Sample%20Contracts/file.docx
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

export async function runExtractFromSupabaseStorage() {
  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  const bucket = process.env.SUPABASE_STORAGE_BUCKET;
  const folder =
    (process.env.SUPABASE_STORAGE_FOLDER || 'HouseMonk Sample Contracts').replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');

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
  const sorted = [...new Set(paths)].sort();

  /** @type {{ name: string, buffer: Buffer, storagePath?: string }[]} */
  let entries = [];

  if (sorted.length > 0) {
    console.log(`Found ${sorted.length} file(s) via Storage API. Downloading...`);
    for (const objectPath of sorted) {
      const buf = await downloadObject(supabase, bucket, objectPath);
      const baseName = path.basename(objectPath);
      entries.push({ name: baseName, buffer: buf, storagePath: objectPath });
    }
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
    return { processed: 0 };
  }

  console.log(`Extracting ${entries.length} file(s)...`);

  const client = new OpenAI({ apiKey });
  await runExtractionPipeline(client, entries);

  return { processed: entries.length };
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
