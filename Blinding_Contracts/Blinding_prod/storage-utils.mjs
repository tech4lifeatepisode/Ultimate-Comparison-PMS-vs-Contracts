import path from 'path';

const PDF_EXT = new Set(['.pdf']);

/**
 * @param {string} name
 */
export function isPdfFileName(name) {
  return PDF_EXT.has(path.extname(name).toLowerCase());
}

/** @param {string} name */
export function isEnvTruthy(name) {
  const v = process.env[name];
  if (v == null) return false;
  const s = String(v).trim();
  if (s === '') return false;
  return /^(1|true|yes|on)$/i.test(s);
}

export function getSupabaseKey() {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY
  );
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {string} prefix
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
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {string} prefix
 */
export async function collectPdfObjectPaths(supabase, bucket, prefix) {
  const data = await listAllStorageItems(supabase, bucket, prefix);
  const out = [];

  for (const item of data || []) {
    const rel = prefix ? `${prefix}/${item.name}` : item.name;

    if (isPdfFileName(item.name)) {
      out.push(rel);
      continue;
    }

    const meta = item.metadata;
    const fileSize =
      meta && typeof meta.size === 'number' ? meta.size : meta && meta.size != null ? Number(meta.size) : null;
    if (fileSize != null && !Number.isNaN(fileSize)) continue;

    const childPrefix = prefix ? `${prefix}/${item.name}` : item.name;
    try {
      const nested = await collectPdfObjectPaths(supabase, bucket, childPrefix);
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
export async function downloadObject(supabase, bucket, objectPath) {
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
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} bucket
 * @param {string} objectPath
 * @param {Buffer} body
 */
export async function uploadPdfObject(supabase, bucket, objectPath, body) {
  const { error } = await supabase.storage.from(bucket).upload(objectPath, body, {
    contentType: 'application/pdf',
    upsert: true,
  });
  if (error) throw new Error(`Upload "${objectPath}": ${error.message}`);
}
