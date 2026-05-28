import path from 'path';

const DEFAULT_TABLE = 'contract_extractions';

/** @returns {string} Postgres table name for extraction rows (validated). */
export function getExtractionTableName() {
  const raw = (process.env.EXTRACTION_TABLE || DEFAULT_TABLE).trim();
  if (!/^[a-z][a-z0-9_]*$/.test(raw)) {
    throw new Error(`Invalid EXTRACTION_TABLE "${raw}" (use lowercase letters, digits, underscores).`);
  }
  return raw;
}

/**
 * Normalize NC token to numeric string without leading zeros (3, 702, 1345).
 * @param {string} token e.g. "NC_0702", "702", "NC_3"
 */
export function normalizeNcNumber(token) {
  const s = String(token).trim();
  const m = s.match(/NC[_\s-]*(\d+)/i) || s.match(/^(\d+)$/);
  if (!m) return null;
  const n = parseInt(m[1], 10);
  return Number.isFinite(n) ? String(n) : null;
}

/**
 * @param {string | undefined} raw comma/newline/semicolon-separated NC list
 * @returns {Set<string> | null} normalized NC numbers, or null if no filter
 */
export function parseNcFilterSet(raw) {
  const text = raw?.trim();
  if (!text) return null;
  const set = new Set();
  for (const part of text.split(/[\n,;]+/)) {
    const nc = normalizeNcNumber(part);
    if (nc) set.add(nc);
  }
  return set.size > 0 ? set : null;
}

/**
 * @param {string} objectPath storage path e.g. "To Fill 2/NC_0702.pdf"
 * @returns {string | null}
 */
export function extractNcFromStoragePath(objectPath) {
  const base = path.basename(objectPath, path.extname(objectPath)).replace(/\.docx$/i, '');
  const m = base.match(/NC[_\s-]*(\d+)/i);
  return m ? normalizeNcNumber(m[1]) : null;
}

/**
 * @param {string} objectPath
 * @param {Set<string> | null} ncFilter
 */
export function pathMatchesNcFilter(objectPath, ncFilter) {
  if (!ncFilter) return true;
  const nc = extractNcFromStoragePath(objectPath);
  return nc != null && ncFilter.has(nc);
}

/** NCs missed by OpenAI OCR — default for OCR rerun script. */
export const DEFAULT_OCR_RERUN_NCS =
  'NC_3,NC_4,NC_15,NC_17,NC_18,NC_26,NC_362,NC_702,NC_703,NC_750,NC_795,NC_829,NC_857,NC_876,NC_890,NC_953,NC_954,NC_1070,NC_1104,NC_1108,NC_1127,NC_1141,NC_1214,NC_1222,NC_1248,NC_1345';

/** Storage folders to scan for the OCR rerun batch. */
export const DEFAULT_OCR_RERUN_FOLDERS = ['To Fill 1', 'To Fill 2', 'Fill 3 NC_1250-NC_1470'];

/** Fallback folders when an NC is not in source folders (e.g. NC_3 only in Fill 1 Blinded). */
export const DEFAULT_OCR_RERUN_FALLBACK_FOLDERS = [
  'Fill 1 Blinded',
  'Fill 2 Blinded',
  'Fill 3 NC_1250-NC_1470 Blinded',
  'Blinded Missing',
];

/**
 * @param {string | undefined} raw
 * @param {string[]} defaultList
 */
export function foldersListFromEnv(raw, defaultList) {
  const text = raw?.trim();
  if (!text) return defaultList;
  return text
    .split(/[\n,;]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

/**
 * @param {Set<string>} ncFilter
 * @param {Map<string, string[]>} pathsByNc
 * @returns {string[]}
 */
export function missingNcsFromMap(ncFilter, pathsByNc) {
  return [...ncFilter].filter((nc) => !pathsByNc.has(nc)).sort((a, b) => Number(a) - Number(b));
}

/**
 * @param {Set<string>} ncFilter
 * @param {Map<string, string[]>} pathsByNc
 */
export function logNcCoverage(label, ncFilter, pathsByNc) {
  const missing = missingNcsFromMap(ncFilter, pathsByNc);
  const found = [...pathsByNc.keys()].sort((a, b) => Number(a) - Number(b));
  console.log(`\n--- ${label} ---`);
  console.log(`  Found ${found.length}/${ncFilter.size} NC(s): ${found.join(', ') || '(none)'}`);
  if (missing.length > 0) {
    console.log(`  Missing ${missing.length} NC(s): ${missing.join(', ')}`);
    for (const nc of missing) {
      console.log(`    NC_${nc}: no file in scanned folders`);
    }
  }
  for (const [nc, paths] of [...pathsByNc.entries()].sort((a, b) => Number(a[0]) - Number(b[0]))) {
    if (paths.length > 1) {
      console.log(`  NC_${nc}: ${paths.length} paths (will extract first pending):`);
      paths.forEach((p) => console.log(`    - ${p}`));
    }
  }
  return missing;
}
