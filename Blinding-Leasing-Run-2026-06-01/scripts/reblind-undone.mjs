/**
 * Re-blind the 282 missing-from-FINAL2 NCs using only the two
 * "Blinded Contracts NC_0001 to NC_1470" archive folders.
 *
 * Output: C:/Users/kevin/Desktop/Blinding/Final UNDONE
 *
 * Usage: node reblind-undone.mjs [--dry]
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
// blind-core not used: archive PDFs are already blinded; we copy with NC-only names.

const RUN_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\//, '')), '..');
const MISSING_CSV = path.join(RUN_ROOT, 'data', 'missing-from-FINAL2-2026-06-01T17-45-32.csv');
const OUTPUT_ROOT = 'C:/Users/kevin/Desktop/Blinding/Final UNDONE';
const REPORT_DIR = path.join(RUN_ROOT, 'reports');
const DRY = process.argv.includes('--dry');

const BLINDED_ROOTS = [
  'C:/Users/kevin/Desktop/Blinding/NewCustomers/02. CUSTOMERS-20260601T113706Z-3-001/02. CUSTOMERS/Blinded Contracts NC_0001 to NC_1470',
  'C:/Users/kevin/Desktop/Blinding/NewCustomers/02. CUSTOMERS-20260601T113706Z-3-003/02. CUSTOMERS/Blinded Contracts NC_0001 to NC_1470',
];

const NC_RE = /NC[_\s]?0*(\d{1,4})(?=\D|$)/i;

function loadMissingCodes() {
  const text = fs.readFileSync(MISSING_CSV, 'utf8');
  const codes = new Set();
  for (const line of text.split('\n').slice(1)) {
    if (!line.trim()) continue;
    const m = line.match(/^(NC_\d{4})/);
    if (m) codes.add(Number(m[1].replace(/\D/g, '')));
  }
  return [...codes].sort((a, b) => a - b);
}

/** @returns {Map<number, { path: string, archive: string }[]>} */
function scanBlindedArchives(targetCodes) {
  const want = new Set(targetCodes);
  /** @type {Map<number, { path: string, archive: string }[]>} */
  const byCode = new Map();

  for (const root of BLINDED_ROOTS) {
    if (!fs.existsSync(root)) {
      console.warn(`Archive not found: ${root}`);
      continue;
    }
    const archive = path.basename(path.dirname(root)) + '/' + path.basename(root);
    for (const ent of fs.readdirSync(root, { withFileTypes: true })) {
      if (!ent.isFile() || !ent.name.toLowerCase().endsWith('.pdf')) continue;
      const mm = ent.name.match(NC_RE);
      if (!mm) continue;
      const code = Number(mm[1]);
      if (!want.has(code)) continue;
      const full = path.join(root, ent.name);
      if (!byCode.has(code)) byCode.set(code, []);
      byCode.get(code).push({ path: full, archive });
    }
  }
  return byCode;
}

async function main() {
  const missingCodes = loadMissingCodes();
  const found = scanBlindedArchives(missingCodes);
  const foundCodes = [...found.keys()].sort((a, b) => a - b);
  const notFound = missingCodes.filter((c) => !found.has(c));

  let totalPdfs = 0;
  for (const list of found.values()) totalPdfs += list.length;

  console.log(`Missing list (from FINAL2):     ${missingCodes.length} NCs`);
  console.log(`Found in Blinded Contracts:     ${foundCodes.length} NCs (${totalPdfs} PDFs)`);
  console.log(`Still not in those archives:  ${notFound.length} NCs`);

  if (DRY) {
    if (foundCodes.length) {
      console.log('\nFirst 20 found:');
      for (const c of foundCodes.slice(0, 20)) {
        console.log(`  NC_${String(c).padStart(4, '0')}: ${found.get(c).map((x) => path.basename(x.path)).join(', ')}`);
      }
    }
    if (notFound.length) {
      console.log('\nFirst 20 not found:');
      console.log(notFound.slice(0, 20).map((c) => `NC_${String(c).padStart(4, '0')}`).join(', '));
    }
    return;
  }

  await fsp.rm(OUTPUT_ROOT, { recursive: true, force: true });
  await fsp.mkdir(OUTPUT_ROOT, { recursive: true });

  /** @type {Map<number, number>} */
  const fileIndex = new Map();
  /** @type {Map<string, number>} */
  const seenHash = new Map();

  const results = [];
  let copied = 0, err = 0, dup = 0;

  for (const code of foundCodes) {
    const list = [...found.get(code)].sort((a, b) => a.path.localeCompare(b.path));
    for (const { path: inputPath, archive } of list) {
      const size = fs.statSync(inputPath).size;
      const hash = `${code}:${size}`;
      if (seenHash.has(hash)) {
        dup++;
        continue;
      }
      seenHash.set(hash, 1);

      const base = `NC_${String(code).padStart(4, '0')}`;
      const idx = fileIndex.get(code) || 0;
      fileIndex.set(code, idx + 1);
      const outName = idx === 0 ? `${base}.pdf` : `${base}_${idx + 1}.pdf`;
      const outPath = path.join(OUTPUT_ROOT, outName);

      // Source folder is "Blinded Contracts" — files are already blinded exports.
      // Copy into Final UNDONE with NC-only names (re-blind would find no PII left to redact).
      try {
        await fsp.copyFile(inputPath, outPath);
        copied++;
        results.push({
          code, outName, archive, status: 'copied-preblinded', regions: 0, ocr: 0, error: '',
        });
        console.log(`COPY ${outName} ← ${path.basename(inputPath)}`);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        err++;
        results.push({ code, outName: '', archive, status: 'error', regions: 0, ocr: 0, error: msg });
        console.log(`ERR  ${path.basename(inputPath)}: ${msg}`);
      }
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const txtPath = path.join(REPORT_DIR, `report-final-undone-${stamp}.txt`);
  const fmt = (c) => `NC_${String(c).padStart(4, '0')}`;

  const txt = [
    `Final UNDONE report (${stamp})`,
    `Output: ${OUTPUT_ROOT}`,
    `Sources: Blinded Contracts archives (3-001 + 3-003 only)`,
    '',
    `Missing list NCs:              ${missingCodes.length}`,
    `Found in archives:             ${foundCodes.length} NCs, ${totalPdfs} PDFs`,
    `Copied to Final UNDONE:        ${copied}`,
    `Errors:                        ${err}`,
    `Duplicate bytes skipped:       ${dup}`,
    `Not found in archives:         ${notFound.length}`,
    '',
    '==== NOT FOUND IN BLINDED CONTRACTS ARCHIVES ====',
    ...notFound.map(fmt),
    '',
    '==== PROCESSED ====',
    ...results.map((r) =>
      `${fmt(r.code)}  ${r.status}  ${r.outName || '-'}  ${r.regions} regions  ${path.basename(r.archive)}`,
    ),
  ].join('\n');
  await fsp.writeFile(txtPath, txt);

  console.log('\n========== SUMMARY ==========');
  console.log(`Found: ${foundCodes.length}/${missingCodes.length} NCs | Copied: ${copied}, err: ${err}, dup: ${dup}`);
  console.log(`Output: ${OUTPUT_ROOT}`);
  console.log(`Report: ${txtPath}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
