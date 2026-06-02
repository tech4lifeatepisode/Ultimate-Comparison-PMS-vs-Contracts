/**
 * Build the FINAL blinded folder.
 *
 * Filters (a customer NC is included only if ALL hold):
 *   1. The NC appears in Re-blind.xlsx.
 *   2. Leasing sheet ("Leasing Update", sheet3) Column G ("Label") == "Let"
 *      (excludes Expired / Cancelled / In Progress / blank).
 *   3. Leasing sheet Column E ("Category") is NOT exactly "B2B"
 *      (keeps B2C and B2B2C; drops pure B2B).
 *
 * For every kept NC it blinds all contract-type PDFs (contract / reserva /
 * cancellation / terminacion) into a single flat output folder: FINAL.
 *
 * Usage:  node reblind-final.mjs [--dry]
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { blindPdfBuffer } from './blind-core.mjs';

const XLSX_DIR = 'C:/Users/kevin/Desktop/Blinding/_xlsx_tmp';        // Re-blind.xlsx (unzipped)
const SEG_DIR = 'C:/Users/kevin/Desktop/Blinding/_seg_tmp';          // Seguimiento Leasing (unzipped)
const NEWCUSTOMERS_ROOT = 'C:/Users/kevin/Desktop/Blinding/NewCustomers';
const OUTPUT_ROOT = 'C:/Users/kevin/Desktop/Blinding/FINAL';
const RUN_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\//, '')), '..');
const REPORT_DIR = path.join(RUN_ROOT, 'reports');
const DRY = process.argv.includes('--dry');

const NC_FOLDER_RE = /^NC[_\s]?0*(\d{1,4})(?=\D|$)/i;
const NC_ANY_RE = /NC[_\s]?0*(\d{1,4})(?=\D|$)/gi;

// ---------- shared xlsx helpers ----------
function readSharedStrings(dir) {
  const ssXml = fs.readFileSync(`${dir}/xl/sharedStrings.xml`, 'utf8');
  const strings = [];
  const siRe = /<si>(.*?)<\/si>/gs;
  let m;
  while ((m = siRe.exec(ssXml))) {
    const texts = [...m[1].matchAll(/<t[^>]*>(.*?)<\/t>/gs)].map((x) => x[1]);
    strings.push(texts.join('').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>'));
  }
  return strings;
}

// ---------- Re-blind.xlsx: all NC codes ----------
function loadReblindCodes() {
  const strings = readSharedStrings(XLSX_DIR);
  const shXml = fs.readFileSync(`${XLSX_DIR}/xl/worksheets/sheet1.xml`, 'utf8');
  const codeToLabel = new Map();
  const rowRe = /<row[^>]*>(.*?)<\/row>/gs;
  let m;
  while ((m = rowRe.exec(shXml))) {
    const cRe = /<c r="[A-Z]+\d+"([^>]*)>(?:<v>(.*?)<\/v>)?<\/c>/gs;
    let cm;
    while ((cm = cRe.exec(m[1]))) {
      if (cm[2] === undefined) continue;
      const text = /t="s"/.test(cm[1]) ? strings[Number(cm[2])] : cm[2];
      if (text == null) continue;
      for (const x of String(text).matchAll(NC_ANY_RE)) {
        const code = Number(x[1]);
        if (!codeToLabel.has(code)) codeToLabel.set(code, String(text).trim());
      }
    }
  }
  return codeToLabel;
}

// ---------- Leasing Update (sheet3): code -> { category(E), label(G) } ----------
function loadLeasing() {
  const strings = readSharedStrings(SEG_DIR);
  const shXml = fs.readFileSync(`${SEG_DIR}/xl/worksheets/sheet3.xml`, 'utf8');
  const map = new Map();
  const rowRe = /<row r="(\d+)"[^>]*>(.*?)<\/row>/gs;
  let m;
  while ((m = rowRe.exec(shXml))) {
    if (Number(m[1]) < 5) continue; // header is row 4
    const cells = {};
    const cRe = /<c r="([A-Z]+)\d+"([^>]*)>(?:<v>(.*?)<\/v>)?<\/c>/gs;
    let cm;
    while ((cm = cRe.exec(m[2]))) {
      if (cm[3] === undefined) continue;
      const col = cm[1].replace(/\d+/g, '');
      cells[col] = /t="s"/.test(cm[2]) ? strings[Number(cm[3])] : cm[3];
    }
    const code = cells['C'];
    if (!code) continue;
    const cc = String(code).match(NC_FOLDER_RE);
    if (!cc) continue;
    map.set(Number(cc[1]), {
      category: (cells['E'] || '').trim(),
      label: (cells['G'] || '').trim(),
    });
  }
  return map;
}

// ---------- folder classification (which PDFs are contract-type) ----------
function classifyFolder(name) {
  if (/informaci|information/i.test(name)) return 'info';
  if (/payment|pagos|deposito|renta|titularidad/i.test(name)) return 'payment';
  if (/\bpet\b|queja|apoyo emocional/i.test(name)) return 'other';
  if (/contrac|contrat|cancel|reserva|terminaci|adenda|burofax/i.test(name)) return 'contract';
  return 'other';
}
// Filenames that indicate a contract-type document (used for loose PDFs that
// sit in an archive root / generic folder with the NC code only in the name).
const CONTRACT_FILE_RE = /contrac|contrat|reserva|cancel|terminaci|adenda|burofax|hospedaje|arrendamiento|acuerdo|alojamiento|lease|docusign/i;

/**
 * A PDF is contract-type if it lives inside an explicit contract folder, OR it
 * is a loose file whose name matches contract keywords and it is NOT inside an
 * information/payment folder (which would make it a non-contract document).
 */
function isContractTypePath(fullPath) {
  const folders = path.relative(NEWCUSTOMERS_ROOT, fullPath).split(path.sep).slice(0, -1);
  const cls = folders.map(classifyFolder);
  if (cls.includes('contract')) return true;
  if (cls.includes('info') || cls.includes('payment')) return false;
  return CONTRACT_FILE_RE.test(path.basename(fullPath));
}
function codeFromDir(dirPath) {
  let code = null;
  for (const seg of dirPath.split(path.sep)) {
    const mm = seg.match(NC_FOLDER_RE);
    if (mm) code = Number(mm[1]);
  }
  return code;
}
function codeFromPath(fullPath) {
  const dirCode = codeFromDir(path.dirname(fullPath));
  if (dirCode != null) return dirCode;
  const mm = path.basename(fullPath).match(NC_FOLDER_RE);
  return mm ? Number(mm[1]) : null;
}

function scanTree() {
  const contractPdfsByCode = new Map();
  const folderCodes = new Set();
  const outputResolved = path.resolve(OUTPUT_ROOT).toLowerCase();
  (function walk(dir) {
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        if (/blinded|^final$/i.test(e.name)) continue;
        if (path.resolve(full).toLowerCase() === outputResolved) continue;
        const mm = e.name.match(NC_FOLDER_RE);
        if (mm) folderCodes.add(Number(mm[1]));
        walk(full);
      } else if (e.isFile() && e.name.toLowerCase().endsWith('.pdf')) {
        if (!isContractTypePath(full)) continue;
        const code = codeFromPath(full);
        if (code == null) continue;
        if (!contractPdfsByCode.has(code)) contractPdfsByCode.set(code, []);
        contractPdfsByCode.get(code).push(full);
      }
    }
  })(NEWCUSTOMERS_ROOT);
  return { contractPdfsByCode, folderCodes };
}

function passesLeasingFilter(rec) {
  if (!rec) return false;
  if (!/^let$/i.test(rec.label)) return false; // only Let
  if (/^b2b$/i.test(rec.category)) return false; // drop pure B2B (keep B2C / B2B2C)
  return true;
}

async function main() {
  const codeToLabel = loadReblindCodes();
  const leasing = loadLeasing();
  const allCodes = [...codeToLabel.keys()].sort((a, b) => a - b);

  // Apply the two leasing filters.
  const eligible = [];
  const droppedExpiredCancelled = [];
  const droppedB2B = [];
  const droppedNotInLeasing = [];
  for (const code of allCodes) {
    const rec = leasing.get(code);
    if (!rec) { droppedNotInLeasing.push(code); continue; }
    if (!/^let$/i.test(rec.label)) { droppedExpiredCancelled.push(`${code}:${rec.label || 'blank'}`); continue; }
    if (/^b2b$/i.test(rec.category)) { droppedB2B.push(code); continue; }
    eligible.push(code);
  }

  const { contractPdfsByCode, folderCodes } = scanTree();
  const matched = eligible.filter((c) => contractPdfsByCode.has(c));
  const eligibleNoPdf = eligible.filter((c) => !contractPdfsByCode.has(c));

  console.log(`Re-blind sheet codes        : ${allCodes.length}`);
  console.log(`Dropped (Expired/Cancelled) : ${droppedExpiredCancelled.length}`);
  console.log(`Dropped (B2B)               : ${droppedB2B.length}`);
  console.log(`Dropped (not in leasing)    : ${droppedNotInLeasing.length}`);
  console.log(`Eligible (Let & not B2B)    : ${eligible.length}`);
  console.log(`Eligible WITH contract PDF  : ${matched.length}`);
  console.log(`Eligible but NO PDF found   : ${eligibleNoPdf.length}`);

  if (DRY) {
    console.log('\nEligible NCs:', eligible.map((c) => 'NC_' + String(c).padStart(4, '0')).join(', '));
    console.log('\nEligible but no PDF:', eligibleNoPdf.map((c) => 'NC_' + String(c).padStart(4, '0')).join(', '));
    return;
  }

  await fsp.rm(OUTPUT_ROOT, { recursive: true, force: true });
  await fsp.mkdir(OUTPUT_ROOT, { recursive: true });
  const usedNames = new Map();
  function flatOutputPath(inputPath) {
    const baseName = path.basename(inputPath);
    const size = fs.statSync(inputPath).size;
    if (usedNames.has(baseName)) {
      if (usedNames.get(baseName) === size) return null;
      const ext = path.extname(baseName);
      const stem = baseName.slice(0, -ext.length || undefined);
      let i = 2, candidate;
      do { candidate = `${stem} (${i})${ext}`; i++; } while (usedNames.has(candidate));
      usedNames.set(candidate, size);
      return path.join(OUTPUT_ROOT, candidate);
    }
    usedNames.set(baseName, size);
    return path.join(OUTPUT_ROOT, baseName);
  }

  const results = [];
  let okFiles = 0, skipFiles = 0, errFiles = 0, dupSkipped = 0;
  for (const code of matched) {
    const rec = leasing.get(code);
    for (const inputPath of contractPdfsByCode.get(code)) {
      const relLabel = path.relative(NEWCUSTOMERS_ROOT, inputPath);
      const outPath = flatOutputPath(inputPath);
      if (outPath === null) { dupSkipped++; continue; }
      try {
        const buf = await fsp.readFile(inputPath);
        const { buffer, redactionRegions, viaOcr } = await blindPdfBuffer(buf, path.basename(inputPath));
        await fsp.writeFile(outPath, buffer);
        okFiles++;
        results.push({ code, cat: rec.category, relLabel, status: 'ok', regions: redactionRegions, ocr: viaOcr ? 1 : 0, error: '' });
        console.log(`OK   ${relLabel} (${redactionRegions} regions${viaOcr ? ', OCR' : ''})`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (/No redaction regions|Could not locate/i.test(msg)) {
          skipFiles++;
          results.push({ code, cat: rec.category, relLabel, status: 'skipped-no-regions', regions: 0, ocr: /OCR|image-only/i.test(msg) ? 1 : 0, error: msg });
          console.log(`SKIP ${relLabel} (no party/signature blocks)`);
        } else {
          errFiles++;
          results.push({ code, cat: rec.category, relLabel, status: 'error', regions: 0, ocr: 0, error: msg });
          console.log(`ERR  ${relLabel}: ${msg}`);
        }
      }
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const csvPath = path.join(REPORT_DIR, `report-final-${stamp}.csv`);
  const txtPath = path.join(REPORT_DIR, `report-final-${stamp}.txt`);
  const csv = ['code,category,status,regions,ocr,file,error']
    .concat(results.map((r) => [
      `NC_${String(r.code).padStart(4, '0')}`, r.cat, r.status, r.regions, r.ocr,
      `"${r.relLabel.replace(/"/g, '""')}"`, `"${(r.error || '').replace(/"/g, '""')}"`,
    ].join(',')))
    .join('\n');
  await fsp.writeFile(csvPath, csv);

  const fmt = (c) => `NC_${String(c).padStart(4, '0')}`;
  const txt = [
    `FINAL blind report  (${stamp})`,
    `Output folder: ${OUTPUT_ROOT}`,
    '',
    'Filters: in Re-blind.xlsx  AND  Leasing G="Let"  AND  Leasing E != "B2B"',
    '',
    `Re-blind sheet codes        : ${allCodes.length}`,
    `Dropped Expired/Cancelled   : ${droppedExpiredCancelled.length}`,
    `Dropped B2B                 : ${droppedB2B.length}`,
    `Dropped not-in-leasing      : ${droppedNotInLeasing.length}`,
    `Eligible (Let & not B2B)    : ${eligible.length}`,
    `Eligible WITH contract PDF  : ${matched.length}`,
    `Eligible but NO PDF found   : ${eligibleNoPdf.length}`,
    '',
    `PDFs blinded OK             : ${okFiles}`,
    `PDFs skipped (no PII)       : ${skipFiles}`,
    `PDFs errored                : ${errFiles}`,
    `Duplicate copies skipped    : ${dupSkipped}`,
    '',
    '==== ELIGIBLE BUT NO CONTRACT PDF FOUND (only docx/html?) ====',
    ...eligibleNoPdf.map((c) => `${fmt(c)}  (${codeToLabel.get(c)})`),
    '',
    '==== DROPPED: B2B ====',
    ...droppedB2B.map((c) => `${fmt(c)}`),
    '',
    '==== DROPPED: NOT IN LEASING SHEET ====',
    ...droppedNotInLeasing.map((c) => `${fmt(c)}  (${codeToLabel.get(c)})`),
  ].join('\n');
  await fsp.writeFile(txtPath, txt);

  console.log('\n========== SUMMARY ==========');
  console.log(`Eligible NCs: ${eligible.length} | with PDF: ${matched.length} | PDFs blinded: ${okFiles}, skipped: ${skipFiles}, errored: ${errFiles}, dup: ${dupSkipped}`);
  console.log(`Report: ${txtPath}`);
  console.log(`Output: ${OUTPUT_ROOT}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
