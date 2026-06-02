/**
 * Build FINAL2 — full Seguimiento-driven blind run (no Re-blind sheet).
 *
 * Filters (NC included only if ALL hold):
 *   1. NC code between 1 and 1483 (inclusive).
 *   2. Present in Seguimiento Leasing sheet3 with Column G ("Label") == "Let".
 *   3. All categories included (B2C, B2B2C, B2B).
 *
 * Blinds all contract-type PDFs into flat FINAL2 as NC_XXXX.pdf (NC_XXXX_2.pdf if multiple).
 *
 * Usage:  node reblind-final2.mjs [--dry]
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { blindPdfBuffer } from './blind-core.mjs';

const SEG_XLSX = 'C:/Users/kevin/Desktop/Blinding/Seguimiento Leasing Carabanchel (new .LIFE).xlsx';
const SEG_DIR = 'C:/Users/kevin/Desktop/Blinding/_seg_tmp';
const NEWCUSTOMERS_ROOT = 'C:/Users/kevin/Desktop/Blinding/NewCustomers';
const OUTPUT_ROOT = 'C:/Users/kevin/Desktop/Blinding/FINAL2';
const RUN_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\//, '')), '..');
const REPORT_DIR = path.join(RUN_ROOT, 'reports');
const DRY = process.argv.includes('--dry');

const NC_MIN = 1;
const NC_MAX = 1483;

const NC_FOLDER_RE = /^NC[_\s]?0*(\d{1,4})(?=\D|$)/i;

/** Unzip the live Seguimiento workbook so counts match what you see in Excel. */
async function refreshSeguimientoExtract() {
  if (!fs.existsSync(SEG_XLSX)) {
    throw new Error(`Seguimiento workbook not found: ${SEG_XLSX}`);
  }
  const zipPath = `${SEG_DIR}_refresh.zip`;
  await fsp.copyFile(SEG_XLSX, zipPath);
  await fsp.rm(SEG_DIR, { recursive: true, force: true });
  const { execSync } = await import('node:child_process');
  execSync(`powershell -NoProfile -Command "Expand-Archive -LiteralPath '${zipPath.replace(/'/g, "''")}' -DestinationPath '${SEG_DIR.replace(/'/g, "''")}' -Force"`, { stdio: 'pipe' });
  await fsp.unlink(zipPath);
  console.log(`Seguimiento refreshed from ${path.basename(SEG_XLSX)}`);
}

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

/** @returns {Map<number, { category: string, label: string }>} */
function loadLeasing() {
  const strings = readSharedStrings(SEG_DIR);
  const shXml = fs.readFileSync(`${SEG_DIR}/xl/worksheets/sheet3.xml`, 'utf8');
  const map = new Map();
  const rowRe = /<row r="(\d+)"[^>]*>(.*?)<\/row>/gs;
  let m;
  while ((m = rowRe.exec(shXml))) {
    if (Number(m[1]) < 5) continue;
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
    const num = Number(cc[1]);
    if (num < NC_MIN || num > NC_MAX) continue;
    map.set(num, {
      category: (cells['E'] || '').trim(),
      label: (cells['G'] || '').trim(),
    });
  }
  return map;
}

function classifyFolder(name) {
  if (/informaci|information/i.test(name)) return 'info';
  if (/payment|pagos|deposito|renta|titularidad/i.test(name)) return 'payment';
  if (/\bpet\b|queja|apoyo emocional/i.test(name)) return 'other';
  if (/contrac|contrat|cancel|reserva|terminaci|adenda|burofax/i.test(name)) return 'contract';
  return 'other';
}

const CONTRACT_FILE_RE = /contrac|contrat|reserva|cancel|terminaci|adenda|burofax|hospedaje|arrendamiento|acuerdo|alojamiento|lease|docusign/i;
const SKIP_PATH_RE = /blinded contracts|informaci|information|payment|pagos|titularidad/i;

/**
 * Any PDF under an NC customer folder except info/payment/blinded-archive paths.
 * Prefer contract-type paths; if none exist for an NC, any other PDF in the tree counts.
 */
function isBlindablePdf(fullPath) {
  const rel = path.relative(NEWCUSTOMERS_ROOT, fullPath);
  if (SKIP_PATH_RE.test(rel)) return false;
  const folders = rel.split(path.sep).slice(0, -1);
  const cls = folders.map(classifyFolder);
  if (cls.includes('info') || cls.includes('payment')) return false;
  if (cls.includes('contract')) return true;
  if (CONTRACT_FILE_RE.test(path.basename(fullPath))) return true;
  // Loose PDF anywhere else in the NC folder (user: "some pdf in their folder")
  return codeFromPath(fullPath) != null;
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
  /** @type {Map<number, string[]>} */
  const contractPdfsByCode = new Map();
  const outputResolved = path.resolve(OUTPUT_ROOT).toLowerCase();
  const finalResolved = path.resolve('C:/Users/kevin/Desktop/Blinding/FINAL').toLowerCase();

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
        if (/blinded|^final2?$|^final$/i.test(e.name)) continue;
        const resolved = path.resolve(full).toLowerCase();
        if (resolved === outputResolved || resolved === finalResolved) continue;
        walk(full);
      } else if (e.isFile() && e.name.toLowerCase().endsWith('.pdf')) {
        if (!isBlindablePdf(full)) continue;
        const code = codeFromPath(full);
        if (code == null || code < NC_MIN || code > NC_MAX) continue;
        if (!contractPdfsByCode.has(code)) contractPdfsByCode.set(code, []);
        contractPdfsByCode.get(code).push(full);
      }
    }
  })(NEWCUSTOMERS_ROOT);

  return contractPdfsByCode;
}

function passesLeasingFilter(rec) {
  return Boolean(rec && /^let$/i.test(rec.label));
}

function countLetRowsInSheet() {
  const strings = readSharedStrings(SEG_DIR);
  const shXml = fs.readFileSync(`${SEG_DIR}/xl/worksheets/sheet3.xml`, 'utf8');
  let letRows = 0;
  const rowRe = /<row r="(\d+)"[^>]*>(.*?)<\/row>/gs;
  let m;
  while ((m = rowRe.exec(shXml))) {
    if (Number(m[1]) < 5) continue;
    const cells = {};
    const cRe = /<c r="([A-Z]+)\d+"([^>]*)>(?:<v>(.*?)<\/v>)?<\/c>/gs;
    let cm;
    while ((cm = cRe.exec(m[2]))) {
      if (cm[3] === undefined) continue;
      const col = cm[1].replace(/\d+/g, '');
      cells[col] = /t="s"/.test(cm[2]) ? strings[Number(cm[3])] : cm[3];
    }
    if (/^let$/i.test(String(cells.G || '').trim())) letRows++;
  }
  return letRows;
}

async function main() {
  await refreshSeguimientoExtract();
  const leasing = loadLeasing();
  const allInSheet = [...leasing.keys()].sort((a, b) => a - b);
  const letRowsInSheet = countLetRowsInSheet();

  const eligible = [];
  const letNcCodes = new Set();
  const droppedExpiredCancelled = [];
  const byCategory = { B2C: 0, B2B2C: 0, B2B: 0, other: 0 };
  for (const code of allInSheet) {
    const rec = leasing.get(code);
    if (!/^let$/i.test(rec.label)) {
      droppedExpiredCancelled.push(`${code}:${rec.label || 'blank'}`);
      continue;
    }
    letNcCodes.add(code);
    const cat = (rec.category || '').toUpperCase();
    if (cat === 'B2C') byCategory.B2C++;
    else if (cat === 'B2B2C') byCategory.B2B2C++;
    else if (cat === 'B2B') byCategory.B2B++;
    else byCategory.other++;
    eligible.push(code);
  }

  const contractPdfsByCode = scanTree();
  const matched = eligible.filter((c) => contractPdfsByCode.has(c));
  const eligibleNoPdf = eligible.filter((c) => !contractPdfsByCode.has(c));

  let totalPdfs = 0;
  for (const c of matched) totalPdfs += contractPdfsByCode.get(c).length;

  console.log(`NC range                         : ${NC_MIN}–${NC_MAX}`);
  console.log(`Seguimiento "Let" rows (col G)   : ${letRowsInSheet}`);
  console.log(`Unique NC with Label=Let         : ${letNcCodes.size}`);
  console.log(`Dropped (not Let)                : ${droppedExpiredCancelled.length}`);
  console.log(`Eligible Let (all categories)    : ${eligible.length}`);
  console.log(`  B2C: ${byCategory.B2C}  B2B2C: ${byCategory.B2B2C}  B2B: ${byCategory.B2B}  other: ${byCategory.other}`);
  console.log(`Eligible WITH contract PDF  : ${matched.length}`);
  console.log(`Contract PDFs to blind      : ${totalPdfs}`);
  console.log(`Eligible but NO PDF found   : ${eligibleNoPdf.length}`);

  if (DRY) {
    console.log('\n[DRY RUN] No files written.');
    return;
  }

  await fsp.rm(OUTPUT_ROOT, { recursive: true, force: true });
  await fsp.mkdir(OUTPUT_ROOT, { recursive: true });

  /** @type {Map<number, number>} */
  const fileIndexByCode = new Map();
  /** @type {Map<string, number>} */
  const seenHash = new Map();

  function flatOutputPath(code, inputPath) {
    const size = fs.statSync(inputPath).size;
    const hash = `${code}:${size}`;
    if (seenHash.has(hash)) return null;
    seenHash.set(hash, 1);

    const base = `NC_${String(code).padStart(4, '0')}`;
    const idx = fileIndexByCode.get(code) || 0;
    fileIndexByCode.set(code, idx + 1);
    const name = idx === 0 ? `${base}.pdf` : `${base}_${idx + 1}.pdf`;
    return path.join(OUTPUT_ROOT, name);
  }

  const results = [];
  let okFiles = 0, skipFiles = 0, errFiles = 0, dupSkipped = 0;

  for (const code of matched) {
    const rec = leasing.get(code);
    const paths = [...contractPdfsByCode.get(code)].sort();
    for (const inputPath of paths) {
      const relLabel = path.relative(NEWCUSTOMERS_ROOT, inputPath);
      const outPath = flatOutputPath(code, inputPath);
      if (outPath === null) {
        dupSkipped++;
        continue;
      }
      try {
        const buf = await fsp.readFile(inputPath);
        const { buffer, redactionRegions, viaOcr } = await blindPdfBuffer(buf, path.basename(inputPath));
        await fsp.writeFile(outPath, buffer);
        okFiles++;
        results.push({
          code, cat: rec.category, outName: path.basename(outPath), relLabel,
          status: 'ok', regions: redactionRegions, ocr: viaOcr ? 1 : 0, error: '',
        });
        console.log(`OK   ${path.basename(outPath)} ← ${relLabel} (${redactionRegions} regions${viaOcr ? ', OCR' : ''})`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (/No redaction regions|Could not locate/i.test(msg)) {
          skipFiles++;
          results.push({
            code, cat: rec.category, outName: '', relLabel, status: 'skipped-no-regions',
            regions: 0, ocr: /OCR|image-only/i.test(msg) ? 1 : 0, error: msg,
          });
          console.log(`SKIP ${relLabel} (no party/signature blocks)`);
        } else {
          errFiles++;
          results.push({
            code, cat: rec.category, outName: '', relLabel, status: 'error',
            regions: 0, ocr: 0, error: msg,
          });
          console.log(`ERR  ${relLabel}: ${msg}`);
        }
      }
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const csvPath = path.join(REPORT_DIR, `report-final2-${stamp}.csv`);
  const txtPath = path.join(REPORT_DIR, `report-final2-${stamp}.txt`);
  const fmt = (c) => `NC_${String(c).padStart(4, '0')}`;

  const csv = ['code,category,output,status,regions,ocr,file,error']
    .concat(results.map((r) => [
      fmt(r.code), r.cat, r.outName, r.status, r.regions, r.ocr,
      `"${r.relLabel.replace(/"/g, '""')}"`, `"${(r.error || '').replace(/"/g, '""')}"`,
    ].join(',')))
    .join('\n');
  await fsp.writeFile(csvPath, csv);

  const txt = [
    `FINAL2 blind report (${stamp})`,
    `Output: ${OUTPUT_ROOT}`,
    '',
    'Filters: Seguimiento only | NC 1–1483 | G="Let" | B2C+B2B2C+B2B',
    '',
    `Seguimiento Let rows (col G)  : ${letRowsInSheet}`,
    `Unique NC with Label=Let      : ${letNcCodes.size}`,
    `Dropped (not Let)             : ${droppedExpiredCancelled.length}`,
    `Eligible Let (all categories) : ${eligible.length}`,
    `  B2C: ${byCategory.B2C}  B2B2C: ${byCategory.B2B2C}  B2B: ${byCategory.B2B}`,
    `Eligible WITH contract PDF  : ${matched.length}`,
    `Contract PDFs to blind      : ${totalPdfs}`,
    `PDFs blinded OK             : ${okFiles}`,
    `PDFs skipped                : ${skipFiles}`,
    `PDFs errored                : ${errFiles}`,
    `Duplicate copies skipped    : ${dupSkipped}`,
    '',
    '==== ELIGIBLE BUT NO CONTRACT PDF ====',
    ...eligibleNoPdf.map((c) => fmt(c)),
  ].join('\n');
  await fsp.writeFile(txtPath, txt);

  console.log('\n========== SUMMARY ==========');
  console.log(`Eligible NCs: ${eligible.length} | with PDF: ${matched.length}`);
  console.log(`PDFs blinded: ${okFiles}, skipped: ${skipFiles}, errored: ${errFiles}, dup: ${dupSkipped}`);
  console.log(`Report: ${txtPath}`);
  console.log(`Output: ${OUTPUT_ROOT}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
