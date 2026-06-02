/**
 * Compare Seguimiento Let NCs vs FINAL2 output → missing list.
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';

const SEG_DIR = 'C:/Users/kevin/Desktop/Blinding/_seg_tmp';
const SEG_XLSX = 'C:/Users/kevin/Desktop/Blinding/Seguimiento Leasing Carabanchel (new .LIFE).xlsx';
const FINAL2 = 'C:/Users/kevin/Desktop/Blinding/FINAL2';
const FINAL = 'C:/Users/kevin/Desktop/Blinding/FINAL';
const NEWCUSTOMERS_ROOT = 'C:/Users/kevin/Desktop/Blinding/NewCustomers';
const RUN_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\//, '')), '..');
const REPORT = path.join(RUN_ROOT, 'reports', 'report-final2-2026-06-01T17-39-27-927Z.txt');
const DATA_DIR = path.join(RUN_ROOT, 'data');
const NC_RE = /^NC[_\s]?0*(\d{1,4})(?=\D|$)/i;
const NC_MIN = 1;
const NC_MAX = 1483;

function readSharedStrings(dir) {
  const ssXml = fs.readFileSync(`${dir}/xl/sharedStrings.xml`, 'utf8');
  const strings = [];
  const siRe = /<si>(.*?)<\/si>/gs;
  let m;
  while ((m = siRe.exec(ssXml))) {
    const texts = [...m[1].matchAll(/<t[^>]*>(.*?)<\/t>/gs)].map((x) => x[1]);
    strings.push(texts.join(''));
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
      cells[cm[1].replace(/\d+/g, '')] = /t="s"/.test(cm[2]) ? strings[Number(cm[3])] : cm[3];
    }
    const mm = cells.C && String(cells.C).match(NC_RE);
    if (!mm) continue;
    const num = Number(mm[1]);
    if (num < NC_MIN || num > NC_MAX) continue;
    map.set(num, { category: (cells.E || '').trim(), label: (cells.G || '').trim() });
  }
  return map;
}

function codesFromFolder(dir) {
  const codes = new Set();
  if (!fs.existsSync(dir)) return codes;
  for (const f of fs.readdirSync(dir)) {
    const mm = f.match(/^NC_(\d{4})(?:_\d+)?\.pdf$/i);
    if (mm) codes.add(Number(mm[1]));
  }
  return codes;
}

function classifyFolder(name) {
  if (/informaci|information/i.test(name)) return 'info';
  if (/payment|pagos|deposito|renta|titularidad/i.test(name)) return 'payment';
  if (/contrac|contrat|cancel|reserva|terminaci|adenda|burofax/i.test(name)) return 'contract';
  return 'other';
}

const SKIP_PATH_RE = /blinded contracts|informaci|information|payment|pagos|titularidad/i;
const CONTRACT_FILE_RE = /contrac|contrat|reserva|cancel|terminaci|adenda|burofax|hospedaje|arrendamiento|acuerdo|alojamiento|lease|docusign/i;

function codeFromPath(fullPath) {
  for (const seg of fullPath.split(path.sep)) {
    const mm = seg.match(NC_RE);
    if (mm) return Number(mm[1]);
  }
  const mm = path.basename(fullPath).match(NC_RE);
  return mm ? Number(mm[1]) : null;
}

function isBlindablePdf(fullPath) {
  const rel = path.relative(NEWCUSTOMERS_ROOT, fullPath);
  if (SKIP_PATH_RE.test(rel)) return false;
  const folders = rel.split(path.sep).slice(0, -1);
  const cls = folders.map(classifyFolder);
  if (cls.includes('info') || cls.includes('payment')) return false;
  if (cls.includes('contract')) return true;
  if (CONTRACT_FILE_RE.test(path.basename(fullPath))) return true;
  return codeFromPath(fullPath) != null;
}

/** @returns {Map<number, string[]>} */
function scanPdfs() {
  const byCode = new Map();
  (function walk(dir) {
    let entries;
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const e of entries) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        if (/blinded|^final2?$|^final$/i.test(e.name)) continue;
        walk(full);
      } else if (e.isFile() && e.name.toLowerCase().endsWith('.pdf')) {
        if (!isBlindablePdf(full)) continue;
        const code = codeFromPath(full);
        if (code == null || code < NC_MIN || code > NC_MAX) continue;
        if (!byCode.has(code)) byCode.set(code, []);
        byCode.get(code).push(full);
      }
    }
  })(NEWCUSTOMERS_ROOT);
  return byCode;
}

/** @returns {Map<number, string>} */
function loadSkippedFromReport() {
  const map = new Map();
  if (!fs.existsSync(REPORT)) return map;
  const csvPath = REPORT.replace('.txt', '.csv');
  if (fs.existsSync(csvPath)) {
    const lines = fs.readFileSync(csvPath, 'utf8').split('\n').slice(1);
    for (const line of lines) {
      if (!line.trim()) continue;
      const m = line.match(/^(NC_\d{4}),([^,]+),([^,]*),([^,]+),/);
      if (!m) continue;
      const code = Number(m[1].replace(/\D/g, ''));
      const status = m[4];
      if (status === 'skipped-no-regions') map.set(code, line);
    }
    return map;
  }
  return map;
}

const leasing = loadLeasing();
const letCodes = [...leasing.entries()]
  .filter(([, r]) => /^let$/i.test(r.label))
  .map(([c]) => c)
  .sort((a, b) => a - b);

const final2Codes = codesFromFolder(FINAL2);
const finalCodes = codesFromFolder(FINAL);
const pdfsByCode = scanPdfs();
const skipped = loadSkippedFromReport();

const fmt = (c) => `NC_${String(c).padStart(4, '0')}`;

/** @type {{ code: number, category: string, reason: string, detail?: string }[]} */
const missing = [];

for (const code of letCodes) {
  if (final2Codes.has(code)) continue;
  const cat = leasing.get(code)?.category || '';
  const pdfs = pdfsByCode.get(code);
  let reason;
  let detail;
  if (!pdfs?.length) {
    reason = 'NO_PDF_IN_ARCHIVE';
    detail = 'No blindable PDF under NewCustomers (often docx/html only)';
  } else if (skipped.has(code)) {
    reason = 'SKIPPED_NO_SIGNATURE';
    detail = pdfs.map((p) => path.basename(p)).join('; ');
  } else {
    reason = 'NOT_IN_FINAL2';
    detail = pdfs.map((p) => path.basename(p)).join('; ');
  }
  missing.push({ code, category: cat, reason, detail });
}

const byReason = {};
for (const m of missing) {
  byReason[m.reason] = byReason[m.reason] || [];
  byReason[m.reason].push(m);
}

const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
const outPath = path.join(DATA_DIR, `missing-from-FINAL2-${stamp}.txt`);
const csvPath = path.join(DATA_DIR, `missing-from-FINAL2-${stamp}.csv`);

let report = `Missing from FINAL2 (vs Seguimiento Let, NC 1–1483)\n`;
report += `Generated: ${new Date().toISOString()}\n\n`;
report += `Seguimiento Let (unique NC):     ${letCodes.length}\n`;
report += `Present in FINAL2:                 ${final2Codes.size}\n`;
report += `Present in FINAL (old run):        ${finalCodes.size}\n`;
report += `Missing from FINAL2:             ${missing.length}\n\n`;

for (const [reason, list] of Object.entries(byReason).sort()) {
  report += `--- ${reason} (${list.length}) ---\n`;
  for (const item of list.sort((a, b) => a.code - b.code)) {
    report += `${fmt(item.code)}  [${item.category}]  ${item.detail || ''}\n`;
  }
  report += '\n';
}

const csv = ['nc_code,category,reason,detail']
  .concat(missing.sort((a, b) => a.code - b.code).map((m) =>
    [fmt(m.code), m.category, m.reason, `"${(m.detail || '').replace(/"/g, '""')}"`].join(','),
  ))
  .join('\n');

await fsp.writeFile(outPath, report);
await fsp.writeFile(csvPath, csv);
console.log(report);
console.log(`\nSaved: ${outPath}`);
console.log(`Saved: ${csvPath}`);
