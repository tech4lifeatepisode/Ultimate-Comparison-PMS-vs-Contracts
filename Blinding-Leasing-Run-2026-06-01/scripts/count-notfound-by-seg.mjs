/**
 * Cross-reference the 78 "not found in Blinded Contracts" NCs with Seguimiento categories.
 */
import fs from 'fs';
import fsp from 'fs/promises';
import { execSync } from 'node:child_process';

const SEG_XLSX = 'C:/Users/kevin/Desktop/Blinding/Seguimiento Leasing Carabanchel (new .LIFE).xlsx';
const SEG_DIR = 'C:/Users/kevin/Desktop/Blinding/_seg_tmp';
const MISSING_CSV =
  'C:/Users/kevin/Desktop/Blinding/Ultimate-Comparison-PMS-vs-Contracts-main/Ultimate-Comparison-PMS-vs-Contracts-main/Blinding_Contracts/Blinding_prod/missing-from-FINAL2-2026-06-01T17-45-32.csv';
const BLINDED_ROOTS = [
  'C:/Users/kevin/Desktop/Blinding/NewCustomers/02. CUSTOMERS-20260601T113706Z-3-001/02. CUSTOMERS/Blinded Contracts NC_0001 to NC_1470',
  'C:/Users/kevin/Desktop/Blinding/NewCustomers/02. CUSTOMERS-20260601T113706Z-3-003/02. CUSTOMERS/Blinded Contracts NC_0001 to NC_1470',
];
const NC_RE = /NC[_\s]?0*(\d{1,4})(?=\D|$)/i;

async function refreshSeguimientoExtract() {
  const zipPath = `${SEG_DIR}_refresh.zip`;
  await fsp.copyFile(SEG_XLSX, zipPath);
  await fsp.rm(SEG_DIR, { recursive: true, force: true });
  execSync(
    `powershell -NoProfile -Command "Expand-Archive -LiteralPath '${zipPath.replace(/'/g, "''")}' -DestinationPath '${SEG_DIR.replace(/'/g, "''")}' -Force"`,
    { stdio: 'pipe' },
  );
  await fsp.unlink(zipPath);
}

function readSharedStrings(dir) {
  const ssXml = fs.readFileSync(`${dir}/xl/sharedStrings.xml`, 'utf8');
  const strings = [];
  const siRe = /<si>(.*?)<\/si>/gs;
  let m;
  while ((m = siRe.exec(ssXml))) {
    const texts = [...m[1].matchAll(/<t[^>]*>(.*?)<\/t>/gs)].map((x) => x[1]);
    strings.push(texts.join('').replace(/&amp;/g, '&'));
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
    map.set(Number(mm[1]), { category: (cells.E || '').trim(), label: (cells.G || '').trim() });
  }
  return map;
}

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

function scanBlindedArchives(targetCodes) {
  const want = new Set(targetCodes);
  const byCode = new Map();
  for (const root of BLINDED_ROOTS) {
    if (!fs.existsSync(root)) continue;
    for (const ent of fs.readdirSync(root, { withFileTypes: true })) {
      if (!ent.isFile() || !ent.name.toLowerCase().endsWith('.pdf')) continue;
      const mm = ent.name.match(NC_RE);
      if (!mm) continue;
      const code = Number(mm[1]);
      if (!want.has(code)) continue;
      byCode.set(code, true);
    }
  }
  return byCode;
}

const fmt = (c) => `NC_${String(c).padStart(4, '0')}`;

await refreshSeguimientoExtract();
const leasing = loadLeasing();
const missingCodes = loadMissingCodes();
const found = scanBlindedArchives(missingCodes);
const notFound = missingCodes.filter((c) => !found.has(c));

const byCat = { B2C: [], B2B2C: [], B2B: [], other: [], missingInSeg: [] };
for (const c of notFound) {
  const rec = leasing.get(c);
  if (!rec) {
    byCat.missingInSeg.push(fmt(c));
    continue;
  }
  if (rec.category === 'B2C') byCat.B2C.push({ code: fmt(c), label: rec.label });
  else if (rec.category === 'B2B2C') byCat.B2B2C.push({ code: fmt(c), label: rec.label });
  else if (rec.category === 'B2B') byCat.B2B.push({ code: fmt(c), label: rec.label });
  else byCat.other.push({ code: fmt(c), category: rec.category, label: rec.label });
}

console.log(`Not found in Blinded Contracts archives: ${notFound.length} NCs\n`);
console.log('By Seguimiento Category (col E):');
console.log(`  B2C:   ${byCat.B2C.length}`);
console.log(`  B2B2C: ${byCat.B2B2C.length}`);
console.log(`  B2B:   ${byCat.B2B.length}`);
console.log(`  other: ${byCat.other.length}`);
console.log(`  not in Seguimiento: ${byCat.missingInSeg.length}`);

const byLabel = { Let: 0, other: 0 };
for (const c of notFound) {
  const rec = leasing.get(c);
  if (rec?.label === 'Let') byLabel.Let++;
  else byLabel.other++;
}
console.log('\nBy Seguimiento Label (col G):');
console.log(`  Let:   ${byLabel.Let}`);
console.log(`  other: ${byLabel.other}`);

for (const [name, list] of [
  ['B2C', byCat.B2C],
  ['B2B2C', byCat.B2B2C],
  ['B2B', byCat.B2B],
]) {
  console.log(`\n--- ${name} (${list.length}) ---`);
  console.log(list.map((x) => x.code).join(', '));
}

if (byCat.other.length) {
  console.log(`\n--- other (${byCat.other.length}) ---`);
  for (const x of byCat.other) console.log(`${x.code}  cat=${x.category}  label=${x.label}`);
}
if (byCat.missingInSeg.length) {
  console.log(`\n--- not in Seguimiento (${byCat.missingInSeg.length}) ---`);
  console.log(byCat.missingInSeg.join(', '));
}
