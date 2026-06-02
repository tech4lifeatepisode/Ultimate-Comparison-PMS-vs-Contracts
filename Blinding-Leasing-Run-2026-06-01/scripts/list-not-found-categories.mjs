import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';

const RUN_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\//, '')), '..');
const SEG_DIR = 'C:/Users/kevin/Desktop/Blinding/_seg_tmp';
const NOT_FOUND = path.join(RUN_ROOT, 'data', 'NOT-IN-BLINDED-ARCHIVES-78.txt');
const NC_RE = /^NC[_\s]?0*(\d{1,4})(?=\D|$)/i;

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
    map.set(Number(mm[1]), {
      category: (cells.E || '').trim(),
      label: (cells.G || '').trim(),
    });
  }
  return map;
}

const codes = fs.readFileSync(NOT_FOUND, 'utf8')
  .split('\n')
  .map((l) => l.trim())
  .filter((l) => /^NC_\d{4}$/.test(l))
  .map((l) => Number(l.replace(/\D/g, '')))
  .sort((a, b) => a - b);

const leasing = loadLeasing();
const byCat = { B2C: [], B2B2C: [], B2B: [], other: [], missingInSheet: [] };

for (const code of codes) {
  const rec = leasing.get(code);
  const fmt = `NC_${String(code).padStart(4, '0')}`;
  if (!rec) {
    byCat.missingInSheet.push(fmt);
    continue;
  }
  const cat = (rec.category || '').toUpperCase();
  if (cat === 'B2C') byCat.B2C.push(fmt);
  else if (cat === 'B2B2C') byCat.B2B2C.push(fmt);
  else if (cat === 'B2B') byCat.B2B.push(fmt);
  else byCat.other.push(`${fmt} (${rec.category || 'blank'})`);
}

const out = [
  'NOT FOUND in Blinded Contracts archives (78 NCs)',
  'Cross-checked vs Seguimiento Leasing (col E = Category, col G = Label)',
  '',
  `Total: ${codes.length}`,
  `B2C:    ${byCat.B2C.length}`,
  `B2B2C:  ${byCat.B2B2C.length}`,
  `B2B:    ${byCat.B2B.length}`,
  `Other:  ${byCat.other.length}`,
  `Not in Seguimiento sheet: ${byCat.missingInSheet.length}`,
  '',
  '==== ALL 78 (sorted) ====',
  ...codes.map((c) => {
    const rec = leasing.get(c);
    const fmt = `NC_${String(c).padStart(4, '0')}`;
    return `${fmt}  [${rec?.category || '?'}]  Label=${rec?.label || '?'}`;
  }),
  '',
  '==== B2C only ====',
  ...byCat.B2C,
  '',
  '==== B2B2C only ====',
  ...byCat.B2B2C,
  '',
  '==== B2B only ====',
  ...byCat.B2B,
].join('\n');

const pathOut = 'C:/Users/kevin/Desktop/Blinding/Final UNDONE/NOT-FOUND-78-WITH-CATEGORY.txt';
await fsp.writeFile(pathOut, out);
console.log(out);
