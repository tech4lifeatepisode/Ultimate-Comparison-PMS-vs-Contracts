/**
 * Last run: blind all PDFs in "Missed 78" → "LAST RUN" (NC_XXXX.pdf names).
 * Includes Airbnb host reservation redaction logic.
 *
 * Usage: node reblind-last-run.mjs [--dry]
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { blindPdfBuffer } from './blind-core.mjs';

const INPUT_ROOT = 'C:/Users/kevin/Desktop/Blinding/Missed 78';
const OUTPUT_ROOT = 'C:/Users/kevin/Desktop/Blinding/LAST RUN';
const RUN_ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\//, '')), '..');
const REPORT_DIR = path.join(RUN_ROOT, 'reports');
const DRY = process.argv.includes('--dry');

const NC_RE = /NC[_\s]?0*(\d{1,4})(?=\D|$)/gi;

/** Airbnb filenames → NC when no NC_ in name */
const AIRBNB_NC = [
  [/MARCELO_STARLING/i, 1446],
  [/MARILENA_MASTRODINA/i, 1409],
  [/FAUSTO_GROSSI/i, 1466],
  [/DAVID_MILLER/i, 1472],
];

function codeFromFileName(name) {
  const hits = [...name.matchAll(NC_RE)].map((m) => Number(m[1]));
  if (hits.length) return hits[0];
  for (const [re, code] of AIRBNB_NC) {
    if (re.test(name)) return code;
  }
  return null;
}

function listPdfs(dir) {
  /** @type {string[]} */
  const out = [];
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, ent.name);
    if (ent.isDirectory()) out.push(...listPdfs(full));
    else if (ent.isFile() && ent.name.toLowerCase().endsWith('.pdf')) out.push(full);
  }
  return out;
}

async function main() {
  const allPdfs = listPdfs(INPUT_ROOT);
  /** @type {Map<number, { path: string; name: string }[]>} */
  const byCode = new Map();

  for (const full of allPdfs) {
    const name = path.basename(full);
    const code = codeFromFileName(name);
    if (code == null) {
      console.warn(`SKIP (no NC code): ${name}`);
      continue;
    }
    if (!byCode.has(code)) byCode.set(code, []);
    byCode.get(code).push({ path: full, name });
  }

  const codes = [...byCode.keys()].sort((a, b) => a - b);
  console.log(`PDFs in Missed 78: ${allPdfs.length}`);
  console.log(`NC codes resolved:    ${codes.length}`);

  if (DRY) {
    for (const c of codes) {
      const files = byCode.get(c);
      console.log(`  NC_${String(c).padStart(4, '0')}: ${files.map((f) => f.name).join(', ')}`);
    }
    return;
  }

  await fsp.rm(OUTPUT_ROOT, { recursive: true, force: true });
  await fsp.mkdir(OUTPUT_ROOT, { recursive: true });

  const results = [];
  let ok = 0, skip = 0, err = 0;
  const seenHash = new Set();

  for (const code of codes) {
    const files = [...byCode.get(code)].sort((a, b) => a.name.localeCompare(b.name));
    let idx = 0;
    for (const { path: inputPath, name } of files) {
      const size = fs.statSync(inputPath).size;
      const hash = `${code}:${size}`;
      if (seenHash.has(hash)) continue;
      seenHash.add(hash);

      const base = `NC_${String(code).padStart(4, '0')}`;
      const outName = idx === 0 ? `${base}.pdf` : `${base}_${idx + 1}.pdf`;
      idx++;
      const outPath = path.join(OUTPUT_ROOT, outName);

      try {
        const buf = await fsp.readFile(inputPath);
        const { buffer, redactionRegions, viaOcr } = await blindPdfBuffer(buf, name);
        await fsp.writeFile(outPath, buffer);
        ok++;
        results.push({ code, outName, src: name, status: 'ok', regions: redactionRegions, ocr: viaOcr ? 1 : 0 });
        console.log(`OK   ${outName} ← ${name} (${redactionRegions} regions${viaOcr ? ', OCR' : ''})`);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        if (/No redaction regions|Could not locate/i.test(msg)) {
          skip++;
          results.push({ code, outName: '', src: name, status: 'skipped', regions: 0, ocr: 0, error: msg });
          console.log(`SKIP ${name}: ${msg}`);
        } else {
          err++;
          results.push({ code, outName: '', src: name, status: 'error', regions: 0, ocr: 0, error: msg });
          console.log(`ERR  ${name}: ${msg}`);
        }
      }
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const txtPath = path.join(REPORT_DIR, `report-last-run-${stamp}.txt`);
  const txt = [
    `LAST RUN report (${stamp})`,
    `Input:  ${INPUT_ROOT}`,
    `Output: ${OUTPUT_ROOT}`,
    '',
    `PDFs scanned: ${allPdfs.length}`,
    `NC codes:     ${codes.length}`,
    `Blinded OK:   ${ok}`,
    `Skipped:      ${skip}`,
    `Errors:       ${err}`,
    '',
    ...results.map((r) =>
      `NC_${String(r.code).padStart(4, '0')}  ${r.status}  ${r.outName || '-'}  ${r.regions}  ${r.src}${r.error ? '  ' + r.error : ''}`,
    ),
  ].join('\n');
  await fsp.writeFile(txtPath, txt);
  console.log(`\nOutput: ${OUTPUT_ROOT}`);
  console.log(`Report: ${txtPath}`);
  console.log(`OK: ${ok}, skip: ${skip}, err: ${err}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
