/**
 * Blind all contract PDFs under Pendientes Blinding/Pendientes into
 * Pendientes Blinding/Blinded Pendientes, preserving subfolder layout
 * with " Blinded" appended to category folder names.
 *
 * Naming:
 *   - Root Pendientes PDFs        → NC_XXXX.pdf
 *   - Accesos anticipados         → NC_XXXX_AA.pdf
 *   - Reserva                     → NC_XXXX_RES.pdf
 *   - Trcickies                   → NC_XXXX_TRK.pdf
 *
 * Usage: node blind-pendientes.mjs [--dry] [--limit=N] [--reblind]
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { blindPdfBuffer } from './blind-core.mjs';
import { INPUT_PENDIENTES, OUTPUT_BLINDED_PENDIENTES, REPORT_DIR } from './blinding-paths.mjs';

const DRY = process.argv.includes('--dry');
const REBLIND = process.argv.includes('--reblind');
const limitArg = process.argv.find((a) => a.startsWith('--limit='));
const LIMIT = limitArg ? Number(limitArg.split('=')[1]) : null;

const NC_RE = /NC[_\s]?0*(\d{1,4})(?=\D|$)/i;

/** @type {Record<string, { outFolder: string, suffix: string }>} */
const CATEGORY_MAP = {
  'accesos anticipados': { outFolder: 'Accesos anticipados Blinded', suffix: '_AA' },
  reserva: { outFolder: 'Reserva Blinded', suffix: '_RES' },
  trcickies: { outFolder: 'Trcickies Blinded', suffix: '_TRK' },
};

function categoryFromRelDir(relDir) {
  if (!relDir) return null;
  const top = relDir.split(/[/\\]/)[0].trim().toLowerCase();
  return CATEGORY_MAP[top] || null;
}

function ncCodeFromName(name) {
  const mm = name.match(NC_RE);
  return mm ? Number(mm[1]) : null;
}

function fmtCode(code) {
  return `NC_${String(code).padStart(4, '0')}`;
}

async function collectPdfs(root) {
  /** @type {{ inputPath: string, relDir: string, fileName: string }[]} */
  const out = [];
  async function walk(dir, relDir) {
    for (const ent of await fsp.readdir(dir, { withFileTypes: true })) {
      const full = path.join(dir, ent.name);
      const childRel = relDir ? path.join(relDir, ent.name) : ent.name;
      if (ent.isDirectory()) {
        await walk(full, childRel);
      } else if (ent.isFile() && ent.name.toLowerCase().endsWith('.pdf')) {
        out.push({
          inputPath: full,
          relDir: relDir ? path.dirname(childRel) : '',
          fileName: ent.name,
        });
      }
    }
  }
  await walk(root, '');
  return out.sort((a, b) => a.inputPath.localeCompare(b.inputPath));
}

function outputPathFor(entry, fileIndexByKey) {
  const code = ncCodeFromName(entry.fileName);
  const cat = categoryFromRelDir(entry.relDir);
  const outRoot = OUTPUT_BLINDED_PENDIENTES;
  const outDir = cat ? path.join(outRoot, cat.outFolder) : outRoot;

  let base;
  if (code != null) {
    base = fmtCode(code) + (cat?.suffix || '');
  } else {
    base = path.basename(entry.fileName, '.pdf');
  }

  const key = `${outDir}|${base}`;
  const idx = fileIndexByKey.get(key) || 0;
  fileIndexByKey.set(key, idx + 1);
  const name = idx === 0 ? `${base}.pdf` : `${base}_${idx + 1}.pdf`;
  return path.join(outDir, name);
}

async function main() {
  if (!fs.existsSync(INPUT_PENDIENTES)) {
    throw new Error(`Input folder not found: ${INPUT_PENDIENTES}`);
  }

  let pdfs = await collectPdfs(INPUT_PENDIENTES);
  if (LIMIT != null) pdfs = pdfs.slice(0, LIMIT);

  console.log(`Input : ${INPUT_PENDIENTES}`);
  console.log(`Output: ${OUTPUT_BLINDED_PENDIENTES}`);
  console.log(`PDFs  : ${pdfs.length}`);

  if (DRY) {
    console.log('\n[DRY RUN] Planned outputs (first 25):');
    const fileIndexByKey = new Map();
    for (const entry of pdfs.slice(0, 25)) {
      const outPath = outputPathFor(entry, fileIndexByKey);
      console.log(`  ${path.relative(INPUT_PENDIENTES, entry.inputPath)}`);
      console.log(`    → ${path.relative(OUTPUT_BLINDED_PENDIENTES, outPath)}`);
    }
    return;
  }

  if (!REBLIND) {
    await fsp.rm(OUTPUT_BLINDED_PENDIENTES, { recursive: true, force: true });
  }
  await fsp.mkdir(OUTPUT_BLINDED_PENDIENTES, { recursive: true });

  /** @type {Map<string, number>} */
  const fileIndexByKey = new Map();
  const results = [];
  let ok = 0;
  let skip = 0;
  let err = 0;

  for (const entry of pdfs) {
    const outPath = outputPathFor(entry, fileIndexByKey);
    await fsp.mkdir(path.dirname(outPath), { recursive: true });
    const relIn = path.relative(INPUT_PENDIENTES, entry.inputPath);

    try {
      const buf = await fsp.readFile(entry.inputPath);
      const { buffer, redactionRegions, viaOcr } = await blindPdfBuffer(buf, entry.fileName);
      await fsp.writeFile(outPath, buffer);
      ok++;
      results.push({
        input: relIn,
        output: path.relative(OUTPUT_BLINDED_PENDIENTES, outPath),
        status: 'ok',
        regions: redactionRegions,
        ocr: viaOcr ? 1 : 0,
        error: '',
      });
      console.log(
        `OK   ${path.basename(outPath)} ← ${entry.fileName} (${redactionRegions} regions${viaOcr ? ', OCR' : ''})`,
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (/No redaction regions|Could not locate/i.test(msg)) {
        skip++;
        results.push({
          input: relIn,
          output: '',
          status: 'skipped-no-regions',
          regions: 0,
          ocr: /OCR|image-only/i.test(msg) ? 1 : 0,
          error: msg,
        });
        console.log(`SKIP ${entry.fileName} (no party/signature blocks)`);
      } else {
        err++;
        results.push({
          input: relIn,
          output: '',
          status: 'error',
          regions: 0,
          ocr: 0,
          error: msg,
        });
        console.log(`ERR  ${entry.fileName}: ${msg}`);
      }
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const reportPrefix = REBLIND ? 'report-reblind-pendientes' : 'report-blinded-pendientes';
  const csvPath = path.join(REPORT_DIR, `${reportPrefix}-${stamp}.csv`);
  const txtPath = path.join(REPORT_DIR, `${reportPrefix}-${stamp}.txt`);

  const csv = ['input,output,status,regions,ocr,error']
    .concat(
      results.map((r) =>
        [r.input, r.output, r.status, r.regions, r.ocr, `"${(r.error || '').replace(/"/g, '""')}"`].join(','),
      ),
    )
    .join('\n');
  await fsp.writeFile(csvPath, csv);

  const txt = [
    `Blinded Pendientes report (${stamp})`,
    `Input : ${INPUT_PENDIENTES}`,
    `Output: ${OUTPUT_BLINDED_PENDIENTES}`,
    '',
    `PDFs total     : ${pdfs.length}`,
    `Blinded OK     : ${ok}`,
    `Skipped        : ${skip}`,
    `Errors         : ${err}`,
    '',
    '==== SKIPPED ====',
    ...results.filter((r) => r.status === 'skipped-no-regions').map((r) => `${r.input}  ${r.error}`),
    '',
    '==== ERRORS ====',
    ...results.filter((r) => r.status === 'error').map((r) => `${r.input}  ${r.error}`),
  ].join('\n');
  await fsp.writeFile(txtPath, txt);

  console.log('\n========== SUMMARY ==========');
  console.log(`Total: ${pdfs.length} | OK: ${ok}, skipped: ${skip}, errors: ${err}`);
  console.log(`Report: ${txtPath}`);
  console.log(`Output: ${OUTPUT_BLINDED_PENDIENTES}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
