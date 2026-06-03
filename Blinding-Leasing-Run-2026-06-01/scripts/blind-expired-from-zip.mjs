/**
 * Blind all Seguimiento "Expired" contract PDFs from Contracts Zip into
 * Blinded Expired Contracts/.
 *
 * Contract sources (all folder naming variants):
 *   - 02. CONTRACT, 02. CONTRATO, 2. CONTRACT (and similar)
 *   - Loose contract-type PDFs under the NC customer folder
 *   - Blinded archive PDFs only when no unblinded customer copy exists
 *
 * Usage:
 *   node blind-expired-from-zip.mjs [--dry] [--limit N] [--nc NC_0015]
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { blindPdfBuffer } from './blind-core.mjs';
import {
  CONTRACTS_EXTRACTED,
  CONTRACTS_ZIP_DIR,
  MASTER_CHUNKS_DIR,
  OUTPUT_BLINDED_EXPIRED,
  REPORT_DIR,
} from './blinding-paths.mjs';

const DRY = process.argv.includes('--dry');
const limitArg = process.argv.find((a) => a.startsWith('--limit='));
const LIMIT = limitArg ? Number(limitArg.split('=')[1]) : null;
const ncArg = process.argv.find((a) => a.startsWith('--nc='));
const ONLY_NC = ncArg ? Number(ncArg.split('=')[1].replace(/\D/g, '')) : null;

const NC_FOLDER_RE = /NC[_\s]?0*(\d{1,4})(?=\D|$)/i;
const CONTRACT_FILE_RE =
  /contrac|contrat|reserva|cancel|terminaci|adenda|burofax|hospedaje|arrendamiento|acuerdo|alojamiento|lease|docusign/i;

/** @typedef {{ zipPath: string, entryPath: string, source: string, priority: number }} ZipContractRef */

function classifyFolder(name) {
  if (/informaci|information/i.test(name)) return 'info';
  if (/payment|pagos|deposito|renta|titularidad/i.test(name)) return 'payment';
  if (/\bpet\b|queja|apoyo emocional/i.test(name)) return 'other';
  if (/contrac|contrat|cancel|reserva|terminaci|adenda|burofax/i.test(name)) return 'contract';
  return 'other';
}

function codeFromZipPath(entryPath) {
  for (const seg of entryPath.split('/')) {
    const mm = seg.match(NC_FOLDER_RE);
    if (mm) return Number(mm[1]);
  }
  const base = path.posix.basename(entryPath);
  const mm = base.match(NC_FOLDER_RE);
  return mm ? Number(mm[1]) : null;
}

function contractSourceLabel(entryPath) {
  const m = entryPath.match(/\/(\d+\.\s*CONTR[^/]*)\//i);
  if (m) return `folder:${m[1]}`;
  if (/Blinded [Cc]ontracts/i.test(entryPath)) {
    return /613-\s*LET tag/i.test(entryPath) ? 'blinded:613-let' : 'blinded:archive';
  }
  if (/Complete_con_Docusign/i.test(entryPath)) return 'docusign';
  return 'loose';
}

function sourcePriority(entryPath) {
  if (/Blinded [Cc]ontracts/i.test(entryPath)) return 20;
  if (/\/\d+\.\s*CONTR/i.test(entryPath)) return 1;
  if (/Complete_con_Docusign/i.test(entryPath)) return 2;
  return 5;
}

function isContractPdfEntry(entryPath) {
  if (!/\.pdf$/i.test(entryPath)) return false;
  const base = path.posix.basename(entryPath);
  const folders = entryPath.split('/').slice(0, -1);
  const cls = folders.map(classifyFolder);
  if (cls.includes('info') || cls.includes('payment')) {
    return CONTRACT_FILE_RE.test(base);
  }
  if (/\/\d+\.\s*CONTR/i.test(entryPath)) return true;
  if (/Blinded [Cc]ontracts/i.test(entryPath)) return true;
  return CONTRACT_FILE_RE.test(base);
}

function isContractDocxEntry(entryPath) {
  if (!/\.docx$/i.test(entryPath)) return false;
  return /\/\d+\.\s*CONTR/i.test(entryPath) || CONTRACT_FILE_RE.test(path.posix.basename(entryPath));
}

function loadExpiredFromMaster() {
  /** @type {Map<number, { category: string, status: string }>} */
  const map = new Map();
  for (const file of fs.readdirSync(MASTER_CHUNKS_DIR).filter((f) => f.endsWith('.tsv')).sort()) {
    for (const line of fs.readFileSync(path.join(MASTER_CHUNKS_DIR, file), 'utf8').split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('Unique code')) continue;
      const parts = trimmed.split('\t');
      if (parts.length < 5) continue;
      const mm = parts[0].trim().match(NC_FOLDER_RE);
      if (!mm) continue;
      const code = Number(mm[1]);
      const label = parts[parts.length - 1].trim();
      if (label !== 'Expired') continue;
      map.set(code, {
        category: (parts[2] || '').trim(),
        status: (parts[3] || '').trim(),
      });
    }
  }
  return map;
}

function listZipEntries() {
  /** @type {ZipContractRef[]} */
  const refs = [];
  for (const zipPath of fs.readdirSync(CONTRACTS_ZIP_DIR).filter((f) => f.endsWith('.zip')).sort()) {
    const fullZip = path.join(CONTRACTS_ZIP_DIR, zipPath);
    const out = execFileSync('tar', ['-tf', fullZip], { encoding: 'utf8', maxBuffer: 512 * 1024 * 1024 });
    for (const entryPath of out.split('\n')) {
      const ep = entryPath.trim();
      if (!ep) continue;
      if (!isContractPdfEntry(ep)) continue;
      const code = codeFromZipPath(ep);
      if (code == null) continue;
      refs.push({
        zipPath: fullZip,
        entryPath: ep,
        source: contractSourceLabel(ep),
        priority: sourcePriority(ep),
      });
    }
  }
  return refs;
}

function listDocxOnlyByCode(expiredCodes) {
  /** @type {Map<number, string[]>} */
  const docxByCode = new Map();
  const want = new Set(expiredCodes);
  for (const zipPath of fs.readdirSync(CONTRACTS_ZIP_DIR).filter((f) => f.endsWith('.zip')).sort()) {
    const fullZip = path.join(CONTRACTS_ZIP_DIR, zipPath);
    const out = execFileSync('tar', ['-tf', fullZip], { encoding: 'utf8', maxBuffer: 512 * 1024 * 1024 });
    for (const entryPath of out.split('\n')) {
      const ep = entryPath.trim();
      if (!ep || !isContractDocxEntry(ep)) continue;
      const code = codeFromZipPath(ep);
      if (code == null || !want.has(code)) continue;
      if (!docxByCode.has(code)) docxByCode.set(code, []);
      docxByCode.get(code).push(ep);
    }
  }
  return docxByCode;
}

/** @param {ZipContractRef[]} refs */
function pickRefsForExpired(expiredCodes, refs) {
  /** @type {Map<number, ZipContractRef[]>} */
  const byCode = new Map();
  for (const code of expiredCodes) byCode.set(code, []);

  for (const ref of refs) {
    const code = codeFromZipPath(ref.entryPath);
    if (code == null || !expiredCodes.has(code)) continue;
    byCode.get(code).push(ref);
  }

  /** @type {Map<number, ZipContractRef[]>} */
  const picked = new Map();
  for (const [code, list] of byCode) {
    if (!list.length) continue;
    const customer = list.filter((r) => r.priority < 20);
    const chosen = customer.length ? customer : list;
    chosen.sort((a, b) => a.priority - b.priority || a.entryPath.localeCompare(b.entryPath));
    picked.set(code, chosen);
  }
  return picked;
}

function extractEntry(ref) {
  const cacheRoot = path.join(CONTRACTS_EXTRACTED, '_pdf_cache');
  fs.mkdirSync(cacheRoot, { recursive: true });
  const key = crypto.createHash('sha1').update(`${ref.zipPath}|${ref.entryPath}`).digest('hex');
  const localPath = path.join(cacheRoot, `${key}.pdf`);
  if (fs.existsSync(localPath)) return localPath;
  const data = execFileSync('tar', ['-xOf', ref.zipPath, ref.entryPath], {
    stdio: ['pipe', 'pipe', 'pipe'],
    maxBuffer: 256 * 1024 * 1024,
  });
  fs.writeFileSync(localPath, data);
  return localPath;
}

async function main() {
  if (!fs.existsSync(CONTRACTS_ZIP_DIR)) {
    throw new Error(`Contracts Zip not found: ${CONTRACTS_ZIP_DIR}`);
  }
  if (!fs.existsSync(MASTER_CHUNKS_DIR)) {
    throw new Error(`Master chunks not found: ${MASTER_CHUNKS_DIR}`);
  }

  const expiredMap = loadExpiredFromMaster();
  let expiredCodes = [...expiredMap.keys()].sort((a, b) => a - b);
  if (ONLY_NC != null) expiredCodes = expiredCodes.filter((c) => c === ONLY_NC);
  if (LIMIT != null) expiredCodes = expiredCodes.slice(0, LIMIT);

  console.log(`Expired NCs in master list : ${expiredMap.size}`);
  console.log(`Processing NCs             : ${expiredCodes.length}${ONLY_NC ? ` (only NC_${String(ONLY_NC).padStart(4, '0')})` : ''}`);

  console.log('Indexing contract PDFs in Contracts Zip...');
  const allRefs = listZipEntries();
  const expiredSet = new Set(expiredCodes);
  const pickedByCode = pickRefsForExpired(expiredSet, allRefs);

  const matched = expiredCodes.filter((c) => pickedByCode.has(c));
  const noPdf = expiredCodes.filter((c) => !pickedByCode.has(c));
  let totalPdfs = 0;
  for (const c of matched) totalPdfs += pickedByCode.get(c).length;

  const docxOnly = listDocxOnlyByCode(expiredSet);
  const docxOnlyNoPdf = noPdf.filter((c) => docxOnly.has(c));

  console.log(`Expired WITH contract PDF  : ${matched.length}`);
  console.log(`Contract PDFs to process   : ${totalPdfs}`);
  console.log(`Expired with NO PDF          : ${noPdf.length}`);
  console.log(`  (docx-only in zip)         : ${docxOnlyNoPdf.length}`);

  if (DRY) {
    console.log('\n[DRY RUN] Sample picks:');
    for (const code of matched.slice(0, 15)) {
      const refs = pickedByCode.get(code);
      console.log(
        `  NC_${String(code).padStart(4, '0')} (${expiredMap.get(code)?.category}): ${refs.length} PDF(s)`,
      );
      for (const r of refs) console.log(`    [${r.source}] ${path.posix.basename(r.entryPath)}`);
    }
    if (noPdf.length) {
      console.log('\nNo PDF (first 20):', noPdf.slice(0, 20).map((c) => `NC_${String(c).padStart(4, '0')}`).join(', '));
    }
    return;
  }

  await fsp.rm(OUTPUT_BLINDED_EXPIRED, { recursive: true, force: true });
  await fsp.mkdir(OUTPUT_BLINDED_EXPIRED, { recursive: true });

  /** @type {Map<number, number>} */
  const fileIndexByCode = new Map();
  /** @type {Map<string, number>} */
  const seenHash = new Map();
  const results = [];
  let okFiles = 0;
  let copiedBlinded = 0;
  let skipFiles = 0;
  let errFiles = 0;
  let dupSkipped = 0;

  function flatOutputPath(code, inputPath) {
    const size = fs.statSync(inputPath).size;
    const hash = `${code}:${size}`;
    if (seenHash.has(hash)) return null;
    seenHash.set(hash, 1);

    const base = `NC_${String(code).padStart(4, '0')}`;
    const idx = fileIndexByCode.get(code) || 0;
    fileIndexByCode.set(code, idx + 1);
    const name = idx === 0 ? `${base}.pdf` : `${base}_${idx + 1}.pdf`;
    return path.join(OUTPUT_BLINDED_EXPIRED, name);
  }

  for (const code of matched) {
    const meta = expiredMap.get(code);
    const refs = pickedByCode.get(code);
    for (const ref of refs) {
      const relLabel = ref.entryPath;
      let localPath;
      try {
        localPath = extractEntry(ref);
      } catch (err) {
        errFiles++;
        const msg = err instanceof Error ? err.message : String(err);
        results.push({
          code, cat: meta?.category || '', outName: '', relLabel, source: ref.source,
          status: 'error-extract', regions: 0, ocr: 0, error: msg,
        });
        console.log(`ERR  extract ${relLabel}: ${msg}`);
        continue;
      }

      const outPath = flatOutputPath(code, localPath);
      if (outPath === null) {
        dupSkipped++;
        continue;
      }

      const useCopyOnly = ref.priority >= 20;
      try {
        if (useCopyOnly) {
          await fsp.copyFile(localPath, outPath);
          copiedBlinded++;
          results.push({
            code, cat: meta?.category || '', outName: path.basename(outPath), relLabel,
            source: ref.source, status: 'copied-blinded-archive', regions: 0, ocr: 0, error: '',
          });
          console.log(`COPY ${path.basename(outPath)} ← [${ref.source}] ${path.posix.basename(relLabel)}`);
          continue;
        }

        const buf = await fsp.readFile(localPath);
        const { buffer, redactionRegions, viaOcr } = await blindPdfBuffer(buf, path.basename(localPath));
        await fsp.writeFile(outPath, buffer);
        okFiles++;
        results.push({
          code, cat: meta?.category || '', outName: path.basename(outPath), relLabel,
          source: ref.source, status: 'ok', regions: redactionRegions, ocr: viaOcr ? 1 : 0, error: '',
        });
        console.log(
          `OK   ${path.basename(outPath)} ← [${ref.source}] ${path.posix.basename(relLabel)} ` +
            `(${redactionRegions} regions${viaOcr ? ', OCR' : ''})`,
        );
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (/No redaction regions|Could not locate/i.test(msg)) {
          skipFiles++;
          results.push({
            code, cat: meta?.category || '', outName: '', relLabel, source: ref.source,
            status: 'skipped-no-regions', regions: 0, ocr: /OCR|image-only/i.test(msg) ? 1 : 0, error: msg,
          });
          console.log(`SKIP [${ref.source}] ${path.posix.basename(relLabel)} (no party/signature blocks)`);
        } else {
          errFiles++;
          results.push({
            code, cat: meta?.category || '', outName: '', relLabel, source: ref.source,
            status: 'error', regions: 0, ocr: 0, error: msg,
          });
          console.log(`ERR  [${ref.source}] ${path.posix.basename(relLabel)}: ${msg}`);
        }
      }
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const csvPath = path.join(REPORT_DIR, `report-blinded-expired-${stamp}.csv`);
  const txtPath = path.join(REPORT_DIR, `report-blinded-expired-${stamp}.txt`);
  const fmt = (c) => `NC_${String(c).padStart(4, '0')}`;

  const csv = ['code,category,output,status,source,regions,ocr,zip_entry,error']
    .concat(
      results.map((r) => [
        fmt(r.code), r.cat, r.outName, r.status, r.source, r.regions, r.ocr,
        `"${r.relLabel.replace(/"/g, '""')}"`, `"${(r.error || '').replace(/"/g, '""')}"`,
      ].join(',')),
    )
    .join('\n');
  await fsp.writeFile(csvPath, csv);

  const txt = [
    `Blinded Expired Contracts report (${stamp})`,
    `Output: ${OUTPUT_BLINDED_EXPIRED}`,
    `Source: ${CONTRACTS_ZIP_DIR}`,
    '',
    'Filter: Seguimiento Label = Expired',
    'Contract folders: 02. CONTRACT, 02. CONTRATO, 2. CONTRACT, blinded archives (fallback)',
    '',
    `Expired NCs processed          : ${expiredCodes.length}`,
    `Expired WITH contract PDF      : ${matched.length}`,
    `Contract PDFs processed        : ${totalPdfs}`,
    `PDFs blinded OK                : ${okFiles}`,
    `PDFs copied (blinded archive)  : ${copiedBlinded}`,
    `PDFs skipped (no PII blocks)   : ${skipFiles}`,
    `PDFs errored                   : ${errFiles}`,
    `Duplicate copies skipped       : ${dupSkipped}`,
    '',
    '==== EXPIRED WITH NO PDF IN ZIP ====',
    ...noPdf.map((c) => `${fmt(c)}  (${expiredMap.get(c)?.category || ''})`),
    '',
    '==== EXPIRED DOCX-ONLY (no PDF in contract folder) ====',
    ...docxOnlyNoPdf.map((c) => `${fmt(c)}  ${(docxOnly.get(c) || []).map((p) => path.posix.basename(p)).join('; ')}`),
  ].join('\n');
  await fsp.writeFile(txtPath, txt);

  console.log('\n========== SUMMARY ==========');
  console.log(`Expired NCs with PDF: ${matched.length} | blinded: ${okFiles}, copied: ${copiedBlinded}, skipped: ${skipFiles}, errors: ${errFiles}`);
  console.log(`Report: ${txtPath}`);
  console.log(`Output: ${OUTPUT_BLINDED_EXPIRED}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
