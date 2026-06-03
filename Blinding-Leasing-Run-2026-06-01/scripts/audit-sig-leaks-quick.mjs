/** Scan blinded PDFs for real signature leaks (honorific names, Docusign IDs on p16-18) */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';
import { OUTPUT_BLINDED_PENDIENTES } from './blinding-paths.mjs';

function isHonorificName(text) {
  const t = text.trim();
  return /^(d\.?|dn\.?|d[ñn]a\.?|da\.?|do[nñ]a?|don)\s+[A-Za-zÁÉÍÓÚÑáéíóúñ]{2,}/i.test(t);
}

function isDocusignEnvelope(text) {
  return /Docusign\s+Envelope\s+ID:/i.test(text);
}

function isDocusignHex(text) {
  const tokens = text.trim().split(/\s+/);
  return tokens.some((t) => /^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$/i.test(t));
}

async function scanPdf(pdfPath) {
  const buf = await fsp.readFile(pdfPath);
  const doc = await getDocument({ data: new Uint8Array(buf), useSystemFonts: true }).promise;
  /** @type {{ page: number, text: string, kind: string }[]} */
  const leaks = [];
  for (let pageNum = 15; pageNum <= Math.min(20, doc.numPages); pageNum++) {
    const page = await doc.getPage(pageNum);
    const tc = await page.getTextContent();
    const parts = tc.items.filter((i) => 'str' in i && i.str.trim()).map((i) => i.str.trim());
    for (const t of parts) {
      if (isHonorificName(t)) leaks.push({ page: pageNum, text: t, kind: 'honorific' });
      if (isDocusignEnvelope(t) || isDocusignHex(t)) leaks.push({ page: pageNum, text: t.slice(0, 80), kind: 'docusign' });
    }
  }
  return leaks;
}

async function collectPdfs(root) {
  const out = [];
  async function walk(dir) {
    for (const ent of await fsp.readdir(dir, { withFileTypes: true })) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) await walk(full);
      else if (ent.name.endsWith('.pdf')) out.push(full);
    }
  }
  await walk(root);
  return out;
}

const pdfs = await collectPdfs(OUTPUT_BLINDED_PENDIENTES);
let failed = 0;
for (const pdf of pdfs) {
  const leaks = await scanPdf(pdf);
  if (leaks.length) {
    failed++;
    console.log(`FAIL ${path.relative(OUTPUT_BLINDED_PENDIENTES, pdf)}`);
    for (const l of leaks) console.log(`  p${l.page} [${l.kind}] ${l.text}`);
  }
}
console.log(`\nFailed: ${failed} / ${pdfs.length}`);
