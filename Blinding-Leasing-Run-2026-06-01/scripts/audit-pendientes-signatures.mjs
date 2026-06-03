/**
 * Audit blinded Pendientes PDFs for remaining PII on signature pages (14–20).
 * Usage: node audit-pendientes-signatures.mjs
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';
import { OUTPUT_BLINDED_PENDIENTES, INPUT_PENDIENTES, REPORT_DIR } from './blinding-paths.mjs';

const SIG_PAGE_RANGE = [14, 15, 16, 17, 18, 19, 20];

function isHonorificName(text) {
  const t = text.trim();
  if (!/[A-Za-zÁÉÍÓÚÑáéíóúñ]{2,}/.test(t)) return false;
  return /^(d\.?|dn\.?|d[ñn]a\.?|da\.?|do[nñ]a?|don|sr\.?|sra\.?|sres\.?|sta\.?)\s+[A-Za-zÁÉÍÓÚÑáéíóúñ]/i.test(t);
}

function isDocusignCode(text) {
  const norm = text.replace(/[\u2026]/g, '').trim();
  const tokens = norm.split(/\s+/).filter(Boolean);
  if (!tokens.length) return false;
  if (!tokens.every((tok) => /^[0-9A-Za-z.\-]{6,}$/.test(tok))) return false;
  return tokens.some(
    (tok) => /\d/.test(tok) && /[A-Za-z]/.test(tok) && tok.replace(/[.\-]/g, '').length >= 8,
  );
}

function isIdentityDoc(text) {
  const t = text.trim();
  if (/^(NIF|NIE|DNI|CIF|Pasaporte|Documento)\b/i.test(t)) return true;
  if (/\b(con|n[ºo°.]?\s*)?(DNI|NIE|NIF)\b/i.test(t) && /\d/.test(t)) return true;
  return /\b[XYZ]?\d{7,8}[-\s]?[A-Z]\b/i.test(t) || /\b\d{8}[-\s]?[A-Z]\b/i.test(t);
}

function isLikelyPersonName(text) {
  const t = text.trim();
  if (t.length < 6 || t.length > 80) return false;
  if (/^(La|El|Los|Las|En|De|Y|Enrique|Chamari|Empresa|Cliente|Avalista)\b/i.test(t)) return false;
  if (/docusign|envelope|prueba|conformidad|carabanchel|node/i.test(t)) return false;
  const words = t.split(/\s+/).filter((w) => w.length > 1);
  if (words.length < 2 || words.length > 8) return false;
  return words.every((w) => /^[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+$/i.test(w) || /^[A-ZÁÉÍÓÚÑ]{2,}$/.test(w));
}

function classifyLeak(line) {
  const t = line.trim();
  if (!t || t.length < 4) return null;
  if (/enrique\s+oliete|chamari/i.test(t)) return null;
  if (isHonorificName(t)) return 'honorific-name';
  if (isDocusignCode(t)) return 'docusign-id';
  if (isIdentityDoc(t)) return 'identity-doc';
  if (isLikelyPersonName(t)) return 'person-name';
  return null;
}

async function extractSigPages(pdfPath) {
  const buf = await fsp.readFile(pdfPath);
  const doc = await getDocument({ data: new Uint8Array(buf), useSystemFonts: true }).promise;
  /** @type {{ page: number, line: string, kind: string }[]} */
  const leaks = [];

  for (const pageNum of SIG_PAGE_RANGE) {
    if (pageNum > doc.numPages) continue;
    const page = await doc.getPage(pageNum);
    const textContent = await page.getTextContent();
    const lines = [];
    let cur = '';
    for (const item of textContent.items) {
      if (!('str' in item)) continue;
      if (item.str.includes('\n')) {
        const parts = item.str.split('\n');
        for (let i = 0; i < parts.length; i++) {
          if (i > 0) { if (cur.trim()) lines.push(cur); cur = ''; }
          cur += parts[i];
        }
      } else {
        cur += item.str;
      }
    }
    if (cur.trim()) lines.push(cur);

    const hasSigHeader = lines.some((l) => /El\s*(Cliente|Avalista)|La\s*Empresa/i.test(l));
    const isSparse = lines.join('').replace(/\s/g, '').length < 300;
    if (!hasSigHeader && !isSparse && pageNum < 16) continue;

    for (const line of lines) {
      const kind = classifyLeak(line);
      if (kind) leaks.push({ page: pageNum, line: line.trim().slice(0, 120), kind });
    }
  }
  return leaks;
}

async function collectPdfs(root) {
  /** @type {string[]} */
  const out = [];
  async function walk(dir) {
    for (const ent of await fsp.readdir(dir, { withFileTypes: true })) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) await walk(full);
      else if (ent.name.toLowerCase().endsWith('.pdf')) out.push(full);
    }
  }
  await walk(root);
  return out.sort();
}

function mapToSource(blindedPath) {
  const base = path.basename(blindedPath, '.pdf');
  const mm = base.match(/NC_(\d{4})(?:_(AA|RES|TRK))?/i);
  if (!mm) return null;
  const code = mm[1];
  const suffix = mm[2]?.toUpperCase();

  /** @type {string[]} */
  const candidates = [];
  if (suffix === 'AA') {
    candidates.push(path.join(INPUT_PENDIENTES, 'Accesos anticipados'));
  } else if (suffix === 'RES') {
    candidates.push(path.join(INPUT_PENDIENTES, 'Reserva'));
  } else if (suffix === 'TRK') {
    candidates.push(path.join(INPUT_PENDIENTES, 'Trcickies'));
  } else {
    candidates.push(INPUT_PENDIENTES);
  }

  for (const dir of candidates) {
    if (!fs.existsSync(dir)) continue;
    for (const f of fs.readdirSync(dir)) {
      if (f.toLowerCase().endsWith('.pdf') && new RegExp(`NC[_\\s]?0*${Number(code)}(?=\\D|$)`, 'i').test(f)) {
        return path.join(dir, f);
      }
    }
  }
  return null;
}

async function main() {
  const pdfs = await collectPdfs(OUTPUT_BLINDED_PENDIENTES);
  console.log(`Auditing ${pdfs.length} blinded PDFs on signature pages ${SIG_PAGE_RANGE.join('-')}...\n`);

  /** @type {{ blinded: string, source: string|null, leaks: object[] }[]} */
  const failed = [];
  let clean = 0;

  for (const pdf of pdfs) {
    const leaks = await extractSigPages(pdf);
    if (leaks.length) {
      failed.push({ blinded: pdf, source: mapToSource(pdf), leaks });
      console.log(`FAIL ${path.relative(OUTPUT_BLINDED_PENDIENTES, pdf)} (${leaks.length} leak(s))`);
      for (const l of leaks.slice(0, 5)) {
        console.log(`  p${l.page} [${l.kind}] ${l.line}`);
      }
      if (leaks.length > 5) console.log(`  ... +${leaks.length - 5} more`);
    } else {
      clean++;
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const reportPath = path.join(REPORT_DIR, `audit-pendientes-signatures-${stamp}.txt`);
  const lines = [
    `Signature page audit (${stamp})`,
    `Folder: ${OUTPUT_BLINDED_PENDIENTES}`,
    `Clean: ${clean} | Failed: ${failed.length}`,
    '',
  ];
  for (const f of failed) {
    lines.push(`==== ${path.relative(OUTPUT_BLINDED_PENDIENTES, f.blinded)} ====`);
    lines.push(`Source: ${f.source || 'NOT FOUND'}`);
    for (const l of f.leaks) lines.push(`  p${l.page} [${l.kind}] ${l.line}`);
    lines.push('');
  }
  await fsp.writeFile(reportPath, lines.join('\n'));

  console.log(`\nClean: ${clean} | Failed: ${failed.length}`);
  console.log(`Report: ${reportPath}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
