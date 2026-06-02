/**
 * Read-only audit v2: real client PII leaks only (ignores company address/code noise).
 */
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import mupdf from 'mupdf';
import { createWorker } from 'tesseract.js';

const FINAL = 'C:/Users/kevin/Desktop/Blinding/FINAL';
const OCR_SCALE = 2.5;

const COMPANY_LINE = /enrique\s+oliete|chamari\s+itg|la\s+empresa|docsigned\s+by|docu\s*signed/i;

function isClientPrintedName(text) {
  const t = text.trim();
  if (!t || COMPANY_LINE.test(t)) return false;
  if (/^(d\.?|d[ñn]a\.?|dn\.?|do[nñ]a)\s+[A-ZÁÉÍÓÚÑ]/i.test(t)) return true;
  // name without honorific under signature (OCR often drops "D.")
  if (/\b(pasaporte|nie|dni)\b/i.test(t) && /[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+\s+[A-ZÁÉÍÓÚÑ]/i.test(t)) return true;
  return false;
}

function isClientId(text) {
  if (COMPANY_LINE.test(text)) return false;
  return (
    /\bDNI\s*[:/]?\s*[0-9XYZ]/i.test(text) ||
    /\bNIE\s*[:/]?\s*[0-9XYZ]/i.test(text) ||
    /\bPASAPORTE\s*[:/]?\s*\d{6,}/i.test(text) ||
    /\b\d{8}[A-Z]\b/.test(text) ||
    /\b[XYZ]\d{7}[A-Z]\b/i.test(text)
  );
}

function isClientAddress(text) {
  if (COMPANY_LINE.test(text)) return false;
  if (/direcci[oó]n\s+registral|aguacate|tulipero|carabanchel.*28044|28058\s+madrid/i.test(text)) return false;
  return (
    (/\bDirecci[oó]n\b/i.test(text) || /\bC[oó]digo\s+Postal\b/i.test(text)) &&
    /\d{5}/.test(text) &&
    /de\s+otra\s+parte|cliente|avalista/i.test(text)
  );
}

function classifyLeak(text) {
  if (isClientPrintedName(text)) return 'printed_name';
  if (isClientId(text)) return 'id_number';
  if (isClientAddress(text)) return 'client_address';
  return null;
}

function* ocrLines(data) {
  for (const b of data.blocks || [])
    for (const p of b.paragraphs || [])
      for (const l of p.lines || []) {
        const t = (l.text || '').trim();
        if (t) yield t;
      }
}

async function ocrPage(buf, pageNum, worker) {
  const doc = mupdf.Document.openDocument(buf, 'application/pdf');
  const page = doc.loadPage(pageNum - 1);
  const pix = page.toPixmap(mupdf.Matrix.scale(OCR_SCALE, OCR_SCALE), mupdf.ColorSpace.DeviceRGB, false);
  const { data } = await worker.recognize(Buffer.from(pix.asPNG()), {}, { blocks: true });
  return [...ocrLines(data)];
}

function pagesFor(n) {
  const s = new Set([1, 2, n]);
  if (n >= 28) for (const p of [16, 17, 18]) if (p <= n) s.add(p);
  if (n <= 4) for (let p = 1; p <= n; p++) s.add(p);
  return [...s].sort((a, b) => a - b);
}

const files = readdirSync(FINAL).filter((f) => f.endsWith('.pdf')).sort();
const worker = await createWorker('spa');
/** @type {{ name: string, issues: { page: number, kind: string, snippet: string }[] }[]} */
const bad = [];

try {
  for (let i = 0; i < files.length; i++) {
    const name = files[i];
    process.stdout.write(`\r[${i + 1}/${files.length}] ${name.slice(0, 55).padEnd(55)}`);
    const buf = readFileSync(join(FINAL, name));
    const doc = mupdf.Document.openDocument(buf, 'application/pdf');
    const n = doc.countPages();
    const issues = [];
    for (const p of pagesFor(n)) {
      const lines = await ocrPage(buf, p, worker);
      for (const line of lines) {
        const kind = classifyLeak(line);
        if (kind) {
          const snip = line.length > 80 ? `${line.slice(0, 80)}…` : line;
          issues.push({ page: p, kind, snippet: snip });
        }
      }
    }
    if (issues.length) bad.push({ name, issues });
  }
} finally {
  await worker.terminate();
}

console.log('\n');
let report = `FINAL audit v2 (client PII only)\nScanned: ${files.length}\nWith issues: ${bad.length}\n\n`;
for (const r of bad) {
  report += `${r.name}\n`;
  for (const iss of r.issues) report += `  p${iss.page} [${iss.kind}] ${iss.snippet}\n`;
  report += '\n';
}
if (!bad.length) report += 'No client PII leaks detected.\n';

const out = `audit-final-v2-${new Date().toISOString().replace(/[:.]/g, '-')}.txt`;
writeFileSync(out, report);
console.log(report);
console.log(`Saved: ${out}`);
