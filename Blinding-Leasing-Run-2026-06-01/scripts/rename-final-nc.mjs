/**
 * Rename all PDFs in FINAL to NC_XXXX.pdf only (no names/details).
 * Multiple PDFs per NC → NC_XXXX_2.pdf, NC_XXXX_3.pdf, …
 */
import { readdirSync, renameSync } from 'node:fs';
import { join } from 'node:path';

const FINAL = 'C:/Users/kevin/Desktop/Blinding/FINAL';
const NC_RE = /NC[_\s]?0*(\d{1,4})(?=\D|$)/i;

const files = readdirSync(FINAL)
  .filter((f) => f.toLowerCase().endsWith('.pdf'))
  .map((name) => {
    const m = name.match(NC_RE);
    if (!m) return null;
    return { name, code: Number(m[1]) };
  })
  .filter(Boolean);

if (files.length === 0) {
  console.error('No PDFs found in FINAL');
  process.exit(1);
}

const noCode = readdirSync(FINAL).filter((f) => f.endsWith('.pdf') && !NC_RE.test(f));
if (noCode.length) {
  console.error('Could not extract NC code from:', noCode.join(', '));
  process.exit(1);
}

/** @type {Map<number, typeof files>} */
const byCode = new Map();
for (const f of files) {
  const list = byCode.get(f.code) || [];
  list.push(f);
  byCode.set(f.code, list);
}

// Stable order so _2/_3 assignment is reproducible
for (const list of byCode.values()) list.sort((a, b) => a.name.localeCompare(b.name));

/** @type {{ from: string, to: string }[]} */
const plan = [];
for (const [code, list] of [...byCode.entries()].sort((a, b) => a[0] - b[0])) {
  const base = `NC_${String(code).padStart(4, '0')}`;
  list.forEach((f, i) => {
    const to = i === 0 ? `${base}.pdf` : `${base}_${i + 1}.pdf`;
    plan.push({ from: f.name, to });
  });
}

// Two-phase rename avoids collisions (e.g. two files → same NC_XXXX.pdf)
for (let i = 0; i < plan.length; i++) {
  renameSync(join(FINAL, plan[i].from), join(FINAL, `__renaming_${i}.pdf`));
}
for (let i = 0; i < plan.length; i++) {
  renameSync(join(FINAL, `__renaming_${i}.pdf`), join(FINAL, plan[i].to));
}

console.log(`Renamed ${plan.length} PDFs in FINAL:\n`);
for (const { from, to } of plan) console.log(`  ${from}\n    → ${to}`);
console.log(`\nUnique NC codes: ${byCode.size}`);
const multi = [...byCode.entries()].filter(([, l]) => l.length > 1);
if (multi.length) {
  console.log(`Multiple PDFs per NC (${multi.length}):`);
  for (const [code, list] of multi) {
    console.log(`  NC_${String(code).padStart(4, '0')}: ${list.length} files`);
  }
}
