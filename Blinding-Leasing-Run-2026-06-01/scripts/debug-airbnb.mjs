import { readFileSync } from 'node:fs';
import { detectRedactionBoxes } from './blind-core.mjs';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';

const path = process.argv[2] || 'C:/Users/kevin/Desktop/Blinding/Missed 78/AIRBNB_MARCELO_STARLING (1).pdf';
const buf = readFileSync(path);
const { pages, boxes } = await detectRedactionBoxes(buf, path.split(/[/\\]/).pop());
console.log('boxes:', boxes.length, boxes);
for (const [p, pd] of pages) {
  console.log(`\n--- p${p} ---`);
  for (const l of pd.lines) console.log(`y=${l.y.toFixed(0)} | ${l.text.slice(0, 120)}`);
}
