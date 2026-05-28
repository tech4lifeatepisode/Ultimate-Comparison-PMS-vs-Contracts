/**
 * Blind PDFs in this folder → Locally Blinded/
 * Uses production blinding logic from Blinding_prod/blind-core.mjs
 */
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { blindPdfBuffer } from '../Blinding_Contracts/Blinding_prod/blind-core.mjs';
import { isPdfFileName } from '../Blinding_Contracts/Blinding_prod/storage-utils.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const INPUT_DIR = __dirname;
const OUTPUT_DIR = path.join(__dirname, 'Locally Blinded');

async function main() {
  await fs.mkdir(OUTPUT_DIR, { recursive: true });

  const entries = await fs.readdir(INPUT_DIR, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && isPdfFileName(e.name) && e.name !== 'blind-local.mjs')
    .map((e) => e.name)
    .sort();

  if (files.length === 0) {
    console.log('No PDF files found in this folder.');
    process.exit(0);
  }

  console.log(`Blinding ${files.length} contract(s) → Locally Blinded/\n`);

  let failed = 0;
  for (const name of files) {
    console.log(`Processing: ${name}`);
    const inputPath = path.join(INPUT_DIR, name);
    let outName = name;
    if (isPdfFileName(name) && !name.toLowerCase().endsWith('.pdf')) {
      outName = name.replace(/_pdf$/i, '.pdf');
    }
    const outputPath = path.join(OUTPUT_DIR, outName);

    try {
      const buffer = await fs.readFile(inputPath);
      const { buffer: blinded, redactionRegions, pagesRasterized } = await blindPdfBuffer(buffer, name);
      await fs.writeFile(outputPath, blinded);
      console.log(`  Saved: ${outputPath} (${redactionRegions} regions, ${pagesRasterized} page(s))\n`);
    } catch (err) {
      failed++;
      console.error(`  Error: ${err instanceof Error ? err.message : err}\n`);
    }
  }

  console.log(`Done. ${files.length - failed} succeeded, ${failed} failed.`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
