import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dataDir = path.join(__dirname, '..', 'data');

function readLines(name) {
  const p = path.join(dataDir, name);
  if (!fs.existsSync(p)) return new Set();
  return new Set(
    fs
      .readFileSync(p, 'utf8')
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter(Boolean),
  );
}

function normalizeNc(code) {
  const m = String(code).trim().match(/NC_(\d+)/i);
  if (!m) return null;
  return `NC_${m[1].padStart(4, '0')}`;
}

function parseMasterExpired(masterPath) {
  const text = fs.readFileSync(masterPath, 'utf8');
  const expired = new Set();
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('Unique code')) continue;
    const parts = trimmed.split('\t');
    if (parts.length < 5) continue;
    const code = normalizeNc(parts[0]);
    const label = parts[parts.length - 1].trim();
    if (code && label === 'Expired') expired.add(code);
  }
  return expired;
}

const masterPath = process.argv[2];
if (!masterPath) {
  console.error('Usage: node compare-expired-ncs.mjs <master-nc-status.tsv>');
  process.exit(1);
}

const expired = parseMasterExpired(masterPath);
const inFolder = readLines('zip-nc-folders.txt');
const withContract = readLines('zip-nc-with-contract.txt');
const blinded = readLines('zip-nc-blinded.txt');

const missingFolder = [...expired].filter((nc) => !inFolder.has(nc)).sort();
const inFolderNoContract = [...expired]
  .filter((nc) => inFolder.has(nc) && !withContract.has(nc))
  .sort();
const missingEntirely = [...expired]
  .filter((nc) => !inFolder.has(nc) && !blinded.has(nc))
  .sort();
const presentWithContract = [...expired].filter((nc) => withContract.has(nc)).sort();
const presentBlindedOnly = [...expired]
  .filter((nc) => !withContract.has(nc) && blinded.has(nc))
  .sort();

const report = {
  masterExpiredCount: expired.size,
  inZipFolder: [...expired].filter((nc) => inFolder.has(nc)).length,
  withContractFile: presentWithContract.length,
  blindedOnly: presentBlindedOnly.length,
  missingFolder,
  inFolderNoContract,
  missingEntirely,
};

const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const outBase = path.join(dataDir, `expired-vs-zip-${stamp}`);
fs.writeFileSync(`${outBase}.json`, JSON.stringify(report, null, 2));
fs.writeFileSync(
  `${outBase}.txt`,
  [
    `Master Expired NCs: ${report.masterExpiredCount}`,
    `In zip customer folder: ${report.inZipFolder}`,
    `With 02. CONTRACT pdf/docx: ${report.withContractFile}`,
    `Blinded-only (no 02. CONTRACT): ${report.blindedOnly.length}`,
    `Missing customer folder: ${report.missingFolder.length}`,
    `In folder but no contract file: ${report.inFolderNoContract.length}`,
    '',
    '=== Missing customer folder ===',
    ...report.missingFolder,
    '',
    '=== In folder but no 02. CONTRACT file ===',
    ...report.inFolderNoContract,
    '',
    '=== Blinded-only Expired ===',
    ...report.presentBlindedOnly,
  ].join('\n'),
);

console.log(`Master Expired: ${report.masterExpiredCount}`);
console.log(`With contract in zip: ${report.withContractFile}`);
console.log(`Missing folder: ${report.missingFolder.length}`);
console.log(`In folder, no contract: ${report.inFolderNoContract.length}`);
console.log(`Report: ${outBase}.txt`);
