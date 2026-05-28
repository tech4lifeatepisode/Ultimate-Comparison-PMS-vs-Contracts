/**
 * Audit target NC list against contract_extractions and contract_extractions_ocr_rerun.
 */
import dotenv from '../Contract Extraction/contract extraction prod/node_modules/dotenv/lib/main.js';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '../Contract Extraction/contract extraction prod/node_modules/@supabase/supabase-js/dist/index.mjs';
import { normalizeNcNumber } from '../Contract Extraction/contract extraction prod/extraction-config.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

dotenv.config({ path: path.resolve(__dirname, '../Contract Extraction/contract extraction prod/.env') });

export const TARGET_NCS = [
  '3', '4', '15', '17', '18', '26', '39', '41', '158', '176', '362', '440', '570',
  '702', '703', '750', '795', '829', '857', '876', '890', '953', '954', '1070',
  '1104', '1108', '1127', '1141', '1214', '1222', '1248', '1345',
];

function getSupabaseKey() {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY
  );
}

function rowNc(row) {
  return normalizeNcNumber(row.nc || '') || null;
}

function isGoodRow(row) {
  if (row.error) return false;
  if (!row.name || String(row.name).trim() === '') return false;
  if (String(row.base_rent || '').toLowerCase() === 'unknown') return false;
  if (String(row.final_rent || '').toLowerCase() === 'unknown') return false;
  return true;
}

async function fetchAllRows(tableName) {
  const url = process.env.SUPABASE_URL;
  const key = getSupabaseKey();
  const supabase = createClient(url, key);
  const rows = [];
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from(tableName)
      .select('nc,file_name,name,base_rent,final_rent,error,created_at')
      .order('created_at', { ascending: false })
      .range(from, from + 999);
    if (error) throw new Error(`${tableName}: ${error.message}`);
    if (!data?.length) break;
    rows.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }
  return rows;
}

export async function auditNcExtractionStatus() {
  const main = await fetchAllRows('contract_extractions');
  const rerun = await fetchAllRows('contract_extractions_ocr_rerun');

  /** @type {Map<string, object[]>} */
  const byNcMain = new Map();
  /** @type {Map<string, object[]>} */
  const byNcRerun = new Map();

  for (const row of main) {
    const nc = rowNc(row);
    if (!nc) continue;
    if (!byNcMain.has(nc)) byNcMain.set(nc, []);
    byNcMain.get(nc).push(row);
  }
  for (const row of rerun) {
    const nc = rowNc(row);
    if (!nc) continue;
    if (!byNcRerun.has(nc)) byNcRerun.set(nc, []);
    byNcRerun.get(nc).push(row);
  }

  console.log(`\nNC audit (${TARGET_NCS.length} targets)\n`);
  console.log('NC\tocr_rerun\tmain\tbest_source\tname\tbase_rent\tfinal_rent\tfile');
  console.log('-'.repeat(120));

  let okRerun = 0;
  let okMain = 0;
  let missing = 0;
  let weak = 0;

  for (const nc of TARGET_NCS) {
    const rerunRows = byNcRerun.get(nc) || [];
    const mainRows = byNcMain.get(nc) || [];
    const bestRerun = rerunRows.find(isGoodRow) || rerunRows[0];
    const bestMain = mainRows.find(isGoodRow) || mainRows[0];
    const best = bestRerun && isGoodRow(bestRerun) ? bestRerun : bestMain;

    let status = 'MISSING';
    if (bestRerun && isGoodRow(bestRerun)) {
      status = 'ocr_rerun OK';
      okRerun++;
    } else if (bestMain && isGoodRow(bestMain)) {
      status = 'main only';
      okMain++;
    } else if (bestRerun || bestMain) {
      status = 'WEAK';
      weak++;
    } else {
      missing++;
    }

    const row = bestRerun || bestMain;
    console.log(
      [
        nc.padStart(4),
        String(rerunRows.length).padStart(3),
        String(mainRows.length).padStart(4),
        status.padEnd(12),
        (row?.name || '').slice(0, 28).padEnd(28),
        (row?.base_rent || '-').toString().padEnd(10),
        (row?.final_rent || '-').toString().padEnd(10),
        (row?.file_name || '').slice(0, 50),
      ].join('\t'),
    );
  }

  console.log('\nSummary:');
  console.log(`  ocr_rerun OK: ${okRerun}/${TARGET_NCS.length}`);
  console.log(`  main only (good): ${okMain}`);
  console.log(`  weak/partial rows: ${weak}`);
  console.log(`  missing entirely: ${missing}`);

  return { okRerun, okMain, weak, missing };
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));

if (isDirectRun) {
  auditNcExtractionStatus()
    .then(() => process.exit(0))
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
