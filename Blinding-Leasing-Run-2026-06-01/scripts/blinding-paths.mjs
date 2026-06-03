/**
 * Local paths for Claudio's laptop (Contract Scanning workspace).
 * Edit BLINDING_ROOT here if you relocate PDFs / Excel inputs.
 */
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const RUN_ROOT = path.resolve(__dirname, '..');
export const SCRIPTS_DIR = __dirname;
export const REPORT_DIR = path.join(RUN_ROOT, 'reports');
export const DATA_DIR = path.join(RUN_ROOT, 'data');

/** Parent workspace */
export const CONTRACT_SCANNING_ROOT = path.resolve(RUN_ROOT, '..');

/** Customer PDFs, Seguimiento, and blinded output folders */
export const BLINDING_ROOT = path.join(CONTRACT_SCANNING_ROOT, 'Blinding');

export const NEWCUSTOMERS_ROOT = path.join(BLINDING_ROOT, 'NewCustomers');
export const SEG_XLSX = path.join(BLINDING_ROOT, 'Seguimiento Leasing Carabanchel (new .LIFE).xlsx');
export const REBLIND_XLSX = path.join(BLINDING_ROOT, 'Re-blind.xlsx');
export const SEG_DIR = path.join(BLINDING_ROOT, '_seg_tmp');
export const XLSX_DIR = path.join(BLINDING_ROOT, '_xlsx_tmp');

export const OUTPUT_FINAL = path.join(BLINDING_ROOT, 'FINAL');
export const OUTPUT_FINAL2 = path.join(BLINDING_ROOT, 'FINAL2');
export const OUTPUT_FINAL_UNDONE = path.join(BLINDING_ROOT, 'Final UNDONE');
export const OUTPUT_LAST_RUN = path.join(BLINDING_ROOT, 'LAST RUN');
export const INPUT_MISSED_78 = path.join(BLINDING_ROOT, 'Missed 78');

/** This run: Contracts Zip + Seguimiento Expired label */
export const CONTRACTS_ZIP_DIR = path.join(RUN_ROOT, 'Contracts Zip');
export const CONTRACTS_EXTRACTED = path.join(RUN_ROOT, 'Contracts Extracted');
export const OUTPUT_BLINDED_EXPIRED = path.join(RUN_ROOT, 'Blinded Expired Contracts');
export const MASTER_CHUNKS_DIR = path.join(DATA_DIR, 'master-chunks');

/** Pendientes batch (local PDF folders) */
export const PENDIENTES_BLINDING_ROOT = path.join(RUN_ROOT, 'Pendientes Blinding');
export const INPUT_PENDIENTES = path.join(PENDIENTES_BLINDING_ROOT, 'Pendientes');
export const OUTPUT_BLINDED_PENDIENTES = path.join(PENDIENTES_BLINDING_ROOT, 'Blinded Pendientes');

export const BLINDED_ARCHIVE_ROOTS = [
  path.join(
    NEWCUSTOMERS_ROOT,
    '02. CUSTOMERS-20260601T113706Z-3-001',
    '02. CUSTOMERS',
    'Blinded Contracts NC_0001 to NC_1470',
  ),
  path.join(
    NEWCUSTOMERS_ROOT,
    '02. CUSTOMERS-20260601T113706Z-3-003',
    '02. CUSTOMERS',
    'Blinded Contracts NC_0001 to NC_1470',
  ),
];

export const DEFAULT_AIRBNB_SAMPLE = path.join(INPUT_MISSED_78, 'AIRBNB_MARCELO_STARLING (1).pdf');

export const MISSING_FINAL2_CSV = path.join(DATA_DIR, 'missing-from-FINAL2-2026-06-01T17-45-32.csv');
export const NOT_FOUND_78_TXT = path.join(DATA_DIR, 'NOT-IN-BLINDED-ARCHIVES-78.txt');
