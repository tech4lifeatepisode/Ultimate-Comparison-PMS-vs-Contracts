/**
 * Source → destination folder pairs inside SUPABASE_STORAGE_BUCKET (e.g. Contracts).
 * Override with BLINDING_FOLDER_PAIRS_JSON env var if needed.
 */
import path from 'path';

export const DEFAULT_BLINDING_FOLDER_PAIRS = [
  { source: 'To Fill 1', destination: 'Fill 1 Blinded' },
  { source: 'To Fill 2', destination: 'Fill 2 Blinded' },
  { source: 'Fill 3 NC_1250-NC_1470', destination: 'Fill 3 NC_1250-NC_1470 Blinded' },
];

/** Destination for blinded office / non-PDF contracts (Word, OpenDocument). */
export const OFFICE_BLINDING_DESTINATION = 'Blinded Missing';

/**
 * @returns {string}
 */
export function getOfficeBlindingDestination() {
  const raw = process.env.OFFICE_BLINDING_DESTINATION;
  if (raw != null && String(raw).trim() !== '') return normalizeFolder(raw);
  return OFFICE_BLINDING_DESTINATION;
}

/**
 * Source folders scanned for office documents (same sources as PDF blinding).
 * @returns {string[]}
 */
export function getOfficeBlindingSources() {
  return getBlindingFolderPairs().map((p) => p.source);
}

/**
 * Map each source folder to its PDF blinding destination (legacy office output locations).
 * @returns {Map<string, string>}
 */
export function getLegacyOfficeOutputDestinationsBySource() {
  const map = new Map();
  for (const { source, destination } of getBlindingFolderPairs()) {
    map.set(source, destination);
  }
  return map;
}

/**
 * @returns {{ source: string, destination: string }[]}
 */
export function getBlindingFolderPairs() {
  const raw = process.env.BLINDING_FOLDER_PAIRS_JSON;
  if (raw?.trim()) {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length === 0) {
      throw new Error('BLINDING_FOLDER_PAIRS_JSON must be a non-empty JSON array.');
    }
    for (const pair of parsed) {
      if (!pair?.source || !pair?.destination) {
        throw new Error('Each BLINDING_FOLDER_PAIRS_JSON entry needs source and destination.');
      }
    }
    return parsed.map((p) => ({
      source: normalizeFolder(p.source),
      destination: normalizeFolder(p.destination),
    }));
  }
  return DEFAULT_BLINDING_FOLDER_PAIRS;
}

/**
 * @param {string} folder
 */
export function normalizeFolder(folder) {
  return String(folder).replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
}

/**
 * @param {string} sourceObjectPath
 * @param {string} sourceFolder
 * @param {string} destFolder
 */
export function outputPathForSource(sourceObjectPath, sourceFolder, destFolder) {
  const base = sourceObjectPath.slice(sourceFolder.length).replace(/^\/+/, '');
  const fileName = base.includes('/') ? base.split('/').pop() : base;
  return `${destFolder}/${fileName}`;
}

/**
 * Blinded output path for office sources: same basename with .pdf extension.
 * @param {string} sourceObjectPath
 * @param {string} sourceFolder
 * @param {string} destFolder
 */
export function outputPdfPathForOfficeSource(sourceObjectPath, sourceFolder, destFolder) {
  const fileName = path.basename(sourceObjectPath);
  const ext = path.extname(fileName);
  const pdfName = ext ? fileName.slice(0, -ext.length) + '.pdf' : `${fileName}.pdf`;
  return `${destFolder}/${pdfName}`;
}

/**
 * Legacy + current possible blinded output paths for an office source file.
 * @param {string} sourceObjectPath
 * @param {string} sourceFolder
 */
export function possibleOfficeBlindedOutputPaths(sourceObjectPath, sourceFolder) {
  const destFolder = getOfficeBlindingDestination();
  /** @type {string[]} */
  const paths = [outputPdfPathForOfficeSource(sourceObjectPath, sourceFolder, destFolder)];

  const legacyDest = getLegacyOfficeOutputDestinationsBySource().get(sourceFolder);
  if (legacyDest) {
    paths.push(...possibleBlindedOutputPaths(sourceObjectPath, sourceFolder, legacyDest));
  }

  return [...new Set(paths)];
}

/**
 * Possible blinded output paths for a source file (for skip-if-exists checks).
 * @param {string} sourceObjectPath
 * @param {string} sourceFolder
 * @param {string} destFolder
 */
export function possibleBlindedOutputPaths(sourceObjectPath, sourceFolder, destFolder) {
  const sameName = outputPathForSource(sourceObjectPath, sourceFolder, destFolder);
  const ext = path.extname(sameName).toLowerCase();
  /** @type {string[]} */
  const paths = [sameName];
  if (ext && ext !== '.pdf') {
    paths.push(sameName.slice(0, -ext.length) + '.pdf');
    paths.push(`${sameName}.pdf`);
  }
  return [...new Set(paths)];
}
