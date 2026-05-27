/**
 * Source → destination folder pairs inside SUPABASE_STORAGE_BUCKET (e.g. Contracts).
 * Override with BLINDING_FOLDER_PAIRS_JSON env var if needed.
 */

export const DEFAULT_BLINDING_FOLDER_PAIRS = [
  { source: 'To Fill 1', destination: 'Fill 1 Blinded' },
  { source: 'To Fill 2', destination: 'Fill 2 Blinded' },
  { source: 'Fill 3 NC_1250-NC_1470', destination: 'Fill 3 NC_1250-NC_1470 Blinded' },
];

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
