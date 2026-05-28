import path from 'path';

/** Extensions we send to the OpenAI Responses API (PDF, Office, images). */
export const CONTRACT_FILE_EXTENSIONS = new Set([
  '.pdf',
  '.docx',
  '.png',
  '.jpg',
  '.jpeg',
  '.webp',
  '.gif',
]);

export const DOCX_MIME_TYPE =
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document';

/**
 * Some Supabase uploads use "_pdf" suffix instead of ".pdf" (e.g. NC_0003_..._pdf).
 * @param {string} name
 */
export function isContractFileName(name) {
  const base = path.basename(name);
  const ext = path.extname(base).toLowerCase();
  if (CONTRACT_FILE_EXTENSIONS.has(ext)) return true;
  if (/_pdf$/i.test(base)) return true;
  return false;
}

/**
 * @param {string} fileName
 */
export function mimeFromContractFileName(fileName) {
  const base = path.basename(fileName);
  const ext = path.extname(base).toLowerCase();
  if (ext === '.pdf' || /_pdf$/i.test(base)) return 'application/pdf';
  if (ext === '.docx') return DOCX_MIME_TYPE;
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  return 'application/octet-stream';
}
