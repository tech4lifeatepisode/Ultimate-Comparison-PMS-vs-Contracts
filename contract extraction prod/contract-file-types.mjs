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
 * @param {string} fileName
 */
export function mimeFromContractFileName(fileName) {
  const ext = path.extname(fileName).toLowerCase();
  if (ext === '.pdf') return 'application/pdf';
  if (ext === '.docx') return DOCX_MIME_TYPE;
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  return 'application/octet-stream';
}
