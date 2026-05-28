import path from 'path';
import { fileURLToPath } from 'url';
import { createWorkerConverter } from '@matbee/libreoffice-converter/server';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** @type {import('@matbee/libreoffice-converter/server').WorkerConverter | null} */
let converter = null;

function getWasmPath() {
  if (process.env.WASM_PATH?.trim()) return process.env.WASM_PATH.trim();
  return path.join(__dirname, 'node_modules/@matbee/libreoffice-converter/wasm');
}

async function getConverter() {
  if (!converter) {
    process.env.WASM_PATH = getWasmPath();
    converter = await createWorkerConverter();
  }
  return converter;
}

/**
 * @param {Buffer} fileBuffer
 * @param {string} fileName
 */
export async function convertOfficeDocumentToPdf(fileBuffer, fileName) {
  const c = await getConverter();
  const result = await c.convert(fileBuffer, { outputFormat: 'pdf' }, fileName);
  return Buffer.from(result.data);
}

export async function destroyDocumentConverter() {
  if (converter) {
    await converter.destroy();
    converter = null;
  }
}
