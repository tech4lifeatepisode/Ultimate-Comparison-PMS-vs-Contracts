import 'dotenv/config';
import OpenAI from 'openai';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { syncExtractionsToSupabase } from './supabase-output.mjs';
import {
  CONTRACT_FILE_EXTENSIONS,
  DOCX_MIME_TYPE,
  mimeFromContractFileName,
} from './contract-file-types.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_MODEL = process.env.OPENAI_MODEL_DEFAULT || 'gpt-5.2-2025-12-11';
const CONTRACTS_DIR = path.join(__dirname, 'contracts');
const OUTPUT_DIR = path.join(__dirname, 'extracted information saved csv');

const SUPPORTED_EXT = CONTRACT_FILE_EXTENSIONS;

/**
 * NC reference code from filenames like `NC_0450_NAME...pdf` → `0450`
 * @param {string} fileName
 */
function extractNcFromFileName(fileName) {
  const base = path.basename(fileName, path.extname(fileName)).replace(/\.docx$/i, '');
  const m = base.match(/^NC[_\s-]*(\d+)/i);
  return m ? m[1] : '';
}

/**
 * Heuristic full name from filename segment after NC_XXXX_
 * @param {string} fileName
 */
function extractNameFromFileName(fileName) {
  let base = path.basename(fileName, path.extname(fileName));
  base = base.replace(/\.docx$/i, '');
  const m = base.match(/^NC[_\s-]*\d+[_\s-]+(.+)$/i);
  if (!m) return '';
  let rest = m[1].replace(/\.docx$/i, '');
  rest = rest.replace(/docx$/i, '');
  return rest
    .split(/_+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * @param {Buffer} fileBuffer
 * @param {string} mimeType
 * @param {string} filename
 */
function prepareFileInput(fileBuffer, mimeType, filename = 'document') {
  const normalizedMimeType = (mimeType || 'application/octet-stream').toLowerCase();
  const base64 = fileBuffer.toString('base64');
  const normalizedFilename = filename.replace(/[^a-zA-Z0-9._-]/g, '_');

  if (normalizedMimeType === 'application/pdf') {
    return {
      type: 'input_file',
      filename: normalizedFilename.endsWith('.pdf') ? normalizedFilename : `${normalizedFilename}.pdf`,
      file_data: `data:application/pdf;base64,${base64}`,
    };
  }

  if (
    normalizedMimeType === DOCX_MIME_TYPE ||
    normalizedFilename.toLowerCase().endsWith('.docx')
  ) {
    const fn = normalizedFilename.toLowerCase().endsWith('.docx')
      ? normalizedFilename
      : `${normalizedFilename}.docx`;
    return {
      type: 'input_file',
      filename: fn,
      file_data: `data:${DOCX_MIME_TYPE};base64,${base64}`,
    };
  }

  return {
    type: 'input_image',
    detail: 'auto',
    image_url: `data:${normalizedMimeType};base64,${base64}`,
  };
}

const EXTRACTION_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    name_second_paragraph_page1: {
      type: 'string',
      nullable: true,
      description:
        'Nombre completo del huésped/inquilino tal como aparece en el SEGUNDO párrafo de la página 1 del contrato (no usar el filename). Si no hay segundo párrafo claro, null.',
    },
    guest_id_type: {
      type: 'string',
      enum: ['DNI', 'PASAPORTE', 'unknown'],
      description:
        'Tipo de documento del huésped/arrendatario (página 1, datos identificativos o primeras cláusulas). DNI o NIE (documento español) → DNI. Pasaporte → PASAPORTE. Si no consta, unknown.',
    },
    guest_id_number: {
      type: 'string',
      nullable: true,
      description:
        'Número completo: DNI (8 dígitos + letra), NIE (letra inicial X/Y/Z + 7 dígitos + letra), o número de pasaporte tal como en el PDF. Sin inventar. null si no aparece.',
    },
    check_in_date: {
      type: 'string',
      nullable: true,
      description:
        'Fecha de entrada / inicio de estancia (check-in) según ANEXO 1, ~página 17, SECCIÓN E. Solo YYYY-MM-DD. Si no consta, null.',
    },
    check_out_date: {
      type: 'string',
      nullable: true,
      description:
        'Fecha de salida / fin de estancia (check-out) según ANEXO 1, ~página 17, SECCIÓN E. Solo YYYY-MM-DD. Si no consta, null.',
    },
    rent_section_f_pages_17_18: {
      type: 'string',
      description:
        'Renta/canon principal: buscar en páginas 17–18 del PDF, SECCIÓN F. Número sin símbolo de moneda, punto decimal (ej. "1310.00"). Si no consta en §F, "unknown".',
    },
    deposit_section_h_pages_18_19: {
      type: 'string',
      description:
        'Importe en euros de fianza/depósito si aparece cifra explícita en SECCIÓN H (~p. 18–19). Solo dígitos y punto decimal, o "unknown" si no hay cifra clara ahí.',
    },
    fianza_rule_from_wording: {
      type: 'string',
      enum: ['explicit_in_section_h', 'one_month_of_rent', 'half_month_of_rent', 'unknown'],
      description:
        'explicit_in_section_h: hay importe explícito en §H. one_month_of_rent: el contrato dice que la fianza equivale a UNA mensualidad/un mes de renta/canon (sin cifra en §H o además). half_month_of_rent: MEDIA mensualidad, mitad del mes, 50% del canon. unknown: no se deduce.',
    },
    deposit_wording_snippet: {
      type: 'string',
      nullable: true,
      description:
        'Cita breve (máx. 240 caracteres) del párrafo o cláusula donde se define la fianza (§H u otra cláusula de fianza). Incluye la expresión clave (ej. "una mensualidad", "media mensualidad"). null si no hay texto útil.',
    },
  },
  required: [
    'name_second_paragraph_page1',
    'guest_id_type',
    'guest_id_number',
    'check_in_date',
    'check_out_date',
    'rent_section_f_pages_17_18',
    'deposit_section_h_pages_18_19',
    'fianza_rule_from_wording',
    'deposit_wording_snippet',
  ],
};

/**
 * @param {string} cell
 */
function escapeCsvCell(cell) {
  const s = cell == null ? '' : String(cell);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/**
 * Prefer document name; else filename-derived name.
 * @param {string | null | undefined} fromDoc
 * @param {string} fromFile
 */
function resolveName(fromDoc, fromFile) {
  const a = (fromDoc && String(fromDoc).trim()) || '';
  if (a) return a;
  return fromFile || '';
}

/**
 * @param {string} s
 * @returns {number | null}
 */
function parseMoneyAmount(s) {
  if (s == null || s === 'unknown') return null;
  const t = String(s).trim().replace(/\s/g, '').replace(',', '.');
  const n = parseFloat(t);
  return Number.isFinite(n) ? n : null;
}

/**
 * If the model missed the enum but left wording, infer one month vs half month (Spanish).
 * @param {string} wording
 * @returns {'one_month_of_rent' | 'half_month_of_rent' | null}
 */
function inferDepositRuleFromWording(wording) {
  if (!wording || typeof wording !== 'string') return null;
  const w = wording
    .toLowerCase()
    .normalize('NFD')
    .replace(/\p{M}/gu, '');

  if (
    /\bmedia\s+mensualidad\b/.test(w) ||
    /\bmedio\s+mes\b/.test(w) ||
    /\bmitad\s+de\s+(una\s+)?mensualidad\b/.test(w) ||
    /\b50\s*%/.test(w) ||
    /\b50\s*por\s*ciento\b/.test(w) ||
    /equivalente\s+a\s+media\b/.test(w) ||
    /\bmedia\s+de\s+la\s+mensualidad\b/.test(w)
  ) {
    return 'half_month_of_rent';
  }

  if (
    /\buna\s+mensualidad\b/.test(w) ||
    /\bun\s+mes\s+(de\s+)?(renta|canon)\b/.test(w) ||
    /\bequivalente\s+a\s+una\s+mensualidad\b/.test(w) ||
    /\bun\s+mes\s+de\s+renta\b/.test(w) ||
    /\bfianza\s+legal\b.*\bmensualidad\b/.test(w) ||
    /\bequivalente\s+al\s+canon\s+de\s+una\s+mensualidad\b/.test(w)
  ) {
    return 'one_month_of_rent';
  }

  return null;
}

/**
 * Resolves final deposit: explicit € in §H, else computed from rent × rule or inferred from wording.
 * @param {string} depositRaw
 * @param {string} rule
 * @param {string} rentRaw
 * @param {string} wording
 */
function resolveDeposit(depositRaw, rule, rentRaw, wording) {
  const explicit = parseMoneyAmount(depositRaw);
  if (explicit != null) {
    return {
      deposit: explicit.toFixed(2),
      source: 'explicit_section_h',
    };
  }

  const rent = parseMoneyAmount(rentRaw);
  if (rent == null) {
    return { deposit: 'unknown', source: 'unknown' };
  }

  let effectiveRule = rule;
  if (effectiveRule === 'unknown' && wording) {
    const inferred = inferDepositRuleFromWording(wording);
    if (inferred) effectiveRule = inferred;
  }

  if (effectiveRule === 'one_month_of_rent') {
    return { deposit: rent.toFixed(2), source: 'computed_1x_rent' };
  }
  if (effectiveRule === 'half_month_of_rent') {
    return { deposit: (rent / 2).toFixed(2), source: 'computed_half_rent' };
  }

  return { deposit: 'unknown', source: 'unknown' };
}

/**
 * @param {string} t
 */
function formatIdTypeForCsv(t) {
  if (t === 'DNI') return 'D.N.I';
  if (t === 'PASAPORTE') return 'PASAPORTE';
  return '';
}

/**
 * @param {import('openai').OpenAI} client
 * @param {Buffer} fileBuffer
 * @param {string} mimeType
 * @param {string} fileName
 * @param {string} ncHint
 * @param {string} nameFromFileHint
 */
async function extractSpanishRentalContract(client, fileBuffer, mimeType, fileName, ncHint, nameFromFileHint) {
  const instructions = `Eres un extractor para contratos de hospedaje/arrendamiento en español.

Prioriza la UBICACIÓN en el PDF (la numeración de página puede coincidir con "página 17 impresa" del anexo; si el visor muestra otra numeración, localiza ANEXO 1 y las secciones por su TÍTULO "E", "F", "H").

1) name_second_paragraph_page1: Lee la PÁGINA 1 del contrato. El nombre del huésped/arrendatario suele estar en el SEGUNDO párrafo (no el primero). Copia el nombre tal cual. Si no existe un segundo párrafo identificable, devuelve null. (No rellenes con el nombre del archivo.)

1b) guest_id_type y guest_id_number: En la página 1 o bloque de datos del huésped, identifica el documento: DNI/NIE español → guest_id_type DNI; pasaporte extranjero → PASAPORTE. Copia el número exacto (DNI: 12345678A; NIE: X1234567L; pasaporte: alfanumérico según conste). Si no aparece documento, guest_id_type unknown y guest_id_number null.

2) check_in_date y check_out_date: En ANEXO 1, SECCIÓN E (suele estar hacia la página 17 del PDF), extrae por separado la fecha de entrada/inicio de estancia (check-in) y la fecha de salida/fin (check-out). Cada una en formato YYYY-MM-DD. Si no aparece el Anexo 1 o la sección E, null en la que falte.

3) rent_section_f_pages_17_18: En SECCIÓN F (páginas ~17–18), importe de la renta/canon periódico acordado. Solo cifras y punto decimal, sin €. Si no aparece en §F, "unknown".

4) deposit_section_h_pages_18_19: En SECCIÓN H (páginas ~18–19), si hay CIFRA explícita de fianza en euros, repítela (solo número). Si NO hay cifra clara en §H, devuelve "unknown".

5) fianza_rule_from_wording y deposit_wording_snippet: Si no hay cifra explícita en §H (o para documentar la regla), busca en TODO el contrato (prioriza §H y cláusulas de fianza/depósito) la redacción típica:
   - UNA mensualidad / un mes de renta / equivalente al canon de una mensualidad / "fianza legal" equivalente a una renta → fianza_rule_from_wording = one_month_of_rent
   - MEDIA mensualidad / mitad de una mensualidad / 50% del canon / medio mes de renta → half_month_of_rent
   - Si además hay cifra explícita en §H que coincide con esa regla, usa explicit_in_section_h y rellena deposit_section_h_pages_18_19 con el número.
   - Si hay cifra explícita en §H, fianza_rule_from_wording = explicit_in_section_h
   - Si no encuentras ni cifra ni redacción clara: unknown
   En deposit_wording_snippet copia una frase corta literal o casi literal donde conste (máx. 240 caracteres). Si hay redacción de fianza (una/media mensualidad) aunque el modelo dude en el enum, sigue rellenando deposit_wording_snippet: el sistema puede calcular el importe a partir del canon.

Pistas de contexto (no inventar datos; solo ayuda si el PDF coincide):
- Código NC del expediente (referencia): ${ncHint || 'desconocido'}
- Nombre aproximado por nombre de archivo (solo si el PDF no da nombre claro en página 1): ${nameFromFileHint || 'N/A'}

Devuelve JSON estricto según el esquema. Usa null o "unknown" cuando falte información; no adivines cifras.`;

  const payload = {
    model: DEFAULT_MODEL,
    temperature: 0,
    max_output_tokens: 1200,
    input: [
      {
        role: 'user',
        content: [
          { type: 'input_text', text: instructions },
          prepareFileInput(fileBuffer, mimeType, fileName),
        ],
      },
    ],
    text: {
      format: {
        type: 'json_schema',
        name: 'spanish_contract_sections_extraction',
        strict: true,
        schema: EXTRACTION_SCHEMA,
      },
    },
  };

  const response = await client.responses.parse(payload);
  let parsed = response.output_parsed;
  if (!parsed && response.output_text) {
    try {
      parsed = JSON.parse(response.output_text);
    } catch {
      parsed = {};
    }
  }
  parsed = parsed || {};

  const name = resolveName(parsed.name_second_paragraph_page1, nameFromFileHint);
  const idTypeRaw = parsed.guest_id_type ?? 'unknown';
  const idTypeDisplay = formatIdTypeForCsv(idTypeRaw);
  const idNumber = (parsed.guest_id_number && String(parsed.guest_id_number).trim()) || '';

  const checkIn =
    (parsed.check_in_date && String(parsed.check_in_date).trim()) || '';
  const checkOut =
    (parsed.check_out_date && String(parsed.check_out_date).trim()) || '';
  const rent = parsed.rent_section_f_pages_17_18 ?? 'unknown';
  const depositRaw = parsed.deposit_section_h_pages_18_19 ?? 'unknown';
  const rule = parsed.fianza_rule_from_wording ?? 'unknown';
  const wording =
    (parsed.deposit_wording_snippet && String(parsed.deposit_wording_snippet).trim()) || '';

  const { deposit, source: depositSource } = resolveDeposit(depositRaw, rule, rent, wording);

  return {
    nc: ncHint,
    name,
    id_type: idTypeDisplay,
    id_number: idNumber,
    check_in_date: checkIn,
    check_out_date: checkOut,
    rent,
    deposit,
    deposit_wording: wording,
    deposit_source: depositSource,
    model_used: DEFAULT_MODEL,
  };
}

/**
 * @param {import('openai').OpenAI} client
 * @param {{ name: string, buffer: Buffer, storagePath?: string }[]} fileEntries
 */
export async function runExtractionPipeline(client, fileEntries) {
  await fs.mkdir(CONTRACTS_DIR, { recursive: true });
  await fs.mkdir(OUTPUT_DIR, { recursive: true });

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const outPath = path.join(OUTPUT_DIR, `contract_extractions_${timestamp}.csv`);

  const header = [
    'NC',
    'Name',
    'Id type',
    'ID number',
    'check_in_date',
    'check_out_date',
    'Rent',
    'Deposit',
    'deposit_wording',
    'deposit_source',
    'file_name',
    'model_used',
    'error',
  ];

  const rows = [header.join(',')];
  /** @type {object[]} */
  const dataRows = [];

  for (const entry of fileEntries.sort((a, b) => a.name.localeCompare(b.name))) {
    const name = entry.name;
    const buf = entry.buffer;
    const rowFileName = entry.storagePath || name;
    const mime = mimeFromContractFileName(name);
    const nc = extractNcFromFileName(name);
    const nameFromFile = extractNameFromFileName(name);

    try {
      const data = await extractSpanishRentalContract(client, buf, mime, name, nc, nameFromFile);
      rows.push(
        [
          escapeCsvCell(data.nc),
          escapeCsvCell(data.name),
          escapeCsvCell(data.id_type),
          escapeCsvCell(data.id_number),
          escapeCsvCell(data.check_in_date),
          escapeCsvCell(data.check_out_date),
          escapeCsvCell(data.rent),
          escapeCsvCell(data.deposit),
          escapeCsvCell(data.deposit_wording),
          escapeCsvCell(data.deposit_source),
          escapeCsvCell(rowFileName),
          escapeCsvCell(data.model_used),
          escapeCsvCell(''),
        ].join(','),
      );
      dataRows.push({
        nc: data.nc,
        name: data.name,
        id_type: data.id_type,
        id_number: data.id_number,
        check_in_date: data.check_in_date,
        check_out_date: data.check_out_date,
        rent: data.rent,
        deposit: data.deposit,
        deposit_wording: data.deposit_wording,
        deposit_source: data.deposit_source,
        file_name: rowFileName,
        model_used: data.model_used,
        error: '',
      });
      console.log(`OK: ${rowFileName}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      rows.push(
        [
          escapeCsvCell(nc),
          escapeCsvCell(nameFromFile),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(rowFileName),
          escapeCsvCell(DEFAULT_MODEL),
          escapeCsvCell(msg),
        ].join(','),
      );
      dataRows.push({
        nc,
        name: nameFromFile,
        id_type: '',
        id_number: '',
        check_in_date: '',
        check_out_date: '',
        rent: '',
        deposit: '',
        deposit_wording: '',
        deposit_source: '',
        file_name: rowFileName,
        model_used: DEFAULT_MODEL,
        error: msg,
      });
      console.error(`FAIL: ${rowFileName} — ${msg}`);
    }
  }

  await fs.writeFile(outPath, rows.join('\r\n') + '\r\n', 'utf8');
  console.log(`\nWrote: ${outPath}`);

  try {
    await syncExtractionsToSupabase({ csvPath: outPath, dataRows });
  } catch (syncErr) {
    console.error(
      'Supabase sync failed (CSV still saved locally):',
      syncErr instanceof Error ? syncErr.message : syncErr,
    );
  }
}

async function main() {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    console.error('Missing OPENAI_API_KEY. Copy .env.example to .env and set your key.');
    process.exit(1);
  }

  const client = new OpenAI({ apiKey });

  const dirEntries = await fs.readdir(CONTRACTS_DIR, { withFileTypes: true });
  const files = dirEntries
    .filter((e) => e.isFile() && SUPPORTED_EXT.has(path.extname(e.name).toLowerCase()))
    .map((e) => e.name);

  if (files.length === 0) {
    console.log(
      `No contract files found in "${path.basename(CONTRACTS_DIR)}". Add .pdf or image files and run again.`,
    );
    process.exit(0);
  }

  /** @type {{ name: string, buffer: Buffer }[]} */
  const fileEntries = [];
  for (const name of files) {
    const full = path.join(CONTRACTS_DIR, name);
    const buf = await fs.readFile(full);
    fileEntries.push({ name, buffer: buf });
  }

  await runExtractionPipeline(client, fileEntries);
}

const isExtractContractsCli =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isExtractContractsCli) {
  main().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
