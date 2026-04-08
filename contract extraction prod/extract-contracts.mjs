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
        'Nombre completo del huésped (persona física), típicamente en página 1 tras "De otra parte," o en el segundo párrafo. NUNCA la empresa arrendadora (p. ej. CHAMARI ITG, S.L. ni CHAMARI). Si no hay nombre de persona, null.',
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
        'Check-in YYYY-MM-DD: tras "Fecha de entrada:" dentro de "e) Duración de la estancia:" (y ANEXO 1 SECCIÓN E si aplica). No null si esa etiqueta tiene fecha.',
    },
    check_out_date: {
      type: 'string',
      nullable: true,
      description:
        'Check-out YYYY-MM-DD: tras "Fecha de salida:" dentro de "e) Duración de la estancia:". No null si esa etiqueta tiene fecha.',
    },
    rent_section_f_pages_17_18: {
      type: 'string',
      description:
        'Precio/canon: tras "f) Precio:" y/o SECCIÓN F (p. 17–18). Solo dígitos y punto decimal. Evita "unknown" si hay cifra bajo "f) Precio:" o §F.',
    },
    deposit_section_h_pages_18_19: {
      type: 'string',
      description:
        'Cifra de fianza/depósito en € si consta bajo "i) Depósito:", "Forma de pago:", o SECCIÓN H. Solo dígitos y punto decimal, o "unknown" solo si no hay cifra en ningún sitio tras buscar esas etiquetas.',
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
 * Company / landlord — never use as guest name.
 * @param {string | null | undefined} s
 */
function isChamariName(s) {
  if (s == null || typeof s !== 'string') return false;
  const u = s.toUpperCase().normalize('NFD').replace(/\p{M}/gu, '');
  return u.includes('CHAMARI');
}

/**
 * Prefer document name; else filename-derived name. Rejects CHAMARI / landlord strings.
 * @param {string | null | undefined} fromDoc
 * @param {string} fromFile
 */
function resolveName(fromDoc, fromFile) {
  let a = (fromDoc && String(fromDoc).trim()) || '';
  if (isChamariName(a)) a = '';
  if (a) return a;
  return (fromFile && String(fromFile).trim()) || '';
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
 * @param {string} instructions
 * @param {Buffer} fileBuffer
 * @param {string} mimeType
 * @param {string} fileName
 */
async function parseContractExtractionResponse(client, instructions, fileBuffer, mimeType, fileName) {
  const payload = {
    model: DEFAULT_MODEL,
    temperature: 0,
    max_output_tokens: 1600,
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
  return parsed || {};
}

/**
 * Second pass: same document, stricter anchors when first pass left gaps or CHAMARI as name.
 * @param {string} ncHint
 * @param {string} nameFromFileHint
 */
function buildRefinementInstructions(ncHint, nameFromFileHint) {
  return `REINTENTO OBLIGATORIO — Relee el MISMO documento y devuelve el JSON completo otra vez.

Corrige si hace falta:
- Nombre del huésped: persona física. NUNCA "CHAMARI ITG, S.L.", "CHAMARI", ni la arrendadora; suele estar en página 1 tras "De otra parte," (no el segundo párrafo si allí solo está la empresa).
- Fechas YYYY-MM-DD: bajo "e) Duración de la estancia:" → "Fecha de entrada:" (check-in) y "Fecha de salida:" (check-out). Busca también ANEXO 1 / SECCIÓN E si hace falta.
- Precio/canon: bajo "f) Precio:" y/o SECCIÓN F; no uses "unknown" si hay cifra.
- Depósito/fianza: bajo "i) Depósito:", "Forma de pago:", y SECCIÓN H; cifra o redacción para una/media mensualidad.

Pistas: NC ${ncHint || 'desconocido'}; nombre por archivo (solo si falta en PDF): ${nameFromFileHint || 'N/A'}`;
}

/**
 * @param {object} parsed
 * @param {string} checkIn
 * @param {string} checkOut
 * @param {string} rent
 * @param {string} depositResolved
 */
function shouldRunRefinementPass(parsed, checkIn, checkOut, rent, depositResolved) {
  if (isChamariName(parsed.name_second_paragraph_page1)) return true;
  if (!checkIn || !checkOut) return true;
  if (!rent || rent === 'unknown') return true;
  if (!depositResolved || depositResolved === 'unknown') return true;
  return false;
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

REGLAS CRÍTICAS (prioridad):
- NUNCA uses como nombre de huésped "CHAMARI ITG, S.L.", "CHAMARI", ni variantes: es la empresa arrendadora, no la persona. Si tras "De otra parte," solo aparece la empresa, busca el nombre del huésped (persona física) en el mismo bloque, líneas siguientes, o en el segundo párrafo de la página 1.
- Nombre del huésped: PÁGINA 1, típicamente después de "De otra parte,". Si no hay esa etiqueta, el segundo párrafo de la página 1 (no el nombre del archivo salvo pista abajo).
- Check-in / check-out: en "e) Duración de la estancia:" la fecha de entrada va tras "Fecha de entrada:" y la de salida tras "Fecha de salida:". Formato YYYY-MM-DD. Prioriza también ANEXO 1 SECCIÓN E (~p. 17) si el contrato lo enlaza. No dejes fechas vacías si esas etiquetas tienen valores en el PDF.
- Precio/renta: bajo "f) Precio:" y en SECCIÓN F (páginas ~17–18). Solo cifras y punto decimal, sin €. No devuelvas "unknown" si existe precio bajo "f) Precio:" o §F.
- Depósito/fianza: bajo "i) Depósito:", "Forma de pago:", y SECCIÓN H (p. ~18–19). Busca cifra en €; si solo hay texto (una mensualidad, media mensualidad), rellena deposit_wording_snippet y fianza_rule_from_wording. Evita "unknown" si hay información bajo esas etiquetas.

Prioriza la UBICACIÓN en el PDF (la numeración de página puede coincidir con "página 17 impresa" del anexo; localiza ANEXO 1 y secciones por título "E", "F", "H" y las etiquetas literales anteriores).

1) name_second_paragraph_page1: Huésped persona física; página 1 tras "De otra parte," o segundo párrafo. Nunca CHAMARI/empresa. null solo si no hay nombre de persona.

1b) guest_id_type y guest_id_number: Página 1 o bloque huésped: DNI/NIE → DNI; pasaporte → PASAPORTE. Número exacto. Si no consta, guest_id_type unknown y guest_id_number null.

2) check_in_date y check_out_date: "Fecha de entrada:" y "Fecha de salida:" dentro de "e) Duración de la estancia:" y/o ANEXO 1 SECCIÓN E. YYYY-MM-DD.

3) rent_section_f_pages_17_18: "f) Precio:" y/o SECCIÓN F. Sin €.

4) deposit_section_h_pages_18_19: Cifra explícita bajo "i) Depósito:", "Forma de pago:", o §H.

5) fianza_rule_from_wording y deposit_wording_snippet:
   - UNA mensualidad / un mes de renta / equivalente al canon de una mensualidad → one_month_of_rent
   - MEDIA mensualidad / mitad / 50% → half_month_of_rent
   - Cifra explícita en §H o bajo "i) Depósito:" → explicit_in_section_h y rellena la cifra en deposit_section_h_pages_18_19
   - Si no hay cifra pero sí redacción, unknown solo en el enum si no encaja; igualmente deposit_wording_snippet con cita breve (máx. 240 caracteres).

Pistas (no inventar; solo si coincide el PDF):
- NC: ${ncHint || 'desconocido'}
- Nombre aproximado por archivo (si el PDF no da nombre claro de persona): ${nameFromFileHint || 'N/A'}

Devuelve JSON según el esquema. Agota la búsqueda por "De otra parte,", "e) Duración", "Fecha de entrada/salida", "f) Precio:", "i) Depósito:", "Forma de pago:" y ANEXO/§F/§H antes de null o "unknown". No inventes cifras que no estén en el documento.`;

  let parsed = await parseContractExtractionResponse(client, instructions, fileBuffer, mimeType, fileName);

  let checkIn = (parsed.check_in_date && String(parsed.check_in_date).trim()) || '';
  let checkOut = (parsed.check_out_date && String(parsed.check_out_date).trim()) || '';
  let rent = parsed.rent_section_f_pages_17_18 ?? 'unknown';
  const depositRaw = parsed.deposit_section_h_pages_18_19 ?? 'unknown';
  const rule = parsed.fianza_rule_from_wording ?? 'unknown';
  let wording = (parsed.deposit_wording_snippet && String(parsed.deposit_wording_snippet).trim()) || '';

  let { deposit, source: depositSource } = resolveDeposit(depositRaw, rule, rent, wording);

  if (
    shouldRunRefinementPass(parsed, checkIn, checkOut, rent, deposit) &&
    process.env.EXTRACTION_SKIP_REFINEMENT_PASS !== 'true'
  ) {
    const refined = `${instructions}\n\n${buildRefinementInstructions(ncHint, nameFromFileHint)}`;
    parsed = await parseContractExtractionResponse(client, refined, fileBuffer, mimeType, fileName);
    checkIn = (parsed.check_in_date && String(parsed.check_in_date).trim()) || '';
    checkOut = (parsed.check_out_date && String(parsed.check_out_date).trim()) || '';
    rent = parsed.rent_section_f_pages_17_18 ?? 'unknown';
    const depositRaw2 = parsed.deposit_section_h_pages_18_19 ?? 'unknown';
    const rule2 = parsed.fianza_rule_from_wording ?? 'unknown';
    wording = (parsed.deposit_wording_snippet && String(parsed.deposit_wording_snippet).trim()) || '';
    const resolved2 = resolveDeposit(depositRaw2, rule2, rent, wording);
    deposit = resolved2.deposit;
    depositSource = resolved2.source;
  }

  const name = resolveName(parsed.name_second_paragraph_page1, nameFromFileHint);
  const idTypeRaw = parsed.guest_id_type ?? 'unknown';
  const idTypeDisplay = formatIdTypeForCsv(idTypeRaw);
  const idNumber = (parsed.guest_id_number && String(parsed.guest_id_number).trim()) || '';

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
