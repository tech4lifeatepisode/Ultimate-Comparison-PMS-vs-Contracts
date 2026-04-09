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
    is_constancia_deposito_reserva: {
      type: 'boolean',
      description:
        'true si el título del documento es (o equivale a) CONSTANCIA DE DEPÓSITO - RESERVA DE ALOJAMIENTO TEMPORAL.',
    },
    contract_title_header: {
      type: 'string',
      nullable: true,
      description:
        'Encabezado / título principal del contrato (primera página o bloque de título). null si no hay título claro.',
    },
    tenant_name_after_de_otra_parte: {
      type: 'string',
      nullable: true,
      description:
        'Nombre completo del arrendatario inmediatamente DESPUÉS de "De otra parte,". null si no consta.',
    },
    guest_id_type: {
      type: 'string',
      enum: ['DNI', 'PASAPORTE', 'ID', 'unknown'],
      description:
        'Tras el nombre: DNI/NIE → DNI; Pasaporte → PASAPORTE; otro documento genérico → ID; si no consta, unknown.',
    },
    guest_id_number: {
      type: 'string',
      nullable: true,
      description: 'Número del documento tras el tipo. null si no aparece.',
    },
    number_of_people: {
      type: 'integer',
      description:
        'Valor tras "Nº de personas:" / "- Nº de personas:". Si no se menciona en el documento, 1.',
    },
    unit_type: {
      type: 'string',
      nullable: true,
      description: 'Texto tras "Tipo de unidad alojativa:". null si no consta.',
    },
    check_in_date: {
      type: 'string',
      nullable: true,
      description:
        'Check-in YYYY-MM-DD tras "Fecha de entrada:" en "Duración de la estancia:" o ANEXO 1 SECCIÓN E.',
    },
    check_out_date: {
      type: 'string',
      nullable: true,
      description:
        'Check-out YYYY-MM-DD tras "Fecha de salida:" en "Duración de la estancia:" o SECCIÓN E. Si is_constancia_deposito_reserva, null.',
    },
    base_rent_from_precio: {
      type: 'string',
      description:
        'Renta base bajo "Precio:" (solo dígitos y punto, ej. "1015.00"). Si aparece "Considerando 100 euros de suplemento por segunda persona." y el importe mostrado incluye esos 100 €, RESTAR 100 aquí (ej. 1115 → 1015). Si no hay precio claro, "unknown".',
    },
    second_person_supplement_euros: {
      type: 'string',
      nullable: true,
      description:
        'Si aparece la frase del suplemento por segunda persona (100 €), "100"; si no, null.',
    },
    second_person_supplement_label: {
      type: 'string',
      nullable: true,
      description:
        'Si aplica, "suplemento por segunda persona"; si no, null.',
    },
    discount_type: {
      type: 'string',
      nullable: true,
      description:
        'En "Descuento Aplicado en el Precio:" el tipo (ej. "10%"). null si no hay.',
    },
    price_after_discount_euros: {
      type: 'string',
      nullable: true,
      description:
        'Precio mensual TRAS el descuento: el importe en euros (solo número) que aparece tras la frase "por lo que el importe a abonar mensualmente será de" (puede haber salto de línea antes del número). null si no hay descuento o no consta esa cifra.',
    },
    additional_services: {
      type: 'array',
      description:
        'Solo servicios bajo la línea "Pensión:" (meal plan u opciones de pensión). NO incluir suplemento segunda persona aquí. Array vacío si no hay Pensión.',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          service_name: {
            type: 'string',
            description: 'Texto de la opción Pensión tal como en el PDF (ej. "Pensión: low Meal Plan").',
          },
          monthly_price_text: {
            type: 'string',
            description: 'Precio tal como en el PDF (ej. "155€/mes").',
          },
        },
        required: ['service_name', 'monthly_price_text'],
      },
    },
    rent_section_f_pages_17_18: {
      type: 'string',
      description:
        'Respaldo: canon en SECCIÓN F (~p. 17–18) si existe; solo número o "unknown".',
    },
    deposit_section_h_pages_18_19: {
      type: 'string',
      description:
        'Cifra explícita de fianza en SECCIÓN H (~p. 18–19) o "unknown".',
    },
    fianza_rule_from_wording: {
      type: 'string',
      enum: ['explicit_in_section_h', 'one_month_of_rent', 'half_month_of_rent', 'unknown'],
      description:
        'explicit_in_section_h, one_month_of_rent, half_month_of_rent, o unknown.',
    },
    deposit_wording_snippet: {
      type: 'string',
      nullable: true,
      description:
        'Cita breve (máx. 240 caracteres) de la cláusula de fianza/depósito. null si no hay.',
    },
  },
  required: [
    'is_constancia_deposito_reserva',
    'contract_title_header',
    'tenant_name_after_de_otra_parte',
    'guest_id_type',
    'guest_id_number',
    'number_of_people',
    'unit_type',
    'check_in_date',
    'check_out_date',
    'base_rent_from_precio',
    'second_person_supplement_euros',
    'second_person_supplement_label',
    'discount_type',
    'price_after_discount_euros',
    'additional_services',
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
 * Parse amounts like "155€/mes", "1.115,00 €"
 * @param {string | null | undefined} s
 * @returns {number | null}
 */
function parseMoneyFlexible(s) {
  if (s == null || s === '' || s === 'unknown') return null;
  const direct = parseMoneyAmount(s);
  if (direct != null) return direct;
  const cleaned = String(s)
    .replace(/€/g, '')
    .replace(/\/mes/gi, '')
    .replace(/\s/g, '')
    .trim();
  const fromDigit = cleaned.slice(Math.max(0, cleaned.search(/\d/)));
  const eu = fromDigit.match(/^(\d{1,3}(?:\.\d{3})*),(\d{1,2})$/);
  if (eu) {
    const n = parseFloat(eu[1].replace(/\./g, '') + '.' + eu[2]);
    return Number.isFinite(n) ? n : null;
  }
  const t = fromDigit.replace(/\./g, '').replace(',', '.');
  const m = t.match(/-?\d+(?:\.\d+)?/);
  if (!m) return null;
  const n = parseFloat(m[0]);
  return Number.isFinite(n) ? n : null;
}

/**
 * Final monthly rent = (price after discount if present, else base) + segunda persona + Pensión extras.
 * Equivalent to base − discount_amount + extras when price_after = base − discount_amount.
 * @param {number | null} base
 * @param {number | null} priceAfterDiscount
 * @param {number | null} secondPerson
 * @param {number[]} extras
 */
function computeFinalRentPrice(base, priceAfterDiscount, secondPerson, extras) {
  const sp = secondPerson ?? 0;
  const ex = (extras || []).reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0);
  const core = priceAfterDiscount != null ? priceAfterDiscount : base;
  if (core == null) return null;
  return core + sp + ex;
}

/**
 * @param {number | null} base
 * @param {number | null} priceAfterDiscount
 */
function computeDiscountAmountFromBaseAndPriceAfter(base, priceAfterDiscount) {
  if (base == null || priceAfterDiscount == null) return null;
  const d = base - priceAfterDiscount;
  return Number.isFinite(d) ? d : null;
}

/**
 * Extra CSV columns only: suplemento segunda persona + líneas Pensión (meal plan).
 * @param {string | null | undefined} serviceName
 */
function isPensionExtraName(serviceName) {
  if (!serviceName || typeof serviceName !== 'string') return false;
  const n = serviceName
    .normalize('NFD')
    .replace(/\p{M}/gu, '')
    .toLowerCase();
  return /\bpension\b/.test(n) || /\bmeal\s*plan\b/.test(n);
}

/**
 * Build extra_name / extra_price and numeric pension amounts for final rent.
 * @param {object} parsed
 */
function buildExtrasForCsvAndTotals(parsed) {
  const names = [];
  const prices = [];
  const pensionAmounts = [];

  const label = parsed.second_person_supplement_label && String(parsed.second_person_supplement_label).trim();
  const spEuros = parseMoneyAmount(parsed.second_person_supplement_euros);
  if (label && spEuros != null) {
    names.push(label);
    prices.push(String(spEuros));
  } else if (spEuros != null && spEuros > 0) {
    names.push('suplemento por segunda persona');
    prices.push(String(spEuros));
  }

  const services = Array.isArray(parsed.additional_services) ? parsed.additional_services : [];
  for (const row of services) {
    const sn = row && row.service_name != null ? String(row.service_name) : '';
    if (!sn || !isPensionExtraName(sn)) continue;
    const amt = parseMoneyFlexible(row.monthly_price_text);
    names.push(sn.trim());
    prices.push(amt != null ? String(amt) : (row.monthly_price_text != null ? String(row.monthly_price_text) : ''));
    if (amt != null) pensionAmounts.push(amt);
  }

  return {
    extra_name: names.length ? names.join('; ') : '',
    extra_price: prices.length ? prices.join('; ') : '',
    pensionAmounts,
  };
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
 * Amount after "por un importe de" in depósito / fianza wording (Spanish formats).
 * @param {string | null | undefined} wording
 * @returns {number | null}
 */
function parseExplicitDepositImporteDe(wording) {
  if (!wording || typeof wording !== 'string') return null;
  const m = wording.match(
    /por\s+un\s+importe\s+de\s*([\d]{1,3}(?:\.\d{3})*(?:,\d{1,2})?|[\d]+(?:[.,]\d{1,2})?)\s*€?/i,
  );
  if (!m) return null;
  return parseMoneyFlexible(m[1]);
}

/**
 * Exact standard clauses: compute deposit from base rent only (not final).
 * @param {string | null | undefined} wording
 */
function matchesUnaMensualidadFirmaWording(wording) {
  if (!wording || typeof wording !== 'string') return false;
  const n = wording
    .normalize('NFD')
    .replace(/\p{M}/gu, '')
    .toLowerCase()
    .replace(/\s+/g, ' ');
  return (
    n.includes('equivalente a una mensualidad') &&
    n.includes('se abonara en su totalidad en la fecha de la firma del acuerdo')
  );
}

/**
 * @param {string | null | undefined} wording
 */
function matchesMediaMensualidadFirmaWording(wording) {
  if (!wording || typeof wording !== 'string') return false;
  const n = wording
    .normalize('NFD')
    .replace(/\p{M}/gu, '')
    .toLowerCase()
    .replace(/\s+/g, ' ');
  return (
    n.includes('equivalente a media mensualidad') &&
    n.includes('se abonara en su totalidad en la fecha de la firma del acuerdo')
  );
}

/**
 * Resolves deposit: explicit §H; else "por un importe de" in wording; else standard una/media clauses use base rent;
 * else inferred rule × base rent (not final rent).
 * @param {string} depositRaw
 * @param {string} rule
 * @param {string} baseRentRaw
 * @param {string} wording
 */
function resolveDeposit(depositRaw, rule, baseRentRaw, wording) {
  const explicit = parseMoneyAmount(depositRaw);
  if (explicit != null) {
    return {
      deposit: explicit.toFixed(2),
      source: 'explicit_section_h',
    };
  }

  const fromImporteDe = parseExplicitDepositImporteDe(wording);
  if (fromImporteDe != null) {
    return {
      deposit: fromImporteDe.toFixed(2),
      source: 'explicit_from_section',
    };
  }

  const base = parseMoneyAmount(baseRentRaw);
  if (base == null) {
    return { deposit: 'unknown', source: 'unknown' };
  }

  if (wording && matchesUnaMensualidadFirmaWording(wording)) {
    return { deposit: base.toFixed(2), source: 'computed_1x_base_rent' };
  }
  if (wording && matchesMediaMensualidadFirmaWording(wording)) {
    return { deposit: (base / 2).toFixed(2), source: 'computed_half_base_rent' };
  }

  let effectiveRule = rule;
  if (effectiveRule === 'unknown' && wording) {
    const inferred = inferDepositRuleFromWording(wording);
    if (inferred) effectiveRule = inferred;
  }

  if (effectiveRule === 'one_month_of_rent') {
    return { deposit: base.toFixed(2), source: 'computed_1x_rent' };
  }
  if (effectiveRule === 'half_month_of_rent') {
    return { deposit: (base / 2).toFixed(2), source: 'computed_half_rent' };
  }

  return { deposit: 'unknown', source: 'unknown' };
}

/**
 * @param {string} t
 */
function formatIdTypeForCsv(t) {
  if (t === 'DNI') return 'D.N.I';
  if (t === 'PASAPORTE') return 'PASAPORTE';
  if (t === 'ID') return 'ID';
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

Contexto del sistema (no inventar datos; usar solo si coincide con el PDF):
- Nombre del archivo: ${fileName}
- Código NC extraído del nombre de archivo: ${ncHint || 'desconocido'}
- Nombre aproximado por archivo (solo respaldo si el PDF no da nombre): ${nameFromFileHint || 'N/A'}

Localiza secciones por títulos ("Duración de la estancia", ANEXO 1, SECCIÓN E / F / H); la numeración de página del visor puede diferir.

1) is_constancia_deposito_reserva y contract_title_header: Lee el título/encabezado principal. Si es (o equivale a) "CONSTANCIA DE DEPÓSITO - RESERVA DE ALOJAMIENTO TEMPORAL", is_constancia_deposito_reserva = true; entonces check_out_date = null; extrae fecha de entrada y depósito si constan; deja en null lo que no exista.

2) tenant_name_after_de_otra_parte: Nombre completo del arrendatario inmediatamente DESPUÉS de "De otra parte," (o variantes). Si no consta, null.

3) guest_id_type y guest_id_number: Tras el nombre, tipo de documento (DNI/NIE → DNI; pasaporte → PASAPORTE; otro ID → ID) y número exacto. Si no hay documento, unknown y null.

4) number_of_people: Valor tras "Nº de personas:" o "- Nº de personas:". Si no se menciona en el documento, devuelve 1.

5) unit_type: Texto tras "Tipo de unidad alojativa:". null si no consta.

6) check_in_date / check_out_date: Tras "Fecha de entrada:" y "Fecha de salida:" dentro de "Duración de la estancia:" (también puedes usar ANEXO 1 SECCIÓN E). Formato YYYY-MM-DD. Si is_constancia_deposito_reserva, check_out_date = null.

7) base_rent_from_precio: Importe principal bajo "Precio:" (solo número con punto decimal, ej. "1015.00"). Si aparece "Considerando 100 euros de suplemento por segunda persona." y el precio mostrado incluye esos 100 €, RESTA 100 del importe mostrado y pon el resultado en base_rent_from_precio (ej. 1115 → 1015). second_person_supplement_euros = "100" y second_person_supplement_label = "suplemento por segunda persona" en ese caso; si no hay esa frase, null en esos campos.

8) Descuento: Si existe "Descuento Aplicado en el Precio:", discount_type = porcentaje o tipo (ej. "10%"). price_after_discount_euros = el importe mensual en euros que aparece tras la frase "por lo que el importe a abonar mensualmente será de" (puede haber salto de línea; solo el número, ej. "837.00"). El sistema calculará el descuento en € como precio base menos ese importe. Si no hay descuento o no consta esa cifra, null.

9) additional_services: SOLO líneas relacionadas con "Pensión:" (meal plan / opciones de pensión). NO incluir el suplemento por segunda persona aquí. Array vacío si no hay Pensión.

10) rent_section_f_pages_17_18: Respaldo — canon en SECCIÓN F (~p. 17–18) si existe.

11) deposit_section_h_pages_18_19, fianza_rule_from_wording, deposit_wording_snippet: En SECCIÓN H (~p. 18–19), cifra explícita de fianza si la hay; si no hay cifra clara, "unknown". Reglas: explicit_in_section_h si hay cifra en §H; one_month_of_rent / half_month_of_rent según redacción (una mensualidad vs media/50%); deposit_wording_snippet: cita breve (máx. 240 caracteres) de la cláusula de fianza.

Devuelve JSON estricto según el esquema. Usa null o "unknown" cuando falte información; no adivines cifras.`;

  const payload = {
    model: DEFAULT_MODEL,
    temperature: 0,
    max_output_tokens: 3000,
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

  const name = resolveName(parsed.tenant_name_after_de_otra_parte, nameFromFileHint);
  const contractTitle =
    (parsed.contract_title_header && String(parsed.contract_title_header).trim()) || '';
  const idTypeRaw = parsed.guest_id_type ?? 'unknown';
  const idTypeDisplay = formatIdTypeForCsv(idTypeRaw);
  const idNumber = (parsed.guest_id_number && String(parsed.guest_id_number).trim()) || '';
  const numberOfPeople = Math.max(1, Number(parsed.number_of_people) || 1);
  const unitType = (parsed.unit_type && String(parsed.unit_type).trim()) || '';

  let checkIn = (parsed.check_in_date && String(parsed.check_in_date).trim()) || '';
  let checkOut = (parsed.check_out_date && String(parsed.check_out_date).trim()) || '';
  if (parsed.is_constancia_deposito_reserva) {
    checkOut = '';
  }

  const baseRentRaw = parsed.base_rent_from_precio ?? 'unknown';
  const base = parseMoneyAmount(baseRentRaw);
  const secondPerson = parseMoneyAmount(parsed.second_person_supplement_euros);
  const priceAfterDiscount = parseMoneyAmount(parsed.price_after_discount_euros);
  const { extra_name, extra_price, pensionAmounts } = buildExtrasForCsvAndTotals(parsed);

  const finalRent = computeFinalRentPrice(base, priceAfterDiscount, secondPerson, pensionAmounts);
  const finalRentStr = finalRent != null ? finalRent.toFixed(2) : 'unknown';

  const discountAmount = computeDiscountAmountFromBaseAndPriceAfter(base, priceAfterDiscount);
  const discountPriceStr =
    discountAmount != null ? discountAmount.toFixed(2) : '';
  const priceAfterDiscountStr =
    priceAfterDiscount != null
      ? priceAfterDiscount.toFixed(2)
      : parsed.price_after_discount_euros != null && String(parsed.price_after_discount_euros).trim()
        ? String(parsed.price_after_discount_euros).trim()
        : '';

  const baseRentForDeposit =
    base != null ? baseRentRaw : (parsed.rent_section_f_pages_17_18 ?? 'unknown');

  const depositRaw = parsed.deposit_section_h_pages_18_19 ?? 'unknown';
  const rule = parsed.fianza_rule_from_wording ?? 'unknown';
  const wording =
    (parsed.deposit_wording_snippet && String(parsed.deposit_wording_snippet).trim()) || '';

  const { deposit, source: depositSource } = resolveDeposit(depositRaw, rule, baseRentForDeposit, wording);

  const discountType =
    parsed.discount_type != null && String(parsed.discount_type).trim()
      ? String(parsed.discount_type).trim()
      : '';

  return {
    nc: ncHint,
    contract_title: contractTitle,
    name,
    id_type: idTypeDisplay,
    id_number: idNumber,
    number_of_people: String(numberOfPeople),
    unit_type: unitType,
    check_in_date: checkIn,
    check_out_date: checkOut,
    base_rent: baseRentRaw,
    extra_name,
    extra_price,
    discount_type: discountType,
    price_after_discount: priceAfterDiscountStr,
    discount_price: discountPriceStr,
    final_rent: finalRentStr,
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
    'contract_title',
    'Name',
    'Id type',
    'ID number',
    'number_of_people',
    'unit_type',
    'check_in_date',
    'check_out_date',
    'base_rent',
    'extra_name',
    'extra_price',
    'discount_type',
    'price_after_discount',
    'discount_price',
    'final_rent',
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
          escapeCsvCell(data.contract_title),
          escapeCsvCell(data.name),
          escapeCsvCell(data.id_type),
          escapeCsvCell(data.id_number),
          escapeCsvCell(data.number_of_people),
          escapeCsvCell(data.unit_type),
          escapeCsvCell(data.check_in_date),
          escapeCsvCell(data.check_out_date),
          escapeCsvCell(data.base_rent),
          escapeCsvCell(data.extra_name),
          escapeCsvCell(data.extra_price),
          escapeCsvCell(data.discount_type),
          escapeCsvCell(data.price_after_discount),
          escapeCsvCell(data.discount_price),
          escapeCsvCell(data.final_rent),
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
        contract_title: data.contract_title,
        name: data.name,
        id_type: data.id_type,
        id_number: data.id_number,
        number_of_people: data.number_of_people,
        unit_type: data.unit_type,
        check_in_date: data.check_in_date,
        check_out_date: data.check_out_date,
        base_rent: data.base_rent,
        extra_name: data.extra_name,
        extra_price: data.extra_price,
        discount_type: data.discount_type,
        price_after_discount: data.price_after_discount,
        discount_price: data.discount_price,
        final_rent: data.final_rent,
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
          escapeCsvCell(''),
          escapeCsvCell(nameFromFile),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
          escapeCsvCell(''),
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
        contract_title: '',
        name: nameFromFile,
        id_type: '',
        id_number: '',
        number_of_people: '',
        unit_type: '',
        check_in_date: '',
        check_out_date: '',
        base_rent: '',
        extra_name: '',
        extra_price: '',
        discount_type: '',
        price_after_discount: '',
        discount_price: '',
        final_rent: '',
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
      `No contract files found in "${path.basename(CONTRACTS_DIR)}". Add .pdf, .docx, or image files and run again.`,
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
