import { createCanvas, loadImage } from '@napi-rs/canvas';
import { PDFDocument } from 'pdf-lib';
import mupdf from 'mupdf';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';
import { createWorker } from 'tesseract.js';

const LEFT_MARGIN = 85;
const RIGHT_MARGIN = 85;
const LINE_Y_TOLERANCE = 4;
const BOX_PAD_X = 6;
const BOX_PAD_Y = 8;
const RENDER_SCALE = 2;

// Below this many extracted characters a PDF is treated as image-only (scanned)
// and re-read with OCR so we can locate the party / signature blocks by text
// instead of guessing positions.
const TEXT_LAYER_MIN_CHARS = 50;
const OCR_SCALE = 2.5;
const OCR_LANG = 'spa';

/**
 * @typedef {{ str: string, x: number, y: number, width: number, height: number }} TextItem
 * @typedef {{ y: number, text: string, items: TextItem[] }} TextLine
 * @typedef {{ page: number, x: number, y: number, width: number, height: number }} RedactionBox
 */

function itemsToBox(items) {
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;

  for (const item of items) {
    minX = Math.min(minX, item.x);
    minY = Math.min(minY, item.y);
    maxX = Math.max(maxX, item.x + item.width);
    maxY = Math.max(maxY, item.y + item.height);
  }

  return { x: minX, y: minY, width: maxX - minX, height: maxY - minY };
}

function contentWidth(pageWidth) {
  return pageWidth - LEFT_MARGIN - RIGHT_MARGIN;
}

function makeRedactionBox(page, pageWidth, box, widthOverride) {
  const width = widthOverride ?? contentWidth(pageWidth) + BOX_PAD_X * 2;
  return {
    page,
    x: LEFT_MARGIN - BOX_PAD_X,
    y: box.y - BOX_PAD_Y,
    width,
    height: box.height + BOX_PAD_Y * 2,
  };
}

function groupItemsIntoLines(items, yTolerance = LINE_Y_TOLERANCE) {
  const sorted = [...items].sort((a, b) => a.y - b.y || a.x - b.x);
  /** @type {TextLine[]} */
  const lines = [];

  for (const item of sorted) {
    let line = lines.find((l) => Math.abs(l.y - item.y) <= yTolerance);
    if (!line) {
      line = { y: item.y, text: '', items: [] };
      lines.push(line);
    }
    line.items.push(item);
  }

  for (const line of lines) {
    line.items.sort((a, b) => a.x - b.x);
    line.text = line.items.map((i) => i.str).join('');
    line.y = line.items.reduce((sum, i) => sum + i.y, 0) / line.items.length;
  }

  return lines.sort((a, b) => a.y - b.y);
}

async function extractPages(pdfBuffer) {
  const data = new Uint8Array(pdfBuffer);
  const doc = await getDocument({ data, useSystemFonts: true }).promise;
  /** @type {Map<number, { lines: TextLine[], pageWidth: number, pageHeight: number }>} */
  const pages = new Map();

  for (let pageNum = 1; pageNum <= doc.numPages; pageNum++) {
    const page = await doc.getPage(pageNum);
    const viewport = page.getViewport({ scale: 1 });
    const textContent = await page.getTextContent();
    /** @type {TextItem[]} */
    const items = [];

    for (const item of textContent.items) {
      if (!('str' in item) || !item.str) continue;
      const tx = item.transform;
      const x = tx[4];
      const fontSize = Math.hypot(tx[0], tx[1]) || 10;
      const baselineFromTop = viewport.height - tx[5];
      const y = baselineFromTop - fontSize * 0.85;
      const width = item.width || item.str.length * fontSize * 0.5;
      const height = fontSize * 1.15;
      items.push({ str: item.str, x, y, width, height });
    }

    pages.set(pageNum, {
      lines: groupItemsIntoLines(items),
      pageWidth: viewport.width,
      pageHeight: viewport.height,
    });
  }

  return pages;
}

function countExtractedChars(pages) {
  let total = 0;
  for (const pageData of pages.values()) {
    for (const line of pageData.lines) total += line.text.length;
  }
  return total;
}

/** Yield every recognized word (with bbox) from a Tesseract result. */
function* iterateOcrWords(data) {
  for (const block of data.blocks || []) {
    for (const para of block.paragraphs || []) {
      for (const line of para.lines || []) {
        for (const word of line.words || []) yield word;
      }
    }
  }
}

/**
 * Re-read an image-only (scanned) PDF with OCR, producing the same
 * page/line/item structure as {@link extractPages} so the existing text-based
 * detection works unchanged. Word boxes are mapped back to PDF points.
 * @param {Buffer} pdfBuffer
 */
async function extractPagesViaOCR(pdfBuffer) {
  const doc = mupdf.Document.openDocument(pdfBuffer, 'application/pdf');
  const pageCount = doc.countPages();
  const worker = await createWorker(OCR_LANG);

  /** @type {Map<number, { lines: TextLine[], pageWidth: number, pageHeight: number }>} */
  const pages = new Map();

  try {
    for (let i = 0; i < pageCount; i++) {
      const page = doc.loadPage(i);
      const bounds = page.getBounds();
      const pageWidth = bounds[2] - bounds[0];
      const pageHeight = bounds[3] - bounds[1];

      const pixmap = page.toPixmap(mupdf.Matrix.scale(OCR_SCALE, OCR_SCALE), mupdf.ColorSpace.DeviceRGB, false);
      const png = Buffer.from(pixmap.asPNG());
      const { data } = await worker.recognize(png, {}, { blocks: true });

      /** @type {TextItem[]} */
      const items = [];
      for (const word of iterateOcrWords(data)) {
        const str = (word.text || '').trim();
        if (!str || !word.bbox) continue;
        const { x0, y0, x1, y1 } = word.bbox;
        items.push({
          // trailing space so groupItemsIntoLines' join('') reconstructs words
          str: `${str} `,
          x: x0 / OCR_SCALE,
          y: y0 / OCR_SCALE,
          width: (x1 - x0) / OCR_SCALE,
          height: (y1 - y0) / OCR_SCALE,
        });
      }

      pages.set(i + 1, { lines: groupItemsIntoLines(items), pageWidth, pageHeight });
    }
  } finally {
    await worker.terminate();
  }

  return pages;
}

/**
 * Extract page text, transparently falling back to OCR for scanned/image-only
 * PDFs (no usable text layer).
 * @param {Buffer} pdfBuffer
 * @param {string} [fileName]
 */
async function extractPagesAuto(pdfBuffer, fileName = 'document.pdf') {
  const pages = await extractPages(pdfBuffer);
  if (countExtractedChars(pages) >= TEXT_LAYER_MIN_CHARS) {
    return { pages, viaOcr: false };
  }
  console.log(`  ${fileName}: no text layer detected → running OCR (${pages.size} pages, this is slower)...`);
  const ocrPages = await extractPagesViaOCR(pdfBuffer);
  return { pages: ocrPages, viaOcr: true };
}

function mergeFullWidthPage1Boxes(boxes, pageWidth, pageNum = 1) {
  if (boxes.length === 0) return [];

  const full = contentWidth(pageWidth) + BOX_PAD_X * 2;
  /** @type {RedactionBox[]} */
  const partial = [];
  /** @type {RedactionBox[]} */
  const mergeable = [];

  for (const box of boxes) {
    if (box.width < full * 0.9) partial.push(box);
    else mergeable.push(box);
  }

  /** @type {RedactionBox[]} */
  const result = [];

  if (mergeable.length > 0) {
    const minY = Math.min(...mergeable.map((b) => b.y));
    const maxY = Math.max(...mergeable.map((b) => b.y + b.height));
    result.push({
      page: pageNum,
      x: LEFT_MARGIN - BOX_PAD_X,
      y: minY,
      width: full,
      height: maxY - minY,
    });
  }

  result.push(...partial);
  return result.sort((a, b) => a.y - b.y);
}

function adelanteIndex(text) {
  const m = text.toLowerCase().match(/\(\s*en\s*adelante/);
  return m ? m.index ?? -1 : -1;
}

function endsWithSplitEnAdelante(text) {
  return /\(\s*en\s*$/i.test(text.trim());
}

function isAdelanteContinuation(text) {
  return /^adelante,\s*el/i.test(text.trim());
}

/**
 * Marks the end of a party-identification block when there is no explicit
 * "(en adelante, el «Cliente»)" parenthetical (e.g. the ADENDA / termination
 * variants that go straight to "En adelante la Empresa y el Cliente serán
 * referidas..."). Prevents the collector from running past the parties into the
 * body of the document.
 */
function isPartyBlockEnd(text) {
  const t = text.trim();
  return (
    /^y?\s*en\s+adelante\b/i.test(t) ||
    /^las\s+partes\b/i.test(t) ||
    /reconoci[eé]ndonos\s+mutuamente/i.test(t) ||
    /^expone\b/i.test(t) ||
    /^cl[aá]usulas?\b/i.test(t)
  );
}

const MAX_PARTY_BLOCK_LINES = 8;

function partyStartIndex(text) {
  const m = text.match(/de\s*otra\s*parte,?/i);
  if (!m || m.index === undefined) return -1;
  return m.index + m[0].length;
}

/**
 * @param {TextLine} line
 * @param {number} charStart
 * @param {number} page
 * @param {number} pageWidth
 * @param {number} [charEnd]
 */
function boxFromCharRange(line, charStart, page, pageWidth, charEnd = line.text.length) {
  let charPos = 0;
  /** @type {TextItem[]} */
  const partialItems = [];
  for (const item of line.items) {
    const itemStart = charPos;
    const itemEnd = charPos + item.str.length;
    if (itemEnd > charStart && itemStart < charEnd) partialItems.push(item);
    charPos = itemEnd;
  }
  if (!partialItems.length) return null;

  const box = itemsToBox(partialItems);
  const ratio = (charEnd - charStart) / line.text.length;
  const widthOverride =
    charEnd < line.text.length
      ? contentWidth(pageWidth) * ratio + BOX_PAD_X * 3
      : undefined;
  return makeRedactionBox(page, pageWidth, box, widthOverride);
}

function findPartyIdentificationBoxes(lines, pageWidth, pageNum = 1) {
  /** @type {RedactionBox[][]} */
  const blockGroups = [];
  /** @type {RedactionBox[]} */
  let currentBlock = [];
  let collecting = false;

  for (const line of lines) {
    const text = line.text.trim();
    const partyStart = partyStartIndex(line.text);

    if (partyStart >= 0) {
      if (currentBlock.length) blockGroups.push(currentBlock);
      currentBlock = [];
      collecting = true;

      if (partyStart < line.text.length && line.text.slice(partyStart).trim()) {
        const adelanteAt = adelanteIndex(line.text);
        if (adelanteAt > partyStart) {
          const box = boxFromCharRange(line, partyStart, pageNum, pageWidth, adelanteAt);
          if (box) currentBlock.push(box);
          collecting = false;
          blockGroups.push(currentBlock);
          currentBlock = [];
        } else {
          const box = boxFromCharRange(line, partyStart, pageNum, pageWidth);
          if (box) currentBlock.push(box);
        }
      }
      continue;
    }

    if (!collecting) continue;

    if (isAdelanteContinuation(text) || isPartyBlockEnd(text)) {
      collecting = false;
      if (currentBlock.length) {
        blockGroups.push(currentBlock);
        currentBlock = [];
      }
      continue;
    }

    const adelanteAt = adelanteIndex(text);
    if (adelanteAt >= 0) {
      if (adelanteAt > 0) {
        const box = boxFromCharRange(line, 0, pageNum, pageWidth, adelanteAt);
        if (box) currentBlock.push(box);
      }
      collecting = false;
      if (currentBlock.length) {
        blockGroups.push(currentBlock);
        currentBlock = [];
      }
      continue;
    }

    if (endsWithSplitEnAdelante(line.text)) {
      const idx = line.text.search(/\(\s*en\s*$/i);
      if (idx > 0) {
        const box = boxFromCharRange(line, 0, pageNum, pageWidth, idx);
        if (box) currentBlock.push(box);
      }
      collecting = false;
      if (currentBlock.length) {
        blockGroups.push(currentBlock);
        currentBlock = [];
      }
      continue;
    }

    if (!text) continue;
    currentBlock.push(makeRedactionBox(pageNum, pageWidth, itemsToBox(line.items)));

    // Safety net: a party block is only a few wrapped lines. If we somehow keep
    // collecting past that, stop rather than blacking out half the page.
    if (currentBlock.length >= MAX_PARTY_BLOCK_LINES) {
      collecting = false;
      blockGroups.push(currentBlock);
      currentBlock = [];
    }
  }

  if (currentBlock.length) blockGroups.push(currentBlock);

  /** @type {RedactionBox[]} */
  const result = [];
  for (const group of blockGroups) {
    result.push(...mergeFullWidthPage1Boxes(group, pageWidth, pageNum));
  }
  return result;
}

function isUnderscoreLine(text) {
  const stripped = text.replace(/\s/g, '');
  return /^_{5,}$/.test(stripped) || (stripped.length >= 5 && /^[_\-=~.]+$/.test(stripped));
}

/**
 * Signature-block labels. We redact the "El Cliente" / "El Avalista" parties and
 * keep the company side ("La Empresa" → Enrique Oliete / Chamari) visible, matching
 * the contract blinding behaviour.
 */
const SIGNATURE_LABEL_TOKENS = ['LaEmpresa', 'ElCliente', 'ElAvalista'];

function compactText(text) {
  return text.replace(/\s+/g, '');
}

/**
 * True only when the line consists exclusively of signature labels
 * (e.g. "La Empresa", "El Cliente", or the two-column "La EmpresaEl Cliente").
 * This intentionally rejects body text like "...para el Cliente. El Cliente se compromete..."
 * so clause paragraphs are never redacted as signatures.
 */
function isSignatureHeaderLine(text) {
  let rest = compactText(text);
  if (!rest) return false;
  let changed = true;
  while (changed) {
    changed = false;
    for (const token of SIGNATURE_LABEL_TOKENS) {
      if (rest.startsWith(token)) {
        rest = rest.slice(token.length);
        changed = true;
      }
    }
  }
  return rest === '';
}

function lineHasClientLabel(text) {
  const c = compactText(text);
  return c.includes('ElCliente') || c.includes('ElAvalista');
}

function lineHasEmpresaLabel(text) {
  return compactText(text).includes('LaEmpresa');
}

/**
 * @param {TextLine} line
 * @returns {{ minX: number, maxX: number, minY: number, maxY: number, height: number }}
 */
function lineExtent(line) {
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (const item of line.items) {
    minX = Math.min(minX, item.x);
    maxX = Math.max(maxX, item.x + item.width);
    minY = Math.min(minY, item.y);
    maxY = Math.max(maxY, item.y + item.height);
  }
  return { minX, maxX, minY, maxY, height: maxY - minY };
}

/**
 * A signature header is only "real" if an underscore signature line follows it
 * within a few lines. Guards against stray standalone labels in body text.
 */
function hasSignatureLineNearby(lines, headerIdx) {
  let seen = 0;
  for (let j = headerIdx + 1; j < lines.length && seen < 6; j++) {
    const t = lines[j].text.trim();
    if (!t) continue;
    if (isSignatureHeaderLine(t)) return false;
    seen++;
    if (isUnderscoreLine(t)) return true;
  }
  return false;
}

/**
 * A printed party name under a signature, e.g. "D. Iñaki Martinez Ajenjo",
 * "Dña. María Laura Ajenjo", "Dn. Juan", "Doña Ana López". This is the text we
 * must hide (the handwritten signature itself, above the line, stays visible).
 */
function isHonorificNameLine(text) {
  const t = text.trim();
  if (!/[A-Za-zÁÉÍÓÚÑáéíóúñ]{2,}/.test(t)) return false;
  return /^(d\.?|dn\.?|d[ñn]a\.?|da\.?|do[nñ]a?|don|sr\.?|sra\.?|sres\.?|sta\.?)\s+[A-Za-zÁÉÍÓÚÑáéíóúñ]/i.test(t);
}

/**
 * A DocuSign signature-ID stamp, e.g. "89F0E2B188E9453...", "EA60FD7C5F2470...".
 * These sit just below the signature and must be hidden. OCR frequently mangles
 * the hex (0→O, 6→B, 5→S...), so we accept any line made of long uppercase
 * alphanumeric runs that mix letters and digits rather than requiring valid hex.
 */
function isDocusignCodeLine(text) {
  // Normalise the trailing truncation marker (unicode "…" or "...") then test.
  const norm = text.replace(/[\u2026]/g, '').trim();
  const tokens = norm.split(/\s+/).filter(Boolean);
  if (!tokens.length) return false;
  if (!tokens.every((tok) => /^[0-9A-Za-z.\-]{6,}$/.test(tok))) return false;
  return tokens.some(
    (tok) => /\d/.test(tok) && /[A-Za-z]/.test(tok) && tok.replace(/[.\-]/g, '').length >= 8,
  );
}

function isDocusignEnvelopeLine(text) {
  return /Docusign\s+Envelope\s+ID:/i.test(text);
}

/** NIF / NIE / DNI printed under a signature block. */
function isIdentityDocumentLine(text) {
  const t = text.trim();
  if (/^(NIF|NIE|DNI|CIF|Pasaporte)\b/i.test(t)) return true;
  if (/\b(NIF|NIE|DNI)\s*[nºo°.:]*\s*[XYZ]?\d{7,8}[-\s]?[A-Z]\b/i.test(t)) return true;
  if (/^\s*[XYZ]?\d{7,8}[-\s]?[A-Z]\s*$/i.test(t)) return true;
  if (/\bcon\s+(NIF|NIE|DNI)\b/i.test(t) && /\d{7,8}[A-Z]/i.test(t)) return true;
  return false;
}

/** Company-side identifiers that must always stay visible. */
function isCompanySideLine(text) {
  return /enrique\s+oliete|chamari|la\s+empresa/i.test(text);
}

// Horizontal gap (points) that separates two side-by-side signature columns on
// the same text line. Word spaces inside a name are far smaller than this.
const COLUMN_GAP = 28;

/**
 * Split a text line into "cells" — runs of items separated by a horizontal gap
 * larger than {@link COLUMN_GAP}. This recovers individual signature columns
 * even when two parties share a single text line (e.g. company on the left and
 * client on the right: "D. Enrique Oliete GutiérrezDña. María Martínez Ruiz").
 * @param {TextLine} line
 */
function splitLineIntoCells(line) {
  const items = [...line.items].sort((a, b) => a.x - b.x);
  /** @type {{ items: TextItem[] }[]} */
  const cells = [];
  let cur = null;
  let prevRight = -Infinity;
  for (const it of items) {
    if (!cur || it.x - prevRight > COLUMN_GAP) {
      cur = { items: [] };
      cells.push(cur);
    }
    cur.items.push(it);
    prevRight = Math.max(prevRight, it.x + it.width);
  }
  return cells.map((c) => {
    const minX = Math.min(...c.items.map((i) => i.x));
    const maxX = Math.max(...c.items.map((i) => i.x + i.width));
    const minY = Math.min(...c.items.map((i) => i.y));
    const maxY = Math.max(...c.items.map((i) => i.y + i.height));
    return { text: c.items.map((i) => i.str).join('').trim(), minX, maxX, minY, maxY, cx: (minX + maxX) / 2 };
  });
}

/** Classify a label cell as the company side or the client/guarantor side. */
function labelKind(text) {
  const c = compactText(text);
  if (/^LaEmpresa$/i.test(c)) return 'company';
  if (/^El(Cliente|Avalista)$/i.test(c)) return 'client';
  return null;
}

/**
 * Collect signature-label anchors on a page: each "La Empresa" / "El Cliente" /
 * "El Avalista" with its centre-x and y. Handles labels that share a line
 * (two/three-column headers) by splitting the line into cells first.
 * @param {TextLine[]} lines
 */
function collectLabelAnchors(lines) {
  /** @type {{ kind: 'company'|'client', cx: number, minX: number, maxX: number, y: number }[]} */
  const anchors = [];
  for (const line of lines) {
    if (!isSignatureHeaderLine(line.text)) continue; // pure label line only
    for (const cell of splitLineIntoCells(line)) {
      const kind = labelKind(cell.text);
      if (kind) anchors.push({ kind, cx: cell.cx, minX: cell.minX, maxX: cell.maxX, y: cell.minY });
    }
  }
  return anchors;
}

/** All underscore "signature line" rules on a page, with x-extent and y. */
function collectSignatureLines(lines) {
  /** @type {{ minX: number, maxX: number, y: number }[]} */
  const rules = [];
  for (const line of lines) {
    if (!isUnderscoreLine(line.text)) continue;
    const e = lineExtent(line);
    rules.push({ minX: e.minX, maxX: e.maxX, y: e.minY });
  }
  return rules;
}

function xOverlap(a, b) {
  return a.minX <= b.maxX && b.minX <= a.maxX;
}

/**
 * Assign a content cell to the signature label directly above it. Among labels
 * positioned above the cell, prefer the lowest (closest) ones; if several share
 * that line, pick the one whose centre is nearest the cell (handles columns).
 */
function anchorForCell(anchors, cell) {
  const above = anchors.filter((a) => a.y < cell.minY - 1);
  if (!above.length) return null;
  const maxY = Math.max(...above.map((a) => a.y));
  const sameRow = above.filter((a) => Math.abs(a.y - maxY) <= 6);
  let best = sameRow[0];
  for (const a of sameRow) {
    if (Math.abs(a.cx - cell.cx) < Math.abs(best.cx - cell.cx)) best = a;
  }
  return best;
}

/**
 * Redact the "El Cliente" / "El Avalista" signature blocks on a page.
 *
 * Strategy (layout-agnostic — works for single column, two/three columns side
 * by side, and vertically stacked parties):
 *   1. Find every signature label and every underscore signature rule.
 *   2. For each line below the first label, split it into cells (columns).
 *   3. Assign each cell to the label above it. Company cells are kept visible.
 *   4. A client cell is redacted when it sits *below* its column's signature
 *      rule (covers the printed name — including wrapped lines — and the
 *      Docusign code stamp) OR it independently looks like a name/code (covers
 *      OCR'd / image pages that have no underscore rule).
 *
 * The handwritten signature itself sits *above* the rule and is left visible.
 *
 * @param {number} pageNum
 * @param {TextLine[]} lines
 * @param {number} pageWidth
 */
function findSignatureSectionBoxes(pageNum, lines, pageWidth) {
  const anchors = collectLabelAnchors(lines);
  if (!anchors.some((a) => a.kind === 'client')) return [];

  const rules = collectSignatureLines(lines);
  const firstLabelY = Math.min(...anchors.map((a) => a.y));
  /** @type {RedactionBox[]} */
  const boxes = [];

  for (const line of lines) {
    const t = line.text.trim();
    if (!t) continue;
    if (lineExtent(line).minY < firstLabelY - 1) continue; // above the signatures
    if (/^\d+$/.test(t)) continue; // page number
    if (isSignatureHeaderLine(t)) continue; // the labels themselves
    if (isUnderscoreLine(t)) continue; // the signature rule itself

    for (const cell of splitLineIntoCells(line)) {
      if (!cell.text) continue;
      if (isDocusignEnvelopeLine(cell.text)) continue; // handled by findDocusignEnvelopeIdBoxes
      const anchor = anchorForCell(anchors, cell);
      if (!anchor || anchor.kind !== 'client') continue;
      if (isCompanySideLine(cell.text)) continue; // never hide the company party

      const ruleAbove = rules
        .filter((r) => r.y < cell.minY && r.y >= anchor.y - 2 && xOverlap(r, cell))
        .sort((a, b) => b.y - a.y)[0];
      const belowRule = Boolean(ruleAbove);
      if (
        !belowRule &&
        !isHonorificNameLine(cell.text) &&
        !isDocusignCodeLine(cell.text) &&
        !isIdentityDocumentLine(cell.text)
      ) continue;

      // Extend the box up to the signature rule so the Docusign ID stamp (which
      // is baked into the signature image just above the printed name and is not
      // in the text layer) is also covered. The cursive signature sits higher up
      // and stays visible.
      const top = ruleAbove && ruleAbove.y > cell.minY - 80
        ? Math.min(cell.minY - BOX_PAD_Y, ruleAbove.y - 48)
        : cell.minY - BOX_PAD_Y;

      const x = Math.max(LEFT_MARGIN - BOX_PAD_X, cell.minX - BOX_PAD_X * 2);
      const right = Math.min(pageWidth - RIGHT_MARGIN + BOX_PAD_X, cell.maxX + BOX_PAD_X * 2);
      boxes.push({
        page: pageNum,
        x,
        y: top,
        width: Math.max(right - x, cell.maxX - cell.minX + BOX_PAD_X * 2),
        height: cell.maxY - top + BOX_PAD_Y,
      });
    }
  }

  return boxes;
}

/**
 * DocuSign footer watermark ("Docusign Envelope ID: …") — redact the UUID on every page.
 * @param {number} pageNum
 * @param {TextLine[]} lines
 * @param {number} pageWidth
 */
function findDocusignEnvelopeIdBoxes(pageNum, lines, pageWidth) {
  /** @type {RedactionBox[]} */
  const boxes = [];
  for (const line of lines) {
    const t = line.text.trim();
    const m = t.match(/Docusign\s+Envelope\s+ID:\s*([0-9A-F-]{36})/i);
    if (!m || m.index === undefined) continue;
    const idStart = t.indexOf(m[1]);
    const box = boxFromCharRange(line, idStart, pageNum, pageWidth);
    if (box) boxes.push(box);
  }
  return boxes;
}

/**
 * Avalista / client printed names that spill onto the page after the signature
 * block (common when multiple guarantors sign — page 16 has rules, page 17 names).
 * @param {number} pageNum
 * @param {{ lines: TextLine[], pageWidth: number }} pageData
 * @param {{ lines: TextLine[], pageWidth: number }} prevPageData
 */
function findSignatureContinuationBoxes(pageNum, pageData, prevPageData) {
  const lines = pageData.lines;
  const pageWidth = pageData.pageWidth;
  if (collectLabelAnchors(lines).some((a) => a.kind === 'client')) return [];

  const txt = pageText(pageData);
  if (/^Anexo\s/i.test(txt.trim()) || /Condiciones\s+Particulares/i.test(txt)) return [];

  const prevTxt = pageText(prevPageData);
  const prevHadSigBlock =
    /prueba\s+de\s+conformidad/i.test(prevTxt) ||
    collectLabelAnchors(prevPageData.lines).some((a) => a.kind === 'client');
  if (!prevHadSigBlock) return [];

  const compactLen = txt.replace(/\s/g, '').length;
  const hasLeakLine = lines.some((l) => {
    const t = l.text.trim();
    return (
      isHonorificNameLine(t) ||
      isDocusignCodeLine(t) ||
      isIdentityDocumentLine(t)
    );
  });
  if (!hasLeakLine || compactLen > 500) return [];

  /** @type {RedactionBox[]} */
  const boxes = [];
  for (const line of lines) {
    const t = line.text.trim();
    if (!t || /^\d+$/.test(t)) continue;
    if (isCompanySideLine(t)) continue;
    if (
      isHonorificNameLine(t) ||
      isDocusignCodeLine(t) ||
      isIdentityDocumentLine(t)
    ) {
      boxes.push(makeRedactionBox(pageNum, pageWidth, itemsToBox(line.items)));
    }
  }
  return boxes;
}

function pageText(pageData) {
  return pageData.lines.map((l) => l.text).join(' ');
}

/** Airbnb host reservation printout (no contract party block). */
function isAirbnbHostReservationPage(pageData) {
  const txt = pageText(pageData);
  return /airbnb\.es\/hosting|airbnb\.com\/hosting/i.test(txt) &&
    (/Confirmada|Estancia en curso|Información sobre/i.test(txt));
}

/**
 * Redact guest PII on Airbnb host reservation PDFs (names, phone, profile, door code, reservation URL).
 * @param {number} pageNum
 * @param {TextLine[]} lines
 * @param {number} pageWidth
 */
function findAirbnbReservationBoxes(pageNum, lines, pageWidth) {
  /** @type {RedactionBox[]} */
  const boxes = [];
  let profileStartY = null;

  for (let i = 0; i < lines.length; i++) {
    const t = lines[i].text.trim();
    if (!t) continue;
    const ext = lineExtent(lines[i]);

    if (/^Teléfono:/i.test(t)) {
      boxes.push(makeRedactionBox(pageNum, pageWidth, itemsToBox(lines[i].items)));
      continue;
    }

    if (/^Vive en /i.test(t)) {
      boxes.push(makeRedactionBox(pageNum, pageWidth, itemsToBox(lines[i].items)));
      continue;
    }

    if (/hosting\/reservations\/details\//i.test(t)) {
      boxes.push(makeRedactionBox(pageNum, pageWidth, itemsToBox(lines[i].items)));
      continue;
    }

    if (/Factura con IVA/i.test(t)) {
      boxes.push(makeRedactionBox(pageNum, pageWidth, itemsToBox(lines[i].items)));
      continue;
    }

    if (/Información sobre /i.test(t)) {
      profileStartY = ext.minY;
      continue;
    }

    if (profileStartY != null && /Enviar o solicitar dinero/i.test(t)) {
      const prev = lines[i - 1];
      const bottom = prev ? lineExtent(prev).maxY : ext.minY;
      boxes.push({
        page: pageNum,
        x: LEFT_MARGIN - BOX_PAD_X,
        y: profileStartY - BOX_PAD_Y,
        width: contentWidth(pageWidth) + BOX_PAD_X * 2,
        height: bottom - profileStartY + BOX_PAD_Y * 2,
      });
      profileStartY = null;
      continue;
    }

    if (/Código de la puerta/i.test(t)) {
      for (let j = i + 1; j < Math.min(i + 4, lines.length); j++) {
        const nt = lines[j].text.trim();
        if (/^\d{4,8}$/.test(nt)) {
          boxes.push(makeRedactionBox(pageNum, pageWidth, itemsToBox(lines[j].items)));
          break;
        }
      }
      continue;
    }

    // Guest display name directly under "Confirmada" / "Estancia en curso".
    const prev = lines[i - 1]?.text.trim() || '';
    if (/^(Confirmada|Estancia en curso)$/i.test(prev) &&
        !/Alojamiento|viajer|€|noches|Moderno|Luminoso/i.test(t)) {
      boxes.push(makeRedactionBox(pageNum, pageWidth, itemsToBox(lines[i].items)));
    }
  }

  return boxes;
}

function findAllRedactionBoxes(pages) {
  /** @type {RedactionBox[]} */
  const boxes = [];
  let isAirbnbDoc = false;
  for (const [, pageData] of pages) {
    if (isAirbnbHostReservationPage(pageData)) { isAirbnbDoc = true; break; }
  }

  for (const [pageNum, pageData] of pages) {
    if (isAirbnbDoc) {
      boxes.push(...findAirbnbReservationBoxes(pageNum, pageData.lines, pageData.pageWidth));
      continue;
    }
    // Party identification ("De una parte / De otra parte"). Not always page 1:
    // some scanned bundles put the annexes first and the signed agreement last.
    if (/de\s+una\s+parte/i.test(pageText(pageData))) {
      boxes.push(...findPartyIdentificationBoxes(pageData.lines, pageData.pageWidth, pageNum));
    }
    boxes.push(...findSignatureSectionBoxes(pageNum, pageData.lines, pageData.pageWidth));
    boxes.push(...findDocusignEnvelopeIdBoxes(pageNum, pageData.lines, pageData.pageWidth));
    const prevPage = pages.get(pageNum - 1);
    if (prevPage) {
      boxes.push(...findSignatureContinuationBoxes(pageNum, pageData, prevPage));
    }
  }

  return boxes;
}

/**
 * OCR a specific subset of pages of a (digital) PDF, returning the same
 * line/item structure as {@link extractPages}. Used to recover signature blocks
 * that were flattened to images on otherwise-digital documents.
 * @param {Buffer} pdfBuffer
 * @param {number[]} pageNums 1-based page numbers
 */
async function ocrSpecificPages(pdfBuffer, pageNums) {
  const doc = mupdf.Document.openDocument(pdfBuffer, 'application/pdf');
  const worker = await createWorker(OCR_LANG);
  /** @type {Map<number, { lines: TextLine[], pageWidth: number, pageHeight: number }>} */
  const pages = new Map();

  try {
    for (const pageNum of pageNums) {
      const page = doc.loadPage(pageNum - 1);
      const bounds = page.getBounds();
      const pageWidth = bounds[2] - bounds[0];
      const pageHeight = bounds[3] - bounds[1];

      const pixmap = page.toPixmap(mupdf.Matrix.scale(OCR_SCALE, OCR_SCALE), mupdf.ColorSpace.DeviceRGB, false);
      const png = Buffer.from(pixmap.asPNG());
      const { data } = await worker.recognize(png, {}, { blocks: true });

      /** @type {TextItem[]} */
      const items = [];
      for (const word of iterateOcrWords(data)) {
        const str = (word.text || '').trim();
        if (!str || !word.bbox) continue;
        const { x0, y0, x1, y1 } = word.bbox;
        items.push({
          str: `${str} `,
          x: x0 / OCR_SCALE,
          y: y0 / OCR_SCALE,
          width: (x1 - x0) / OCR_SCALE,
          height: (y1 - y0) / OCR_SCALE,
        });
      }
      pages.set(pageNum, { lines: groupItemsIntoLines(items), pageWidth, pageHeight });
    }
  } finally {
    await worker.terminate();
  }

  return pages;
}

/**
 * On digital PDFs the closing/signature page is sometimes a flattened image
 * (DocuSign), so the text layer only contains the "...en prueba de conformidad..."
 * paragraph (or almost nothing) and no name/code text to redact. Identify those
 * pages so we can OCR just them and recover the signature block.
 * @param {Map<number, { lines: TextLine[], pageWidth: number }>} pages
 * @param {Set<number>} pagesWithSignatureBox
 */
function findImageSignaturePages(pages, pagesWithSignatureBox) {
  /** @type {Set<number>} */
  const candidates = new Set();
  for (const [pageNum, pageData] of pages) {
    if (pagesWithSignatureBox.has(pageNum)) continue;
    const txt = pageText(pageData);
    const compactLen = txt.replace(/\s/g, '').length;
    const isClosing = /prueba\s+de\s+conformidad/i.test(txt);
    if (!isClosing) continue;
    // The closing paragraph itself, plus (optionally) the next page, may carry
    // the image signature block.
    candidates.add(pageNum);
    const next = pages.get(pageNum + 1);
    if (next && !pagesWithSignatureBox.has(pageNum + 1)) {
      const nextLen = pageText(next).replace(/\s/g, '').length;
      if (nextLen < 250) candidates.add(pageNum + 1);
    }
  }

  // Long contracts (32+ pages) put the signatures around pages 16-18. OCR any of
  // those pages that still lack text-layer signature redactions.
  if (pages.size >= 30) {
    for (const p of [16, 17, 18]) {
      if (pages.has(p) && !pagesWithSignatureBox.has(p)) candidates.add(p);
    }
  }

  // Avalista names often continue on the page after the signature block.
  const p16 = pages.get(16);
  if (p16 && /El\s*Avalista/i.test(pageText(p16)) && pages.has(17) && !pagesWithSignatureBox.has(17)) {
    candidates.add(17);
  }

  return [...candidates].sort((a, b) => a - b);
}

/**
 * Full redaction-box computation: text-based detection plus a single-page OCR
 * pass for image-flattened signature pages on digital documents.
 * @param {Buffer} fileBuffer
 * @param {string} fileName
 */
async function computeRedaction(fileBuffer, fileName) {
  const { pages, viaOcr } = await extractPagesAuto(fileBuffer, fileName);
  const boxes = findAllRedactionBoxes(pages);

  // Whole-document OCR already ran (scanned doc) → nothing more to recover.
  if (!viaOcr) {
    const pagesWithSig = new Set(
      pages.size
        ? [...pages].flatMap(([pageNum, pd]) =>
            findSignatureSectionBoxes(pageNum, pd.lines, pd.pageWidth).length ? [pageNum] : [],
          )
        : [],
    );
    const ocrPageNums = findImageSignaturePages(pages, pagesWithSig);
    if (ocrPageNums.length) {
      console.log(`  ${fileName}: signature block is image-only on page(s) ${ocrPageNums.join(', ')} → OCR fallback...`);
      const ocrPages = await ocrSpecificPages(fileBuffer, ocrPageNums);
      for (const [pageNum, pd] of ocrPages) {
        boxes.push(...findSignatureSectionBoxes(pageNum, pd.lines, pd.pageWidth));
        boxes.push(...findDocusignEnvelopeIdBoxes(pageNum, pd.lines, pd.pageWidth));
        if (/de\s+una\s+parte/i.test(pageText(pd))) {
          boxes.push(...findPartyIdentificationBoxes(pd.lines, pd.pageWidth, pageNum));
        }
        const prevPage = pages.get(pageNum - 1) || ocrPages.get(pageNum - 1);
        if (prevPage) {
          boxes.push(...findSignatureContinuationBoxes(pageNum, pd, prevPage));
        }
      }
    }
  }

  return { pages, boxes, viaOcr };
}

/**
 * Render a PDF page with MuPDF, burn in redaction boxes, return PNG buffer.
 * @param {Buffer} pdfBuffer
 * @param {number} pageNum
 * @param {RedactionBox[]} boxes
 */
async function renderPageWithRedactions(pdfBuffer, pageNum, boxes) {
  const doc = mupdf.Document.openDocument(pdfBuffer, 'application/pdf');
  const page = doc.loadPage(pageNum - 1);
  const matrix = mupdf.Matrix.scale(RENDER_SCALE, RENDER_SCALE);
  const pixmap = page.toPixmap(matrix, mupdf.ColorSpace.DeviceRGB, false);
  const pagePng = pixmap.asPNG();

  const img = await loadImage(pagePng);
  const canvas = createCanvas(img.width, img.height);
  const ctx = canvas.getContext('2d');
  ctx.drawImage(img, 0, 0);

  ctx.fillStyle = '#000000';
  for (const box of boxes) {
    ctx.fillRect(
      box.x * RENDER_SCALE,
      box.y * RENDER_SCALE,
      box.width * RENDER_SCALE,
      box.height * RENDER_SCALE,
    );
  }

  return canvas.toBuffer('image/png');
}

async function applyRedactions(pdfBuffer, boxes) {
  const srcDoc = await PDFDocument.load(pdfBuffer, { ignoreEncryption: true });
  const outDoc = await PDFDocument.create();
  const pageCount = srcDoc.getPageCount();

  /** @type {Map<number, RedactionBox[]>} */
  const byPage = new Map();
  for (const box of boxes) {
    const list = byPage.get(box.page) || [];
    list.push(box);
    byPage.set(box.page, list);
  }

  for (let i = 0; i < pageCount; i++) {
    const pageNum = i + 1;
    const pageBoxes = byPage.get(pageNum) || [];
    const { width, height } = srcDoc.getPage(i).getSize();

    if (pageBoxes.length === 0) {
      const [copied] = await outDoc.copyPages(srcDoc, [i]);
      outDoc.addPage(copied);
      continue;
    }

    const png = await renderPageWithRedactions(pdfBuffer, pageNum, pageBoxes);
    const img = await outDoc.embedPng(png);
    const newPage = outDoc.addPage([width, height]);
    newPage.drawImage(img, { x: 0, y: 0, width, height });
  }

  return Buffer.from(await outDoc.save());
}

/**
 * Detect redaction boxes without applying them (for tests / calibration).
 * Falls back to OCR for image-only PDFs.
 * @param {Buffer} fileBuffer
 * @param {string} [fileName]
 */
export async function detectRedactionBoxes(fileBuffer, fileName = 'document.pdf') {
  return computeRedaction(fileBuffer, fileName);
}

/**
 * Blind personal data in a PDF buffer (Cliente / Avalista parties + signatures).
 * Text-based for digital PDFs; OCR-based for scanned/image-only PDFs.
 * @param {Buffer} fileBuffer
 * @param {string} [fileName]
 */
export async function blindPdfBuffer(fileBuffer, fileName = 'contract.pdf') {
  const { viaOcr, boxes } = await computeRedaction(fileBuffer, fileName);

  if (boxes.length === 0) {
    if (viaOcr) {
      throw new Error(
        `Could not locate party/signature blocks in ${fileName} after OCR ` +
          `(image-only PDF). Needs manual blinding or a clearer scan.`,
      );
    }
    throw new Error(`No redaction regions found in ${fileName} (De otra parte / El Cliente / El Avalista).`);
  }

  const pagesRasterized = new Set(boxes.map((b) => b.page)).size;

  console.log(
    `  ${fileName}: ${boxes.length} region(s) across ${pagesRasterized} page(s)` +
      `${viaOcr ? ' [OCR]' : ''}`,
  );

  const buffer = await applyRedactions(fileBuffer, boxes);
  return {
    buffer,
    redactionRegions: boxes.length,
    pagesRasterized,
    viaOcr,
  };
}
