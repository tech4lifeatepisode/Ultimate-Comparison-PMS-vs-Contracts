import { createCanvas, loadImage } from '@napi-rs/canvas';
import { PDFDocument } from 'pdf-lib';
import mupdf from 'mupdf';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';

const LEFT_MARGIN = 85;
const RIGHT_MARGIN = 85;
const LINE_Y_TOLERANCE = 4;
const BOX_PAD_X = 6;
const BOX_PAD_Y = 8;
const RENDER_SCALE = 2;

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

function mergeFullWidthPage1Boxes(boxes, pageWidth) {
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
      page: 1,
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

function findPartyIdentificationBoxes(lines, pageWidth) {
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
          const box = boxFromCharRange(line, partyStart, 1, pageWidth, adelanteAt);
          if (box) currentBlock.push(box);
          collecting = false;
          blockGroups.push(currentBlock);
          currentBlock = [];
        } else {
          const box = boxFromCharRange(line, partyStart, 1, pageWidth);
          if (box) currentBlock.push(box);
        }
      }
      continue;
    }

    if (!collecting) continue;

    if (isAdelanteContinuation(text)) {
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
        const box = boxFromCharRange(line, 0, 1, pageWidth, adelanteAt);
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
        const box = boxFromCharRange(line, 0, 1, pageWidth, idx);
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
    currentBlock.push(makeRedactionBox(1, pageWidth, itemsToBox(line.items)));
  }

  if (currentBlock.length) blockGroups.push(currentBlock);

  /** @type {RedactionBox[]} */
  const result = [];
  for (const group of blockGroups) {
    result.push(...mergeFullWidthPage1Boxes(group, pageWidth));
  }
  return result;
}

function isUnderscoreLine(text) {
  const stripped = text.replace(/\s/g, '');
  return /^_{5,}$/.test(stripped) || (stripped.length >= 5 && /^[_\-=~.]+$/.test(stripped));
}

function isTenantNameLine(text) {
  const t = text.trim();
  return (
    /^(D\.|Dña\.|Dña\.\S|Sr\.|Sra\.)\s*.+/i.test(t) &&
    !/Enrique Oliete/i.test(t)
  );
}

const SIGNATURE_SECTION_LABELS = ['El Cliente', 'El Avalista'];

function findSignatureSectionBoxes(pageNum, lines, pageWidth) {
  /** @type {RedactionBox[]} */
  const boxes = [];

  for (const label of SIGNATURE_SECTION_LABELS) {
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].text.trim() !== label) continue;

      /** @type {RedactionBox[]} */
      const sectionBoxes = [];

      for (let j = i + 1; j < lines.length; j++) {
        const next = lines[j];
        const nextText = next.text.trim();
        if (!nextText) continue;
        if (/^\d+$/.test(nextText)) break;
        if (nextText.includes('Docusign Envelope')) continue;
        if (SIGNATURE_SECTION_LABELS.includes(nextText)) break;

        if (isUnderscoreLine(nextText) || isTenantNameLine(nextText)) {
          const box = itemsToBox(next.items);
          sectionBoxes.push({
            page: pageNum,
            x: box.x - BOX_PAD_X,
            y: box.y - BOX_PAD_Y,
            width: Math.max(box.width + BOX_PAD_X * 2, contentWidth(pageWidth) * 0.55),
            height: box.height + BOX_PAD_Y * 2,
          });
        } else if (sectionBoxes.length > 0) {
          break;
        }
      }

      boxes.push(...sectionBoxes);
    }
  }

  return boxes;
}

function findAllRedactionBoxes(pages) {
  /** @type {RedactionBox[]} */
  const boxes = [];

  const page1 = pages.get(1);
  if (page1) {
    boxes.push(...findPartyIdentificationBoxes(page1.lines, page1.pageWidth));
  }

  for (const [pageNum, pageData] of pages) {
    const hasSignatureBlock = pageData.lines.some(
      (l) =>
        l.text.includes('Y, en prueba de conformidad') ||
        l.text.includes('La Empresa') ||
        l.text.trim() === 'El Avalista',
    );
    if (!hasSignatureBlock) continue;
    boxes.push(...findSignatureSectionBoxes(pageNum, pageData.lines, pageData.pageWidth));
  }

  return boxes;
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
  const srcDoc = await PDFDocument.load(pdfBuffer);
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
 * Blind personal data in a PDF buffer (Cliente / Avalista parties + signatures).
 * @param {Buffer} fileBuffer
 * @param {string} [fileName]
 */
export async function blindPdfBuffer(fileBuffer, fileName = 'contract.pdf') {
  const pages = await extractPages(fileBuffer);
  const boxes = findAllRedactionBoxes(pages);

  if (boxes.length === 0) {
    throw new Error(`No redaction regions found in ${fileName} (De otra parte / El Cliente / El Avalista).`);
  }

  const page1Count = boxes.filter((b) => b.page === 1).length;
  const sigCount = boxes.length - page1Count;
  const pagesRasterized = new Set(boxes.map((b) => b.page)).size;

  console.log(`  ${fileName}: page 1 regions=${page1Count}, signature regions=${sigCount}, rasterized pages=${pagesRasterized}`);

  const buffer = await applyRedactions(fileBuffer, boxes);
  return {
    buffer,
    redactionRegions: boxes.length,
    pagesRasterized,
  };
}
