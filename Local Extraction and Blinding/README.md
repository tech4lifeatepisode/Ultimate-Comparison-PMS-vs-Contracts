# Local Extraction and Blinding

Drop **source contract PDFs** here for one-off local processing. PDF files and `Locally Blinded/` outputs are **gitignored** (personal data).

## Folder layout

```text
Local Extraction and Blinding/
  *.pdf                          ← source contracts (not committed)
  Locally Blinded/               ← blinded PDF output (not committed)
  blind-local.mjs
  extract-local-to-ocr-rerun.mjs
  audit-nc-extraction-status.mjs
```

## 1. Blind contracts locally

Redacts tenant party blocks and signatures using production blinding logic.

```powershell
cd "Contract extraction\Local Extraction and Blinding"
node blind-local.mjs
```

Output: `Locally Blinded/<same filename>.pdf`

## 2. Extract → Supabase `contract_extractions_ocr_rerun`

Uses **OpenAI** from `Contract Extraction/contract extraction sandbox/.env` and **Supabase** from `contract extraction prod/.env`.

```powershell
cd "Contract extraction\Contract Extraction\contract extraction prod"
node "../../Local Extraction and Blinding/extract-local-to-ocr-rerun.mjs"
```

- Reads PDFs in this folder (not `Locally Blinded/`)
- Skips paths already in `contract_extractions_ocr_rerun` with prefix `Local Extraction and Blinding/`
- Inserts rows into **`contract_extractions_ocr_rerun`**

Run `supabase-schema-ocr-rerun.sql` in Supabase before the first insert.

## 3. Audit NC coverage

Compares 32 target NCs against `contract_extractions` and `contract_extractions_ocr_rerun`:

```powershell
cd "Contract extraction\Contract Extraction\contract extraction prod"
node "../../Local Extraction and Blinding/audit-nc-extraction-status.mjs"
```

Reports: `ocr_rerun OK`, `main only`, `WEAK` (unknown rent), or `MISSING`.

## Notes

- Filenames may contain `NC_0015` anywhere (Docusign / CONTRATO FIRMADO names), not only at the start.
- Files ending in `_pdf` (no dot) are supported by the extraction pipeline.
- Prefer extracting **unblinded** source PDFs; blinded copies may reduce OCR quality.
