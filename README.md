# Contract Extraction & Blinding

Monorepo for Spanish rental contract **extraction** (OpenAI → Supabase), **blinding** (PDF redaction), and **PMS comparison** workflows.

**Production service:** [contract-extraction-joyy.onrender.com](https://contract-extraction-joyy.onrender.com)

## Repository layout

| Path | Purpose |
|------|---------|
| `Contract Extraction/contract extraction prod/` | Production extraction + Render HTTP server |
| `Contract Extraction/contract extraction sandbox/` | Local OpenAI extraction testing (no Supabase required) |
| `Blinding_Contracts/Blinding_prod/` | Production PDF/office blinding from Supabase Storage |
| `Local Extraction and Blinding/` | Manual local PDFs: blind, extract, audit NC status |
| `PMS Comparison Workflow/` | Upload contracts, build Rosetta, compare vs extraction |
| `render.yaml` | Render deploy blueprint (Node 22) |

## Quick start (production extraction)

1. Copy `Contract Extraction/contract extraction prod/.env.example` → `.env` and fill `OPENAI_API_KEY`, Supabase keys.
2. Run `supabase-schema.sql` (and `supabase-schema-ocr-rerun.sql` for OCR reruns) in Supabase SQL Editor.
3. Local batch: `npm run extract:storage:all` from `contract extraction prod/`.
4. Render: set env vars, deploy from `main`, call `POST /extract-all` or `/extract-nc-rerun-all`.

See **`Contract Extraction/contract extraction prod/README.md`** for endpoints, env vars, and OCR rerun.

## OCR rerun table

Missed NC contracts are re-extracted into **`contract_extractions_ocr_rerun`** (separate from `contract_extractions`).

- Schema: `contract Extraction/contract extraction prod/supabase-schema-ocr-rerun.sql`
- Render env: `EXTRACTION_TABLE=contract_extractions_ocr_rerun`, `AUTO_NC_OCR_RERUN_FROM_STORAGE=true`
- Local all-folders: `npm run extract:nc-ocr-rerun`

## Local manual workflow

Drop PDFs in **`Local Extraction and Blinding/`** (gitignored). See that folder’s README for blind + extract + audit scripts.

## Gitignore

Secrets (`.env`), `node_modules/`, local contract PDFs, blinded outputs, and CSV extraction exports are **not committed**. Only scripts, schemas, and `.env.example` files are tracked.

## Deploy (Render)

Root `package.json` postinstalls `contract extraction prod/`, which starts `server.mjs` (extraction + blinding endpoints).
