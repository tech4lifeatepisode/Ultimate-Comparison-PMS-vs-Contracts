# Contract Extraction (Production)

OpenAI extraction from Supabase Storage → Postgres + CSV. Runs on Render as an HTTP service alongside blinding.

## Setup

```powershell
cd "Contract Extraction\contract extraction prod"
copy .env.example .env
npm install
```

Fill `.env`: `OPENAI_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_STORAGE_BUCKET=Contracts`.

Run in Supabase SQL Editor:

- `supabase-schema.sql` — main table `contract_extractions`
- `supabase-schema-ocr-rerun.sql` — rerun table `contract_extractions_ocr_rerun`

## npm scripts

| Script | Command |
|--------|---------|
| Start Render server | `npm start` |
| Extract local `contracts/` folder | `npm run extract` |
| One Storage batch | `npm run extract:storage` |
| All pending in one Storage folder | `npm run extract:storage:all` |
| OCR rerun (3 folders + NC filter) | `npm run extract:nc-ocr-rerun` |
| Verify Storage vs DB | `npm run verify:extraction` |

## HTTP endpoints (Render)

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Service status and env hints |
| `POST /extract` | One batch (`MAX_EXTRACTION_FILES`) |
| `POST /extract-all` | All pending in `SUPABASE_STORAGE_FOLDER` |
| `POST /extract-nc-rerun-all` | OCR rerun across To Fill 1, To Fill 2, Fill 3 |
| `POST /blind-all` | Full blinding run (separate pipeline) |

Optional header: `X-Extract-Secret: <EXTRACT_TRIGGER_SECRET>`

## Storage folders

| Folder | Use |
|--------|-----|
| `To Fill 1` | Batch 1 (~first 600 customers) |
| `To Fill 2` | Batch 2 (remainder) |
| `Fill 3 NC_1250-NC_1470` | NC 1250–1470 upload batch |

## Key environment variables

| Variable | Typical value |
|----------|----------------|
| `EXTRACTION_TABLE` | `contract_extractions` or `contract_extractions_ocr_rerun` |
| `EXTRACTION_NC_FILTER` | Comma-separated NC list for filtered reruns |
| `SKIP_ALREADY_EXTRACTED` | `true` |
| `MAX_EXTRACTION_FILES` | `10` |
| `AUTO_NC_OCR_RERUN_FROM_STORAGE` | `true` on Render to rerun on boot |
| `AUTO_BLIND_FROM_STORAGE` | `false` while extracting (avoid competing jobs) |

See `.env.example` for the full list.

## OCR rerun workflow

1. Apply `supabase-schema-ocr-rerun.sql`.
2. Set `EXTRACTION_TABLE=contract_extractions_ocr_rerun`.
3. Trigger `/extract-nc-rerun-all` or `npm run extract:nc-ocr-rerun`.
4. Optional: `supabase-dedupe-ocr-rerun.sql` to remove duplicate rows.
5. Local missing NCs: use `Local Extraction and Blinding/extract-local-to-ocr-rerun.mjs`.

## File types

Supported: `.pdf`, `.docx`, images, and uploads ending in `_pdf` (no dot extension).

NC codes are parsed from filenames containing `NC_XXXX` anywhere in the name.
