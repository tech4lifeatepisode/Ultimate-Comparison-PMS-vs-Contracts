-- Remove duplicate OCR rerun rows (same file_name), keeping the newest row per path.
-- Run in Supabase SQL Editor after overlapping Render deploys created duplicates.

delete from public.contract_extractions_ocr_rerun old
using public.contract_extractions_ocr_rerun keep
where old.file_name = keep.file_name
  and old.file_name is not null
  and old.created_at < keep.created_at;

-- Optional: list NCs still missing from the 26-NC rerun target
-- select nc, count(*) from public.contract_extractions_ocr_rerun group by nc order by nc;
