-- Run in Supabase SQL Editor when you want extraction to start again from the first files
-- (with SKIP_ALREADY_EXTRACTED=true, an empty table means nothing is skipped).
-- This deletes all rows but keeps the v2 column layout.

truncate table public.contract_extractions restart identity;
