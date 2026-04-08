-- Run this in Supabase SQL Editor (Project: jujvtuyksxkoclegjznb) before first extraction.
-- Creates the single table for contract extraction rows.

create table if not exists public.contract_extractions (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  nc text,
  name text,
  id_type text,
  id_number text,
  check_in_date text,
  check_out_date text,
  rent text,
  deposit text,
  deposit_wording text,
  deposit_source text,
  file_name text,
  model_used text,
  error text
);

comment on table public.contract_extractions is 'One row per contract file; columns match CSV export.';

-- Storage: create a bucket in Dashboard > Storage (name must match SUPABASE_STORAGE_BUCKET, e.g. "contracts").
-- Files are uploaded under the prefix set in SUPABASE_STORAGE_FOLDER (e.g. "To Fill 1/") inside that bucket.

-- Optional: allow anon uploads to that bucket (replace "contracts" with your bucket id).
-- drop policy if exists "storage_insert_contracts_anon" on storage.objects;
-- create policy "storage_insert_contracts_anon"
--   on storage.objects for insert to anon
--   with check (bucket_id = 'contracts');

alter table public.contract_extractions enable row level security;

-- Allow inserts using the anon key (used by the extraction script). Tighten this in production.
drop policy if exists "contract_extractions_insert_anon" on public.contract_extractions;
create policy "contract_extractions_insert_anon"
  on public.contract_extractions
  for insert
  to anon
  with check (true);

-- SKIP_ALREADY_EXTRACTED needs to SELECT file_name. Anon cannot read without this (RLS).
-- Service role bypasses RLS; if you only use service_role in production, this is optional.
drop policy if exists "contract_extractions_select_anon" on public.contract_extractions;
create policy "contract_extractions_select_anon"
  on public.contract_extractions
  for select
  to anon
  using (true);

drop policy if exists "contract_extractions_select_authenticated" on public.contract_extractions;
-- Optional: allow read for authenticated users only; adjust as needed.
-- create policy "contract_extractions_select_authenticated" on public.contract_extractions for select to authenticated using (true);
