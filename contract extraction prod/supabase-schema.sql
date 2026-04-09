-- Run this in Supabase SQL Editor (Project: jujvtuyksxkoclegjznb) before first extraction.
-- Creates the table for contract extraction rows (CSV v2 headers).
-- Existing projects: run supabase-migration-csv-v2-headers.sql if the table was created from an older schema.

create table if not exists public.contract_extractions (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  nc text,
  contract_title text,
  name text,
  id_type text,
  id_number text,
  number_of_people text,
  unit_type text,
  check_in_date text,
  check_out_date text,
  rent text,
  base_rent text,
  extra_name text,
  extra_price text,
  discount_type text,
  price_after_discount text,
  discount_price text,
  final_rent text,
  deposit text,
  deposit_wording text,
  deposit_source text,
  file_name text,
  model_used text,
  error text
);

comment on table public.contract_extractions is 'One row per contract file; columns match CSV export (v2).';
comment on column public.contract_extractions.rent is 'Legacy "Rent" from older CSV exports; new pipeline uses base_rent / final_rent.';

-- Storage: create a bucket in Dashboard > Storage (name must match SUPABASE_STORAGE_BUCKET, e.g. "Contracts").
-- Files are uploaded under the prefix set in SUPABASE_STORAGE_FOLDER (e.g. "To Fill 2/") inside that bucket.

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
