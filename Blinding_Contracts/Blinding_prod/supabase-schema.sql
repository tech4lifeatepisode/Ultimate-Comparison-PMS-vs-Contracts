-- Contract blinding tracking (Supabase project jujvtuyksxkoclegjznb, bucket Contracts).
-- Run in Supabase SQL Editor before first production blind run.

create table if not exists public.contract_blindings (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  source_folder text not null,
  source_file_name text not null,
  output_file_name text,
  status text not null default 'pending'
    check (status in ('pending', 'processing', 'success', 'error')),
  error text,
  redaction_regions integer,
  pages_rasterized integer,
  completed_at timestamptz,
  constraint contract_blindings_source_file_name_key unique (source_file_name)
);

create index if not exists contract_blindings_status_idx
  on public.contract_blindings (status);

create index if not exists contract_blindings_source_folder_idx
  on public.contract_blindings (source_folder);

comment on table public.contract_blindings is
  'Tracks PDF blinding from source storage folders to blinded output folders in SUPABASE_STORAGE_BUCKET.';

comment on column public.contract_blindings.source_file_name is
  'Full storage path relative to bucket root, e.g. To Fill 1/NC_0450.pdf';

comment on column public.contract_blindings.output_file_name is
  'Full storage path of blinded PDF, e.g. Fill 1 Blinded/NC_0450.pdf';

alter table public.contract_blindings enable row level security;

drop policy if exists contract_blindings_insert_anon on public.contract_blindings;
create policy contract_blindings_insert_anon
  on public.contract_blindings for insert to anon with check (true);

drop policy if exists contract_blindings_select_anon on public.contract_blindings;
create policy contract_blindings_select_anon
  on public.contract_blindings for select to anon using (true);

drop policy if exists contract_blindings_update_anon on public.contract_blindings;
create policy contract_blindings_update_anon
  on public.contract_blindings for update to anon using (true) with check (true);

-- Optional: service_role bypasses RLS when SUPABASE_SERVICE_ROLE_KEY is used (recommended on Render).
