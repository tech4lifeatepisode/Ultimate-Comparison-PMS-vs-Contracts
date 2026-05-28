-- OCR rerun table for NC contracts missed by the first OpenAI extraction pass.
-- Run in Supabase SQL Editor (Project: jujvtuyksxkoclegjznb) BEFORE triggering the rerun.
--
-- Target NCs (27):
-- NC_3, NC_4, NC_15, NC_17, NC_18, NC_26, NC_362, NC_702, NC_703, NC_750, NC_795,
-- NC_829, NC_857, NC_876, NC_890, NC_953, NC_954, NC_1070, NC_1104, NC_1108, NC_1127,
-- NC_1141, NC_1214, NC_1222, NC_1248, NC_1345
--
-- Render / local env: EXTRACTION_TABLE=contract_extractions_ocr_rerun

create table if not exists public.contract_extractions_ocr_rerun (
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

create index if not exists contract_extractions_ocr_rerun_file_name_idx
  on public.contract_extractions_ocr_rerun (file_name);

create index if not exists contract_extractions_ocr_rerun_nc_idx
  on public.contract_extractions_ocr_rerun (nc);

comment on table public.contract_extractions_ocr_rerun is
  'OCR rerun rows for NC contracts missed in contract_extractions; same v2 schema.';

comment on column public.contract_extractions_ocr_rerun.rent is
  'Legacy "Rent"; new pipeline uses base_rent / final_rent.';

alter table public.contract_extractions_ocr_rerun enable row level security;

drop policy if exists contract_extractions_ocr_rerun_insert_anon on public.contract_extractions_ocr_rerun;
create policy contract_extractions_ocr_rerun_insert_anon
  on public.contract_extractions_ocr_rerun for insert to anon with check (true);

drop policy if exists contract_extractions_ocr_rerun_select_anon on public.contract_extractions_ocr_rerun;
create policy contract_extractions_ocr_rerun_select_anon
  on public.contract_extractions_ocr_rerun for select to anon using (true);

-- Optional: compare rerun vs original
-- select r.nc, r.file_name, r.error as rerun_error, o.error as original_error
-- from public.contract_extractions_ocr_rerun r
-- left join public.contract_extractions o on o.file_name = r.file_name
-- order by r.nc;
