-- DANGER: Drops contract_extractions and recreates with full v2 columns + RLS policies.
-- Use if the table was recreated manually and is missing columns (inserts would fail or omit fields).
-- After this, run: supabase-clear-extractions-for-rerun.sql is unnecessary (table is empty).

drop table if exists public.contract_extractions cascade;

create table public.contract_extractions (
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

comment on table public.contract_extractions is 'CSV v2 export rows; rent mirrors final_rent for legacy views.';
comment on column public.contract_extractions.rent is 'Same as final_rent for legacy consumers.';

alter table public.contract_extractions enable row level security;

drop policy if exists "contract_extractions_insert_anon" on public.contract_extractions;
create policy "contract_extractions_insert_anon"
  on public.contract_extractions for insert to anon with check (true);

drop policy if exists "contract_extractions_select_anon" on public.contract_extractions;
create policy "contract_extractions_select_anon"
  on public.contract_extractions for select to anon using (true);
