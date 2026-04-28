-- Run once in Supabase SQL Editor after deploying the new CSV column layout.
-- Adds columns to match contract_extractions_*.csv (v2): extended pricing, extras, deposit fields.
-- Safe to re-run: uses IF NOT EXISTS.

alter table public.contract_extractions
  add column if not exists contract_title text,
  add column if not exists number_of_people text,
  add column if not exists unit_type text,
  add column if not exists base_rent text,
  add column if not exists extra_name text,
  add column if not exists extra_price text,
  add column if not exists discount_type text,
  add column if not exists price_after_discount text,
  add column if not exists discount_price text,
  add column if not exists final_rent text;

comment on column public.contract_extractions.rent is 'Legacy CSV column "Rent" from older exports; newer rows use base_rent / final_rent.';
comment on column public.contract_extractions.price_after_discount is 'Monthly amount after "por lo que el importe a abonar mensualmente será de".';
comment on column public.contract_extractions.discount_price is 'Computed: base_rent minus price_after_discount (euro discount amount).';
