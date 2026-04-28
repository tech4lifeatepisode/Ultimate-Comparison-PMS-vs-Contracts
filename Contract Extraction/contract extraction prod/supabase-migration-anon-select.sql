-- Run once in Supabase SQL Editor if SKIP_ALREADY_EXTRACTED sees 0 "already" rows while the table has data.
-- Anon could INSERT (policy) but not SELECT file_name until this policy exists (unless you use service_role only).

drop policy if exists "contract_extractions_select_anon" on public.contract_extractions;
create policy "contract_extractions_select_anon"
  on public.contract_extractions
  for select
  to anon
  using (true);
