-- ============================================================
-- The Tribune — โครงฐานข้อมูลสำหรับระบบบัญชีผู้ใช้ + บันทึกการเข้าใช้
-- ใช้กับ Supabase (Postgres) · รันในหน้า SQL Editor ครั้งเดียว
-- ============================================================
--
-- ต่างจากที่เสนอไว้ในเอกสารตรงนี้: Supabase มีตาราง auth.users กับระบบ session
-- ให้อยู่แล้ว เราจึงไม่สร้าง users / sessions เอง (ถ้าสร้างเองเท่ากับกลับไป
-- รับผิดชอบการเก็บรหัสผ่านเองซึ่งเป็นสิ่งที่ตั้งใจเลี่ยงตั้งแต่แรก)
-- เหลือของที่ต้องทำเองสองตาราง: profiles กับ login_events
--
-- ============================================================

-- ── โปรไฟล์ผู้ใช้ ─────────────────────────────────────────
-- ผูกกับ auth.users แบบหนึ่งต่อหนึ่ง ไว้เก็บของที่เป็นของผู้ใช้เอง
-- เช่นรายการโปรดกับอินดิเคเตอร์ที่ตอนนี้อยู่ใน localStorage ของแต่ละเครื่อง
create table if not exists public.profiles (
  id           uuid primary key references auth.users (id) on delete cascade,
  display_name text,
  favorites    jsonb not null default '[]'::jsonb,
  indicators   jsonb not null default '[]'::jsonb,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);

comment on table public.profiles is
  'ข้อมูลผู้ใช้ที่เว็บเก็บเอง (Supabase ดูแลอีเมลกับรหัสผ่านให้ในตาราง auth.users)';

-- สร้างโปรไฟล์ให้อัตโนมัติทันทีที่มีคนสมัคร
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer set search_path = public
as $$
begin
  insert into public.profiles (id) values (new.id) on conflict do nothing;
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();


-- ── ประวัติการเข้าใช้ ─────────────────────────────────────
-- บันทึกทั้งที่สำเร็จและไม่สำเร็จ ถ้าเก็บเฉพาะที่สำเร็จจะมองไม่เห็นการไล่เดารหัสผ่าน
create table if not exists public.login_events (
  id          bigint generated always as identity primary key,
  user_id     uuid references auth.users (id) on delete set null,
  email_tried text,          -- ไว้ดูตอนที่ยังไม่รู้ว่าเป็นใคร (login ไม่ผ่าน)
  event       text not null check (event in
                ('success','fail','lockout','logout','reset_request','reset_done')),
  ip          inet,          -- เลขเต็ม — ใช้กับงานความปลอดภัยเท่านั้น
  ip_prefix   text,          -- ตัดท้ายแล้ว (203.150.11.42 → 203.150.11.0) ใช้ทำสถิติ
  country     text,          -- รหัสประเทศสองตัวจาก header ของ edge
  user_agent  text,
  created_at  timestamptz not null default now()
);

comment on column public.login_events.ip is
  'ข้อมูลส่วนบุคคลตาม พ.ร.บ.คุ้มครองข้อมูลส่วนบุคคล — ลบอัตโนมัติเมื่อครบ 90 วัน';

create index if not exists login_events_user_time_idx
  on public.login_events (user_id, created_at desc);
create index if not exists login_events_time_idx
  on public.login_events (created_at);
-- ไว้ตรวจว่ามี IP ไหนไล่ยิงรหัสผ่านหลายบัญชีหรือเปล่า
create index if not exists login_events_ip_time_idx
  on public.login_events (ip, created_at desc) where event = 'fail';


-- ── สิทธิ์การเข้าถึงรายแถว ────────────────────────────────
alter table public.profiles     enable row level security;
alter table public.login_events enable row level security;

-- โปรไฟล์: เจ้าของอ่านและแก้ของตัวเองได้เท่านั้น
drop policy if exists "อ่านโปรไฟล์ตัวเอง" on public.profiles;
create policy "อ่านโปรไฟล์ตัวเอง" on public.profiles
  for select using (auth.uid() = id);

drop policy if exists "แก้โปรไฟล์ตัวเอง" on public.profiles;
create policy "แก้โปรไฟล์ตัวเอง" on public.profiles
  for update using (auth.uid() = id) with check (auth.uid() = id);

-- ประวัติการเข้าใช้: เจ้าของ "อ่าน" ของตัวเองได้ แต่เขียนไม่ได้
-- ไม่มี policy สำหรับ insert โดยเจตนา — เขียนได้เฉพาะ Edge Function
-- ที่ใช้ service_role key เท่านั้น ไม่งั้นหน้าเว็บจะปลอมประวัติได้เอง
drop policy if exists "อ่านประวัติตัวเอง" on public.login_events;
create policy "อ่านประวัติตัวเอง" on public.login_events
  for select using (auth.uid() = user_id);


-- ── ลบข้อมูลเมื่อครบอายุการเก็บ ───────────────────────────
-- ประกาศไว้ในนโยบายความเป็นส่วนตัวว่าเก็บ 90 วัน จึงต้องลบจริงตามนั้น
create or replace function public.purge_old_login_events()
returns integer
language plpgsql
security definer set search_path = public
as $$
declare
  removed integer;
begin
  delete from public.login_events
   where created_at < now() - interval '90 days';
  get diagnostics removed = row_count;
  return removed;
end;
$$;

-- ตั้งเวลาให้ทำงานทุกวันตีสาม (ต้องเปิด extension pg_cron ในหน้า Database → Extensions ก่อน)
-- select cron.schedule('purge-login-events', '17 3 * * *',
--                      $$select public.purge_old_login_events()$$);
