-- Run this in Supabase SQL Editor (or via migrations) to create the tables used by the app.
-- Storage:
-- - Create a public bucket named `property-images` (or set SUPABASE_BUCKET in Vercel).

create table if not exists public.properties (
  id bigserial primary key,
  name text not null,
  area text not null,
  status text not null,
  priority_group text not null default 'medium',
  size text,
  address text,
  price text,
  availability text,
  description text,
  features text,
  notes text,
  display_image text,
  building_id bigint,
  use_unit_details integer not null default 1,
  broker_id bigint,
  video_filename text,
  youtube_video_id text,
  power_phase text,
  power_amps text,
  height_eave_apex text,
  height_eave_roller_shutter text,
  parking_bays text,
  yard_space text,
  property_type text not null default 'industrial',
  latitude double precision,
  longitude double precision,
  created_at timestamptz not null default now()
);

create table if not exists public.property_images (
  id bigserial primary key,
  property_id bigint not null references public.properties(id) on delete cascade,
  storage_path text not null, -- stores Supabase Storage path inside bucket
  image_order integer not null default 0,
  created_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'property_images'
      and column_name = 'image_order'
  ) then
    alter table public.property_images
      add column image_order integer not null default 0;
  end if;
end
$$;

create index if not exists property_images_property_id_order_idx
  on public.property_images(property_id, image_order, id);

create table if not exists public.home_featured_slots (
  slot integer primary key,
  property_id bigint references public.properties(id) on delete set null,
  feature_style text default 'orbit',
  constraint home_featured_slots_slot_check check (slot in (1,2))
);

create table if not exists public.agents (
  id bigserial primary key,
  name text not null,
  slug text unique not null,
  created_at timestamptz not null default now()
);

create table if not exists public.deals (
  id bigserial primary key,
  agent_id bigint not null references public.agents(id) on delete restrict,
  property_name text,
  property_address text not null,
  deal_date text,
  lease_period text,
  link_url text,
  asking_rental text,
  actual_rental text,
  escalation_period text,
  invoice_total double precision,
  agent_share_percent double precision not null default 50,
  notes text,
  deal_image text,
  lease_start_date text,
  lease_end_date text,
  deal_amount_type text not null default 'net_before_tax',
  is_expected integer not null default 0,
  beneficial_occupation_date text,
  lease_commencement_date text,
  map_latitude double precision,
  map_longitude double precision,
  show_on_done_deals integer not null default 0,
  created_at timestamptz not null default now()
);

create index if not exists deals_agent_id_idx on public.deals(agent_id, id desc);

create table if not exists public.agent_payouts (
  id bigserial primary key,
  agent_id bigint not null references public.agents(id) on delete cascade,
  payout_date text not null,
  amount double precision not null,
  notes text,
  created_at timestamptz not null default now()
);

create index if not exists agent_payouts_agent_id_idx on public.agent_payouts(agent_id, id desc);

create table if not exists public.buildings (
  id bigserial primary key,
  name text not null,
  description text,
  size_text text,
  features text,
  display_image text,
  created_at timestamptz not null default now()
);

create table if not exists public.building_images (
  id bigserial primary key,
  building_id bigint not null references public.buildings(id) on delete cascade,
  storage_path text not null,
  image_order integer not null default 0,
  created_at timestamptz not null default now()
);

create index if not exists building_images_building_id_order_idx
  on public.building_images(building_id, image_order, id);

create table if not exists public.property_enquiries (
  id bigserial primary key,
  property_id bigint,
  property_label text,
  property_address text,
  enquirer_name text not null,
  phone text not null,
  email text,
  message text,
  created_at timestamptz not null default now()
);

