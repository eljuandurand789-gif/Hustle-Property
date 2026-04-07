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
  filename text not null, -- stores Supabase Storage path inside bucket
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

