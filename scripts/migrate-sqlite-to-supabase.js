/**
 * One-time migration:
 * - Reads local SQLite (properties.db + property_images)
 * - Uploads files from uploads/ to Supabase Storage bucket
 * - Inserts rows into Supabase Postgres tables: properties + property_images
 *
 * Usage (PowerShell):
 *   $env:SUPABASE_URL="https://<ref>.supabase.co"
 *   $env:SUPABASE_SECRET_KEY="sb_secret_..."
 *   $env:SUPABASE_BUCKET="property-images"   # optional
 *   node scripts/migrate-sqlite-to-supabase.js
 */
const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SECRET_KEY =
  process.env.SUPABASE_SECRET_KEY ||
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SERVICE_KEY ||
  process.env.SUPABASE_SECRET ||
  process.env.SUPABASE_KEY ||
  "";
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || "property-images";

if (!SUPABASE_URL || !SUPABASE_SECRET_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SECRET_KEY in env.");
  process.exit(1);
}

function fetchWithTimeout(url, init) {
  const controller = new AbortController();
  const timeoutMs = Number(process.env.SUPABASE_FETCH_TIMEOUT_MS) || 15000;
  const t = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { ...(init || {}), signal: controller.signal }).finally(() =>
    clearTimeout(t)
  );
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
  global: { fetch: fetchWithTimeout }
});

const repoRoot = path.join(__dirname, "..");
const uploadsDir = path.join(repoRoot, "uploads");
const sqlitePath = path.join(repoRoot, "properties.db");

function safeName(s) {
  return String(s || "")
    .trim()
    .replace(/[^\w.\-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 120);
}

async function uploadFile(localFilename, destPath) {
  const full = path.join(uploadsDir, localFilename);
  if (!fs.existsSync(full)) return null;
  const buf = fs.readFileSync(full);
  const contentType =
    localFilename.toLowerCase().endsWith(".png")
      ? "image/png"
      : localFilename.toLowerCase().endsWith(".webp")
        ? "image/webp"
        : localFilename.toLowerCase().endsWith(".gif")
          ? "image/gif"
          : "image/jpeg";

  const { error } = await supabase.storage.from(SUPABASE_BUCKET).upload(destPath, buf, {
    contentType,
    upsert: true
  });
  if (error) throw error;
  return destPath;
}

async function main() {
  // Lazy load sqlite native module only for local run
  // eslint-disable-next-line global-require
  const Database = require("better-sqlite3");
  if (!fs.existsSync(sqlitePath)) {
    console.error("Missing properties.db at", sqlitePath);
    process.exit(1);
  }
  if (!fs.existsSync(uploadsDir)) {
    console.error("Missing uploads/ at", uploadsDir);
    process.exit(1);
  }

  const db = new Database(sqlitePath, { readonly: true });
  const props = db.prepare("SELECT * FROM properties").all();
  const images = db
    .prepare("SELECT * FROM property_images ORDER BY property_id, image_order, id")
    .all();
  const imgsByProp = new Map();
  for (const img of images) {
    const arr = imgsByProp.get(img.property_id) || [];
    arr.push(img);
    imgsByProp.set(img.property_id, arr);
  }

  console.log(`Found ${props.length} properties in SQLite.`);

  for (const p of props) {
    // Insert property row first (without display_image), then upload and update.
    const insertRow = {
      name: p.name,
      area: p.area,
      status: p.status,
      priority_group: p.priority_group || "medium",
      size: p.size || null,
      address: p.address || null,
      price: p.price || null,
      availability: p.availability || null,
      description: p.description || null,
      features: p.features || null,
      notes: p.notes || null,
      display_image: null,
      building_id: p.building_id || null,
      use_unit_details: p.use_unit_details == null ? 1 : Number(p.use_unit_details),
      broker_id: p.broker_id || null,
      video_filename: null,
      youtube_video_id: p.youtube_video_id || null,
      power_phase: p.power_phase || null,
      power_amps: p.power_amps || null,
      height_eave_apex: p.height_eave_apex || null,
      height_eave_roller_shutter: p.height_eave_roller_shutter || null,
      parking_bays: p.parking_bays || null,
      yard_space: p.yard_space || null,
      property_type: p.property_type || "industrial",
      latitude: p.latitude == null || p.latitude === "" ? null : Number(p.latitude),
      longitude: p.longitude == null || p.longitude === "" ? null : Number(p.longitude)
    };

    const { data: created, error: createErr } = await supabase
      .from("properties")
      .insert([insertRow])
      .select("id")
      .single();
    if (createErr) throw createErr;
    const newId = created.id;
    const prefix = `properties/${newId}`;

    // Upload cover
    let coverPath = null;
    if (p.display_image) {
      const ext = path.extname(p.display_image) || ".jpg";
      const dest = `${prefix}/cover-${safeName(p.name)}${ext}`;
      coverPath = await uploadFile(p.display_image, dest);
    }

    // Upload gallery and insert rows
    const gallery = imgsByProp.get(p.id) || [];
    for (let i = 0; i < gallery.length; i += 1) {
      const g = gallery[i];
      const ext = path.extname(g.filename) || ".jpg";
      const dest = `${prefix}/img-${String(i + 1).padStart(2, "0")}-${safeName(
        p.name
      )}${ext}`;
      const uploaded = await uploadFile(g.filename, dest);
      if (!uploaded) continue;
      const { error: imgErr } = await supabase.from("property_images").insert([
        { property_id: newId, filename: uploaded, image_order: g.image_order || i + 1 }
      ]);
      if (imgErr) throw imgErr;
    }

    // Update display_image after cover upload
    if (coverPath) {
      const { error: upErr } = await supabase
        .from("properties")
        .update({ display_image: coverPath })
        .eq("id", newId);
      if (upErr) throw upErr;
    }

    console.log(`Migrated SQLite property ${p.id} → Supabase ${newId}`);
  }

  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

