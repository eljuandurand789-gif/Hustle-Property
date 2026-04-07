/**
 * One-time migration:
 * - Reads local SQLite (properties.db + property_images)
 * - Uploads files from uploads/ to Supabase Storage bucket
 * - Inserts rows into Supabase Postgres tables: properties + property_images + agents + deals + agent_payouts
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

function stripOuterQuotes(s) {
  const v = String(s || "").trim();
  if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
    return v.slice(1, -1).trim();
  }
  return v;
}

const SUPABASE_PROJECT_REF =
  stripOuterQuotes(process.env.SUPABASE_PROJECT_REF) ||
  stripOuterQuotes(process.env.SUPABASE_REF) ||
  "";

let SUPABASE_URL = stripOuterQuotes(process.env.SUPABASE_URL) || "";
if (!SUPABASE_URL && SUPABASE_PROJECT_REF) {
  SUPABASE_URL = `https://${SUPABASE_PROJECT_REF}.supabase.co`;
}
// Some people paste just the ref into SUPABASE_URL by mistake; fix that too.
if (SUPABASE_URL && !/^https?:\/\//i.test(SUPABASE_URL) && /^[a-z0-9-]{6,}$/i.test(SUPABASE_URL)) {
  SUPABASE_URL = `https://${SUPABASE_URL}.supabase.co`;
}
const SUPABASE_SECRET_KEY =
  stripOuterQuotes(process.env.SUPABASE_SECRET_KEY) ||
  stripOuterQuotes(process.env.SUPABASE_SERVICE_ROLE_KEY) ||
  stripOuterQuotes(process.env.SUPABASE_SERVICE_KEY) ||
  stripOuterQuotes(process.env.SUPABASE_SECRET) ||
  stripOuterQuotes(process.env.SUPABASE_KEY) ||
  "";
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || "property-images";

if (!SUPABASE_URL || !/^https?:\/\/.+/i.test(SUPABASE_URL) || !SUPABASE_SECRET_KEY) {
  console.error("Missing/invalid Supabase env vars.");
  console.error("Set either SUPABASE_URL (full https://...supabase.co) OR SUPABASE_PROJECT_REF.");
  console.error("Also set SUPABASE_SECRET_KEY to your sb_secret_... value.");
  console.error("");
  console.error("Current values:");
  console.error("SUPABASE_URL =", SUPABASE_URL || "(empty)");
  console.error("SUPABASE_PROJECT_REF =", SUPABASE_PROJECT_REF || "(empty)");
  console.error("SUPABASE_SECRET_KEY =", SUPABASE_SECRET_KEY ? "(set)" : "(empty)");
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

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function getMissingColumnFromPgrstMessage(msg) {
  const m = String(msg || "").match(/Could not find the '([^']+)' column/i);
  return m ? m[1] : null;
}

function getMissingColumnFromPg42703Message(msg) {
  // Example: "column property_images.filename does not exist"
  const m = String(msg || "").match(/column\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s+does not exist/i);
  return m ? m[2] : null;
}

async function insertWithDropUnknownColumns(table, row, returning = "id") {
  // PostgREST throws PGRST204 when a column doesn't exist in the schema cache.
  // This lets migration proceed even if your table is missing some columns.
  const payload = clone(row);
  const dropped = [];
  for (let tries = 0; tries < 30; tries += 1) {
    // eslint-disable-next-line no-await-in-loop
    const { data, error } = await supabase
      .from(table)
      .insert([payload])
      .select(returning)
      .single();
    if (!error) return { data, dropped };
    const missing =
      error.code === "PGRST204"
        ? getMissingColumnFromPgrstMessage(error.message)
        : String(error.code) === "42703"
          ? getMissingColumnFromPg42703Message(error.message)
          : null;
    if (missing && Object.prototype.hasOwnProperty.call(payload, missing)) {
      dropped.push(missing);
      delete payload[missing];
      // retry
      // eslint-disable-next-line no-continue
      continue;
    }
    throw error;
  }
  throw new Error(`Too many retries inserting into ${table}. Dropped: ${dropped.join(", ")}`);
}

async function updateWithDropUnknownColumns(table, matchCol, matchVal, row) {
  const payload = clone(row);
  const dropped = [];
  for (let tries = 0; tries < 30; tries += 1) {
    // eslint-disable-next-line no-await-in-loop
    const { error } = await supabase.from(table).update(payload).eq(matchCol, matchVal);
    if (!error) return { dropped };
    const missing =
      error.code === "PGRST204"
        ? getMissingColumnFromPgrstMessage(error.message)
        : String(error.code) === "42703"
          ? getMissingColumnFromPg42703Message(error.message)
          : null;
    if (missing && Object.prototype.hasOwnProperty.call(payload, missing)) {
      dropped.push(missing);
      delete payload[missing];
      // eslint-disable-next-line no-continue
      continue;
    }
    throw error;
  }
  throw new Error(`Too many retries updating ${table}. Dropped: ${dropped.join(", ")}`);
}

function safeName(s) {
  return String(s || "")
    .trim()
    .replace(/[^\w.\-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 120);
}

function safeSlug(s) {
  const base = String(s || "")
    .trim()
    .toLowerCase()
    .replace(/['"]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  return base || `agent-${Date.now()}`;
}

async function uploadFile(localFilename, destPath) {
  const full = path.join(uploadsDir, localFilename);
  if (!fs.existsSync(full)) return null;
  const buf = fs.readFileSync(full);
  const lower = localFilename.toLowerCase();
  const contentType =
    lower.endsWith(".png")
      ? "image/png"
      : lower.endsWith(".webp")
        ? "image/webp"
        : lower.endsWith(".gif")
          ? "image/gif"
          : lower.endsWith(".jpg") || lower.endsWith(".jpeg")
            ? "image/jpeg"
            : "application/octet-stream";

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
  const agents = db.prepare("SELECT * FROM agents ORDER BY id").all();
  const deals = db.prepare("SELECT * FROM deals ORDER BY id").all();
  const payouts = db.prepare("SELECT * FROM agent_payouts ORDER BY id").all();
  const imgsByProp = new Map();
  for (const img of images) {
    const arr = imgsByProp.get(img.property_id) || [];
    arr.push(img);
    imgsByProp.set(img.property_id, arr);
  }

  console.log(`Found ${props.length} properties in SQLite.`);
  console.log(`Found ${agents.length} agents in SQLite.`);
  console.log(`Found ${deals.length} deals in SQLite.`);
  console.log(`Found ${payouts.length} agent payouts in SQLite.`);

  const skipExisting = process.argv.includes("--skip-existing") || true;

  // 1) Agents: build SQLite id -> Supabase id map (use slug unique)
  const agentIdMap = new Map();
  for (const a of agents) {
    const slug = a.slug ? String(a.slug).trim() : safeSlug(a.name);
    const name = a.name ? String(a.name).trim() : slug;
    // eslint-disable-next-line no-await-in-loop
    const { data: existing, error: exErr } = await supabase
      .from("agents")
      .select("id,slug")
      .eq("slug", slug)
      .limit(1)
      .maybeSingle();
    if (exErr && String(exErr.code) !== "42703") throw exErr;
    let sbId = existing && existing.id ? existing.id : null;
    if (!sbId) {
      // eslint-disable-next-line no-await-in-loop
      const { data: created } = await insertWithDropUnknownColumns(
        "agents",
        { name, slug },
        "id"
      );
      sbId = created.id;
      console.log(`Migrated SQLite agent ${a.id} → Supabase ${sbId} (${slug})`);
    } else {
      console.log(`Skip existing agent (already in Supabase): SQLite ${a.id} → Supabase ${sbId} (${slug})`);
    }
    agentIdMap.set(a.id, sbId);
  }

  for (const p of props) {
    if (skipExisting) {
      // Best-effort de-dupe: match by (name, area, address-if-present).
      let q = supabase.from("properties").select("id").eq("name", p.name).eq("area", p.area);
      const addr = p.address && String(p.address).trim();
      if (addr) q = q.eq("address", addr);
      // eslint-disable-next-line no-await-in-loop
      const { data: existingRow, error: exErr } = await q.limit(1).maybeSingle();
      if (exErr && String(exErr.code) !== "42703") {
        throw exErr;
      }
      if (existingRow && existingRow.id) {
        console.log(`Skip existing property (already in Supabase): SQLite ${p.id} → Supabase ${existingRow.id}`);
        // eslint-disable-next-line no-continue
        continue;
      }
    }

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

    const { data: created, dropped: droppedPropCols } = await insertWithDropUnknownColumns(
      "properties",
      insertRow,
      "id"
    );
    const newId = created.id;
    const prefix = `properties/${newId}`;
    if (droppedPropCols.length) {
      console.log(
        `Note: properties columns missing in Supabase (skipped): ${droppedPropCols.join(", ")}`
      );
    }

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
      const imgRow = {
        property_id: newId,
        storage_path: uploaded,
        image_order: g.image_order || i + 1
      };
      const { dropped: droppedImgCols } = await insertWithDropUnknownColumns(
        "property_images",
        imgRow,
        "id"
      );
      if (droppedImgCols.length) {
        console.log(
          `Note: property_images columns missing in Supabase (skipped): ${droppedImgCols.join(
            ", "
          )}`
        );
      }
    }

    // Update display_image after cover upload
    if (coverPath) {
      await updateWithDropUnknownColumns("properties", "id", newId, {
        display_image: coverPath
      });
    }

    console.log(`Migrated SQLite property ${p.id} → Supabase ${newId}`);
  }

  // 2) Deals (use agent_id mapping). Upload deal_image if present.
  for (const d of deals) {
    const sbAgentId = agentIdMap.get(d.agent_id);
    if (!sbAgentId) {
      console.log(`Skip deal ${d.id}: unknown agent_id ${d.agent_id}`);
      // eslint-disable-next-line no-continue
      continue;
    }

    if (skipExisting) {
      // Best-effort de-dupe: match by agent_id + property_address + deal_date + invoice_total (when present)
      let q = supabase
        .from("deals")
        .select("id")
        .eq("agent_id", sbAgentId)
        .eq("property_address", d.property_address || "");
      if (d.deal_date && String(d.deal_date).trim()) q = q.eq("deal_date", String(d.deal_date).trim());
      if (d.invoice_total != null && d.invoice_total !== "") q = q.eq("invoice_total", Number(d.invoice_total));
      // eslint-disable-next-line no-await-in-loop
      const { data: existing, error: exErr } = await q.limit(1).maybeSingle();
      if (exErr && String(exErr.code) !== "42703") throw exErr;
      if (existing && existing.id) {
        console.log(`Skip existing deal (already in Supabase): SQLite ${d.id} → Supabase ${existing.id}`);
        // eslint-disable-next-line no-continue
        continue;
      }
    }

    const insertRow = {
      agent_id: sbAgentId,
      property_name: d.property_name || null,
      property_address: d.property_address || "",
      deal_date: d.deal_date || null,
      lease_period: d.lease_period || null,
      link_url: d.link_url || null,
      asking_rental: d.asking_rental || null,
      actual_rental: d.actual_rental || null,
      escalation_period: d.escalation_period || null,
      invoice_total:
        d.invoice_total == null || d.invoice_total === "" ? null : Number(d.invoice_total),
      agent_share_percent:
        d.agent_share_percent == null || d.agent_share_percent === ""
          ? 50
          : Number(d.agent_share_percent),
      notes: d.notes || null,
      deal_image: null,
      lease_start_date: d.lease_start_date || null,
      lease_end_date: d.lease_end_date || null,
      deal_amount_type: d.deal_amount_type || "net_before_tax",
      is_expected: d.is_expected == null ? 0 : Number(d.is_expected) ? 1 : 0,
      beneficial_occupation_date: d.beneficial_occupation_date || null,
      lease_commencement_date: d.lease_commencement_date || null,
      map_latitude:
        d.map_latitude == null || d.map_latitude === "" ? null : Number(d.map_latitude),
      map_longitude:
        d.map_longitude == null || d.map_longitude === "" ? null : Number(d.map_longitude),
      show_on_done_deals:
        d.show_on_done_deals == null ? 0 : Number(d.show_on_done_deals) ? 1 : 0
    };

    // eslint-disable-next-line no-await-in-loop
    const { data: created, dropped: droppedCols } = await insertWithDropUnknownColumns(
      "deals",
      insertRow,
      "id"
    );
    if (droppedCols.length) {
      console.log(`Note: deals columns missing in Supabase (skipped): ${droppedCols.join(", ")}`);
    }
    const sbDealId = created.id;

    // Upload deal image if present
    if (d.deal_image) {
      const ext = path.extname(String(d.deal_image)) || ".jpg";
      const dest = `deals/${sbDealId}/deal-${safeName(d.property_address || d.property_name || "deal")}${ext}`;
      // eslint-disable-next-line no-await-in-loop
      const uploaded = await uploadFile(String(d.deal_image), dest);
      if (uploaded) {
        // eslint-disable-next-line no-await-in-loop
        await updateWithDropUnknownColumns("deals", "id", sbDealId, { deal_image: uploaded });
      }
    }

    console.log(`Migrated SQLite deal ${d.id} → Supabase ${sbDealId}`);
  }

  // 3) Agent payouts
  for (const p of payouts) {
    const sbAgentId = agentIdMap.get(p.agent_id);
    if (!sbAgentId) {
      console.log(`Skip payout ${p.id}: unknown agent_id ${p.agent_id}`);
      // eslint-disable-next-line no-continue
      continue;
    }

    if (skipExisting) {
      let q = supabase
        .from("agent_payouts")
        .select("id")
        .eq("agent_id", sbAgentId)
        .eq("payout_date", p.payout_date || "");
      if (p.amount != null && p.amount !== "") q = q.eq("amount", Number(p.amount));
      // eslint-disable-next-line no-await-in-loop
      const { data: existing, error: exErr } = await q.limit(1).maybeSingle();
      if (exErr && String(exErr.code) !== "42703") throw exErr;
      if (existing && existing.id) {
        console.log(`Skip existing payout (already in Supabase): SQLite ${p.id} → Supabase ${existing.id}`);
        // eslint-disable-next-line no-continue
        continue;
      }
    }

    const row = {
      agent_id: sbAgentId,
      payout_date: p.payout_date || "",
      amount: p.amount == null || p.amount === "" ? 0 : Number(p.amount),
      notes: p.notes || null
    };
    // eslint-disable-next-line no-await-in-loop
    const { data: created, dropped } = await insertWithDropUnknownColumns(
      "agent_payouts",
      row,
      "id"
    );
    if (dropped.length) {
      console.log(`Note: agent_payouts columns missing in Supabase (skipped): ${dropped.join(", ")}`);
    }
    console.log(`Migrated SQLite payout ${p.id} → Supabase ${created.id}`);
  }

  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

