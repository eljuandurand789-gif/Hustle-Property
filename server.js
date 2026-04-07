const express = require("express");
const http = require("http");
const os = require("os");
const crypto = require("crypto");
const session = require("express-session");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { createClient } = require("@supabase/supabase-js");

const app = express();
if (process.env.VERCEL) {
  app.set("trust proxy", 1);
}
const PORT = Number(process.env.PORT) || 3000;
/** Listen on all interfaces so phones/tablets on the same Wi‑Fi can connect. Override with HOST=127.0.0.1 for local-only. */
const HOST = process.env.HOST || "0.0.0.0";

function lanIPv4Addresses() {
  const nets = os.networkInterfaces();
  const out = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      const v4 = net.family === "IPv4" || net.family === 4;
      if (v4 && !net.internal) {
        out.push(net.address);
      }
    }
  }
  return out;
}

// Geocoding: set GOOGLE_MAPS_API_KEY in the environment for Google Geocoding (accurate pins).
// Without it, OpenStreetMap Nominatim is used as a fallback.

// folders
const uploadsDir = process.env.VERCEL
  ? path.join(os.tmpdir(), "uploads")
  : path.join(__dirname, "uploads");
const publicDir = path.join(__dirname, "public");
const viewsDir = path.join(__dirname, "views");

if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

// Supabase (optional; used when env vars are set)
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SECRET_KEY =
  process.env.SUPABASE_SECRET_KEY ||
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SERVICE_KEY ||
  process.env.SUPABASE_SECRET ||
  process.env.SUPABASE_KEY ||
  "";
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || "property-images";
const useSupabase = Boolean(SUPABASE_URL && SUPABASE_SECRET_KEY);
function fetchWithTimeout(url, init) {
  const controller = new AbortController();
  const timeoutMs = Number(process.env.SUPABASE_FETCH_TIMEOUT_MS) || 8000;
  const t = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { ...(init || {}), signal: controller.signal }).finally(() =>
    clearTimeout(t)
  );
}
const supabase = useSupabase
  ? createClient(SUPABASE_URL, SUPABASE_SECRET_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
      global: { fetch: fetchWithTimeout }
    })
  : null;

// SQLite (only used when Supabase is not enabled)
let db = null;
if (!useSupabase) {
  // Lazy require so Supabase/Vercel deployments don't load native sqlite binaries.
  // Native module cold starts can cause FUNCTION_INVOCATION_TIMEOUT.
  // eslint-disable-next-line global-require
  const Database = require("better-sqlite3");
  db = new Database(path.join(__dirname, "properties.db"));
  db.pragma("foreign_keys = ON");
}

function supabasePublicObjectUrl(storagePath) {
  const p = String(storagePath || "").trim().replace(/^\/+/, "");
  if (!p) return null;
  if (/^https?:\/\//i.test(p)) return p;
  return `${SUPABASE_URL.replace(/\/+$/, "")}/storage/v1/object/public/${encodeURIComponent(
    SUPABASE_BUCKET
  )}/${p}`;
}

// tables (SQLite only)
if (!useSupabase) {
db.exec(`
CREATE TABLE IF NOT EXISTS admins (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS properties (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  area TEXT NOT NULL,
  status TEXT NOT NULL,
  priority_group TEXT NOT NULL DEFAULT 'medium',
  size TEXT,
  address TEXT,
  price TEXT,
  availability TEXT,
  description TEXT,
  features TEXT,
  notes TEXT,
  display_image TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS property_images (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  property_id INTEGER NOT NULL,
  filename TEXT NOT NULL,
  image_order INTEGER NOT NULL DEFAULT 0
);
`);

// Migrate older DBs: property_images existed without image_order before reorder feature
const propertyImageCols = db.prepare("PRAGMA table_info(property_images)").all();
const hasImageOrder = propertyImageCols.some((c) => c.name === "image_order");
if (!hasImageOrder) {
  db.exec(
    "ALTER TABLE property_images ADD COLUMN image_order INTEGER NOT NULL DEFAULT 0"
  );
}

db.exec(`
CREATE TABLE IF NOT EXISTS agents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  slug TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS deals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id INTEGER NOT NULL,
  property_name TEXT,
  property_address TEXT NOT NULL,
  deal_date TEXT,
  lease_period TEXT,
  link_url TEXT,
  asking_rental TEXT,
  actual_rental TEXT,
  escalation_period TEXT,
  invoice_total REAL,
  agent_share_percent REAL NOT NULL DEFAULT 50,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (agent_id) REFERENCES agents(id)
);
`);

db.exec(`
CREATE TABLE IF NOT EXISTS agent_payouts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id INTEGER NOT NULL,
  payout_date TEXT NOT NULL,
  amount REAL NOT NULL,
  notes TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (agent_id) REFERENCES agents(id)
);
`);

const seedEj = db.prepare("SELECT id FROM agents WHERE slug = ?").get("ej-durand");
if (!seedEj) {
  db.prepare("INSERT INTO agents (name, slug) VALUES (?, ?)").run(
    "EJ Durand",
    "ej-durand"
  );
}
} // end sqlite-only schema/seed/migrations

if (!useSupabase) {
const adminRowCount = db.prepare("SELECT COUNT(*) AS c FROM admins").get().c;
if (!adminRowCount) {
  db.prepare("INSERT INTO admins (username, password) VALUES (?, ?)").run(
    "admin",
    "hustle"
  );
  db.prepare("INSERT INTO admins (username, password) VALUES (?, ?)").run(
    "ejdurand",
    "hustle"
  );
}

const dealColInfo = db.prepare("PRAGMA table_info(deals)").all();
const dealColNames = new Set(dealColInfo.map((c) => c.name));
if (!dealColNames.has("deal_image")) {
  db.exec("ALTER TABLE deals ADD COLUMN deal_image TEXT");
}
if (!dealColNames.has("lease_start_date")) {
  db.exec("ALTER TABLE deals ADD COLUMN lease_start_date TEXT");
}
if (!dealColNames.has("lease_end_date")) {
  db.exec("ALTER TABLE deals ADD COLUMN lease_end_date TEXT");
}
if (!dealColNames.has("deal_amount_type")) {
  db.exec(
    "ALTER TABLE deals ADD COLUMN deal_amount_type TEXT NOT NULL DEFAULT 'net_before_tax'"
  );
}
if (!dealColNames.has("is_expected")) {
  db.exec("ALTER TABLE deals ADD COLUMN is_expected INTEGER NOT NULL DEFAULT 0");
}
if (!dealColNames.has("beneficial_occupation_date")) {
  db.exec("ALTER TABLE deals ADD COLUMN beneficial_occupation_date TEXT");
}
if (!dealColNames.has("lease_commencement_date")) {
  db.exec("ALTER TABLE deals ADD COLUMN lease_commencement_date TEXT");
}
if (!dealColNames.has("map_latitude")) {
  db.exec("ALTER TABLE deals ADD COLUMN map_latitude REAL");
}
if (!dealColNames.has("map_longitude")) {
  db.exec("ALTER TABLE deals ADD COLUMN map_longitude REAL");
}
if (!dealColNames.has("show_on_done_deals")) {
  db.exec(
    "ALTER TABLE deals ADD COLUMN show_on_done_deals INTEGER NOT NULL DEFAULT 0"
  );
}

const propColInfo = db.prepare("PRAGMA table_info(properties)").all();
const propColNames = new Set(propColInfo.map((c) => c.name));
if (!propColNames.has("latitude")) {
  db.exec("ALTER TABLE properties ADD COLUMN latitude REAL");
}
if (!propColNames.has("longitude")) {
  db.exec("ALTER TABLE properties ADD COLUMN longitude REAL");
}
if (!propColNames.has("building_id")) {
  db.exec("ALTER TABLE properties ADD COLUMN building_id INTEGER");
}
if (!propColNames.has("use_unit_details")) {
  db.exec("ALTER TABLE properties ADD COLUMN use_unit_details INTEGER NOT NULL DEFAULT 1");
}
if (!propColNames.has("video_filename")) {
  db.exec("ALTER TABLE properties ADD COLUMN video_filename TEXT");
}
if (!propColNames.has("youtube_video_id")) {
  db.exec("ALTER TABLE properties ADD COLUMN youtube_video_id TEXT");
}
if (!propColNames.has("power_phase")) {
  db.exec("ALTER TABLE properties ADD COLUMN power_phase TEXT");
}
if (!propColNames.has("power_amps")) {
  db.exec("ALTER TABLE properties ADD COLUMN power_amps TEXT");
}
if (!propColNames.has("height_eave_apex")) {
  db.exec("ALTER TABLE properties ADD COLUMN height_eave_apex TEXT");
}
if (!propColNames.has("height_eave_roller_shutter")) {
  db.exec("ALTER TABLE properties ADD COLUMN height_eave_roller_shutter TEXT");
}
if (!propColNames.has("parking_bays")) {
  db.exec("ALTER TABLE properties ADD COLUMN parking_bays TEXT");
}
if (!propColNames.has("yard_space")) {
  db.exec("ALTER TABLE properties ADD COLUMN yard_space TEXT");
}
if (!propColNames.has("broker_id")) {
  db.exec(
    "ALTER TABLE properties ADD COLUMN broker_id INTEGER REFERENCES agents(id) ON DELETE SET NULL"
  );
}
if (!propColNames.has("property_type")) {
  db.exec(
    "ALTER TABLE properties ADD COLUMN property_type TEXT NOT NULL DEFAULT 'industrial'"
  );
}
try {
  db.prepare(
    "UPDATE properties SET property_type = 'industrial' WHERE property_type IS NULL OR TRIM(property_type) = ''"
  ).run();
} catch (e) {
  console.error("properties.property_type backfill:", e.message);
}

db.exec(`
CREATE TABLE IF NOT EXISTS home_featured_slots (
  slot INTEGER PRIMARY KEY CHECK (slot IN (1, 2)),
  property_id INTEGER,
  FOREIGN KEY (property_id) REFERENCES properties(id) ON DELETE SET NULL
);
`);

try {
  const hfCols = db.prepare("PRAGMA table_info(home_featured_slots)").all();
  if (!hfCols.some((c) => c.name === "feature_style")) {
    db.exec(
      "ALTER TABLE home_featured_slots ADD COLUMN feature_style TEXT DEFAULT 'orbit'"
    );
  }
} catch (e) {
  console.error("home_featured_slots.feature_style migration:", e.message);
}

db.exec(`
CREATE TABLE IF NOT EXISTS property_enquiries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  property_id INTEGER,
  property_label TEXT,
  property_address TEXT,
  enquirer_name TEXT NOT NULL,
  phone TEXT NOT NULL,
  email TEXT,
  message TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
`);

db.exec(`
CREATE TABLE IF NOT EXISTS buildings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT,
  size_text TEXT,
  features TEXT,
  display_image TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS building_images (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  building_id INTEGER NOT NULL,
  filename TEXT NOT NULL,
  image_order INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (building_id) REFERENCES buildings(id) ON DELETE CASCADE
);
`);
} // end sqlite-only migrations/seed

// app config
app.set("view engine", "ejs");
app.set("views", viewsDir);

// Default 100kb limit breaks long descriptions / many fields on property forms.
app.use(express.urlencoded({ extended: true, limit: "8mb" }));
app.use(express.json({ limit: "2mb" }));

const useStatelessAdminAuth = Boolean(process.env.VERCEL) || useSupabase;
const ADMIN_COOKIE_NAME = "hp_admin";
const ADMIN_COOKIE_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

function appendSetCookie(res, cookieValue) {
  const prev = res.getHeader("Set-Cookie");
  if (!prev) {
    res.setHeader("Set-Cookie", cookieValue);
    return;
  }
  if (Array.isArray(prev)) {
    res.setHeader("Set-Cookie", [...prev, cookieValue]);
    return;
  }
  res.setHeader("Set-Cookie", [String(prev), cookieValue]);
}

// Debug helper for Vercel auth issues (safe: doesn't reveal secret, only cookie presence + validity).
app.get("/admin/auth-debug", (req, res) => {
  const cookies = parseCookies(req);
  const raw = cookies[ADMIN_COOKIE_NAME] || "";
  const verified = verifyAdminToken(raw);
  res.type("json").send(
    JSON.stringify(
      {
        useStatelessAdminAuth,
        hasHpAdminCookie: Boolean(raw),
        hpAdminValid: Boolean(verified),
        hpAdminUsername: verified ? verified.username : null,
        hasSession: Boolean(req.session),
        sessionLoggedIn: Boolean(req.session && req.session.loggedIn),
        host: req.headers.host || null,
        proto: req.headers["x-forwarded-proto"] || null
      },
      null,
      2
    )
  );
});

function parseCookies(req) {
  const header = req.headers && req.headers.cookie ? String(req.headers.cookie) : "";
  const out = {};
  header.split(";").forEach((part) => {
    const s = part.trim();
    if (!s) return;
    const idx = s.indexOf("=");
    if (idx === -1) return;
    const k = s.slice(0, idx).trim();
    const v = s.slice(idx + 1).trim();
    out[k] = decodeURIComponent(v);
  });
  return out;
}

function signAdminToken(username) {
  const ts = Date.now();
  const base = `${username}.${ts}`;
  const secret = String(process.env.SESSION_SECRET || "hustle-property-secret-key");
  const sig = crypto.createHmac("sha256", secret).update(base).digest("hex");
  return `${base}.${sig}`;
}

function verifyAdminToken(token) {
  const raw = String(token || "");
  const parts = raw.split(".");
  if (parts.length < 3) return null;
  const sig = parts.pop();
  const ts = Number(parts.pop());
  const username = parts.join(".");
  if (!username || !Number.isFinite(ts)) return null;
  if (Date.now() - ts > ADMIN_COOKIE_MAX_AGE_MS) return null;
  const base = `${username}.${ts}`;
  const secret = String(process.env.SESSION_SECRET || "hustle-property-secret-key");
  const expected = crypto.createHmac("sha256", secret).update(base).digest("hex");
  try {
    const a = Buffer.from(sig);
    const b = Buffer.from(expected);
    if (a.length !== b.length) return null;
    if (!crypto.timingSafeEqual(a, b)) return null;
  } catch {
    return null;
  }
  return { username };
}

function setAdminCookie(res, username) {
  const token = signAdminToken(username);
  const secure = Boolean(process.env.VERCEL);
  const maxAge = Math.floor(ADMIN_COOKIE_MAX_AGE_MS / 1000);
  const parts = [
    `${ADMIN_COOKIE_NAME}=${encodeURIComponent(token)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
    `Max-Age=${maxAge}`
  ];
  if (secure) parts.push("Secure");
  appendSetCookie(res, parts.join("; "));
}

function clearAdminCookie(res) {
  const secure = Boolean(process.env.VERCEL);
  const parts = [
    `${ADMIN_COOKIE_NAME}=`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
    "Max-Age=0"
  ];
  if (secure) parts.push("Secure");
  appendSetCookie(res, parts.join("; "));
}

app.use(express.static(publicDir));
app.use("/uploads", express.static(uploadsDir));

// Fail fast on Vercel if Supabase isn't configured.
// This avoids cold-start hangs on native SQLite and makes misconfig obvious.
if (process.env.VERCEL && !useSupabase) {
  app.use((req, res) => {
    res
      .status(500)
      .type("text")
      .send(
        "Backend not configured: set SUPABASE_URL and SUPABASE_SECRET_KEY in Vercel Environment Variables, then redeploy."
      );
  });
}

// direct file routes
app.get("/styles.css", (req, res) => {
  res.sendFile(path.join(publicDir, "styles.css"));
});

// Avoid pointless serverless timeouts for missing favicons.
app.get("/favicon.ico", (req, res) => res.status(204).end());
app.get("/favicon.png", (req, res) => res.status(204).end());

// session (saveUninitialized: true so the session cookie is issued before login; login still sets loggedIn)
app.use(
  session({
    secret: process.env.SESSION_SECRET || "hustle-property-secret-key",
    resave: false,
    saveUninitialized: true,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: Boolean(process.env.VERCEL),
      maxAge: 7 * 24 * 60 * 60 * 1000
    }
  })
);

app.get("/health", (req, res) => {
  res.type("text").send("ok");
});

// Small request logger for debugging Vercel timeouts/errors.
app.use((req, res, next) => {
  req._rid = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  next();
});

// uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadsDir),
  filename: (req, file, cb) => {
    const safeName = file.originalname.replace(/\s+/g, "-");
    cb(null, `${Date.now()}-${safeName}`);
  }
});

const upload = multer({
  storage,
  limits: {
    fileSize: 25 * 1024 * 1024,
    // Total file parts per request (cover + all gallery files). Must be >= sum of per-field maxCount.
    files: 45
  }
});

/** Property form: images + optional large video (buildings/deals keep using `upload`). */
const uploadPropertyForm = multer({
  storage,
  limits: {
    fileSize: 200 * 1024 * 1024,
    files: 48
  },
  fileFilter: (req, file, cb) => {
    if (file.fieldname === "propertyVideo") {
      const ok =
        /^video\//.test(file.mimetype) ||
        /\.(mp4|webm|ogg|mov|mkv)$/i.test(file.originalname);
      if (!ok) {
        return cb(
          new Error(
            "Invalid upload: Choose a video file (MP4, WebM, Ogg, MOV, or another common format)."
          )
        );
      }
    }
    cb(null, true);
  }
});

// helpers
function requireLogin(req, res, next) {
  if (useStatelessAdminAuth) {
    const cookies = parseCookies(req);
    const raw = cookies[ADMIN_COOKIE_NAME];
    const ok = verifyAdminToken(raw);
    if (!ok) {
      if (raw) clearAdminCookie(res);
      return res.redirect("/admin/login");
    }
    req.adminUsername = ok.username;
    return next();
  }
  if (!req.session.loggedIn) return res.redirect("/admin/login");
  return next();
}

/** Accepts YouTube watch / embed / shorts / youtu.be URL or an 11-character id; returns id or null. */
function parseYoutubeVideoId(input) {
  if (input == null) return null;
  const raw = String(input).trim();
  if (!raw) return null;
  if (/^[a-zA-Z0-9_-]{11}$/.test(raw)) return raw;
  try {
    const base = /^https?:\/\//i.test(raw) ? undefined : "https://www.youtube.com";
    const u = new URL(raw, base);
    const host = (u.hostname || "").replace(/^www\./, "");
    if (host === "youtu.be") {
      const id = u.pathname.split("/").filter(Boolean)[0];
      return id && /^[a-zA-Z0-9_-]{11}$/.test(id) ? id : null;
    }
    if (
      host === "youtube.com" ||
      host === "m.youtube.com" ||
      host === "music.youtube.com"
    ) {
      if (u.pathname.startsWith("/embed/")) {
        const id = u.pathname.slice(7).split("/")[0]?.split("?")[0];
        return id && /^[a-zA-Z0-9_-]{11}$/.test(id) ? id : null;
      }
      if (u.pathname.startsWith("/shorts/")) {
        const id = u.pathname.slice(8).split("/")[0]?.split("?")[0];
        return id && /^[a-zA-Z0-9_-]{11}$/.test(id) ? id : null;
      }
      const v = u.searchParams.get("v");
      return v && /^[a-zA-Z0-9_-]{11}$/.test(v) ? v : null;
    }
  } catch (_) {
    /* ignore malformed URL */
  }
  return null;
}

async function sbListPropertyImages(propertyId) {
  if (!supabase) return [];
  async function run(sel) {
    return await supabase
      .from("property_images")
      .select(sel)
      .eq("property_id", propertyId)
      .order("image_order", { ascending: true })
      .order("id", { ascending: true });
  }
  // Prefer storage_path-only schema (what you have now). Fallback to filename if needed.
  let data;
  let error;
  ({ data, error } = await run("id, property_id, storage_path, image_order"));
  if (error && String(error.code) === "42703") {
    ({ data, error } = await run("id, property_id, filename, image_order"));
  }
  if (error) throw error;
  return (data || []).map((row) => ({
    ...row,
    filename: row.storage_path || row.filename
  }));
}

function enrichPropertyForRender(property, images) {
  // Matches existing shape expected by templates, but supports Supabase URLs too.
  const imgRows = (images || []).map((img) => ({
    ...img,
    url: supabasePublicObjectUrl(img.filename) || img.filename
  }));
  property.images = imgRows;
  const unitFiles = [];
  if (property.display_image) unitFiles.push(property.display_image);
  imgRows.forEach((img) => unitFiles.push(img.filename));
  const galleryFilenames = dedupeFilenames(unitFiles);
  property.galleryFilenames = galleryFilenames;
  property.galleryUrls = galleryFilenames.map((f) => supabasePublicObjectUrl(f) || f);
  property.displayImage =
    property.display_image || (imgRows && imgRows[0] ? imgRows[0].filename : null);
  property.displayImageUrl = supabasePublicObjectUrl(property.displayImage) || property.displayImage;
  property.cardImage = property.displayImage;
  property.cardImageUrl = supabasePublicObjectUrl(property.cardImage) || property.cardImage;
  property.statusLabel = formatStatusLabel(property.status);
  property.propertyTypeLabel = formatPropertyTypeLabel(property.property_type);
  return property;
}

async function sbGetPublicProperties(filters = {}) {
  const selectedArea = filters.area || "";
  const selectedStatus = filters.status || "";
  const selectedPropertyType = filters.propertyType || "";
  const search = filters.search || "";
  const ampsMin = parseFilterBound(filters.ampsMin);
  const heightMin = parseFilterBound(filters.heightMin);

  const parsed = parseSearchQuery(search);
  let effectiveArea = selectedArea;
  if (!effectiveArea && parsed.areaFromQuery) effectiveArea = parsed.areaFromQuery;
  const textForLike = getKeywordLikeTerm(search, selectedArea, parsed);

  async function runQuery(opts) {
    let q = supabase
      .from("properties")
      .select("*")
      .in("status", ["to-let", "for-sale"]);

    if (effectiveArea) q = q.eq("area", effectiveArea);
    if (selectedStatus) q = q.eq("status", selectedStatus);

    if (
      !opts.skipPropertyType &&
      (selectedPropertyType === "office" || selectedPropertyType === "industrial")
    ) {
      q = q.eq("property_type", selectedPropertyType);
    }

    if (textForLike) {
      const like = `%${textForLike}%`;
      const clauses = [
        `name.ilike.${like}`,
        `address.ilike.${like}`,
        `description.ilike.${like}`
      ];
      if (!opts.minimalSearch) {
        clauses.push(
          `size.ilike.${like}`,
          `price.ilike.${like}`,
          `availability.ilike.${like}`,
          `features.ilike.${like}`
        );
      }
      q = q.or(clauses.join(","));
    }

    return await q.order("id", { ascending: false }).limit(200);
  }

  let data;
  let error;
  ({ data, error } = await runQuery({ skipPropertyType: false, minimalSearch: false }));
  if (error && String(error.code) === "42703") {
    // Missing column in schema — retry a safer query.
    ({ data, error } = await runQuery({ skipPropertyType: true, minimalSearch: true }));
  }
  if (error) throw error;
  let rows = data || [];

  if (ampsMin != null) {
    rows = rows.filter((row) => {
      const a = parseAmpsToNumber(row.power_amps);
      return a != null && a >= ampsMin;
    });
  }
  if (heightMin != null) {
    rows = rows.filter((row) => {
      const h = getListingHeightMetresForFilter(row);
      return h != null && h >= heightMin;
    });
  }

  // Batch fetch images for these properties
  const ids = rows.map((r) => r.id).filter((n) => n != null);
  const imagesByProp = new Map();
  if (ids.length) {
    async function run(sel) {
      return await supabase
        .from("property_images")
        .select(sel)
        .in("property_id", ids)
        .order("image_order", { ascending: true })
        .order("id", { ascending: true });
    }
    let imgs;
    let imgErr;
    ({ data: imgs, error: imgErr } = await run("property_id, storage_path, image_order, id"));
    if (imgErr && String(imgErr.code) === "42703") {
      ({ data: imgs, error: imgErr } = await run("property_id, filename, image_order, id"));
    }
    if (imgErr) throw imgErr;
    (imgs || []).forEach((img) => {
      const normalized = { ...img, filename: img.storage_path || img.filename };
      const arr = imagesByProp.get(img.property_id) || [];
      arr.push(normalized);
      imagesByProp.set(img.property_id, arr);
    });
  }

  rows = rows.map((p) => enrichPropertyForRender(p, imagesByProp.get(p.id) || []));
  const high = shuffleArray(rows.filter((p) => p.priority_group === "high"));
  const medium = shuffleArray(rows.filter((p) => p.priority_group === "medium"));
  const low = shuffleArray(rows.filter((p) => p.priority_group === "low"));
  const other = shuffleArray(
    rows.filter(
      (p) =>
        p.priority_group !== "high" &&
        p.priority_group !== "medium" &&
        p.priority_group !== "low"
    )
  );
  return [...high, ...medium, ...low, ...other];
}

async function sbGetAdminProperties(area = "") {
  // Admin dashboard only needs summary fields. Avoid heavy joins / large payloads.
  async function run(sel) {
    let q = supabase
      .from("properties")
      .select(sel)
      .order("id", { ascending: false })
      .limit(150);
    if (area) q = q.eq("area", area);
    return await q;
  }

  let data;
  let error;
  ({ data, error } = await run(
    "id,name,area,status,priority_group,property_type,created_at"
  ));
  if (error && String(error.code) === "42703") {
    // Older schema: no priority_group/property_type yet.
    ({ data, error } = await run("id,name,area,status,created_at"));
  }
  if (error) throw error;
  let rows = data || [];
  rows = rows.map((p) => ({
    ...p,
    priority_group: p.priority_group || "medium",
    property_type: p.property_type || "industrial"
  }));
  rows = rows.map((p) => enrichPropertyForRender(p, []));
  // Match existing priority sort
  rows.sort((a, b) => {
    const rank = (x) =>
      x === "high" ? 1 : x === "medium" ? 2 : x === "low" ? 3 : 4;
    const ra = rank(a.priority_group);
    const rb = rank(b.priority_group);
    if (ra !== rb) return ra - rb;
    return Number(b.id || 0) - Number(a.id || 0);
  });
  return rows;
}

async function sbUploadMulterFile(file, storagePath) {
  const p = String(storagePath || "").replace(/^\/+/, "");
  const buf = fs.readFileSync(file.path);
  const { error } = await supabase.storage.from(SUPABASE_BUCKET).upload(p, buf, {
    contentType: file.mimetype || "application/octet-stream",
    upsert: true
  });
  // Always try to delete temp file afterward
  try {
    fs.unlinkSync(file.path);
  } catch (_) {}
  if (error) throw error;
  return p;
}

function getMissingColumnFromPg42703Message(msg) {
  const m = String(msg || "").match(/column\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s+does not exist/i);
  return m ? m[2] : null;
}

async function sbInsertWithDropUnknownColumns(table, row, returning = "id") {
  const payload = JSON.parse(JSON.stringify(row || {}));
  const dropped = [];
  for (let tries = 0; tries < 30; tries += 1) {
    // eslint-disable-next-line no-await-in-loop
    const { data, error } = await supabase.from(table).insert([payload]).select(returning).single();
    if (!error) return { data, dropped };
    const missing =
      String(error.code) === "42703"
        ? getMissingColumnFromPg42703Message(error.message)
        : error.code === "PGRST204"
          ? (String(error.message || "").match(/Could not find the '([^']+)' column/i) || [])[1]
          : null;
    if (missing && Object.prototype.hasOwnProperty.call(payload, missing)) {
      dropped.push(missing);
      delete payload[missing];
      continue;
    }
    throw error;
  }
  throw new Error(`Too many retries inserting into ${table}. Dropped: ${dropped.join(", ")}`);
}

async function sbUpdateWithDropUnknownColumns(table, matchCol, matchVal, row) {
  const payload = JSON.parse(JSON.stringify(row || {}));
  const dropped = [];
  for (let tries = 0; tries < 30; tries += 1) {
    // eslint-disable-next-line no-await-in-loop
    const { error } = await supabase.from(table).update(payload).eq(matchCol, matchVal);
    if (!error) return { dropped };
    const missing =
      String(error.code) === "42703"
        ? getMissingColumnFromPg42703Message(error.message)
        : error.code === "PGRST204"
          ? (String(error.message || "").match(/Could not find the '([^']+)' column/i) || [])[1]
          : null;
    if (missing && Object.prototype.hasOwnProperty.call(payload, missing)) {
      dropped.push(missing);
      delete payload[missing];
      continue;
    }
    throw error;
  }
  throw new Error(`Too many retries updating ${table}. Dropped: ${dropped.join(", ")}`);
}

async function sbGetAgents() {
  const { data, error } = await supabase.from("agents").select("*").order("name");
  if (error) throw error;
  return data || [];
}

async function sbGetDealsForAgent(agentId, yearStr) {
  let q = supabase.from("deals").select("*").eq("agent_id", agentId).order("id", { ascending: false });
  const { data, error } = await q;
  if (error) throw error;
  const rows = data || [];
  if (!yearStr || !/^\d{4}$/.test(String(yearStr).trim())) return rows;
  const y = parseInt(String(yearStr).trim(), 10);
  const ys = String(yearStr).trim();
  return rows.filter(
    (d) => dealBelongsToCalendarYear(d, y) && dealIncludedForAgentYearWindow(d, ys)
  );
}

async function sbGetAgentPayouts(agentId) {
  const { data, error } = await supabase
    .from("agent_payouts")
    .select("*")
    .eq("agent_id", agentId)
    .order("payout_date", { ascending: false })
    .order("id", { ascending: false });
  if (error) throw error;
  return data || [];
}

async function sbGetPropertiesForDealPrefill() {
  const { data, error } = await supabase
    .from("properties")
    .select("id, name, address, area, status")
    .order("area", { ascending: true })
    .order("name", { ascending: true })
    .limit(2000);
  if (error) throw error;
  return data || [];
}

async function sbGetListingsForFeaturedPicker() {
  const { data, error } = await supabase
    .from("properties")
    .select("id,name,area,status")
    .in("status", ["to-let", "for-sale"])
    .order("area", { ascending: true })
    .order("name", { ascending: true })
    .limit(2000);
  if (error) throw error;
  return data || [];
}

async function sbValidateFeaturedPropertyId(raw) {
  if (raw === undefined || raw === null || raw === "") return null;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return null;
  const { data, error } = await supabase
    .from("properties")
    .select("id,status")
    .eq("id", n)
    .maybeSingle();
  if (error) throw error;
  if (!data) return null;
  const st = String(data.status || "").trim();
  if (st !== "to-let" && st !== "for-sale") return null;
  return n;
}

async function sbSetFeaturedHomeSlot(slot, propertyId, featureStyle = "orbit") {
  const s = Number(slot);
  if (s !== 1 && s !== 2) throw new Error("Invalid slot");
  const style =
    String(featureStyle || "").trim().toLowerCase() === "api" ? "api" : "orbit";
  const row = {
    slot: s,
    property_id: propertyId == null ? null : Number(propertyId),
    feature_style: style
  };
  const { error } = await supabase
    .from("home_featured_slots")
    .upsert([row], { onConflict: "slot" });
  if (error) throw error;
}

async function sbGetBuildings() {
  const { data, error } = await supabase.from("buildings").select("*").order("name");
  if (error) throw error;
  return data || [];
}

async function sbGetBuildingImages(buildingId) {
  const { data, error } = await supabase
    .from("building_images")
    .select("id, building_id, storage_path, image_order")
    .eq("building_id", buildingId)
    .order("image_order", { ascending: true })
    .order("id", { ascending: true });
  if (error) throw error;
  return (data || []).map((row) => ({
    ...row,
    filename: row.storage_path
  }));
}

function shuffleArray(items) {
  const arr = [...items];
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

const areaFunFactCache = new Map();

function getLocalDateKey() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function hashString(s) {
  let h = 0;
  for (let i = 0; i < s.length; i += 1) {
    h = (h * 31 + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

function stripHtmlTags(s) {
  return String(s || "").replace(/<[^>]+>/g, " ");
}

function decodeHtmlEntities(s) {
  let out = stripHtmlTags(s);
  out = out.replace(/&nbsp;/gi, " ");
  out = out.replace(/&amp;/g, "&");
  out = out.replace(/&lt;/g, "<");
  out = out.replace(/&gt;/g, ">");
  out = out.replace(/&quot;/g, '"');
  out = out.replace(/&#39;/g, "'");
  out = out.replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)));
  out = out.replace(/&#x([0-9a-f]+);/gi, (_, h) =>
    String.fromCharCode(parseInt(h, 16))
  );
  return out.replace(/\s+/g, " ").trim();
}

function extractRssTag(block, tag) {
  const re = new RegExp(`<${tag}[^>\\s]*>([\\s\\S]*?)</${tag}>`, "i");
  const m = block.match(re);
  if (!m) return "";
  let inner = m[1].trim();
  const cd = inner.match(/^<!\[CDATA\[([\s\S]*?)\]\]>$/);
  if (cd) inner = cd[1].trim();
  return inner;
}

function parseGoogleNewsRssItems(xml) {
  const items = [];
  const itemRe = /<item\b[^>]*>([\s\S]*?)<\/item>/gi;
  let m;
  while ((m = itemRe.exec(xml)) !== null && items.length < 20) {
    const block = m[1];
    const title = extractRssTag(block, "title");
    const linkRaw = extractRssTag(block, "link")
      .replace(/^<!\[CDATA\[([\s\S]*?)\]\]>$/, "$1")
      .trim()
      .replace(/&amp;/g, "&");
    const description = extractRssTag(block, "description");
    if (title && linkRaw) {
      items.push({
        title: decodeHtmlEntities(title),
        link: linkRaw,
        snippet: decodeHtmlEntities(description).slice(0, 400)
      });
    }
  }
  return items;
}

function pickDailyIndex(len, areaKey, dateKey) {
  if (len <= 0) return 0;
  return hashString(`${areaKey}|${dateKey}`) % len;
}

function buildFallbackFunFact(areaLabel) {
  const a = areaLabel || "this corridor";
  const lines = [
    `Light industrial and logistics demand near ${a} often tracks port and motorway access — tenants weigh yard depth as heavily as internal height.`,
    `In nodes like ${a}, power availability (3‑phase, amps) can swing deal viability faster than a small rent change.`,
    `Commercial stock around ${a} is watched closely when the City updates zoning or bulk — worth asking what’s in the pipeline for the precinct.`,
    `Parking, roller-shutter height, and truck circulation separate “almost fits” from “works day one” in ${a}-area sheds.`,
    `Seasonal retail and freight peaks can quietly lift short-term demand for well-located units feeding ${a} and the Atlantic seaboard.`
  ];
  const idx = pickDailyIndex(lines.length, a, getLocalDateKey());
  return {
    ok: true,
    headline: "Local property pulse",
    snippet: lines[idx],
    sourceUrl: null,
    areaLabel: a,
    fromNews: false,
    dateKey: getLocalDateKey()
  };
}

async function fetchAreaNewsFunFact(areaRaw) {
  const areaLabel = String(areaRaw || "")
    .trim()
    .slice(0, 120);
  const areaKey = areaLabel || "Western Cape";
  const dateKey = getLocalDateKey();
  const cacheKey = `${dateKey}|${areaKey}`;
  if (areaFunFactCache.has(cacheKey)) {
    return areaFunFactCache.get(cacheKey);
  }

  const searchQ = `${areaKey} South Africa (industrial OR commercial OR warehouse OR property OR letting)`;
  const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(searchQ)}&hl=en-ZA&gl=ZA&ceid=ZA:en`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 12000);
  try {
    const res = await fetch(rssUrl, {
      signal: controller.signal,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        Accept: "application/rss+xml, application/xml, text/xml, */*"
      }
    });
    const xml = await res.text();
    if (!res.ok || !/<item\b/i.test(xml)) {
      const fb = buildFallbackFunFact(areaKey);
      areaFunFactCache.set(cacheKey, fb);
      return fb;
    }
    const parsed = parseGoogleNewsRssItems(xml);
    if (parsed.length === 0) {
      const fb = buildFallbackFunFact(areaKey);
      areaFunFactCache.set(cacheKey, fb);
      return fb;
    }
    const idx = pickDailyIndex(parsed.length, areaKey, dateKey);
    const pick = parsed[idx];
    const snippet =
      pick.snippet && pick.snippet.length > 24
        ? pick.snippet.replace(/^[^A-Za-z0-9]+/, "").slice(0, 280)
        : "";
    const out = {
      ok: true,
      headline: pick.title,
      snippet: snippet || `Headline from today’s news flow around ${areaKey}.`,
      sourceUrl: pick.link,
      areaLabel: areaKey,
      fromNews: true,
      dateKey
    };
    areaFunFactCache.set(cacheKey, out);
    return out;
  } catch {
    const fb = buildFallbackFunFact(areaKey);
    areaFunFactCache.set(cacheKey, fb);
    return fb;
  } finally {
    clearTimeout(timer);
  }
}

function getPropertyImages(propertyId) {
  return db
    .prepare(`
      SELECT *
      FROM property_images
      WHERE property_id = ?
      ORDER BY image_order ASC, id ASC
    `)
    .all(propertyId);
}

function getBuildingImages(buildingId) {
  return db
    .prepare(`
      SELECT *
      FROM building_images
      WHERE building_id = ?
      ORDER BY image_order ASC, id ASC
    `)
    .all(buildingId);
}

function getBuildingsList() {
  if (useSupabase) return [];
  return db.prepare("SELECT id, name FROM buildings ORDER BY name ASC").all();
}

function dedupeFilenames(files) {
  const seen = new Set();
  return files.filter((f) => {
    if (!f || seen.has(f)) return false;
    seen.add(f);
    return true;
  });
}

/** Maps DB / legacy values to select option values: to-let | for-sale | let | sold */
function normalizeStatusForForm(raw) {
  if (raw == null || raw === "") return "";
  const s = String(raw).trim().toLowerCase();
  const hyphen = s.replace(/\s+/g, "-");
  const aliases = {
    "to-let": "to-let",
    tolet: "to-let",
    "to let": "to-let",
    "for-sale": "for-sale",
    forsale: "for-sale",
    "for sale": "for-sale",
    let: "let",
    sold: "sold",
    "let-and-sold": "let"
  };
  if (aliases[s]) return aliases[s];
  if (aliases[hyphen]) return aliases[hyphen];
  const allowed = new Set(["to-let", "for-sale", "let", "sold"]);
  if (allowed.has(hyphen)) return hyphen;
  return hyphen;
}

/** Size: store as "4,500 m²"; form shows numeric part only (with comma thousands) */
function parseSizeForForm(value) {
  if (value == null || value === "") return "";
  const n = parseSizeToSquareMetres(value);
  if (n == null) return "";
  if (Math.abs(n - Math.round(n)) < 1e-9) {
    return Math.round(n).toLocaleString("en-US");
  }
  return n.toLocaleString("en-US", { maximumFractionDigits: 2 });
}

function formatSizeForSave(raw) {
  const v = raw != null ? String(raw).trim() : "";
  if (!v) return "";
  const cleaned = v.replace(/,/g, "");
  const num = cleaned.replace(/[^\d.]/g, "");
  if (!num) return "";
  const n = parseFloat(num);
  if (!Number.isFinite(n)) return "";
  const formatted =
    Math.abs(n - Math.round(n)) < 1e-9
      ? Math.round(n).toLocaleString("en-US")
      : n.toLocaleString("en-US", { maximumFractionDigits: 2 });
  return `${formatted} m²`;
}

/** Numeric m² from stored size string e.g. "9,090 m²" */
function parseSizeToSquareMetres(sizeStr) {
  if (sizeStr == null || sizeStr === "") return null;
  const s = String(sizeStr).trim().replace(/,/g, "");
  const m = s.match(/(\d+(?:\.\d+)?)/);
  if (!m) return null;
  const n = parseFloat(m[1]);
  return Number.isFinite(n) ? n : null;
}

/** First amps number from free text e.g. "60A", "80 A", "100" */
function parseAmpsToNumber(ampsStr) {
  if (ampsStr == null || ampsStr === "") return null;
  const s = String(ampsStr).trim();
  const m = s.match(/(\d+(?:\.\d+)?)/);
  if (!m) return null;
  const n = parseFloat(m[1]);
  return Number.isFinite(n) ? n : null;
}

/** First height number in metres e.g. "8.5 m", "4.2m" */
function parseHeightMetres(str) {
  if (str == null || str === "") return null;
  const s = String(str).trim();
  const m = s.match(/(\d+(?:\.\d+)?)/);
  if (!m) return null;
  const n = parseFloat(m[1]);
  return Number.isFinite(n) ? n : null;
}

/** Best single height (m) for filtering: max of apex and roller shutter eave when both set */
function getListingHeightMetresForFilter(row) {
  const apex = parseHeightMetres(row.height_eave_apex);
  const roller = parseHeightMetres(row.height_eave_roller_shutter);
  if (apex != null && roller != null) return Math.max(apex, roller);
  if (apex != null) return apex;
  if (roller != null) return roller;
  return null;
}

function parseFilterBound(raw) {
  if (raw === undefined || raw === null || raw === "") return null;
  const n = parseFloat(String(raw).replace(/,/g, "").trim());
  return Number.isFinite(n) && n >= 0 ? n : null;
}

/** Ensure display always includes m² where a single size number is intended */
function formatSizeDisplay(sizeStr) {
  if (sizeStr == null || sizeStr === "") return "";
  const s = String(sizeStr).trim();
  if (/\bm\s*²\b|\bm2\b|\bsqm\b/i.test(s)) {
    return s.replace(/\bm2\b/gi, "m²").replace(/\bsqm\b/gi, "m²");
  }
  if (/[-–—]/.test(s) && /\d/.test(s)) return s;
  const n = parseSizeToSquareMetres(s);
  if (n == null) return s;
  const formatted =
    Math.abs(n - Math.round(n)) < 1e-9
      ? Math.round(n).toLocaleString("en-US")
      : n.toLocaleString("en-US", { maximumFractionDigits: 2 });
  return `${formatted} m²`;
}

function getSimilarProperties(excludeId, sizeStr, limit = 6) {
  const target = parseSizeToSquareMetres(sizeStr);
  const rows = db
    .prepare(
      `
    SELECT * FROM properties
    WHERE id != ?
      AND status IN ('to-let', 'for-sale')
      AND area IN ('Maitland', 'Paarden Eiland')
  `
    )
    .all(excludeId);

  const enriched = rows.map((r) => enrichProperty(r));
  if (target == null) {
    return enriched.slice(0, limit);
  }
  return enriched
    .map((p) => {
      const ps = parseSizeToSquareMetres(p.size);
      const diff = ps == null ? Infinity : Math.abs(ps - target);
      return { p, diff, ps };
    })
    .filter((x) => x.ps != null && x.diff <= 500)
    .sort((a, b) => a.diff - b.diff)
    .slice(0, limit)
    .map((x) => x.p);
}

function getRecentEnquiries(limit = 150) {
  return db
    .prepare(
      `SELECT * FROM property_enquiries ORDER BY datetime(created_at) DESC, id DESC LIMIT ?`
    )
    .all(limit);
}

function formatMoneyCommas(n) {
  if (!Number.isFinite(n)) return "";
  const hasCents = Math.abs(n % 1) > 1e-9;
  return hasCents
    ? n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : Math.round(n).toLocaleString("en-US");
}

/**
 * Price: leading amount gets thousand commas; optional trailing text e.g. "per month".
 * Non-numeric values (e.g. "Price on request") are kept as-is without forcing "R ".
 */
function extractPriceParts(raw) {
  const v = raw != null ? String(raw).trim() : "";
  if (!v) return null;
  const cleaned = v.replace(/^\s*R\s*/i, "").replace(/\s+/g, " ").trim();
  if (!cleaned) return null;
  const re = /^([\d,]+(?:\.\d+)?)\s*(.*)$/;
  const m = cleaned.match(re);
  if (!m) return { kind: "text", text: cleaned };
  const n = parseFloat(m[1].replace(/,/g, ""));
  if (!Number.isFinite(n)) return { kind: "text", text: cleaned };
  return { kind: "num", num: n, rest: m[2].trim() };
}

/** Price: store as "R 81,810 per month"; form omits leading R */
function parsePriceForForm(value) {
  if (value == null || value === "") return "";
  const parts = extractPriceParts(value);
  if (!parts) return "";
  if (parts.kind === "text") return parts.text;
  const formatted = formatMoneyCommas(parts.num);
  return parts.rest ? `${formatted} ${parts.rest}` : formatted;
}

function formatPriceForSave(raw) {
  const parts = extractPriceParts(raw);
  if (!parts) return "";
  if (parts.kind === "text") {
    return parts.text;
  }
  const formatted = formatMoneyCommas(parts.num);
  const rest = parts.rest ? ` ${parts.rest}` : "";
  return `R ${formatted}${rest}`;
}

/**
 * Parses amounts like "R 45 000", "45 000 per month", "45,000.50" where
 * extractPriceParts fails (space thousands, etc.).
 */
function parseLooseMoneyAmount(raw) {
  if (raw == null) return null;
  let s = String(raw).trim();
  if (!s) return null;
  s = s.replace(/^\s*R\s*/i, "").trim();
  const m = s.match(/^([\d\s,\u00a0]+(?:\.\d+)?)/);
  if (!m) return null;
  const compact = m[1].replace(/[\s\u00a0]/g, "").replace(/,/g, "");
  const n = parseFloat(compact);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.round(n * 100) / 100;
}

/**
 * Single date for targets / which month & year a deal counts in:
 * lease commencement first, then lease start, deal date, beneficial occupation.
 */
function dealTargetReportingDateParts(deal) {
  if (!deal) return null;
  const pc = parseDealDateParts(deal.lease_commencement_date);
  if (pc) return pc;
  const pl = parseDealDateParts(deal.lease_start_date);
  if (pl) return pl;
  const pd = parseDealDateParts(deal.deal_date);
  if (pd) return pd;
  return parseDealDateParts(deal.beneficial_occupation_date);
}

/** Month index 0–11 for the selected report year (based on commencement-first date). */
function dealMonthIndexForYear(deal, yearNum) {
  if (!deal || !Number.isFinite(yearNum)) return null;
  const p = dealTargetReportingDateParts(deal);
  if (!p || p.y !== yearNum) return null;
  return p.mi;
}

/** Deal appears in calendar year when its commencement-first reporting date falls in that year. */
function dealBelongsToCalendarYear(deal, yearNum) {
  if (!deal || !Number.isFinite(yearNum)) return false;
  const p = dealTargetReportingDateParts(deal);
  return p != null && p.y === yearNum;
}

/** 2025 pipeline: only deals whose reporting month is July–December (same logic as monthly chart). */
function dealIncludedForAgentYearWindow(deal, yearStr) {
  if (String(yearStr).trim() !== "2025") return true;
  const mi = dealMonthIndexForYear(deal, 2025);
  return mi != null && mi >= 6;
}

/** YYYY-MM-DD from commencement-first chain (for sorting / filters). */
function dealReportingIsoDate(deal) {
  if (!deal) return "";
  const keys = [
    deal.lease_commencement_date,
    deal.lease_start_date,
    deal.deal_date,
    deal.beneficial_occupation_date
  ];
  for (const raw of keys) {
    const s = raw && String(raw).trim().slice(0, 10);
    if (s && /^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  }
  return "";
}

/** Normalize stored date strings to YYYY-MM-DD (ISO, DD/MM/YYYY, Date parse). */
function normalizeToIsoDateString(raw) {
  if (raw == null || raw === "") return "";
  const s = String(raw).trim();
  const iso10 = s.slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(iso10)) return iso10;
  const m = /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/.exec(s);
  if (m) {
    const day = parseInt(m[1], 10);
    const month = parseInt(m[2], 10) - 1;
    const year = parseInt(m[3], 10);
    const dt = new Date(year, month, day);
    if (!Number.isNaN(dt.getTime())) return formatDateYMDLocal(dt);
  }
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return formatDateYMDLocal(d);
  return "";
}

/**
 * Public track record: tolerates non-ISO deal_date; falls back to created_at.
 */
function dealReportingIsoDateForPublic(deal) {
  if (!deal) return "";
  const keys = [
    deal.lease_commencement_date,
    deal.lease_start_date,
    deal.deal_date,
    deal.beneficial_occupation_date,
    deal.created_at
  ];
  for (const raw of keys) {
    const iso = normalizeToIsoDateString(raw);
    if (iso) return iso;
  }
  return "";
}

function resolveDealAddressForPublic(d) {
  const a = d.property_address && String(d.property_address).trim();
  if (a) return a;
  const pid = propertyIdFromDealLinkUrl(d.link_url);
  if (!pid) return "";
  const p = db
    .prepare("SELECT address, area FROM properties WHERE id = ?")
    .get(pid);
  if (!p) return "";
  return [p.address, p.area].filter(Boolean).join(", ").trim();
}

function getDealsForAgentInMonth(agentId, yearStr, monthIndex0to11) {
  const deals = getDealsForAgent(agentId, yearStr);
  const y = parseInt(String(yearStr).trim(), 10);
  const filtered = deals.filter(
    (d) => dealMonthIndexForYear(d, y) === monthIndex0to11
  );
  return filtered.sort((a, b) =>
    dealReportingIsoDate(a).localeCompare(dealReportingIsoDate(b))
  );
}

function getMonthIndicesWithDeals(agentId, yearStr) {
  const out = [];
  for (let i = 0; i < 12; i++) {
    const list = getDealsForAgentInMonth(agentId, yearStr, i);
    if (list.length) out.push({ monthIndex: i, count: list.length });
  }
  return out;
}

/** Extract listing id from deal link_url (e.g. …/property/42). */
function propertyIdFromDealLinkUrl(url) {
  if (url == null) return null;
  const m = String(url).trim().match(/\/property\/(\d+)/i);
  return m ? Number(m[1]) : null;
}

function getLatLngFromPropertyId(propertyId) {
  const pid = Number(propertyId);
  if (!Number.isFinite(pid) || pid <= 0) return null;
  const p = db
    .prepare("SELECT latitude, longitude FROM properties WHERE id = ?")
    .get(pid);
  if (!p) return null;
  const lat = Number(p.latitude);
  const lng = Number(p.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  return { lat, lng };
}

function persistDealMapCoords(dealId, lat, lng) {
  const id = Number(dealId);
  if (!Number.isFinite(id) || id <= 0) return;
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
  try {
    db.prepare(
      "UPDATE deals SET map_latitude = ?, map_longitude = ? WHERE id = ?"
    ).run(lat, lng, id);
  } catch (e) {
    console.error("persistDealMapCoords:", e.message);
  }
}

/** After save: copy coords from linked listing when available. */
function syncDealMapCoordsFromPropertyLink(dealId) {
  const d = db.prepare("SELECT * FROM deals WHERE id = ?").get(dealId);
  if (!d) return;
  const pid = propertyIdFromDealLinkUrl(d.link_url);
  if (!pid) return;
  const ll = getLatLngFromPropertyId(pid);
  if (!ll) return;
  persistDealMapCoords(dealId, ll.lat, ll.lng);
}

/**
 * Exact pin: linked listing coords first, then Google/Nominatim geocode of full
 * property address (preferred over stale cache when fixing map accuracy).
 */
async function resolveDealExactLatLng(deal) {
  const addr = deal.property_address && String(deal.property_address).trim();
  const pid = propertyIdFromDealLinkUrl(deal.link_url);
  if (pid) {
    const fromProp = getLatLngFromPropertyId(pid);
    if (fromProp) {
      persistDealMapCoords(deal.id, fromProp.lat, fromProp.lng);
      return fromProp;
    }
  }
  if (process.env.GOOGLE_MAPS_API_KEY && addr) {
    const g = await geocodeAddressForProperty(addr, null);
    if (g) {
      persistDealMapCoords(deal.id, g.lat, g.lng);
      return g;
    }
  }
  const lat0 = Number(deal.map_latitude);
  const lng0 = Number(deal.map_longitude);
  if (Number.isFinite(lat0) && Number.isFinite(lng0)) {
    return { lat: lat0, lng: lng0 };
  }
  if (!addr) return null;
  if (!process.env.GOOGLE_MAPS_API_KEY) {
    await new Promise((r) => setTimeout(r, 1100));
  }
  const g2 = await geocodeAddressForProperty(addr, null);
  if (g2) {
    persistDealMapCoords(deal.id, g2.lat, g2.lng);
    return g2;
  }
  return null;
}

/** Suburb / area label for public done-deals (no street-level detail in UI). */
function publicAreaFromAddress(addr) {
  const s = String(addr || "").trim();
  if (!s) return "Cape Town industrial";
  const lower = s.toLowerCase();
  const suburbs = [
    "Paarden Eiland",
    "Maitland",
    "Epping",
    "Epping Industria",
    "Bellville",
    "Montague Gardens",
    "Parow",
    "Brackenfell",
    "Stikland",
    "Kraaifontein",
    "Blackheath",
    "Kuils River",
    "Ndabeni",
    "Pinelands",
    "Observatory",
    "Salt River",
    "Woodstock",
    "Century City",
    "Milnerton",
    "Airport Industria",
    "Atlantis",
    "Strand",
    "Somerset West",
    "Wetton",
    "Ottery",
    "Muizenberg",
    "Fish Hoek",
    "Tokai",
    "Constantia",
    "Athlone",
    "Goodwood",
    "Elsies River"
  ];
  for (const sub of suburbs) {
    if (lower.includes(sub.toLowerCase())) return sub;
  }
  const parts = s.split(",").map((x) => x.trim()).filter(Boolean);
  if (parts.length >= 2) {
    const pen = parts[parts.length - 2];
    if (pen.length >= 3 && pen.length <= 56 && !/^\d/.test(pen)) return pen;
  }
  const first = parts[0] || s;
  const words = first.split(/\s+/).filter(Boolean);
  if (words.length > 4) return words.slice(-3).join(" ");
  return first.length > 48 ? first.slice(0, 45).trim() + "…" : first;
}

/**
 * Best-effort monthly rent (ZAR) from deal rental text (actual_rental / asking_rental).
 * Strips thousand commas; treats explicit per-annum as monthly ÷ 12 when not clearly per-m².
 */
function parseMonthlyRentZar(rentalStr) {
  if (rentalStr == null || rentalStr === "") return null;
  const raw = String(rentalStr);
  const lower = raw.toLowerCase();
  const cleaned = raw.replace(/,/g, "").replace(/\s+/g, " ").trim();
  const isPerSqm = /\b(?:per|\/)\s*m(?:²|2)\b|\/m2|m²/i.test(raw);
  const m1 = cleaned.match(/R\s*([\d]+(?:\.\d+)?)/i);
  if (m1) {
    const n = parseFloat(m1[1]);
    if (Number.isFinite(n) && n > 0) {
      if (
        /\b(per\s*annum|p\.?\s*a\.?|annual|\/\s*yr|per\s*year)\b/i.test(lower) &&
        !isPerSqm
      ) {
        return n / 12;
      }
      return n;
    }
  }
  const m2 = cleaned.match(/([\d]+(?:\.\d+)?)\s*(?:\/|per|p\.?\s*m|pm|month)/i);
  if (m2) {
    const n = parseFloat(m2[1]);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function formatClosedMonthYearFromIso(iso) {
  if (!iso || String(iso).length < 10) return "";
  const d = new Date(`${String(iso).slice(0, 10)}T12:00:00`);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleDateString("en-ZA", { month: "long", year: "numeric" });
}

/** YYYY-MM-DD for local calendar today (done-deals copy: future close → "Closed for …"). */
function todayIsoDateLocal() {
  const n = new Date();
  const y = n.getFullYear();
  const m = String(n.getMonth() + 1).padStart(2, "0");
  const day = String(n.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * Suffix after "Closed " on done-deals cards: month–year, or "for June 2026" when the reporting date is still ahead.
 */
function formatDoneDealClosedSuffix(iso) {
  if (!iso || String(iso).length < 10) return "";
  const d = new Date(`${String(iso).slice(0, 10)}T12:00:00`);
  if (Number.isNaN(d.getTime())) return "";
  const dealDay = String(iso).slice(0, 10);
  const monthYear = formatClosedMonthYearFromIso(iso);
  if (dealDay > todayIsoDateLocal()) {
    return monthYear ? `for ${monthYear}` : "";
  }
  return monthYear;
}

/** Sum of lease term months where start/end or derived term exist. */
function leaseMonthsSignedForDeal(d) {
  const start = String(d.lease_commencement_date || d.lease_start_date || "")
    .trim()
    .slice(0, 10);
  const end = String(d.lease_end_date || "").trim().slice(0, 10);
  if (
    start &&
    end &&
    /^\d{4}-\d{2}-\d{2}$/.test(start) &&
    /^\d{4}-\d{2}-\d{2}$/.test(end)
  ) {
    const a = new Date(`${start}T12:00:00`);
    const b = new Date(`${end}T12:00:00`);
    if (!Number.isNaN(a.getTime()) && !Number.isNaN(b.getTime()) && b > a) {
      return Math.max(0, Math.round((b - a) / (1000 * 60 * 60 * 24 * 30.44)));
    }
  }
  const term = deriveLeaseTermFromStartEnd(
    d.lease_commencement_date || d.lease_start_date,
    d.lease_end_date
  );
  const y = parseInt(term.years, 10) || 0;
  const mo = parseInt(term.months, 10) || 0;
  return y * 12 + mo;
}

/** Human-readable years + months from summed lease months (all qualifying deals). */
function formatAggregatedLeasePeriod(totalMonthsRaw) {
  const tm = Math.max(0, Math.floor(Number(totalMonthsRaw) || 0));
  if (tm <= 0) return "—";
  const y = Math.floor(tm / 12);
  const mo = tm % 12;
  const parts = [];
  if (y > 0) parts.push(y + (y === 1 ? " year" : " years"));
  if (mo > 0) parts.push(mo + (mo === 1 ? " month" : " months"));
  return parts.join(", ");
}

/**
 * Public done-deals track record (no map). Area-only; no client names.
 *
 * Formulas (all confirmed deals — is_expected = 0; checkbox show_on_done_deals is not required):
 * - totalLeaseVolumeZar = sum of invoice_total
 * - totalLeaseMonthsSigned = Σ leaseMonthsSignedForDeal(d)
 * - leasePeriodDisplay = floor(totalLeaseMonthsSigned ÷ 12) years + (total % 12) months
 * - highestMonthlyRent = max parseMonthlyRentZar(actual_rental || asking_rental)
 * Reporting date for sorting/display: dealReportingIsoDateForPublic (ISO + DD/MM + created_at fallback).
 */
function getDoneDealsPublicStats() {
  const rows = db
    .prepare(
      `SELECT * FROM deals
       WHERE (is_expected IS NULL OR is_expected = 0)`
    )
    .all();
  const showcaseRaw = [];
  const areaSet = new Set();
  let totalLeaseVolumeZar = 0;
  let totalLeaseMonthsSigned = 0;
  let highestMonthlyRent = { amount: 0, area: "" };

  for (const d of rows) {
    const iso = dealReportingIsoDateForPublic(d);
    if (!iso) continue;
    let addr = resolveDealAddressForPublic(d);
    if (!addr) addr = "Cape Town industrial";

    const area = publicAreaFromAddress(addr);
    areaSet.add(area);

    const inv = Number(d.invoice_total);
    if (Number.isFinite(inv) && inv > 0) totalLeaseVolumeZar += inv;

    const rentalStr =
      (d.actual_rental && String(d.actual_rental).trim()) ||
      (d.asking_rental && String(d.asking_rental).trim()) ||
      "";
    const rentNum = parseMonthlyRentZar(rentalStr);
    if (rentNum != null && rentNum > highestMonthlyRent.amount) {
      highestMonthlyRent = { amount: rentNum, area };
    }

    totalLeaseMonthsSigned += leaseMonthsSignedForDeal(d);

    let sizeLabel = "";
    let typeLabel = "Industrial";
    const pid = propertyIdFromDealLinkUrl(d.link_url);
    if (pid) {
      const p = db
        .prepare(
          "SELECT size, property_type FROM properties WHERE id = ?"
        )
        .get(pid);
      if (p) {
        sizeLabel = p.size ? formatSizeDisplay(p.size) : "";
        typeLabel = formatPropertyTypeLabel(p.property_type);
      }
    }

    const sqm = sizeLabel ? parseSizeToSquareMetres(sizeLabel) : null;
    let rateLine = "";
    if (rentNum != null && sqm != null && sqm > 0) {
      const rpsqm = rentNum / sqm;
      rateLine = `R${Math.round(rpsqm).toLocaleString("en-ZA")}/m² achieved`;
    }

    const closedLabel = formatDoneDealClosedSuffix(iso);

    showcaseRaw.push({
      area,
      sizeLabel: sizeLabel || "—",
      typeLabel,
      rental: rentalStr || "On request",
      rateLine,
      closedLabel,
      invoice: Number.isFinite(inv) && inv > 0 ? inv : null,
      sortKey: iso
    });
  }

  showcaseRaw.sort((a, b) => b.sortKey.localeCompare(a.sortKey));
  const showcaseDeals = showcaseRaw;
  const areasSeen = new Map();
  for (const a of areaSet) {
    const t = String(a || "").trim();
    if (!t) continue;
    const key = t.toLowerCase();
    if (!areasSeen.has(key)) areasSeen.set(key, t);
  }
  const areasList = [...areasSeen.values()].sort((a, b) => a.localeCompare(b));

  if (highestMonthlyRent.amount <= 0) {
    highestMonthlyRent = { amount: 0, area: "" };
  }

  const leasePeriodDisplay = formatAggregatedLeasePeriod(totalLeaseMonthsSigned);

  return {
    doneDealsCount: showcaseRaw.length,
    totalLeaseVolumeZar,
    showcaseDeals,
    areasList,
    totalLeaseMonthsSigned,
    leasePeriodDisplay,
    highestMonthlyRent
  };
}

/**
 * Value for targets / bar & pie charts: always commission invoice total (VAT-incl.).
 */
function dealTargetAmountZar(deal) {
  if (!deal) return null;
  const inv = Number(deal.invoice_total);
  if (Number.isFinite(inv) && inv > 0) return Math.round(inv * 100) / 100;
  return null;
}

function sumInvoiceTargetForDeals(deals) {
  let t = 0;
  (deals || []).forEach((d) => {
    const a = dealTargetAmountZar(d);
    if (a != null && a > 0) t += a;
  });
  return Math.round(t * 100) / 100;
}

function parseMoneyAmount(raw) {
  if (raw === "" || raw == null) return null;
  const n = Number(String(raw).replace(/,/g, "").trim());
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.round(n * 100) / 100;
}

/** Total commission invoice (VAT-incl.) — single field `invoice_total_input` (legacy: amount_value). */
function resolveDealInvoiceTotal(body) {
  const raw =
    body &&
    (body.invoice_total_input != null && String(body.invoice_total_input).trim() !== "")
      ? body.invoice_total_input
      : body && body.amount_value != null && String(body.amount_value).trim() !== ""
        ? body.amount_value
        : null;
  const n = parseMoneyAmount(raw);
  if (n == null) return { invoiceTotal: null, dealAmountType: "invoice_total" };
  return { invoiceTotal: n, dealAmountType: "invoice_total" };
}

function availabilityDateValue(value) {
  if (!value) return "";
  const s = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  return "";
}

function formatAvailabilityDisplay(value) {
  if (value == null || value === "") return "—";
  const s = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const d = new Date(`${s}T12:00:00`);
    if (!Number.isNaN(d.getTime())) {
      return d.toLocaleDateString("en-ZA", {
        day: "numeric",
        month: "long",
        year: "numeric"
      });
    }
  }
  return s;
}

function formatStatusLabel(status) {
  if (status == null || status === "") return "—";
  const key = String(status).trim().toLowerCase();
  const labels = {
    "to-let": "TO LET",
    "for-sale": "FOR SALE",
    let: "LET",
    sold: "SOLD",
    "let-and-sold": "LET"
  };
  if (labels[key]) return labels[key];
  return key
    .split("-")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

function normalizePropertyType(raw) {
  const s = String(raw ?? "")
    .trim()
    .toLowerCase();
  if (s === "office") return "office";
  return "industrial";
}

function formatPropertyTypeLabel(raw) {
  return normalizePropertyType(raw) === "office" ? "Office" : "Industrial";
}

function enrichProperty(property) {
  const images = getPropertyImages(property.id);
  property.images = images;
  property.displayImage =
    property.display_image ||
    (images.length > 0 ? images[0].filename : null);
  property.statusLabel = formatStatusLabel(property.status);
  property.propertyTypeLabel = formatPropertyTypeLabel(property.property_type);

  const showUnit =
    property.use_unit_details == null ||
    property.use_unit_details === "" ||
    Number(property.use_unit_details) !== 0;
  property.showUnitDetails = showUnit;

  property.building = null;
  if (property.building_id) {
    const b = db.prepare("SELECT * FROM buildings WHERE id = ?").get(property.building_id);
    if (b) {
      b.images = getBuildingImages(b.id);
      property.building = b;
    }
  }

  property.brokerName = null;
  if (property.broker_id) {
    const br = db.prepare("SELECT name FROM agents WHERE id = ?").get(property.broker_id);
    if (br) property.brokerName = br.name;
  }

  const unitFiles = [];
  if (property.display_image) unitFiles.push(property.display_image);
  images.forEach((img) => unitFiles.push(img.filename));

  let galleryFilenames = dedupeFilenames(unitFiles);
  if (!showUnit && property.building) {
    const b = property.building;
    const buildingFiles = [];
    if (b.display_image) buildingFiles.push(b.display_image);
    (b.images || []).forEach((img) => buildingFiles.push(img.filename));
    galleryFilenames = dedupeFilenames([...buildingFiles, ...unitFiles]);
  }
  property.galleryFilenames = galleryFilenames;

  let cardImage = property.displayImage;
  if (!showUnit && property.building) {
    const b = property.building;
    const bf =
      b.display_image ||
      (b.images && b.images[0] && b.images[0].filename) ||
      null;
    if (bf) cardImage = bf;
  }
  property.cardImage = cardImage;

  property.availabilityDisplay = formatAvailabilityDisplay(property.availability);

  if (property.price != null && String(property.price).trim() !== "") {
    property.price = formatPriceForSave(property.price);
  }
  if (property.size != null && String(property.size).trim() !== "") {
    property.size = formatSizeDisplay(property.size);
  }

  property.powerAmpsDisplay = formatPowerAmpsDisplay(property.power_amps || "");
  property.heightEaveApexDisplay = property.height_eave_apex
    ? formatHeightMetresToEavesDisplay(property.height_eave_apex)
    : "";
  property.heightEaveRollerDisplay = property.height_eave_roller_shutter
    ? formatHeightMetresToEavesDisplay(property.height_eave_roller_shutter)
    : "";

  return property;
}

/**
 * Parses homepage keyword box so area names (e.g. "Maitland") filter the `area`
 * column instead of matching the word inside another suburb's description.
 */
function parseSearchQuery(search) {
  const raw = String(search || "").trim();
  if (!raw) return { areaFromQuery: null, freeText: null };

  const norm = raw.toLowerCase().replace(/\s+/g, " ").trim();
  if (norm === "maitland") {
    return { areaFromQuery: "Maitland", freeText: null };
  }
  if (norm === "paarden eiland") {
    return { areaFromQuery: "Paarden Eiland", freeText: null };
  }

  let m = raw.match(/^\s*Maitland\s+(.+)$/i);
  if (m) return { areaFromQuery: "Maitland", freeText: m[1].trim() };

  m = raw.match(/^\s*Paarden\s+Eiland\s+(.+)$/i);
  if (m) return { areaFromQuery: "Paarden Eiland", freeText: m[1].trim() };

  m = raw.match(/^(.+?)\s+Maitland\s*$/i);
  if (m && m[1].trim()) {
    return { areaFromQuery: "Maitland", freeText: m[1].trim() };
  }

  m = raw.match(/^(.+?)\s+Paarden\s+Eiland\s*$/i);
  if (m && m[1].trim()) {
    return { areaFromQuery: "Paarden Eiland", freeText: m[1].trim() };
  }

  return { areaFromQuery: null, freeText: raw };
}

function getKeywordLikeTerm(search, selectedArea, parsed) {
  const q = String(search || "").trim();
  if (!q) return null;

  if (!selectedArea) {
    if (parsed.areaFromQuery && !parsed.freeText) return null;
    if (parsed.areaFromQuery && parsed.freeText) return parsed.freeText;
    return q;
  }

  if (parsed.areaFromQuery && !parsed.freeText) {
    return null;
  }
  if (parsed.areaFromQuery && parsed.freeText) {
    return parsed.freeText;
  }
  return q;
}

function getPublicProperties(filters = {}) {
  const selectedArea = filters.area || "";
  const selectedStatus = filters.status || "";
  const selectedPropertyType = filters.propertyType || "";
  const search = filters.search || "";
  const ampsMin = parseFilterBound(filters.ampsMin);
  const heightMin = parseFilterBound(filters.heightMin);

  const parsed = parseSearchQuery(search);

  let effectiveArea = selectedArea;
  if (!effectiveArea && parsed.areaFromQuery) {
    effectiveArea = parsed.areaFromQuery;
  }

  const textForLike = getKeywordLikeTerm(search, selectedArea, parsed);

  let sql = `
    SELECT *
    FROM properties
    WHERE status IN ('to-let', 'for-sale')
  `;
  const params = [];

  if (effectiveArea) {
    sql += " AND area = ?";
    params.push(effectiveArea);
  }

  if (selectedStatus) {
    sql += " AND status = ?";
    params.push(selectedStatus);
  }

  if (selectedPropertyType === "office" || selectedPropertyType === "industrial") {
    sql += " AND property_type = ?";
    params.push(selectedPropertyType);
  }

  if (textForLike) {
    sql += `
      AND (
        name LIKE ?
        OR address LIKE ?
        OR size LIKE ?
        OR price LIKE ?
        OR availability LIKE ?
        OR description LIKE ?
        OR features LIKE ?
      )
    `;
    const like = `%${textForLike}%`;
    params.push(like, like, like, like, like, like, like);
  }

  let rows = db.prepare(sql).all(...params);

  if (ampsMin != null) {
    rows = rows.filter((row) => {
      const a = parseAmpsToNumber(row.power_amps);
      return a != null && a >= ampsMin;
    });
  }

  if (heightMin != null) {
    rows = rows.filter((row) => {
      const h = getListingHeightMetresForFilter(row);
      return h != null && h >= heightMin;
    });
  }

  rows = rows.map(enrichProperty);

  const high = shuffleArray(rows.filter((p) => p.priority_group === "high"));
  const medium = shuffleArray(rows.filter((p) => p.priority_group === "medium"));
  const low = shuffleArray(rows.filter((p) => p.priority_group === "low"));
  const other = shuffleArray(
    rows.filter(
      (p) =>
        p.priority_group !== "high" &&
        p.priority_group !== "medium" &&
        p.priority_group !== "low"
    )
  );

  return [...high, ...medium, ...low, ...other];
}

function getAdminProperties(area = "") {
  let sql = "SELECT * FROM properties";
  const params = [];

  if (area) {
    sql += " WHERE area = ?";
    params.push(area);
  }

  sql += `
    ORDER BY
      CASE priority_group
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'low' THEN 3
        ELSE 4
      END,
      id DESC
  `;

  return db.prepare(sql).all(...params).map(enrichProperty);
}

function getAgents() {
  return db.prepare("SELECT * FROM agents ORDER BY name ASC").all();
}

function getDealsForAgent(agentId, yearStr) {
  const sql = "SELECT * FROM deals WHERE agent_id = ? ORDER BY deal_date DESC, id DESC";
  const rows = db.prepare(sql).all(agentId);
  if (!yearStr || !/^\d{4}$/.test(String(yearStr).trim())) return rows;
  const y = parseInt(String(yearStr).trim(), 10);
  const ys = String(yearStr).trim();
  return rows.filter(
    (d) => dealBelongsToCalendarYear(d, y) && dealIncludedForAgentYearWindow(d, ys)
  );
}

/** Default monthly target on total invoice value; 2025 uses R100,000 (see getMonthlyInvoiceTargetForYear). */
const INVOICE_TARGET_MONTHLY_ZAR = 150000;

function getMonthlyInvoiceTargetForYear(yearNum) {
  if (!Number.isFinite(yearNum)) return INVOICE_TARGET_MONTHLY_ZAR;
  if (yearNum === 2025) return 100000;
  return INVOICE_TARGET_MONTHLY_ZAR;
}

/** 2025 counts Jul–Dec only (6 months); other years full calendar. */
function getAnnualTargetMonthsForYear(yearNum) {
  if (!Number.isFinite(yearNum)) return 12;
  if (yearNum === 2025) return 6;
  return 12;
}

function getAnnualInvoiceTargetZar(yearNum) {
  return (
    getMonthlyInvoiceTargetForYear(yearNum) * getAnnualTargetMonthsForYear(yearNum)
  );
}

/**
 * Parse deal_date from DB or form (ISO, or DD/MM/YYYY, DD-MM-YYYY).
 * @returns {{ y: number, mi: number } | null}  mi = month index 0–11
 */
function parseDealDateParts(ds) {
  if (ds == null) return null;
  const s = String(ds).trim();
  if (!s) return null;
  let m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
  if (m) {
    const mi = parseInt(m[2], 10) - 1;
    if (mi < 0 || mi >= 12) return null;
    return { y: parseInt(m[1], 10), mi };
  }
  m = /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/.exec(s);
  if (m) {
    const mi = parseInt(m[2], 10) - 1;
    if (mi < 0 || mi >= 12) return null;
    return { y: parseInt(m[3], 10), mi };
  }
  return null;
}

const VAT_RATE = 0.15;

/** Assume stored amounts include 15% VAT; return amount excluding VAT. */
function amountExclVat15(incl) {
  if (!Number.isFinite(incl) || incl <= 0) return 0;
  return Math.round((incl / (1 + VAT_RATE)) * 100) / 100;
}

function sumInvoiceAndAgentPayout(deals) {
  let totalInvoice = 0;
  let totalAgentCommission = 0;
  (deals || []).forEach((d) => {
    const inv = Number(d.invoice_total);
    if (!Number.isFinite(inv)) return;
    totalInvoice += inv;
    const share = Number(d.agent_share_percent);
    const pct = Number.isFinite(share) ? share : 50;
    totalAgentCommission += inv * (pct / 100);
  });
  return {
    totalInvoice,
    totalAgentCommission,
    totalCompanyShare: totalInvoice - totalAgentCommission
  };
}

/** Invoice & payout with incl. / excl. VAT lines for dashboard display. */
function moneyPairFromDeals(deals) {
  const { totalInvoice, totalAgentCommission } = sumInvoiceAndAgentPayout(deals);
  return {
    invoiceIncl: totalInvoice,
    invoiceExcl: amountExclVat15(totalInvoice),
    payoutIncl: totalAgentCommission,
    payoutExcl: amountExclVat15(totalAgentCommission)
  };
}

function partitionDealsExpected(deals) {
  const actual = [];
  const expected = [];
  (deals || []).forEach((d) => {
    if (Number(d.is_expected) === 1) expected.push(d);
    else actual.push(d);
  });
  return { actual, expected };
}

/** Sums rental (or invoice fallback) per month; splits confirmed vs expected pipeline. */
function computeMonthlyInvoiceSplit(deals, yearNum) {
  const actual = Array(12).fill(0);
  const expected = Array(12).fill(0);
  (deals || []).forEach((d) => {
    const amt = dealTargetAmountZar(d);
    if (amt == null || amt <= 0) return;
    const mi = dealMonthIndexForYear(d, yearNum);
    if (mi == null || mi < 0 || mi > 11) return;
    if (Number(d.is_expected) === 1) expected[mi] += amt;
    else actual[mi] += amt;
  });
  return { actual, expected };
}

/** @deprecated — kept for any code paths; use sumInvoiceAndAgentPayout */
function computeDealStats(deals) {
  const s = sumInvoiceAndAgentPayout(deals);
  return {
    count: deals.length,
    totalInvoice: s.totalInvoice,
    totalAgentCommission: s.totalAgentCommission,
    totalCompanyShare: s.totalCompanyShare
  };
}

function getDealsForAgentInCalendarMonth(agentId, y, monthIndex0to11, dealsOverride) {
  const rows = Array.isArray(dealsOverride)
    ? dealsOverride
    : db
        .prepare(
          `SELECT * FROM deals WHERE agent_id = ? ORDER BY deal_date DESC, id DESC`
        )
        .all(agentId);
  const yearStr = String(y);
  return rows.filter((d) => {
    if (y === 2025 && monthIndex0to11 < 6) return false;
    const mi = dealMonthIndexForYear(d, y);
    if (mi == null || mi !== monthIndex0to11) return false;
    return dealIncludedForAgentYearWindow(d, yearStr);
  });
}

function safeUnlinkUpload(filename) {
  if (!filename) return;
  const p = path.join(uploadsDir, filename);
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch {
    /* ignore */
  }
}

/** Stored deal_date for charts: lease commencement first, else beneficial occupation. */
function resolveDealDateForStorage(body) {
  const comm =
    (body.lease_commencement_date &&
      String(body.lease_commencement_date).trim().slice(0, 10)) ||
    (body.lease_start_date && String(body.lease_start_date).trim().slice(0, 10));
  const ben =
    body.beneficial_occupation_date &&
    String(body.beneficial_occupation_date).trim().slice(0, 10);
  return comm || ben || "";
}

function formatDateYMDLocal(d) {
  if (!d || Number.isNaN(d.getTime())) return null;
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * Lease end from term + commencement. If term is 0/0 on edit, keep existing lease end when present.
 */
function resolveLeaseEndDate(body, existingLeaseEnd) {
  const start =
    (body.lease_commencement_date && String(body.lease_commencement_date).trim()) ||
    (body.lease_start_date && String(body.lease_start_date).trim());
  const y = parseInt(body.lease_years, 10) || 0;
  const m = parseInt(body.lease_months, 10) || 0;
  if (!start || (y === 0 && m === 0)) {
    if (existingLeaseEnd != null && String(existingLeaseEnd).trim() !== "") {
      return String(existingLeaseEnd).trim().slice(0, 10);
    }
    return null;
  }
  const d = new Date(`${start.slice(0, 10)}T12:00:00`);
  if (Number.isNaN(d.getTime())) return null;
  d.setFullYear(d.getFullYear() + y);
  d.setMonth(d.getMonth() + m);
  d.setDate(d.getDate() - 1);
  return formatDateYMDLocal(d);
}

/** Prefill term fields on edit by matching stored lease end (same math as resolveLeaseEndDate). */
function deriveLeaseTermFromStartEnd(startStr, endStr) {
  if (!startStr || !endStr) return { years: "", months: "" };
  const want = String(endStr).trim().slice(0, 10);
  const start = new Date(`${String(startStr).trim().slice(0, 10)}T12:00:00`);
  if (Number.isNaN(start.getTime())) return { years: "", months: "" };
  for (let yy = 0; yy <= 50; yy++) {
    for (let mm = 0; mm <= 11; mm++) {
      const d = new Date(start);
      d.setFullYear(d.getFullYear() + yy);
      d.setMonth(d.getMonth() + mm);
      d.setDate(d.getDate() - 1);
      if (formatDateYMDLocal(d) === want) {
        return { years: String(yy), months: String(mm) };
      }
    }
  }
  return { years: "", months: "" };
}

function buildLeasePeriodText(body, leaseEnd) {
  const y = parseInt(body.lease_years, 10) || 0;
  const mo = parseInt(body.lease_months, 10) || 0;
  const extra = body.lease_period && String(body.lease_period).trim();
  if (leaseEnd) {
    if (y || mo) return `${y} years ${mo} months · ends ${leaseEnd}`;
    if (extra) return `${extra} · ends ${leaseEnd}`;
    return `Ends ${leaseEnd}`;
  }
  return extra || "";
}

function daysUntilLeaseEnd(leaseEndStr) {
  if (!leaseEndStr) return null;
  const end = new Date(`${String(leaseEndStr).slice(0, 10)}T12:00:00`);
  if (Number.isNaN(end.getTime())) return null;
  const today = new Date();
  const t0 = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const e0 = new Date(end.getFullYear(), end.getMonth(), end.getDate());
  return Math.round((e0 - t0) / 86400000);
}

function getLeaseNotificationRows() {
  return db
    .prepare(`
      SELECT d.*, a.name AS agent_name
      FROM deals d
      JOIN agents a ON a.id = d.agent_id
      WHERE d.lease_end_date IS NOT NULL AND trim(d.lease_end_date) != ''
    `)
    .all();
}

const CAPE_AREA_CENTROIDS = {
  Maitland: { lat: -33.924, lng: 18.495 },
  "Paarden Eiland": { lat: -33.917, lng: 18.445 }
};

const AREA_SLUG_TO_NAME = {
  maitland: "Maitland",
  "paarden-eiland": "Paarden Eiland"
};

function getAreaInsights(areaName) {
  const cape =
    "Cape Town’s industrial and commercial market balances logistics, tenant demand, and building specs—power, height, and yard often matter as much as rent per m². Local comparables anchor shortlists and negotiations.";
  if (areaName === "Maitland") {
    return {
      headline: "Maitland",
      subhead: "North of the CBD — strong road links and mixed industrial stock",
      intro: [
        "Maitland sits between the inner city and the northern suburbs, with good connections toward the N1 and N2. The corridor mixes older warehouses, modern logistics, and showroom-style units along busy arterials.",
        "Tenant mix often includes suppliers, light manufacturing, services, and trade-facing businesses that value visibility and access."
      ],
      bullets: [
        "Blend of single- and multi-tenanted parks; yard depth and roller shutter height vary by site",
        "Popular with trade, last-mile, and service operators needing road exposure",
        "Confirm power (amps / phase) and eave height against your fit-out plan"
      ],
      development:
        "Redevelopment and refurbishment continue as older stock is repositioned. Sustainability and energy resilience increasingly sit alongside traditional specs in tenant decisions."
    };
  }
  if (areaName === "Paarden Eiland") {
    return {
      headline: "Paarden Eiland",
      subhead: "Harbour-side logistics and dense industrial fabric",
      intro: [
        "Paarden Eiland is closely tied to the Port of Cape Town and coastal freight flows. You will find warehousing, workshops, cold-chain, and office–warehouse hybrids in a compact industrial node.",
        "Demand often focuses on well-powered units, good roller-shutter access, and practical yard or loading configurations."
      ],
      bullets: [
        "Strong harbour and logistics context for import/export and coastal freight",
        "Wide range of unit ages and sizes—due diligence on condition and compliance is key",
        "Quality stock can move quickly; early viewing helps on well-priced units"
      ],
      development:
        "Infrastructure upgrades and tenant churn continue to reshape the area. Fit-out and energy choices are increasingly part of long-term occupancy planning."
    };
  }
  return {
    headline: areaName,
    subhead: "Cape Town industrial & commercial",
    intro: [cape],
    bullets: [],
    development: cape
  };
}

function jitterCoords(lat, lng, id) {
  const n = (Number(id) % 17) / 8500;
  const m = ((Number(id) * 3) % 17) / 8500;
  return { lat: lat + n - 0.001, lng: lng + m - 0.001 };
}

function coordsForMapRow(row) {
  const fb = CAPE_AREA_CENTROIDS[row.area];
  const base = fb || { lat: -33.9258, lng: 18.4232 };
  return jitterCoords(base.lat, base.lng, row.id);
}

const GEOCODE_FETCH_MS = 12000;

function fetchWithTimeout(url, init = {}, ms = GEOCODE_FETCH_MS) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);
  const merged = { ...init, signal: controller.signal };
  return fetch(url, merged).finally(() => clearTimeout(t));
}

/**
 * Resolve address to lat/lng for map pins.
 * Set GOOGLE_MAPS_API_KEY for Google Geocoding (recommended). Otherwise uses OpenStreetMap Nominatim.
 * Uses a timeout so a slow/blocked network cannot hang the HTTP request indefinitely.
 */
async function geocodeAddressForProperty(address, area) {
  const addr = address && String(address).trim();
  if (!addr) return null;

  const region = area && String(area).trim();
  const query = region
    ? `${addr}, ${region}, Cape Town, Western Cape, South Africa`
    : `${addr}, Cape Town, Western Cape, South Africa`;

  const googleKey = process.env.GOOGLE_MAPS_API_KEY;
  if (googleKey) {
    try {
      const u = new URL("https://maps.googleapis.com/maps/api/geocode/json");
      u.searchParams.set("address", query);
      u.searchParams.set("key", googleKey);
      u.searchParams.set("region", "za");
      const r = await fetchWithTimeout(u);
      const data = await r.json();
      if (data.status === "OK" && data.results && data.results[0]) {
        const loc = data.results[0].geometry.location;
        return { lat: loc.lat, lng: loc.lng };
      }
    } catch (e) {
      console.error("Google Geocoding error:", e.message);
    }
  }

  try {
    const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(query)}`;
    const r = await fetchWithTimeout(url, {
      headers: {
        "User-Agent": "HustleProperty/1.0 (industrial property listings)"
      }
    });
    if (r.ok) {
      const data = await r.json();
      if (data && data[0]) {
        return {
          lat: parseFloat(data[0].lat),
          lng: parseFloat(data[0].lon)
        };
      }
    }
  } catch (e) {
    console.error("Nominatim geocode error:", e.message);
  }

  return null;
}

function schedulePropertyGeocode(propertyId, address, area) {
  const addr = address && String(address).trim();
  const pid = Number(propertyId);
  if (!addr || !Number.isFinite(pid) || pid <= 0) return;

  // On serverless, background tasks can keep the event loop alive and cause timeouts.
  if (process.env.VERCEL || useSupabase) return;

  const im = setImmediate(() => {
    geocodeAddressForProperty(address, area)
      .then((coords) => {
        if (!coords) return;
        try {
          db.prepare(
            "UPDATE properties SET latitude = ?, longitude = ? WHERE id = ?"
          ).run(coords.lat, coords.lng, pid);
        } catch (e) {
          console.error("Geocode DB update failed:", e.message);
        }
      })
      .catch((e) => console.error("Geocode failed:", e.message));
  });
  if (typeof im === "object" && typeof im.unref === "function") im.unref();
}

/** Featured card / carousel: e.g. "300amps" when the field is just a number. */
function formatPowerAmpsDisplay(amps) {
  const s = String(amps || "").trim();
  if (!s) return "";
  if (/amp/i.test(s)) return s;
  return `${s}amps`;
}

/** Featured card / property page: e.g. "3 Metres To Eaves" when stored as "3" or "3m". */
function formatHeightMetresToEavesDisplay(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  if (/metre/i.test(s)) return s;
  const m = /^(\d+(?:\.\d+)?)\s*m?\s*$/i.exec(s);
  if (m) return `${m[1]} Metres To Eaves`;
  return s;
}

function pickCarouselSpecLines(p) {
  const lines = [];
  const phase = (p.power_phase || "").trim();
  const amps = (p.power_amps || "").trim();
  if (phase || amps) {
    let phaseLabel = "";
    if (phase === "3-phase") phaseLabel = "3-phase";
    else if (phase === "single-phase") phaseLabel = "Single-phase";
    const ampsFmt = formatPowerAmpsDisplay(amps);
    if (phaseLabel && ampsFmt) lines.push(`Power: ${phaseLabel} · ${ampsFmt}`);
    else if (phaseLabel) lines.push(`Power: ${phaseLabel}`);
    else if (ampsFmt) lines.push(`Power: ${ampsFmt}`);
  }
  const apex = p.height_eave_apex && String(p.height_eave_apex).trim();
  if (apex) {
    lines.push(`Height to eave (apex): ${formatHeightMetresToEavesDisplay(apex)}`);
  }
  const rs = p.height_eave_roller_shutter && String(p.height_eave_roller_shutter).trim();
  if (rs) {
    lines.push(`Height to eave (roller shutter): ${formatHeightMetresToEavesDisplay(rs)}`);
  }
  const park = p.parking_bays && String(p.parking_bays).trim();
  if (park) lines.push(`Parking bays: ${park}`);
  return lines.slice(0, 3);
}

function mapPropertyToFeaturedSlide(p) {
  const files = [...new Set((p.galleryFilenames || []).filter(Boolean))];
  const urls = useSupabase
    ? p.galleryUrls && p.galleryUrls.length
      ? p.galleryUrls
      : files.map((f) => supabasePublicObjectUrl(f) || f)
    : files.map((f) => `/uploads/${f}`);
  let galleryImages;
  if (p.cardImage) {
    const cover = useSupabase
      ? p.cardImageUrl || supabasePublicObjectUrl(p.cardImage) || p.cardImage
      : p.cardImageUrl
        ? p.cardImageUrl
        : `/uploads/${p.cardImage}`;
    const rest = urls.filter((u) => u !== cover);
    galleryImages = [cover, ...rest].slice(0, 14);
  } else {
    galleryImages = urls.slice(0, 14);
  }
  return {
    id: p.id,
    name: p.name,
    size: p.size || "",
    area: p.area || "",
    price: p.price || "",
    propertyTypeLabel: p.propertyTypeLabel || "Industrial",
    cardImage: p.cardImage,
    cardImageUrl: p.cardImageUrl || null,
    galleryImages,
    features: pickCarouselSpecLines(p),
    url: `/property/${p.id}`
  };
}

async function sbGetHomeFeaturedSlides() {
  const { data: slots, error: slotErr } = await supabase
    .from("home_featured_slots")
    .select("slot, property_id, feature_style")
    .order("slot", { ascending: true });
  if (slotErr) throw slotErr;
  const slotToPid = { 1: null, 2: null };
  const slotToStyle = { 1: "orbit", 2: "orbit" };
  (slots || []).forEach((r) => {
    const sn = Number(r.slot);
    if (sn !== 1 && sn !== 2) return;
    slotToPid[sn] = r.property_id;
    const fs = String(r.feature_style ?? "").trim().toLowerCase();
    if (fs === "api") slotToStyle[sn] = "api";
  });
  const ids = [slotToPid[1], slotToPid[2]]
    .filter(Boolean)
    .map((x) => Number(x))
    .filter((n) => Number.isFinite(n));
  const propsById = new Map();
  if (ids.length) {
    const { data: props, error: propErr } = await supabase.from("properties").select("*").in("id", ids);
    if (propErr) throw propErr;
    (props || []).forEach((p) => propsById.set(Number(p.id), p));
  }
  const imagesByProp = new Map();
  if (ids.length) {
    const { data: imgs, error: imgErr } = await supabase
      .from("property_images")
      .select("property_id, storage_path, image_order, id")
      .in("property_id", ids)
      .order("image_order", { ascending: true })
      .order("id", { ascending: true });
    if (imgErr) throw imgErr;
    (imgs || []).forEach((img) => {
      const arr = imagesByProp.get(img.property_id) || [];
      arr.push({ ...img, filename: img.storage_path });
      imagesByProp.set(img.property_id, arr);
    });
  }

  const out = [];
  for (const slot of [1, 2]) {
    const pid = slotToPid[slot];
    if (!pid) {
      out.push({ slot, empty: true });
      continue;
    }
    const p = propsById.get(Number(pid));
    if (!p) {
      out.push({ slot, empty: true });
      continue;
    }
    enrichPropertyForRender(p, imagesByProp.get(Number(p.id)) || []);
    const slide = mapPropertyToFeaturedSlide(p);
    slide.slot = slot;
    slide.featureStyle = slotToStyle[slot];
    out.push(slide);
  }
  return out;
}

async function sbGetDoneDealsPublicStats() {
  const { data: rows, error } = await supabase
    .from("deals")
    .select("*")
    .or("is_expected.is.null,is_expected.eq.0");
  if (error) throw error;
  const showcaseRaw = [];
  const areaSet = new Set();
  let totalLeaseVolumeZar = 0;
  let totalLeaseMonthsSigned = 0;
  let highestMonthlyRent = { amount: 0, area: "" };

  for (const d of rows || []) {
    const iso = dealReportingIsoDateForPublic(d);
    if (!iso) continue;
    let addr = resolveDealAddressForPublic(d);
    if (!addr) addr = "Cape Town industrial";

    const area = publicAreaFromAddress(addr);
    areaSet.add(area);

    const inv = Number(d.invoice_total);
    if (Number.isFinite(inv) && inv > 0) totalLeaseVolumeZar += inv;

    const rentalStr =
      (d.actual_rental && String(d.actual_rental).trim()) ||
      (d.asking_rental && String(d.asking_rental).trim()) ||
      "";
    const rentNum = parseMonthlyRentZar(rentalStr);
    if (rentNum != null && rentNum > highestMonthlyRent.amount) {
      highestMonthlyRent = { amount: rentNum, area };
    }

    const months = leaseMonthsSignedForDeal(d);
    if (months) totalLeaseMonthsSigned += months;

    const closedLabel = formatDoneDealClosedSuffix(iso);
    showcaseRaw.push({
      area,
      sizeLabel: "—",
      typeLabel: "Industrial",
      rental: rentalStr || "On request",
      rateLine: "",
      closedLabel,
      invoice: Number.isFinite(inv) && inv > 0 ? inv : null,
      sortKey: iso
    });
  }

  showcaseRaw.sort((a, b) => b.sortKey.localeCompare(a.sortKey));
  const areasList = [...areaSet].sort((a, b) => a.localeCompare(b));
  return {
    doneDealsCount: showcaseRaw.length,
    totalLeaseVolumeZar: Math.round(totalLeaseVolumeZar * 100) / 100,
    showcaseDeals: showcaseRaw,
    areasList,
    totalLeaseMonthsSigned,
    leasePeriodDisplay: formatAggregatedLeasePeriod(totalLeaseMonthsSigned),
    highestMonthlyRent
  };
}

function validateFeaturedPropertyId(raw) {
  if (raw === undefined || raw === null || raw === "") return null;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return null;
  const row = db
    .prepare(
      `SELECT id FROM properties WHERE id = ? AND status IN ('to-let', 'for-sale')`
    )
    .get(n);
  return row ? n : null;
}

function setFeaturedHomeSlot(slot, propertyId, featureStyle = "orbit") {
  const style =
    String(featureStyle || "")
      .trim()
      .toLowerCase() === "api"
      ? "api"
      : "orbit";
  const existing = db
    .prepare("SELECT slot FROM home_featured_slots WHERE slot = ?")
    .get(slot);
  if (existing) {
    db.prepare(
      "UPDATE home_featured_slots SET property_id = ?, feature_style = ? WHERE slot = ?"
    ).run(propertyId, style, slot);
  } else {
    db.prepare(
      "INSERT INTO home_featured_slots (slot, property_id, feature_style) VALUES (?, ?, ?)"
    ).run(slot, propertyId, style);
  }
}

/** Two slots for homepage: left (1) and right (2). feature_style: orbit | api */
function getHomeFeaturedSlides() {
  const rows = db
    .prepare(
      "SELECT slot, property_id, feature_style FROM home_featured_slots ORDER BY slot ASC"
    )
    .all();
  const slotToPid = { 1: null, 2: null };
  const slotToStyle = { 1: "orbit", 2: "orbit" };
  rows.forEach((r) => {
    const sn = Number(r.slot);
    slotToPid[sn] = r.property_id;
    const fs = String(r.feature_style ?? "")
      .trim()
      .toLowerCase();
    if (fs === "api") slotToStyle[sn] = "api";
  });

  const out = [];
  for (const slot of [1, 2]) {
    const pid = slotToPid[slot];
    if (!pid) {
      out.push({ slot, empty: true });
      continue;
    }
    const p = db.prepare("SELECT * FROM properties WHERE id = ?").get(pid);
    if (!p) {
      out.push({ slot, empty: true });
      continue;
    }
    const st = String(p.status || "").trim();
    if (st !== "to-let" && st !== "for-sale") {
      out.push({ slot, empty: true, inactive: true });
      continue;
    }
    const ep = enrichProperty(p);
    const featureStyle = slotToStyle[slot] === "api" ? "api" : "orbit";
    out.push({
      ...mapPropertyToFeaturedSlide(ep),
      slot,
      empty: false,
      featureStyle
    });
  }
  return out;
}

function getListingsForFeaturedPicker() {
  return db
    .prepare(
      `SELECT id, name, area, status FROM properties WHERE status IN ('to-let','for-sale') ORDER BY area, name`
    )
    .all();
}

function getPropertiesForDealPrefill() {
  return db
    .prepare(`SELECT id, name, area FROM properties ORDER BY area, name`)
    .all();
}

function getQuizMatchSourceList() {
  return db
    .prepare(`SELECT * FROM properties WHERE status IN ('to-let','for-sale')`)
    .all()
    .map(enrichProperty)
    .map((p) => ({
      id: p.id,
      name: p.name,
      size: p.size || "",
      area: p.area || "",
      price: p.price || "",
      features: ((p.features || "") + " " + (p.yard_space || "")).toLowerCase(),
      description: (p.description || "").toLowerCase(),
      power_phase: (p.power_phase || "").toLowerCase(),
      power_amps: (p.power_amps || "").toLowerCase(),
      property_type: normalizePropertyType(p.property_type),
      cardImage: p.cardImage,
      url: `/property/${p.id}`
    }));
}

function insertHomeEnquiry(label, name, phone, email, message) {
  db.prepare(
    `
    INSERT INTO property_enquiries (
      property_id, property_label, property_address,
      enquirer_name, phone, email, message
    ) VALUES (NULL, ?, NULL, ?, ?, ?, ?)
  `
  ).run(label, name, phone || null, email || null, message || null);
}

// PUBLIC ROUTES
const HOME_FILTER_QUERY_KEYS = [
  "home_search",
  "keywords",
  "search",
  "area",
  "status",
  "property_type",
  "amps_min",
  "height_min"
];

/** True when the URL has filter params (GET search / browse). Uses raw query string so it matches browsers that omit empty keys from req.query. */
function isHomeSearchActive(req) {
  const q = req.query;
  if (q && typeof q === "object") {
    if (q.home_search === "1" || q.home_search === 1) return true;
    for (let i = 0; i < HOME_FILTER_QUERY_KEYS.length; i++) {
      const k = HOME_FILTER_QUERY_KEYS[i];
      if (Object.prototype.hasOwnProperty.call(q, k)) return true;
    }
  }
  const raw = String(req.originalUrl || req.url || "");
  const qi = raw.indexOf("?");
  if (qi === -1) return false;
  let qs = raw.slice(qi + 1);
  const hash = qs.indexOf("#");
  if (hash !== -1) qs = qs.slice(0, hash);
  let params;
  try {
    params = new URLSearchParams(qs);
  } catch (e) {
    return false;
  }
  return HOME_FILTER_QUERY_KEYS.some((k) => params.has(k));
}

function normalizeAreaKeywordForRedirect(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * Keyword is only "Maitland" / "Paarden Eiland", or Area dropdown is set → use area guide URL.
 * Returns { slug, omitKeywords } or null.
 */
function resolveHomeToAreaGuideSlug(keywords, selectedArea) {
  const a = String(selectedArea || "").trim();
  if (a === "Maitland") return { slug: "maitland", omitKeywords: false };
  if (a === "Paarden Eiland") return { slug: "paarden-eiland", omitKeywords: false };
  const k = normalizeAreaKeywordForRedirect(keywords);
  if (k === "maitland") return { slug: "maitland", omitKeywords: true };
  if (k === "paarden eiland") return { slug: "paarden-eiland", omitKeywords: true };
  return null;
}

function buildAreaGuideRedirectQuery(req, omitKeywords) {
  const p = new URLSearchParams();
  if (req.query.home_search === "1" || req.query.home_search === 1) {
    p.set("home_search", "1");
  }
  if (req.query.status !== undefined && String(req.query.status).trim() !== "") {
    p.set("status", String(req.query.status).trim());
  }
  if (req.query.property_type && String(req.query.property_type).trim() !== "") {
    p.set("property_type", String(req.query.property_type).trim());
  }
  if (req.query.amps_min && String(req.query.amps_min).trim() !== "") {
    p.set("amps_min", String(req.query.amps_min).trim());
  }
  if (req.query.height_min && String(req.query.height_min).trim() !== "") {
    p.set("height_min", String(req.query.height_min).trim());
  }
  if (!omitKeywords) {
    const kw = String(req.query.keywords || "").trim();
    const sr = String(req.query.search || "").trim();
    if (kw) p.set("keywords", kw);
    else if (sr) p.set("search", sr);
  }
  p.set("scroll", "results");
  const s = p.toString();
  return s ? `?${s}` : "";
}

app.get("/", async (req, res, next) => {
  const selectedArea = req.query.area || "";
  const keywordsEarly = String(req.query.keywords || req.query.search || "").trim();
  const guide = resolveHomeToAreaGuideSlug(keywordsEarly, selectedArea);
  if (guide) {
    const qs = buildAreaGuideRedirectQuery(req, guide.omitKeywords);
    return res.redirect(302, `/area/${guide.slug}${qs}#home-search-results`);
  }

  const selectedStatus =
    req.query.status === undefined
      ? "to-let"
      : String(req.query.status || "").trim();
  const selectedPropertyType = String(req.query.property_type || "").trim();
  const keywords = keywordsEarly;

  let searchActive = isHomeSearchActive(req);
  if (!searchActive) {
    searchActive =
      String(keywords).trim() !== "" ||
      String(selectedArea).trim() !== "" ||
      String(selectedPropertyType).trim() !== "" ||
      (req.query.amps_min != null && String(req.query.amps_min).trim() !== "") ||
      (req.query.height_min != null && String(req.query.height_min).trim() !== "") ||
      req.query.status !== undefined;
  }

  let properties;
  try {
    properties = supabase
      ? await sbGetPublicProperties({
          area: selectedArea,
          status: selectedStatus,
          propertyType: selectedPropertyType,
          search: keywords,
          ampsMin: req.query.amps_min,
          heightMin: req.query.height_min
        })
      : getPublicProperties({
          area: selectedArea,
          status: selectedStatus,
          propertyType: selectedPropertyType,
          search: keywords,
          ampsMin: req.query.amps_min,
          heightMin: req.query.height_min
        });
  } catch (e) {
    return next(e);
  }

  const featuredHome = supabase ? await sbGetHomeFeaturedSlides() : getHomeFeaturedSlides();
  const quizPropertiesJson = supabase
    ? "[]"
    : JSON.stringify(getQuizMatchSourceList()).replace(/</g, "\\u003c");

  res.render("index", {
    properties,
    selectedArea,
    selectedStatus,
    selectedPropertyType,
    keywords,
    search: keywords,
    ampsMin: req.query.amps_min != null ? String(req.query.amps_min) : "",
    heightMin: req.query.height_min != null ? String(req.query.height_min) : "",
    featuredHome,
    quizPropertiesJson,
    searchActive
  });
});

app.get("/area/:slug", async (req, res, next) => {
  const slug = String(req.params.slug || "")
    .trim()
    .toLowerCase();
  const areaName = AREA_SLUG_TO_NAME[slug];
  if (!areaName) {
    return res.status(404).type("text").send("Area not found");
  }

  const selectedStatus =
    req.query.status === undefined
      ? "to-let"
      : String(req.query.status || "").trim();
  const selectedPropertyType = String(req.query.property_type || "").trim();
  const keywords = String(req.query.keywords || req.query.search || "").trim();

  let properties;
  try {
    properties = supabase
      ? await sbGetPublicProperties({
          area: areaName,
          status: selectedStatus,
          propertyType: selectedPropertyType,
          search: keywords,
          ampsMin: req.query.amps_min,
          heightMin: req.query.height_min
        })
      : getPublicProperties({
          area: areaName,
          status: selectedStatus,
          propertyType: selectedPropertyType,
          search: keywords,
          ampsMin: req.query.amps_min,
          heightMin: req.query.height_min
        });
  } catch (e) {
    return next(e);
  }

  const insights = getAreaInsights(areaName);
  const mapCenter = CAPE_AREA_CENTROIDS[areaName] || {
    lat: -33.9258,
    lng: 18.4232
  };

  res.render("area-listings", {
    areaName,
    areaSlug: slug,
    properties,
    selectedArea: areaName,
    selectedStatus,
    selectedPropertyType,
    keywords,
    search: keywords,
    ampsMin: req.query.amps_min != null ? String(req.query.amps_min) : "",
    heightMin: req.query.height_min != null ? String(req.query.height_min) : "",
    insights,
    mapCenter,
    mapZoom: 14
  });
});

app.get("/property/:id", async (req, res, next) => {
  let property = null;
  try {
    if (supabase) {
      const { data, error } = await supabase
        .from("properties")
        .select("*")
        .eq("id", Number(req.params.id))
        .maybeSingle();
      if (error) throw error;
      property = data || null;
      if (property) {
        const imgs = await sbListPropertyImages(property.id);
        enrichPropertyForRender(property, imgs);
      }
    } else {
      property = db.prepare("SELECT * FROM properties WHERE id = ?").get(req.params.id);
      if (property) enrichProperty(property);
    }
  } catch (e) {
    return next(e);
  }

  if (!property) {
    return res.status(404).send("Property not found");
  }

  const similarProperties = supabase
    ? []
    : getSimilarProperties(property.id, property.size, 6);
  const googleMapsKey = process.env.GOOGLE_MAPS_API_KEY || "";
  const lat =
    property.latitude != null && property.latitude !== ""
      ? Number(property.latitude)
      : null;
  const lng =
    property.longitude != null && property.longitude !== ""
      ? Number(property.longitude)
      : null;
  const hasMapCoords =
    lat != null &&
    lng != null &&
    !Number.isNaN(lat) &&
    !Number.isNaN(lng);

  const areaNameRaw = String(property.area || "").trim();
  let areaGuideSlug = null;
  let areaGuideLabel = null;
  if (areaNameRaw === "Maitland") {
    areaGuideSlug = "maitland";
    areaGuideLabel = "Maitland Area Guide";
  } else if (areaNameRaw === "Paarden Eiland") {
    areaGuideSlug = "paarden-eiland";
    areaGuideLabel = "Paarden Eiland Area Guide";
  }

  res.render("property", {
    property,
    similarProperties,
    googleMapsKey,
    mapLat: hasMapCoords ? lat : null,
    mapLng: hasMapCoords ? lng : null,
    hasMapCoords,
    enquirySent: req.query.enquiry === "sent",
    areaGuideSlug,
    areaGuideLabel
  });
});

app.post("/property/:id/enquiry", (req, res) => {
  if (useSupabase) {
    (async () => {
      const id = Number(req.params.id);
      const { data: prop, error: propErr } = await supabase
        .from("properties")
        .select("id,name,address")
        .eq("id", id)
        .maybeSingle();
      if (propErr) throw propErr;
      if (!prop) return res.status(404).type("text").send("Property not found");

      const name = req.body.name != null ? String(req.body.name).trim() : "";
      const phone = req.body.phone != null ? String(req.body.phone).trim() : "";
      const email = req.body.email != null ? String(req.body.email).trim() : "";
      const message = req.body.message != null ? String(req.body.message).trim() : "";
      if (!name || !phone) {
        return res.status(400).type("text").send("Name and phone are required.");
      }

      const { error } = await supabase.from("property_enquiries").insert([
        {
          property_id: id,
          property_label: prop.name || null,
          property_address: prop.address || null,
          enquirer_name: name,
          phone,
          email: email || null,
          message: message || null
        }
      ]);
      if (error) throw error;
      res.redirect(`/property/${id}?enquiry=sent`);
    })().catch((e) => {
      console.error("supabase enquiry:", e);
      res.status(500).type("text").send("Could not send enquiry.");
    });
    return;
  }
  const id = Number(req.params.id);
  const prop = db.prepare("SELECT * FROM properties WHERE id = ?").get(id);
  if (!prop) {
    return res.status(404).send("Property not found");
  }

  const name = req.body.name != null ? String(req.body.name).trim() : "";
  const phone = req.body.phone != null ? String(req.body.phone).trim() : "";
  const email = req.body.email != null ? String(req.body.email).trim() : "";
  const message = req.body.message != null ? String(req.body.message).trim() : "";

  if (!name || !phone) {
    return res.status(400).send("Name and phone are required.");
  }

  const addr = prop.address && String(prop.address).trim();
  const label = prop.name && String(prop.name).trim();

  db.prepare(
    `
    INSERT INTO property_enquiries (
      property_id, property_label, property_address,
      enquirer_name, phone, email, message
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `
  ).run(
    id,
    label || null,
    addr || null,
    name,
    phone,
    email || null,
    message || null
  );

  res.redirect(`/property/${id}?enquiry=sent`);
});

app.get("/done-deals", async (req, res) => {
  try {
    const stats = useSupabase ? await sbGetDoneDealsPublicStats() : getDoneDealsPublicStats();
    res.render("done-deals", {
      pageTitle: "Track record",
      doneDealsCount: stats.doneDealsCount,
      totalLeaseVolumeZar: stats.totalLeaseVolumeZar,
      showcaseDeals: stats.showcaseDeals || [],
      areasList: stats.areasList || [],
      totalLeaseMonthsSigned: stats.totalLeaseMonthsSigned || 0,
      leasePeriodDisplay: stats.leasePeriodDisplay || "—",
      highestMonthlyRent: stats.highestMonthlyRent || { amount: 0, area: "" }
    });
  } catch (e) {
    console.error("done-deals:", e);
    res.status(500).type("html").send("<p>Page could not be loaded. Try again shortly.</p>");
  }
});

app.get("/faq", (req, res) => {
  res.render("faq");
});

app.get("/contact", (req, res) => {
  res.render("contact", {
    enquirySent: req.query.sent === "1",
    enquiryError: req.query.error === "1"
  });
});

app.post("/contact", (req, res) => {
  if (useSupabase) {
    (async () => {
      const name = req.body.name != null ? String(req.body.name).trim() : "";
      const phone = req.body.phone != null ? String(req.body.phone).trim() : "";
      const email = req.body.email != null ? String(req.body.email).trim() : "";
      const message = req.body.message != null ? String(req.body.message).trim() : "";
      if (!name || !phone) return res.redirect("/contact?error=1");
      const { error } = await supabase.from("property_enquiries").insert([
        {
          property_id: null,
          property_label: "General contact",
          property_address: null,
          enquirer_name: name,
          phone,
          email: email || null,
          message: message || null
        }
      ]);
      if (error) throw error;
      res.redirect("/contact?sent=1");
    })().catch((e) => {
      console.error("supabase contact:", e);
      res.redirect("/contact?error=1");
    });
    return;
  }
  const name = req.body.name != null ? String(req.body.name).trim() : "";
  const phone = req.body.phone != null ? String(req.body.phone).trim() : "";
  const email = req.body.email != null ? String(req.body.email).trim() : "";
  const message = req.body.message != null ? String(req.body.message).trim() : "";

  if (!name || !phone) {
    return res.redirect("/contact?error=1");
  }

  db.prepare(
    `
    INSERT INTO property_enquiries (
      property_id, property_label, property_address,
      enquirer_name, phone, email, message
    ) VALUES (NULL, 'General contact', NULL, ?, ?, ?, ?)
  `
  ).run(name, phone, email || null, message || null);

  res.redirect("/contact?sent=1");
});

app.get("/building/:id", (req, res) => {
  const building = db.prepare("SELECT * FROM buildings WHERE id = ?").get(req.params.id);
  if (!building) {
    return res.status(404).send("Building not found");
  }
  building.images = getBuildingImages(building.id);
  const units = db
    .prepare(`
      SELECT * FROM properties
      WHERE building_id = ? AND status IN ('to-let', 'for-sale')
      ORDER BY name ASC
    `)
    .all(building.id)
    .map(enrichProperty);

  res.render("building-public", { building, units });
});

/** Daily, area-scoped “fun fact” from Google News RSS (cached); falls back to static copy if RSS fails. */
app.get("/api/area-news-fun-fact", async (req, res) => {
  try {
    const area = typeof req.query.area === "string" ? req.query.area : "";
    const data = await fetchAreaNewsFunFact(area);
    res.json(data);
  } catch {
    res.status(500).json({ ok: false, error: "fun_fact_failed" });
  }
});

app.get("/api/map-properties", (req, res) => {
  try {
    const areaParam =
      typeof req.query.area === "string" ? req.query.area.trim() : "";
    const allowedAreas = new Set(["Maitland", "Paarden Eiland"]);
    const areaFilter = allowedAreas.has(areaParam) ? areaParam : "";

    if (useSupabase) {
      (async () => {
        let q = supabase
          .from("properties")
          .select("id,name,address,area,latitude,longitude,property_type")
          .in("status", ["to-let", "for-sale"]);
        if (areaFilter) q = q.eq("area", areaFilter);
        const { data: rows, error } = await q.limit(500);
        if (error) throw error;
        const out = (rows || []).map((row) => {
          const g = coordsForMapRow(row);
          return {
            id: row.id,
            name: row.name,
            address: row.address || "",
            area: row.area || "",
            typeLabel: formatPropertyTypeLabel(row.property_type),
            lat: row.latitude != null ? Number(row.latitude) : g.lat,
            lng: row.longitude != null ? Number(row.longitude) : g.lng,
            thumb: null,
            url: `/property/${row.id}`
          };
        });
        res.json(out);
      })().catch((err) => {
        res.status(500).json({ error: err.message || "Map data error" });
      });
      return;
    }

    let sql = `
        SELECT id, name, address, area, display_image, latitude, longitude, property_type
        FROM properties
        WHERE status IN ('to-let', 'for-sale')
      `;
    const queryParams = [];
    if (areaFilter) {
      sql += " AND area = ?";
      queryParams.push(areaFilter);
    }

    const rows = db.prepare(sql).all(...queryParams);

    const out = [];

    for (const row of rows) {
      let lat = row.latitude;
      let lng = row.longitude;

      if (lat == null || lng == null) {
        const g = coordsForMapRow(row);
        lat = g.lat;
        lng = g.lng;
        db.prepare("UPDATE properties SET latitude = ?, longitude = ? WHERE id = ?").run(
          lat,
          lng,
          row.id
        );
      }

      out.push({
        id: row.id,
        name: row.name,
        address: row.address || "",
        area: row.area || "",
        typeLabel: formatPropertyTypeLabel(row.property_type),
        lat,
        lng,
        thumb: row.display_image ? `/uploads/${row.display_image}` : null,
        url: `/property/${row.id}`
      });
    }

    res.json(out);
  } catch (err) {
    res.status(500).json({ error: err.message || "Map data error" });
  }
});

function getListingBrokers() {
  if (useSupabase) return [];
  return db.prepare("SELECT id, name FROM agents ORDER BY name ASC").all();
}

function parseBrokerId(raw) {
  if (useSupabase) return null;
  if (raw === undefined || raw === null || raw === "") return null;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return null;
  const row = db.prepare("SELECT id FROM agents WHERE id = ?").get(n);
  return row ? n : null;
}

// LOGIN — username + password against `admins` table
app.get("/admin/login", (req, res) => {
  if (useStatelessAdminAuth) {
    const cookies = parseCookies(req);
    const raw = cookies[ADMIN_COOKIE_NAME];
    const ok = verifyAdminToken(raw);
    if (ok) return res.redirect("/admin");
    if (raw) clearAdminCookie(res);
  } else if (req.session.loggedIn) {
    return res.redirect("/admin");
  }
  res.render("login", { error: null });
});

app.post("/admin/login", (req, res) => {
  const username = String(req.body.username ?? "").trim();
  const password = String(req.body.password ?? "");
  if (useSupabase) {
    const expectedUser = String(process.env.ADMIN_USERNAME || "admin").trim();
    const expectedPass = String(process.env.ADMIN_PASSWORD || "").trim();
    const expectedPass2 = String(process.env.ADMIN_PASSWORD_2 || "").trim();
    if (!expectedPass && !expectedPass2) {
      return res.render("login", {
        error:
          "Admin login is not configured for this deployment. Set ADMIN_USERNAME and ADMIN_PASSWORD in Vercel Environment Variables (make sure it’s added for the correct environment: Production + Preview)."
      });
    }
    const ok =
      username &&
      password &&
      username === expectedUser &&
      ((expectedPass && password === expectedPass) || (expectedPass2 && password === expectedPass2));
    if (!ok) {
      return res.render("login", { error: "Invalid username or password" });
    }
    if (useStatelessAdminAuth) {
      setAdminCookie(res, expectedUser);
      return res.redirect("/admin");
    }
    req.session.loggedIn = true;
    req.session.adminUsername = expectedUser;
    return req.session.save((err) => {
      if (err) {
        console.error("admin session save:", err);
        return res.render("login", {
          error: "Could not start session — try again."
        });
      }
      res.redirect("/admin");
    });
  }
  const admin = db
    .prepare("SELECT id, username FROM admins WHERE username = ? AND password = ?")
    .get(username, password);
  if (!admin) return res.render("login", { error: "Invalid username or password" });
  if (useStatelessAdminAuth) {
    setAdminCookie(res, admin.username);
    return res.redirect("/admin");
  }
  req.session.loggedIn = true;
  req.session.adminId = admin.id;
  req.session.adminUsername = admin.username;
  req.session.save((err) => {
    if (err) {
      console.error("admin session save:", err);
      return res.render("login", {
        error: "Could not start session — try again."
      });
    }
    res.redirect("/admin");
  });
});

/** One-click access to admin (no credentials). Same idea as the original passwordless login. */
app.get("/admin/login/quick", (req, res) => {
  // Safer "quick login": require a token in production.
  // Set QUICK_LOGIN_TOKEN in Vercel env vars, then visit /admin/login/quick?t=YOUR_TOKEN
  if (process.env.VERCEL || useSupabase) {
    const token = String(process.env.QUICK_LOGIN_TOKEN || "").trim();
    const provided =
      (req.query && (req.query.t || req.query.token)) != null
        ? String(req.query.t || req.query.token).trim()
        : "";
    if (!token) {
      return res
        .status(403)
        .type("text")
        .send("Quick login is disabled. Set QUICK_LOGIN_TOKEN to enable it.");
    }
    if (!provided || provided !== token) {
      return res.status(403).type("text").send("Invalid quick login token.");
    }
  }
  if (useStatelessAdminAuth) {
    setAdminCookie(res, "quick");
    return res.redirect("/admin");
  }
  req.session.loggedIn = true;
  delete req.session.adminId;
  delete req.session.adminUsername;
  req.session.save((err) => {
    if (err) {
      console.error("admin session save:", err);
      return res.redirect("/admin/login");
    }
    res.redirect("/admin");
  });
});

app.post("/admin/logout", (req, res) => {
  if (useStatelessAdminAuth) {
    clearAdminCookie(res);
    return res.redirect("/admin/login");
  }
  req.session.destroy(() => {
    res.redirect("/admin/login");
  });
});

const AGENT_DAILY_QUOTES = [
  "The deal you don’t chase today is someone else’s commission tomorrow.",
  "Consistency beats intensity — show up, follow up, close up.",
  "Every ‘no’ clears the path to the right ‘yes’.",
  "Your pipeline is a mirror: feed it, and it feeds you.",
  "Industrial grit, premium service — that’s the standard.",
  "Small steps in prospecting become giant jumps in revenue.",
  "Listen more than you pitch; the lease writes itself.",
  "Today’s preparation is tomorrow’s signature.",
  "Momentum is rented — pay the daily deposit.",
  "Be so reliable they forget to shop around.",
  "Clarity closes deals — confusion costs rent.",
  "Hustle with heart; the rest is paperwork.",
  "Your territory grows where your curiosity goes.",
  "Stack wins, not excuses.",
  "The market rewards the visible — stay in front of it.",
  "Precision in follow-up beats perfection in theory.",
  "Energy is contagious — bring it into every room.",
  "Build trust in inches; lose it in seconds — protect it.",
  "Rent rolls up from relationships — invest there first.",
  "Finish strong: the last mile is where deals die or fly."
];

function getDailyMotivationalQuote() {
  const day = Math.floor(Date.now() / 86400000);
  return AGENT_DAILY_QUOTES[day % AGENT_DAILY_QUOTES.length];
}

// ADMIN DASHBOARD
app.get("/admin", requireLogin, async (req, res, next) => {
  const adminArea = req.query.adminArea || "";
  const success = req.query.success || null;
  try {
    const properties = supabase
      ? await sbGetAdminProperties(adminArea)
      : getAdminProperties(adminArea);
    res.render("admin", {
      properties,
      adminArea,
      success
    });
  } catch (e) {
    next(e);
  }
});

// Safety: some cached HTML or mis-posts can hit POST /admin (should be GET).
// Avoid 404s by redirecting to the correct page.
app.post("/admin", (req, res) => {
  if (useStatelessAdminAuth) {
    const cookies = parseCookies(req);
    if (verifyAdminToken(cookies[ADMIN_COOKIE_NAME])) return res.redirect("/admin");
    return res.redirect("/admin/login");
  }
  if (req.session && req.session.loggedIn) return res.redirect("/admin");
  return res.redirect("/admin/login");
});

app.get("/admin/capture-lead", requireLogin, (req, res) => {
  res.render("admin-capture-lead", {
    saved: req.query.saved === "1",
    error: req.query.error === "1"
  });
});

app.post("/admin/capture-lead", requireLogin, (req, res) => {
  if (useSupabase) return res.status(503).type("text").send("Lead capture not migrated yet.");
  const name = req.body.name != null ? String(req.body.name).trim() : "";
  const phone = req.body.phone != null ? String(req.body.phone).trim() : "";
  const email = req.body.email != null ? String(req.body.email).trim() : "";
  const message = req.body.message != null ? String(req.body.message).trim() : "";
  if (!name || !phone) {
    return res.redirect("/admin/capture-lead?error=1");
  }
  db.prepare(
    `
    INSERT INTO property_enquiries (
      property_id, property_label, property_address,
      enquirer_name, phone, email, message
    ) VALUES (NULL, ?, NULL, ?, ?, ?, ?)
  `
  ).run("Captured lead (admin)", name, phone, email || null, message || null);
  res.redirect("/admin/capture-lead?saved=1");
});

app.get("/admin/agent-zone", requireLogin, (req, res) => {
  const run = async () => {
    const agents = useSupabase ? await sbGetAgents() : getAgents();
    const selectedAgentId =
      Number(req.query.agentId) || (agents[0] && agents[0].id) || null;
    const yq = (req.query.year || "").trim();
    const currentY = new Date().getFullYear();
    const year = yq && /^\d{4}$/.test(yq) ? yq : String(currentY);
    const yearNum = parseInt(year, 10);
  let yearOptions = [currentY + 1, currentY, currentY - 1, currentY - 2, currentY - 3];
  if (Number.isFinite(yearNum) && !yearOptions.includes(yearNum)) {
    yearOptions.push(yearNum);
    yearOptions.sort((a, b) => b - a);
  }

  let deals = [];
  let stats = { count: 0, totalInvoice: 0, totalAgentCommission: 0, totalCompanyShare: 0 };
  let monthlyInvoiceActual = Array(12).fill(0);
  let monthlyInvoiceExpected = Array(12).fill(0);
  const invoiceTargetMonthly = getMonthlyInvoiceTargetForYear(yearNum);
  const annualTargetMonths = getAnnualTargetMonthsForYear(yearNum);
  let chartMax = invoiceTargetMonthly;
  let annualInvoiceTargetZar = getAnnualInvoiceTargetZar(yearNum);
  let annualInvoiceConfirmed = 0;
  let annualInvoiceExpected = 0;
  let annualHitPct = 0;
  let pieConfirmedPct = 0;
  let pieTotalPct = 0;
  let dashboardYearConfirmed = {
    invoiceIncl: 0,
    invoiceExcl: 0,
    payoutIncl: 0,
    payoutExcl: 0
  };
  let dashboardYearExpected = {
    invoiceIncl: 0,
    invoiceExcl: 0,
    payoutIncl: 0,
    payoutExcl: 0
  };
  let dashboardMonthConfirmed = {
    invoiceIncl: 0,
    invoiceExcl: 0,
    payoutIncl: 0,
    payoutExcl: 0
  };
  let dashboardMonthExpected = {
    invoiceIncl: 0,
    invoiceExcl: 0,
    payoutIncl: 0,
    payoutExcl: 0
  };
  let dashboardYearConfirmedRental = 0;
  let dashboardYearExpectedRental = 0;
  let dashboardMonthConfirmedRental = 0;
  let dashboardMonthExpectedRental = 0;
  let thisMonthLabel = "";
  let dealsChronological = [];
  if (selectedAgentId) {
    deals = useSupabase
      ? await sbGetDealsForAgent(selectedAgentId, year)
      : getDealsForAgent(selectedAgentId, year);
    dealsChronological = [...deals].sort((a, b) =>
      dealReportingIsoDate(a).localeCompare(dealReportingIsoDate(b))
    );
    stats = computeDealStats(deals);
    const split = computeMonthlyInvoiceSplit(deals, yearNum);
    monthlyInvoiceActual = split.actual;
    monthlyInvoiceExpected = split.expected;
    const monthCombined = monthlyInvoiceActual.map(
      (a, i) => a + monthlyInvoiceExpected[i]
    );
    chartMax = Math.max(invoiceTargetMonthly, ...monthCombined, 1);

    const yPart = partitionDealsExpected(deals);
    dashboardYearConfirmed = moneyPairFromDeals(yPart.actual);
    dashboardYearExpected = moneyPairFromDeals(yPart.expected);
    const confRent = sumInvoiceTargetForDeals(yPart.actual);
    const expRent = sumInvoiceTargetForDeals(yPart.expected);
    annualInvoiceConfirmed = confRent;
    annualInvoiceExpected = expRent;
    dashboardYearConfirmedRental = confRent;
    dashboardYearExpectedRental = expRent;
    annualInvoiceTargetZar = getAnnualInvoiceTargetZar(yearNum);
    if (annualInvoiceTargetZar > 0) {
      const totalRent = confRent + expRent;
      annualHitPct = (totalRent / annualInvoiceTargetZar) * 100;
      pieConfirmedPct = Math.min(100, (confRent / annualInvoiceTargetZar) * 100);
      pieTotalPct = Math.min(100, (totalRent / annualInvoiceTargetZar) * 100);
    }

    const now = new Date();
    thisMonthLabel = now.toLocaleDateString("en-ZA", {
      month: "long",
      year: "numeric"
    });
    const dm = getDealsForAgentInCalendarMonth(
        selectedAgentId,
        now.getFullYear(),
        now.getMonth(),
        deals
      );
    const mPart = partitionDealsExpected(dm);
    dashboardMonthConfirmed = moneyPairFromDeals(mPart.actual);
    dashboardMonthExpected = moneyPairFromDeals(mPart.expected);
    dashboardMonthConfirmedRental = sumInvoiceTargetForDeals(mPart.actual);
    dashboardMonthExpectedRental = sumInvoiceTargetForDeals(mPart.expected);

  }

  const commissionInvoiceTargetAnnual =
    Number.isFinite(invoiceTargetMonthly) && invoiceTargetMonthly > 0
      ? getAnnualInvoiceTargetZar(yearNum)
      : 0;
  const totalInvoiceForYear = stats.totalInvoice || 0;
  const invoiceVsTargetPct =
    commissionInvoiceTargetAnnual > 0
      ? Math.min(999, (totalInvoiceForYear / commissionInvoiceTargetAnnual) * 100)
      : 0;

  res.render("agent-zone", {
    agents,
    selectedAgentId,
    year,
    yearOptions,
    deals,
    stats,
    monthlyInvoiceActual,
    monthlyInvoiceExpected,
    invoiceTargetMonthly,
    annualTargetMonths,
    annualInvoiceTargetZar,
    annualInvoiceConfirmed,
    annualInvoiceExpected,
    annualHitPct,
    pieConfirmedPct,
    pieTotalPct,
    chartMax,
    dashboardYearConfirmed,
    dashboardYearExpected,
    dashboardMonthConfirmed,
    dashboardMonthExpected,
    dashboardYearConfirmedRental,
    dashboardYearExpectedRental,
    dashboardMonthConfirmedRental,
    dashboardMonthExpectedRental,
    thisMonthLabel,
    dailyQuote: getDailyMotivationalQuote(),
    commissionInvoiceTargetAnnual,
    totalInvoiceForYear,
    invoiceVsTargetPct,
    dealsChronological
  });
  };
  run().catch((e) => {
    console.error("agent-zone:", e);
    res.status(500).type("text").send("Agent Zone failed to load.");
  });
});

app.get("/admin/agent-zone/month", requireLogin, (req, res) => {
  const run = async () => {
    const agents = useSupabase ? await sbGetAgents() : getAgents();
    const selectedAgentId =
      Number(req.query.agentId) || (agents[0] && agents[0].id) || null;
    const yq = (req.query.year || "").trim();
    const currentY = new Date().getFullYear();
    const year = yq && /^\d{4}$/.test(yq) ? yq : String(currentY);
  const monthQ = (req.query.month || "").trim();
  let selectedMonth = null;
  let monthDetailDeals = [];
  let monthsWithDeals = [];

  if (!selectedAgentId) {
    return res.redirect("/admin/agent-zone");
  }
  if (monthQ === "" || !/^\d{1,2}$/.test(monthQ)) {
    return res.redirect(
      `/admin/agent-zone?agentId=${selectedAgentId}&year=${encodeURIComponent(year)}`
    );
  }
  const mi = parseInt(monthQ, 10);
  if (mi < 0 || mi > 11) {
    return res.redirect(
      `/admin/agent-zone?agentId=${selectedAgentId}&year=${encodeURIComponent(year)}`
    );
  }
  selectedMonth = mi;
    const deals = useSupabase
      ? await sbGetDealsForAgent(selectedAgentId, year)
      : getDealsForAgent(selectedAgentId, year);
    monthDetailDeals = deals.filter((d) => dealMonthIndexForYear(d, parseInt(year, 10)) === mi);
    const seen = new Set();
    deals.forEach((d) => {
      const idx = dealMonthIndexForYear(d, parseInt(year, 10));
      if (idx != null) seen.add(idx);
    });
    monthsWithDeals = [...seen].sort((a, b) => a - b);

  const monthNames = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
  ];
  const monthNamesShort = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec"
  ];

  res.render("agent-zone-month", {
    agents,
    selectedAgentId,
    year,
    selectedMonth,
    monthDetailDeals,
    monthsWithDeals,
    monthNames,
    monthNamesShort
  });
  };
  run().catch((e) => {
    console.error("agent-zone-month:", e);
    res.status(500).type("text").send("Agent Zone month failed to load.");
  });
});

app.get("/admin/notifications", requireLogin, (req, res) => {
  const rows = getLeaseNotificationRows().map((r) => ({
    ...r,
    daysLeft: daysUntilLeaseEnd(r.lease_end_date)
  }));

  const expiresToday = rows.filter((r) => r.daysLeft === 0);
  const exactly30 = rows.filter((r) => r.daysLeft === 30);
  const next30 = rows
    .filter((r) => r.daysLeft > 0 && r.daysLeft < 30)
    .sort((a, b) => a.daysLeft - b.daysLeft);
  const later = rows
    .filter((r) => r.daysLeft > 30 && r.daysLeft <= 180)
    .sort((a, b) => a.daysLeft - b.daysLeft);
  const overdue = rows
    .filter((r) => r.daysLeft < 0)
    .sort((a, b) => b.daysLeft - a.daysLeft);

  const enquiries = getRecentEnquiries(150);

  res.render("notifications", {
    expiresToday,
    next30,
    exactly30,
    later,
    overdue,
    enquiries
  });
});

app.get("/admin/agent-zone/deals/new", requireLogin, (req, res) => {
  const run = async () => {
  const agents = useSupabase ? await sbGetAgents() : getAgents();
  const agentId = Number(req.query.agentId) || (agents[0] && agents[0].id);
  const yq = (req.query.year || "").trim();
  const filterYear =
    yq && /^\d{4}$/.test(yq) ? yq : String(new Date().getFullYear());
  res.render("agent-deal-form", {
    pageTitle: "Add Deal",
    formAction: "/admin/agent-zone/deals/new",
    agents,
    deal: null,
    selectedAgentId: agentId,
    dealPrefillList: useSupabase ? await sbGetPropertiesForDealPrefill() : getPropertiesForDealPrefill(),
    filterYear
  });
  };
  run().catch((e) => {
    console.error("deal new:", e);
    res.status(500).type("text").send("Could not load deal form.");
  });
});

app.post(
  "/admin/agent-zone/deals/new",
  requireLogin,
  (req, res) => {
    const {
      agent_id,
      property_name,
      property_address,
      lease_period: leasePeriodLegacy,
      link_url,
      actual_rental,
      agent_share_percent,
      notes,
      beneficial_occupation_date,
      lease_commencement_date,
      is_expected,
      show_on_done_deals
    } = req.body;

    const isExpected = is_expected === "1" || is_expected === 1 ? 1 : 0;
    const showOnDoneDeals =
      show_on_done_deals === "1" || show_on_done_deals === 1 ? 1 : 0;

    const deal_date = resolveDealDateForStorage(req.body);
    const lease_start =
      lease_commencement_date && String(lease_commencement_date).trim().slice(0, 10);
    const benStored =
      beneficial_occupation_date &&
      String(beneficial_occupation_date).trim().slice(0, 10);
    const commStored = lease_start || null;

    const lease_end = resolveLeaseEndDate(req.body, null);
    const lease_period =
      buildLeasePeriodText(req.body, lease_end) ||
      (leasePeriodLegacy && String(leasePeriodLegacy).trim()) ||
      "";

    const { invoiceTotal: inv, dealAmountType } = resolveDealInvoiceTotal(req.body);
    const share =
      agent_share_percent === "" || agent_share_percent == null
        ? 50
        : Number(agent_share_percent);

    const askingFmt = "";
    const actualFmt = formatPriceForSave(actual_rental || "");

    if (useSupabase) {
      (async () => {
        const { error } = await supabase.from("deals").insert([
          {
            agent_id: Number(agent_id),
            property_name: property_name || "",
            property_address: property_address || "",
            deal_date: deal_date || "",
            lease_period,
            link_url: link_url || "",
            asking_rental: askingFmt,
            actual_rental: actualFmt,
            escalation_period: "",
            invoice_total: inv != null && Number.isFinite(inv) ? inv : null,
            agent_share_percent: Number.isFinite(share) ? share : 50,
            notes: notes || "",
            deal_image: null,
            lease_start_date: lease_start || null,
            lease_end_date: lease_end || null,
            deal_amount_type: dealAmountType,
            is_expected: isExpected,
            beneficial_occupation_date: benStored || null,
            lease_commencement_date: commStored,
            show_on_done_deals: showOnDoneDeals
          }
        ]);
        if (error) throw error;
        const dealY =
          deal_date && String(deal_date).trim().length >= 4
            ? String(deal_date).trim().slice(0, 4)
            : String(new Date().getFullYear());
        res.redirect(
          `/admin/agent-zone?agentId=${Number(agent_id)}&year=${encodeURIComponent(dealY)}`
        );
      })().catch((e) => {
        console.error("deal create:", e);
        res.status(500).type("text").send("Could not save deal.");
      });
      return;
    }

    const insertInfo = db.prepare(`
      INSERT INTO deals (
        agent_id, property_name, property_address, deal_date, lease_period, link_url,
        asking_rental, actual_rental, escalation_period, invoice_total, agent_share_percent, notes,
        deal_image, lease_start_date, lease_end_date, deal_amount_type, is_expected,
        beneficial_occupation_date, lease_commencement_date, show_on_done_deals
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      Number(agent_id),
      property_name || "",
      property_address || "",
      deal_date || "",
      lease_period,
      link_url || "",
      askingFmt,
      actualFmt,
      "",
      inv != null && Number.isFinite(inv) ? inv : null,
      Number.isFinite(share) ? share : 50,
      notes || "",
      null,
      lease_start || null,
      lease_end || null,
      dealAmountType,
      isExpected,
      benStored || null,
      commStored,
      showOnDoneDeals
    );
    syncDealMapCoordsFromPropertyLink(Number(insertInfo.lastInsertRowid));

    const dealY =
      deal_date && String(deal_date).trim().length >= 4
        ? String(deal_date).trim().slice(0, 4)
        : String(new Date().getFullYear());
    res.redirect(
      `/admin/agent-zone?agentId=${Number(agent_id)}&year=${encodeURIComponent(dealY)}`
    );
  }
);

app.get("/admin/agent-zone/deals/:id/edit", requireLogin, (req, res) => {
  const deal = db.prepare("SELECT * FROM deals WHERE id = ?").get(req.params.id);
  if (!deal) return res.redirect("/admin/agent-zone");

  deal.actual_rental = parsePriceForForm(deal.actual_rental || "");

  const commForTerm =
    (deal.lease_commencement_date && String(deal.lease_commencement_date).trim()) ||
    (deal.lease_start_date && String(deal.lease_start_date).trim()) ||
    "";
  const term = deriveLeaseTermFromStartEnd(commForTerm, deal.lease_end_date);
  deal._lease_years = term.years;
  deal._lease_months = term.months;

  if (deal.invoice_total != null && String(deal.invoice_total).trim() !== "") {
    const invN = Number(deal.invoice_total);
    if (Number.isFinite(invN)) deal.invoice_total_input = Math.round(invN * 100) / 100;
  }

  const agents = getAgents();
  const yq = (req.query.year || "").trim();
  const anchorY =
    (deal.lease_commencement_date && String(deal.lease_commencement_date).trim()) ||
    (deal.lease_start_date && String(deal.lease_start_date).trim()) ||
    (deal.deal_date && String(deal.deal_date).trim()) ||
    (deal.beneficial_occupation_date && String(deal.beneficial_occupation_date).trim()) ||
    "";
  const filterYear =
    yq && /^\d{4}$/.test(yq)
      ? yq
      : anchorY.length >= 4
        ? anchorY.slice(0, 4)
        : String(new Date().getFullYear());
  res.render("agent-deal-form", {
    pageTitle: "Edit Deal",
    formAction: `/admin/agent-zone/deals/${deal.id}/edit`,
    agents,
    deal,
    selectedAgentId: deal.agent_id,
    dealPrefillList: getPropertiesForDealPrefill(),
    filterYear
  });
});

app.post(
  "/admin/agent-zone/deals/:id/edit",
  requireLogin,
  (req, res) => {
    const deal = db.prepare("SELECT * FROM deals WHERE id = ?").get(req.params.id);
    if (!deal) return res.redirect("/admin/agent-zone");

    const {
      agent_id,
      property_name,
      property_address,
      lease_period: leasePeriodLegacy,
      link_url,
      actual_rental,
      agent_share_percent,
      notes,
      beneficial_occupation_date,
      lease_commencement_date,
      is_expected,
      show_on_done_deals
    } = req.body;

    const isExpected = is_expected === "1" || is_expected === 1 ? 1 : 0;
    const showOnDoneDeals =
      show_on_done_deals === "1" || show_on_done_deals === 1 ? 1 : 0;

    const deal_date = resolveDealDateForStorage(req.body);
    const lease_start =
      lease_commencement_date && String(lease_commencement_date).trim().slice(0, 10);
    const benStored =
      beneficial_occupation_date &&
      String(beneficial_occupation_date).trim().slice(0, 10);
    const commStored = lease_start || null;

    const lease_end = resolveLeaseEndDate(req.body, deal.lease_end_date);
    const lease_period =
      buildLeasePeriodText(req.body, lease_end) ||
      (leasePeriodLegacy && String(leasePeriodLegacy).trim()) ||
      "";

    const { invoiceTotal: inv, dealAmountType } = resolveDealInvoiceTotal(req.body);
    const share =
      agent_share_percent === "" || agent_share_percent == null
        ? 50
        : Number(agent_share_percent);

    const dealImage = deal.deal_image;

    const askingFmt = "";
    const actualFmt = formatPriceForSave(actual_rental || "");

    db.prepare(`
      UPDATE deals SET
        agent_id = ?,
        property_name = ?,
        property_address = ?,
        deal_date = ?,
        lease_period = ?,
        link_url = ?,
        asking_rental = ?,
        actual_rental = ?,
        escalation_period = ?,
        invoice_total = ?,
        agent_share_percent = ?,
        notes = ?,
        deal_image = ?,
        lease_start_date = ?,
        lease_end_date = ?,
        deal_amount_type = ?,
        is_expected = ?,
        beneficial_occupation_date = ?,
        lease_commencement_date = ?,
        show_on_done_deals = ?
      WHERE id = ?
    `).run(
      Number(agent_id),
      property_name || "",
      property_address || "",
      deal_date || "",
      lease_period,
      link_url || "",
      askingFmt,
      actualFmt,
      "",
      inv != null && Number.isFinite(inv) ? inv : null,
      Number.isFinite(share) ? share : 50,
      notes || "",
      dealImage,
      lease_start || null,
      lease_end || null,
      dealAmountType,
      isExpected,
      benStored || null,
      commStored,
      showOnDoneDeals,
      deal.id
    );
    syncDealMapCoordsFromPropertyLink(deal.id);

    const dealY =
      deal_date && String(deal_date).trim().length >= 4
        ? String(deal_date).trim().slice(0, 4)
        : String(new Date().getFullYear());
    res.redirect(
      `/admin/agent-zone?agentId=${Number(agent_id)}&year=${encodeURIComponent(dealY)}`
    );
  }
);

app.post("/admin/agent-zone/deals/:id/delete", requireLogin, (req, res) => {
  const deal = db.prepare("SELECT * FROM deals WHERE id = ?").get(req.params.id);
  if (!deal) return res.redirect("/admin/agent-zone");
  safeUnlinkUpload(deal.deal_image);
  db.prepare("DELETE FROM deals WHERE id = ?").run(deal.id);
  const y =
    deal.deal_date && String(deal.deal_date).trim().length >= 4
      ? String(deal.deal_date).trim().slice(0, 4)
      : String(new Date().getFullYear());
  res.redirect(
    `/admin/agent-zone?agentId=${deal.agent_id}&year=${encodeURIComponent(y)}`
  );
});

app.get("/admin/featured-home", requireLogin, (req, res) => {
  if (useSupabase) {
    (async () => {
      const { data: rows, error } = await supabase
        .from("home_featured_slots")
        .select("slot, property_id, feature_style")
        .order("slot", { ascending: true });
      if (error) throw error;
      const slot1 = (rows || []).find((r) => Number(r.slot) === 1)?.property_id ?? null;
      const slot2 = (rows || []).find((r) => Number(r.slot) === 2)?.property_id ?? null;
      const styleOf = (slot) => {
        const raw = (rows || []).find((r) => Number(r.slot) === slot)?.feature_style;
        return String(raw ?? "").trim().toLowerCase() === "api" ? "api" : "orbit";
      };
      const slot1_style = styleOf(1);
      const slot2_style = styleOf(2);
      res.render("featured-home", {
        pageTitle: "Featured Listings",
        listings: await sbGetListingsForFeaturedPicker(),
        slot1,
        slot2,
        slot1_style,
        slot2_style,
        success: req.query.success === "1",
        error: req.query.error === "same" ? "same" : null
      });
    })().catch((e) => {
      console.error("featured-home:", e);
      res.status(500).type("text").send("Could not load featured listings.");
    });
    return;
  }

  const rows = db
    .prepare("SELECT slot, property_id, feature_style FROM home_featured_slots ORDER BY slot")
    .all();
  const slot1 = rows.find((r) => r.slot === 1)?.property_id ?? null;
  const slot2 = rows.find((r) => r.slot === 2)?.property_id ?? null;
  const styleOf = (slot) => {
    const raw = rows.find((r) => r.slot === slot)?.feature_style;
    return String(raw ?? "").trim().toLowerCase() === "api" ? "api" : "orbit";
  };
  const slot1_style = styleOf(1);
  const slot2_style = styleOf(2);
  res.render("featured-home", {
    pageTitle: "Featured Listings",
    listings: getListingsForFeaturedPicker(),
    slot1,
    slot2,
    slot1_style,
    slot2_style,
    success: req.query.success === "1",
    error: req.query.error === "same" ? "same" : null
  });
});

app.post("/admin/featured-home", requireLogin, (req, res) => {
  const st1 =
    String(req.body.slot1_style || "").trim().toLowerCase() === "api" ? "api" : "orbit";
  const st2 =
    String(req.body.slot2_style || "").trim().toLowerCase() === "api" ? "api" : "orbit";

  if (useSupabase) {
    (async () => {
      const id1 = await sbValidateFeaturedPropertyId(req.body.slot1);
      const id2 = await sbValidateFeaturedPropertyId(req.body.slot2);
      if (id1 != null && id2 != null && id1 === id2) {
        return res.redirect("/admin/featured-home?error=same");
      }
      await sbSetFeaturedHomeSlot(1, id1, st1);
      await sbSetFeaturedHomeSlot(2, id2, st2);
      res.redirect("/admin/featured-home?success=1");
    })().catch((e) => {
      console.error("featured-home save:", e);
      res.status(500).type("text").send("Could not save featured listings.");
    });
    return;
  }

  const id1 = validateFeaturedPropertyId(req.body.slot1);
  const id2 = validateFeaturedPropertyId(req.body.slot2);
  if (id1 != null && id2 != null && id1 === id2) {
    return res.redirect("/admin/featured-home?error=same");
  }
  setFeaturedHomeSlot(1, id1, st1);
  setFeaturedHomeSlot(2, id2, st2);
  res.redirect("/admin/featured-home?success=1");
});

/** JSON prefill for agent deal form (logged-in admin). */
app.get("/admin/api/property/:id/prefill", requireLogin, (req, res) => {
  const p = db
    .prepare(
      "SELECT id, name, address, area, price, size, status, latitude, longitude FROM properties WHERE id = ?"
    )
    .get(req.params.id);
  if (!p) return res.status(404).json({ error: "Not found" });
  res.json({
    ...p,
    asking_rental_input: parsePriceForForm(p.price || "")
  });
});

app.get("/admin/buildings", requireLogin, (req, res) => {
  if (useSupabase) return res.status(503).type("text").send("Buildings are not migrated yet.");
  const buildings = db
    .prepare(`
      SELECT b.*,
        (SELECT COUNT(*) FROM properties p WHERE p.building_id = b.id) AS unit_count
      FROM buildings b
      ORDER BY b.name ASC
    `)
    .all();
  res.render("buildings-list", { buildings });
});

app.get("/admin/buildings/new", requireLogin, (req, res) => {
  if (useSupabase) return res.status(503).type("text").send("Buildings are not migrated yet.");
  res.render("building-form", {
    pageTitle: "Add building / park",
    formAction: "/admin/buildings/new",
    building: null
  });
});

app.post(
  "/admin/buildings/new",
  requireLogin,
  upload.fields([
    { name: "displayImage", maxCount: 1 },
    { name: "images", maxCount: 30 }
  ]),
  (req, res) => {
    if (useSupabase) return res.status(503).type("text").send("Buildings are not migrated yet.");
    const { name, description, size_text, features } = req.body;
    const displayImage = req.files?.displayImage?.[0]?.filename || null;

    const result = db
      .prepare(`
        INSERT INTO buildings (name, description, size_text, features, display_image)
        VALUES (?, ?, ?, ?, ?)
      `)
      .run(
        name || "Unnamed",
        description || "",
        size_text || "",
        features || "",
        displayImage
      );

    const buildingId = result.lastInsertRowid;

    if (req.files?.images?.length) {
      req.files.images.forEach((file, index) => {
        db.prepare(`
          INSERT INTO building_images (building_id, filename, image_order)
          VALUES (?, ?, ?)
        `).run(buildingId, file.filename, index + 1);
      });
    }

    res.redirect("/admin/buildings");
  }
);

app.get("/admin/buildings/:id/edit", requireLogin, (req, res) => {
  if (useSupabase) return res.status(503).type("text").send("Buildings are not migrated yet.");
  const building = db.prepare("SELECT * FROM buildings WHERE id = ?").get(req.params.id);
  if (!building) return res.redirect("/admin/buildings");
  building.images = getBuildingImages(building.id);
  res.render("building-form", {
    pageTitle: "Edit building / park",
    formAction: `/admin/buildings/${building.id}/edit`,
    building
  });
});

app.post(
  "/admin/buildings/:id/edit",
  requireLogin,
  upload.fields([
    { name: "displayImage", maxCount: 1 },
    { name: "images", maxCount: 30 }
  ]),
  (req, res) => {
    if (useSupabase) return res.status(503).type("text").send("Buildings are not migrated yet.");
    const id = req.params.id;
    const existing = db.prepare("SELECT * FROM buildings WHERE id = ?").get(id);
    if (!existing) return res.redirect("/admin/buildings");

    const { name, description, size_text, features } = req.body;
    const displayImage =
      req.files?.displayImage?.[0]?.filename || existing.display_image || null;

    db.prepare(`
      UPDATE buildings SET
        name = ?,
        description = ?,
        size_text = ?,
        features = ?,
        display_image = ?
      WHERE id = ?
    `).run(
      name || "Unnamed",
      description || "",
      size_text || "",
      features || "",
      displayImage,
      id
    );

    if (req.files?.images?.length) {
      const currentMax =
        db
          .prepare(`
            SELECT COALESCE(MAX(image_order), 0) AS maxOrder
            FROM building_images
            WHERE building_id = ?
          `)
          .get(id).maxOrder || 0;

      req.files.images.forEach((file, index) => {
        db.prepare(`
          INSERT INTO building_images (building_id, filename, image_order)
          VALUES (?, ?, ?)
        `).run(id, file.filename, currentMax + index + 1);
      });
    }

    res.redirect("/admin/buildings");
  }
);

app.post("/admin/buildings/:id/delete", requireLogin, (req, res) => {
  if (useSupabase) return res.status(503).type("text").send("Buildings are not migrated yet.");
  const b = db.prepare("SELECT * FROM buildings WHERE id = ?").get(req.params.id);
  if (!b) return res.redirect("/admin/buildings");

  getBuildingImages(b.id).forEach((img) => safeUnlinkUpload(img.filename));
  safeUnlinkUpload(b.display_image);

  db.prepare("UPDATE properties SET building_id = NULL WHERE building_id = ?").run(b.id);
  db.prepare("DELETE FROM buildings WHERE id = ?").run(b.id);

  res.redirect("/admin/buildings");
});

// ADD PROPERTY PAGE
app.get("/admin/properties/new", requireLogin, (req, res) => {
  res.render("property-form", {
    pageTitle: "Add Property",
    formAction: "/admin/properties/new",
    property: null,
    buildings: getBuildingsList(),
    listingBrokers: getListingBrokers()
  });
});

// ADD PROPERTY SUBMIT
app.post(
  "/admin/properties/new",
  requireLogin,
  uploadPropertyForm.fields([
    { name: "displayImage", maxCount: 1 },
    { name: "images", maxCount: 40 },
    { name: "propertyVideo", maxCount: 1 }
  ]),
  async (req, res, next) => {
    try {
      const {
        name,
        area,
        status,
        priority_group,
        size,
        address,
        price,
        availability,
        description,
        notes,
        building_id,
        use_unit_details,
        broker_id,
        power_phase,
        power_amps,
        height_eave_apex,
        height_eave_roller_shutter,
        parking_bays,
        yard_space,
        property_type
      } = req.body;

      // Basic required fields (HTML already requires these, but protect server-side too)
      const nameTrim = name != null ? String(name).trim() : "";
      const areaTrim = area != null ? String(area).trim() : "";
      const statusTrim = status != null ? String(status).trim() : "";
      if (!nameTrim || !areaTrim || !statusTrim) {
        return res
          .status(400)
          .type("text")
          .send("Missing required fields: name, area, and status are required.");
      }

      const displayImage = req.files?.displayImage?.[0]?.filename || null;
      let videoFilename = req.files?.propertyVideo?.[0]?.filename || null;
      let youtubeVideoId = parseYoutubeVideoId(req.body.youtube_url);
      if (videoFilename) {
        youtubeVideoId = null;
      } else if (youtubeVideoId) {
        videoFilename = null;
      }

      const bid =
        building_id === "" || building_id == null ? null : Number(building_id);
      const useUnit = use_unit_details === "1" ? 1 : 0;
      const brokerId = parseBrokerId(broker_id);
      const propType = normalizePropertyType(property_type);

      const sizeFormatted = formatSizeForSave(size);
      const priceFormatted = formatPriceForSave(price);
      const availabilityVal =
        availability && String(availability).trim() ? String(availability).trim() : "";

      const pp =
        power_phase === "3-phase" || power_phase === "single-phase"
          ? power_phase
          : "";
      const amps = power_amps && String(power_amps).trim() ? String(power_amps).trim() : "";
      const hApex =
        height_eave_apex && String(height_eave_apex).trim()
          ? String(height_eave_apex).trim()
          : "";
      const hRs =
        height_eave_roller_shutter && String(height_eave_roller_shutter).trim()
          ? String(height_eave_roller_shutter).trim()
          : "";
      const park =
        parking_bays && String(parking_bays).trim() ? String(parking_bays).trim() : "";
      const yardSpace =
        yard_space && String(yard_space).trim() ? String(yard_space).trim() : "";

      if (supabase) {
        // Insert first to get an id, then upload files to Storage and write image rows.
        const { data: created } = await sbInsertWithDropUnknownColumns(
          "properties",
          {
            name: nameTrim,
            area: areaTrim,
            status: statusTrim,
            priority_group: priority_group || "medium",
            size: sizeFormatted,
            address: address || "",
            price: priceFormatted,
            availability: availabilityVal,
            description: description || "",
            features: "",
            notes: notes || "",
            building_id: Number.isFinite(bid) ? bid : null,
            use_unit_details: useUnit,
            broker_id: brokerId,
            video_filename: videoFilename,
            youtube_video_id: youtubeVideoId,
            power_phase: pp,
            power_amps: amps,
            height_eave_apex: hApex,
            height_eave_roller_shutter: hRs,
            parking_bays: park,
            yard_space: yardSpace,
            property_type: propType
          },
          "id"
        );

        const propertyId = created.id;
        const safePrefix = `properties/${propertyId}`;

        // Cover image
        let displayImagePath = null;
        const coverFile = req.files?.displayImage?.[0] || null;
        if (coverFile) {
          const coverPath = `${safePrefix}/cover-${Date.now()}-${String(
            coverFile.originalname || "cover"
          ).replace(/\s+/g, "-")}`;
          displayImagePath = await sbUploadMulterFile(coverFile, coverPath);
        }

        // Gallery images
        const galleryFiles = req.files?.images || [];
        for (let i = 0; i < galleryFiles.length; i += 1) {
          const f = galleryFiles[i];
          const p = `${safePrefix}/img-${Date.now()}-${i + 1}-${String(
            f.originalname || "image"
          ).replace(/\s+/g, "-")}`;
          const uploadedPath = await sbUploadMulterFile(f, p);
          const { error: imgErr } = await supabase.from("property_images").insert([
            {
              property_id: propertyId,
              storage_path: uploadedPath,
              image_order: i + 1
            }
          ]);
          if (imgErr) throw imgErr;
        }

        // Update property with display_image path (after upload)
        if (displayImagePath) {
          await sbUpdateWithDropUnknownColumns("properties", "id", propertyId, {
            display_image: displayImagePath
          });
        }

        res.redirect("/admin?success=created");
        schedulePropertyGeocode(propertyId, address, area);
        return;
      }

      const result = db.prepare(`
      INSERT INTO properties (
        name,
        area,
        status,
        priority_group,
        size,
        address,
        price,
        availability,
        description,
        features,
        notes,
        display_image,
        building_id,
        use_unit_details,
        broker_id,
        video_filename,
        youtube_video_id,
        power_phase,
        power_amps,
        height_eave_apex,
        height_eave_roller_shutter,
        parking_bays,
        yard_space,
        property_type
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
        name,
        area,
        status,
        priority_group || "medium",
        sizeFormatted,
        address || "",
        priceFormatted,
        availabilityVal,
        description || "",
        "",
        notes || "",
        displayImage,
        Number.isFinite(bid) ? bid : null,
        useUnit,
        brokerId,
        videoFilename,
        youtubeVideoId,
        pp,
        amps,
        hApex,
        hRs,
        park,
        yardSpace,
        propType
      );

      const propertyId = result.lastInsertRowid;

      if (req.files?.images?.length) {
        req.files.images.forEach((file, index) => {
          db.prepare(`
          INSERT INTO property_images (property_id, filename, image_order)
          VALUES (?, ?, ?)
        `).run(propertyId, file.filename, index + 1);
        });
      }

      res.redirect("/admin?success=created");

      // Geocode after responding so slow/blocked map APIs cannot reset the browser connection.
      schedulePropertyGeocode(propertyId, address, area);
    } catch (err) {
      next(err);
    }
  }
);

// EDIT PROPERTY PAGE
app.get("/admin/properties/:id/edit", requireLogin, (req, res) => {
  if (supabase) {
    return res
      .status(302)
      .redirect(`/admin/properties/${encodeURIComponent(req.params.id)}/edit-sb`);
  }
  const property = db.prepare("SELECT * FROM properties WHERE id = ?").get(req.params.id);

  if (!property) {
    return res.redirect("/admin");
  }

  enrichProperty(property);
  property.statusSelect = normalizeStatusForForm(property.status);
  property.sizeInput = parseSizeForForm(property.size);
  property.priceInput = parsePriceForForm(property.price);
  property.availabilityDate = availabilityDateValue(property.availability);

  res.render("property-form", {
    pageTitle: "Edit Property",
    formAction: `/admin/properties/${property.id}/edit`,
    property,
    buildings: getBuildingsList(),
    listingBrokers: getListingBrokers()
  });
});

// EDIT PROPERTY PAGE (Supabase)
app.get("/admin/properties/:id/edit-sb", requireLogin, async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    const { data: property, error } = await supabase
      .from("properties")
      .select("*")
      .eq("id", id)
      .maybeSingle();
    if (error) throw error;
    if (!property) return res.redirect("/admin");
    const imgs = await sbListPropertyImages(id);
    enrichPropertyForRender(property, imgs);
    property.statusSelect = normalizeStatusForForm(property.status);
    property.sizeInput = parseSizeForForm(property.size);
    property.priceInput = parsePriceForForm(property.price);
    property.availabilityDate = availabilityDateValue(property.availability);

    res.render("property-form", {
      pageTitle: "Edit Property",
      formAction: `/admin/properties/${property.id}/edit`,
      property,
      buildings: getBuildingsList(),
      listingBrokers: getListingBrokers()
    });
  } catch (e) {
    next(e);
  }
});

// EDIT PROPERTY SUBMIT
app.post(
  "/admin/properties/:id/edit",
  requireLogin,
  uploadPropertyForm.fields([
    { name: "displayImage", maxCount: 1 },
    { name: "images", maxCount: 40 },
    { name: "propertyVideo", maxCount: 1 }
  ]),
  async (req, res, next) => {
    try {
      const id = req.params.id;

      if (supabase) {
        const pid = Number(id);
        const { data: existing, error: exErr } = await supabase
          .from("properties")
          .select("*")
          .eq("id", pid)
          .maybeSingle();
        if (exErr) throw exErr;
        if (!existing) return res.redirect("/admin");

        const {
          name,
          area,
          status,
          priority_group,
          size,
          address,
          price,
          availability,
          description,
          notes,
          building_id,
          use_unit_details,
          broker_id,
          remove_video,
          power_phase,
          power_amps,
          height_eave_apex,
          height_eave_roller_shutter,
          parking_bays,
          yard_space,
          property_type
        } = req.body;

        const bid =
          building_id === "" || building_id == null ? null : Number(building_id);
        const useUnit = use_unit_details === "1" ? 1 : 0;
        const brokerId = parseBrokerId(broker_id);
        const propType = normalizePropertyType(property_type);

        const sizeFormatted = formatSizeForSave(size);
        const priceFormatted = formatPriceForSave(price);
        const availabilityVal =
          availability && String(availability).trim() ? String(availability).trim() : "";

        const pp =
          power_phase === "3-phase" || power_phase === "single-phase"
            ? power_phase
            : "";
        const amps = power_amps && String(power_amps).trim() ? String(power_amps).trim() : "";
        const hApex =
          height_eave_apex && String(height_eave_apex).trim()
            ? String(height_eave_apex).trim()
            : "";
        const hRs =
          height_eave_roller_shutter && String(height_eave_roller_shutter).trim()
            ? String(height_eave_roller_shutter).trim()
            : "";
        const park =
          parking_bays && String(parking_bays).trim() ? String(parking_bays).trim() : "";
        const yardSpace =
          yard_space && String(yard_space).trim() ? String(yard_space).trim() : "";

        // Video: prefer YouTube; ignore uploaded file in Supabase mode
        let videoFilename = existing.video_filename || null;
        let youtubeVideoId =
          existing.youtube_video_id && String(existing.youtube_video_id).trim()
            ? String(existing.youtube_video_id).trim()
            : null;
        if (remove_video === "1") videoFilename = null;
        if (Object.prototype.hasOwnProperty.call(req.body, "youtube_url")) {
          const ytTrim = req.body.youtube_url != null ? String(req.body.youtube_url).trim() : "";
          youtubeVideoId = ytTrim ? parseYoutubeVideoId(ytTrim) : null;
          if (youtubeVideoId) videoFilename = null;
        }
        // If a user tried to upload a file, delete temp file (but don't store it)
        const uploadedVid = req.files?.propertyVideo?.[0];
        if (uploadedVid && uploadedVid.path) {
          try {
            fs.unlinkSync(uploadedVid.path);
          } catch (_) {}
        }

        const safePrefix = `properties/${pid}`;

        // Cover
        let displayImagePath = existing.display_image || null;
        const coverFile = req.files?.displayImage?.[0] || null;
        if (coverFile) {
          const coverPath = `${safePrefix}/cover-${Date.now()}-${String(
            coverFile.originalname || "cover"
          ).replace(/\s+/g, "-")}`;
          displayImagePath = await sbUploadMulterFile(coverFile, coverPath);
        }

        await sbUpdateWithDropUnknownColumns("properties", "id", pid, {
          name,
          area,
          status,
          priority_group: priority_group || "medium",
          size: sizeFormatted,
          address: address || "",
          price: priceFormatted,
          availability: availabilityVal,
          description: description || "",
          notes: notes || "",
          display_image: displayImagePath,
          building_id: Number.isFinite(bid) ? bid : null,
          use_unit_details: useUnit,
          broker_id: brokerId,
          video_filename: videoFilename,
          youtube_video_id: youtubeVideoId,
          power_phase: pp,
          power_amps: amps,
          height_eave_apex: hApex,
          height_eave_roller_shutter: hRs,
          parking_bays: park,
          yard_space: yardSpace,
          property_type: propType
        });

        // Append gallery images
        const galleryFiles = req.files?.images || [];
        if (galleryFiles.length) {
          const { data: maxRow, error: maxErr } = await supabase
            .from("property_images")
            .select("image_order")
            .eq("property_id", pid)
            .order("image_order", { ascending: false })
            .limit(1)
            .maybeSingle();
          if (maxErr) throw maxErr;
          const currentMax = maxRow && maxRow.image_order ? Number(maxRow.image_order) : 0;
          for (let i = 0; i < galleryFiles.length; i += 1) {
            const f = galleryFiles[i];
            const p = `${safePrefix}/img-${Date.now()}-${currentMax + i + 1}-${String(
              f.originalname || "image"
            ).replace(/\s+/g, "-")}`;
            const uploadedPath = await sbUploadMulterFile(f, p);
            const { error: imgErr } = await supabase.from("property_images").insert([
              {
                property_id: pid,
                storage_path: uploadedPath,
                image_order: currentMax + i + 1
              }
            ]);
            if (imgErr) throw imgErr;
          }
        }

        res.redirect("/admin?success=updated");
        if (address && String(address).trim()) {
          schedulePropertyGeocode(pid, address, area);
        }
        return;
      }

      const existing = db
        .prepare("SELECT * FROM properties WHERE id = ?")
        .get(id);

      if (!existing) {
        return res.redirect("/admin");
      }

      const {
        name,
        area,
        status,
        priority_group,
        size,
        address,
        price,
        availability,
        description,
        notes,
        building_id,
        use_unit_details,
        broker_id,
        remove_video,
        power_phase,
        power_amps,
        height_eave_apex,
        height_eave_roller_shutter,
        parking_bays,
        yard_space,
        property_type
      } = req.body;

      let displayImage = existing.display_image || null;
      if (req.files?.displayImage?.[0]?.filename) {
        displayImage = req.files.displayImage[0].filename;
      } else if (req.body.coverFromImageId) {
        const coverRow = db
          .prepare(
            "SELECT filename FROM property_images WHERE id = ? AND property_id = ?"
          )
          .get(Number(req.body.coverFromImageId), Number(id));
        if (coverRow) displayImage = coverRow.filename;
      }

      let videoFilename = existing.video_filename || null;
      let youtubeVideoId =
        existing.youtube_video_id && String(existing.youtube_video_id).trim()
          ? String(existing.youtube_video_id).trim()
          : null;

      if (remove_video === "1") {
        safeUnlinkUpload(videoFilename);
        videoFilename = null;
      }
      if (req.files?.propertyVideo?.[0]?.filename) {
        safeUnlinkUpload(existing.video_filename);
        videoFilename = req.files.propertyVideo[0].filename;
        youtubeVideoId = null;
      } else if (Object.prototype.hasOwnProperty.call(req.body, "youtube_url")) {
        const ytTrim = req.body.youtube_url != null ? String(req.body.youtube_url).trim() : "";
        if (!ytTrim) {
          youtubeVideoId = null;
        } else {
          const parsed = parseYoutubeVideoId(ytTrim);
          youtubeVideoId = parsed;
          if (youtubeVideoId) {
            safeUnlinkUpload(videoFilename);
            videoFilename = null;
          }
        }
      }

      const bid =
        building_id === "" || building_id == null ? null : Number(building_id);
      const useUnit = use_unit_details === "1" ? 1 : 0;
      const brokerId = parseBrokerId(broker_id);

      const sizeFormatted = formatSizeForSave(size);
      const priceFormatted = formatPriceForSave(price);
      const availabilityVal =
        availability && String(availability).trim() ? String(availability).trim() : "";

      const pp =
        power_phase === "3-phase" || power_phase === "single-phase"
          ? power_phase
          : "";
      const amps = power_amps && String(power_amps).trim() ? String(power_amps).trim() : "";
      const hApex =
        height_eave_apex && String(height_eave_apex).trim()
          ? String(height_eave_apex).trim()
          : "";
      const hRs =
        height_eave_roller_shutter && String(height_eave_roller_shutter).trim()
          ? String(height_eave_roller_shutter).trim()
          : "";
      const park =
        parking_bays && String(parking_bays).trim() ? String(parking_bays).trim() : "";
      const yardSpace =
        yard_space && String(yard_space).trim() ? String(yard_space).trim() : "";
      const propType = normalizePropertyType(property_type);

      db.prepare(`
      UPDATE properties
      SET
        name = ?,
        area = ?,
        status = ?,
        priority_group = ?,
        size = ?,
        address = ?,
        price = ?,
        availability = ?,
        description = ?,
        features = ?,
        notes = ?,
        display_image = ?,
        building_id = ?,
        use_unit_details = ?,
        broker_id = ?,
        video_filename = ?,
        youtube_video_id = ?,
        power_phase = ?,
        power_amps = ?,
        height_eave_apex = ?,
        height_eave_roller_shutter = ?,
        parking_bays = ?,
        yard_space = ?,
        property_type = ?
      WHERE id = ?
    `).run(
        name,
        area,
        status,
        priority_group || "medium",
        sizeFormatted,
        address || "",
        priceFormatted,
        availabilityVal,
        description || "",
        existing.features || "",
        notes || "",
        displayImage,
        Number.isFinite(bid) ? bid : null,
        useUnit,
        brokerId,
        videoFilename,
        youtubeVideoId,
        pp,
        amps,
        hApex,
        hRs,
        park,
        yardSpace,
        propType,
        id
      );

      if (req.files?.images?.length) {
        const currentMax =
          db
            .prepare(`
            SELECT COALESCE(MAX(image_order), 0) AS maxOrder
            FROM property_images
            WHERE property_id = ?
          `)
            .get(id).maxOrder || 0;

        req.files.images.forEach((file, index) => {
          db.prepare(`
          INSERT INTO property_images (property_id, filename, image_order)
          VALUES (?, ?, ?)
        `).run(id, file.filename, currentMax + index + 1);
        });
      }

      const addrTrim = address && String(address).trim();
      if (!addrTrim) {
        db.prepare("UPDATE properties SET latitude = NULL, longitude = NULL WHERE id = ?").run(
          id
        );
      }

      res.redirect("/admin?success=updated");

      if (addrTrim) {
        schedulePropertyGeocode(id, address, area);
      }
    } catch (err) {
      next(err);
    }
  }
);

app.post("/admin/properties/:id/delete", requireLogin, (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) {
    return res.redirect("/admin");
  }
  const row = db.prepare("SELECT * FROM properties WHERE id = ?").get(id);
  if (!row) {
    return res.redirect("/admin");
  }
  const images = db
    .prepare("SELECT filename FROM property_images WHERE property_id = ?")
    .all(id);
  images.forEach((img) => safeUnlinkUpload(img.filename));
  safeUnlinkUpload(row.display_image);
  safeUnlinkUpload(row.video_filename);
  db.prepare("DELETE FROM property_images WHERE property_id = ?").run(id);
  db.prepare("DELETE FROM properties WHERE id = ?").run(id);
  res.redirect("/admin?success=deleted");
});

// IMAGE ORDERING
app.post("/admin/images/:imageId/left", requireLogin, (req, res) => {
  const image = db
    .prepare("SELECT * FROM property_images WHERE id = ?")
    .get(req.params.imageId);

  if (!image) {
    return res.redirect("/admin");
  }

  const leftImage = db.prepare(`
    SELECT *
    FROM property_images
    WHERE property_id = ?
      AND image_order < ?
    ORDER BY image_order DESC
    LIMIT 1
  `).get(image.property_id, image.image_order);

  if (leftImage) {
    db.prepare("UPDATE property_images SET image_order = ? WHERE id = ?")
      .run(leftImage.image_order, image.id);

    db.prepare("UPDATE property_images SET image_order = ? WHERE id = ?")
      .run(image.image_order, leftImage.id);
  }

  res.redirect(`/admin/properties/${image.property_id}/edit`);
});

app.post("/admin/images/:imageId/right", requireLogin, (req, res) => {
  const image = db
    .prepare("SELECT * FROM property_images WHERE id = ?")
    .get(req.params.imageId);

  if (!image) {
    return res.redirect("/admin");
  }

  const rightImage = db.prepare(`
    SELECT *
    FROM property_images
    WHERE property_id = ?
      AND image_order > ?
    ORDER BY image_order ASC
    LIMIT 1
  `).get(image.property_id, image.image_order);

  if (rightImage) {
    db.prepare("UPDATE property_images SET image_order = ? WHERE id = ?")
      .run(rightImage.image_order, image.id);

    db.prepare("UPDATE property_images SET image_order = ? WHERE id = ?")
      .run(image.image_order, rightImage.id);
  }

  res.redirect(`/admin/properties/${image.property_id}/edit`);
});

app.post("/admin/api/property-images/reorder", requireLogin, (req, res) => {
  const { propertyId, imageIds } = req.body || {};
  const pid = Number(propertyId);
  if (!pid || !Array.isArray(imageIds) || imageIds.length === 0) {
    return res.status(400).json({ error: "Invalid payload" });
  }

  const rows = db
    .prepare("SELECT id FROM property_images WHERE property_id = ?")
    .all(pid);
  const valid = new Set(rows.map((r) => r.id));
  const ids = imageIds.map((x) => Number(x)).filter((id) => valid.has(id));
  if (ids.length !== imageIds.length || ids.length !== valid.size) {
    return res.status(400).json({ error: "Image list mismatch" });
  }

  const run = db.transaction(() => {
    ids.forEach((id, index) => {
      db.prepare("UPDATE property_images SET image_order = ? WHERE id = ?").run(
        index + 1,
        id
      );
    });
  });
  run();

  res.json({ ok: true });
});

// Default 404 (prevents serverless timeouts on unknown routes)
app.use((req, res) => {
  res.status(404).type("text").send("Not found");
});

// START
app.use((err, req, res, next) => {
  if (err instanceof multer.MulterError) {
    console.error("Upload error:", err.code, err.field || "", err.message);
    if (!res.headersSent) {
      if (err.code === "LIMIT_UNEXPECTED_FILE") {
        const field = err.field || "";
        const hint =
          field === "images"
            ? "Too many gallery images in one save (max 40). Select fewer files, save, then add more."
            : field === "displayImage"
              ? "Only one cover image is allowed. Remove the extra cover file."
              : `Unexpected file field “${field}”. Use the Cover and Gallery uploads only.`;
        return res.status(413).type("text").send(hint);
      }
      if (err.code === "LIMIT_FILE_SIZE" || err.code === "LIMIT_FILE_COUNT") {
        return res.status(413).send(
          "Upload too large or too many files. Use smaller images, a shorter video, or fewer gallery photos (videos on the property form can be up to about 200 MB)."
        );
      }
      return res.status(400).type("text").send(`Upload error: ${err.message || err.code}`);
    }
    return;
  }
  if (
    err &&
    (err.type === "entity.too.large" ||
      err.status === 413 ||
      err.statusCode === 413)
  ) {
    console.error("Body too large:", err.message);
    if (!res.headersSent) {
      return res.status(413).send("Form data too large. Shorten text fields and try again.");
    }
    return;
  }
  if (
    err &&
    typeof err.message === "string" &&
    err.message.startsWith("Invalid upload:")
  ) {
    console.error(err.message);
    if (!res.headersSent) {
      return res
        .status(400)
        .type("text")
        .send(err.message.replace(/^Invalid upload:\s*/, "").trim());
    }
    return;
  }
  console.error(err);
  if (!res.headersSent) {
    const showDetails =
      process.env.VERCEL_DEBUG_ERRORS === "1" ||
      (req &&
        req.query &&
        (req.query.debug === "1" || req.query.debug === "true") &&
        process.env.VERCEL);
    if (showDetails) {
      return res
        .status(500)
        .type("text")
        .send(
          `Error (${req && req._rid ? req._rid : "no-rid"}): ${err && err.message ? err.message : String(err)}`
        );
    }
    res.status(500).send("Something went wrong. Check the server terminal for details.");
  }
});

process.on("unhandledRejection", (reason, p) => {
  console.error("Unhandled Rejection at:", p, "reason:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught Exception — the server will exit:", err);
  process.exit(1);
});

module.exports = app;

if (require.main === module) {
  const server = http.createServer(app);

  server.on("error", (err) => {
    if (err.code === "EADDRINUSE") {
      console.error(
        `\n[ERROR] Port ${PORT} is already in use.\n` +
          `Stop the other Node process (close other terminals running the server, or Task Manager → end "Node.js JavaScript Runtime"), then run npm start again.\n`
      );
    } else {
      console.error("HTTP server error:", err);
    }
    process.exit(1);
  });

  server.listen(PORT, HOST, () => {
    console.log(`Server running on http://127.0.0.1:${PORT}`);
    console.log(`Health check: http://127.0.0.1:${PORT}/health`);
    if (HOST === "0.0.0.0") {
      const addrs = lanIPv4Addresses();
      if (addrs.length) {
        console.log("\nSame network (phone / other devices):");
        addrs.forEach((a) => {
          console.log(`  http://${a}:${PORT}`);
        });
        console.log(
          `(If it does not load on your phone, allow inbound TCP port ${PORT} in Windows Firewall for Private networks.)`
        );
      } else {
        console.log(
          "\n(No non-loopback IPv4 found — connect via http://127.0.0.1:" +
            PORT +
            " or set your Wi‑Fi adapter.)"
        );
      }
    }
  });
}