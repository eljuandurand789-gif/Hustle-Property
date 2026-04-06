const express = require("express");
const session = require("express-session");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const Database = require("better-sqlite3");
const methodOverride = require("method-override");

const app = express();
const PORT = 3000;

// Ensure folders exist
const uploadsDir = path.join(__dirname, "uploads");
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });

// Database
const db = new Database("properties.db");

// Tables
db.exec(`
  CREATE TABLE IF NOT EXISTS admins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    area TEXT NOT NULL CHECK(area IN ('Maitland', 'Paarden Eiland')),
    status TEXT NOT NULL CHECK(status IN ('to-let', 'for-sale', 'let-and-sold')),
    size TEXT,
    address TEXT,
    price TEXT,
    availability TEXT,
    features TEXT,
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS property_images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    FOREIGN KEY(property_id) REFERENCES properties(id) ON DELETE CASCADE
  );
`);

// Seed admin
const existingAdmin = db.prepare("SELECT * FROM admins WHERE username = ?").get("EJCLOSER");
if (!existingAdmin) {
  db.prepare("INSERT INTO admins (username, password) VALUES (?, ?)").run("EJCLOSER", "Lionheart");
}

// App config
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(methodOverride("_method"));
app.use("/public", express.static(path.join(__dirname, "public")));
app.use("/uploads", express.static(path.join(__dirname, "uploads")));

app.use(
  session({
    secret: "replace-this-with-a-strong-secret",
    resave: false,
    saveUninitialized: false,
  })
);

// Multer config
const storage = multer.diskStorage({
  destination: (_, __, cb) => cb(null, uploadsDir),
  filename: (_, file, cb) => {
    const ext = path.extname(file.originalname);
    const safeBase = path.basename(file.originalname, ext).replace(/[^a-zA-Z0-9_-]/g, "_");
    cb(null, `${Date.now()}_${safeBase}${ext}`);
  },
});
const upload = multer({ storage });

// Helpers
function requireAuth(req, res, next) {
  if (!req.session.adminId) return res.redirect("/admin/login");
  next();
}

function mapsLink(address) {
  return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(address || "")}`;
}

function getPropertyWithImages(id) {
  const property = db.prepare("SELECT * FROM properties WHERE id = ?").get(id);
  if (!property) return null;
  const images = db.prepare("SELECT * FROM property_images WHERE property_id = ? ORDER BY id DESC").all(id);
  return { ...property, images };
}

function getAllProperties(filters = {}) {
  let sql = "SELECT * FROM properties WHERE 1=1";
  const params = [];

  if (filters.area) {
    sql += " AND area = ?";
    params.push(filters.area);
  }

  if (filters.status) {
    sql += " AND status = ?";
    params.push(filters.status);
  }

  sql += " ORDER BY id DESC";
  const properties = db.prepare(sql).all(...params);

  return properties.map((p) => {
    const images = db.prepare("SELECT * FROM property_images WHERE property_id = ? ORDER BY id DESC").all(p.id);
    return { ...p, images };
  });
}

// Public routes
app.get("/", (req, res) => {
  const { area, status } = req.query;
  const properties = getAllProperties({ area, status });

  res.render("index", {
    properties,
    filters: { area: area || "", status: status || "" },
    mapsLink,
  });
});

app.get("/property/:id", (req, res) => {
  const property = getPropertyWithImages(req.params.id);
  if (!property) return res.status(404).send("Property not found");

  res.render("property", { property, mapsLink });
});

// Admin routes
app.get("/admin/login", (req, res) => {
  res.render("login", { error: null });
});

app.post("/admin/login", (req, res) => {
  const { username, password } = req.body;
  const admin = db.prepare("SELECT * FROM admins WHERE username = ? AND password = ?").get(username, password);

  if (!admin) {
    return res.render("login", { error: "Invalid username or password." });
  }

  req.session.adminId = admin.id;
  req.session.username = admin.username;
  res.redirect("/admin");
});

app.post("/admin/logout", (req, res) => {
  req.session.destroy(() => res.redirect("/admin/login"));
});

app.get("/admin", requireAuth, (req, res) => {
  const properties = getAllProperties();
  res.render("admin", {
    properties,
    username: req.session.username,
    editing: null,
    mapsLink,
  });
});

app.get("/admin/edit/:id", requireAuth, (req, res) => {
  const properties = getAllProperties();
  const editing = getPropertyWithImages(req.params.id);
  if (!editing) return res.redirect("/admin");

  res.render("admin", {
    properties,
    username: req.session.username,
    editing,
    mapsLink,
  });
});

app.post("/admin/properties", requireAuth, upload.array("images", 20), (req, res) => {
  const {
    name,
    area,
    status,
    size,
    address,
    price,
    availability,
    features,
    notes,
  } = req.body;

  const result = db.prepare(`
    INSERT INTO properties (name, area, status, size, address, price, availability, features, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(name, area, status, size, address, price, availability, features, notes);

  const propertyId = result.lastInsertRowid;

  for (const file of req.files || []) {
    db.prepare("INSERT INTO property_images (property_id, filename) VALUES (?, ?)").run(propertyId, file.filename);
  }

  res.redirect("/admin");
});

app.put("/admin/properties/:id", requireAuth, upload.array("images", 20), (req, res) => {
  const {
    name,
    area,
    status,
    size,
    address,
    price,
    availability,
    features,
    notes,
  } = req.body;

  db.prepare(`
    UPDATE properties
    SET name = ?, area = ?, status = ?, size = ?, address = ?, price = ?, availability = ?, features = ?, notes = ?
    WHERE id = ?
  `).run(name, area, status, size, address, price, availability, features, notes, req.params.id);

  for (const file of req.files || []) {
    db.prepare("INSERT INTO property_images (property_id, filename) VALUES (?, ?)").run(req.params.id, file.filename);
  }

  res.redirect("/admin");
});

app.delete("/admin/properties/:id", requireAuth, (req, res) => {
  const images = db.prepare("SELECT * FROM property_images WHERE property_id = ?").all(req.params.id);

  for (const img of images) {
    const imgPath = path.join(uploadsDir, img.filename);
    if (fs.existsSync(imgPath)) fs.unlinkSync(imgPath);
  }

  db.prepare("DELETE FROM property_images WHERE property_id = ?").run(req.params.id);
  db.prepare("DELETE FROM properties WHERE id = ?").run(req.params.id);

  res.redirect("/admin");
});

app.delete("/admin/images/:id", requireAuth, (req, res) => {
  const image = db.prepare("SELECT * FROM property_images WHERE id = ?").get(req.params.id);
  if (image) {
    const imgPath = path.join(uploadsDir, image.filename);
    if (fs.existsSync(imgPath)) fs.unlinkSync(imgPath);
    db.prepare("DELETE FROM property_images WHERE id = ?").run(req.params.id);
  }
  res.redirect("back");
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});