/**
 * Resize and re-encode images (run locally before deploy).
 * Default: uploads/. Pass folder names relative to repo root, e.g. `node scripts/compress-uploads.js public`.
 * Max dimension 1600px, JPEG/WebP ~78 quality, PNG max compression.
 */
const fs = require("fs");
const path = require("path");
const sharp = require("sharp");

const repoRoot = path.join(__dirname, "..");
const MAX_SIDE = 1600;
const JPEG_Q = 78;
const WEBP_Q = 78;

const IMAGE_EXT = new Set([".jpg", ".jpeg", ".png", ".webp"]);

async function processFile(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (!IMAGE_EXT.has(ext)) return;

  const before = fs.statSync(filePath).size;
  const input = fs.readFileSync(filePath);
  const pipeline = sharp(input).rotate().resize({
    width: MAX_SIDE,
    height: MAX_SIDE,
    fit: "inside",
    withoutEnlargement: true,
  });

  let buf;
  if (ext === ".png") {
    buf = await pipeline.png({ compressionLevel: 9, effort: 10 }).toBuffer();
  } else if (ext === ".webp") {
    buf = await pipeline.webp({ quality: WEBP_Q }).toBuffer();
  } else {
    buf = await pipeline.jpeg({ quality: JPEG_Q, mozjpeg: true }).toBuffer();
  }

  if (buf.length <= before) {
    const tmp = `${filePath}.tmp`;
    fs.writeFileSync(tmp, buf);
    fs.renameSync(tmp, filePath);
    console.log(
      `${path.basename(filePath)}: ${(before / 1024).toFixed(1)} KB → ${(buf.length / 1024).toFixed(1)} KB`,
    );
  }
}

async function processDir(relDir) {
  const dir = path.join(repoRoot, relDir);
  if (!fs.existsSync(dir)) {
    console.error(`Skip missing: ${relDir}`);
    return;
  }
  const names = fs.readdirSync(dir);
  for (const name of names) {
    const fp = path.join(dir, name);
    if (!fs.statSync(fp).isFile()) continue;
    try {
      await processFile(fp);
    } catch (e) {
      console.error(name, e.message);
    }
  }
  const bytes = fs
    .readdirSync(dir)
    .filter((n) => {
      try {
        return fs.statSync(path.join(dir, n)).isFile();
      } catch {
        return false;
      }
    })
    .reduce((s, n) => s + fs.statSync(path.join(dir, n)).size, 0);
  console.log(`Total ${relDir}/ size: ${(bytes / 1024 / 1024).toFixed(2)} MB`);
}

async function main() {
  const dirs =
    process.argv.length > 2 ? process.argv.slice(2) : ["uploads"];
  for (const d of dirs) {
    await processDir(d);
  }
}

main();
