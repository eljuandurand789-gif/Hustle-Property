/**
 * Resize and re-encode images in uploads/ (run locally before deploy).
 * Max dimension 1600px, JPEG/WebP ~78 quality, PNG max compression.
 */
const fs = require("fs");
const path = require("path");
const sharp = require("sharp");

const uploadsDir = path.join(__dirname, "..", "uploads");
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

async function main() {
  if (!fs.existsSync(uploadsDir)) {
    console.error("uploads/ not found");
    process.exit(1);
  }
  const names = fs.readdirSync(uploadsDir);
  for (const name of names) {
    const fp = path.join(uploadsDir, name);
    if (!fs.statSync(fp).isFile()) continue;
    try {
      await processFile(fp);
    } catch (e) {
      console.error(name, e.message);
    }
  }
  const afterTotal = fs
    .readdirSync(uploadsDir)
    .filter((n) => fs.statSync(path.join(uploadsDir, n)).isFile())
    .reduce((s, n) => s + fs.statSync(path.join(uploadsDir, n)).size, 0);
  console.log(`Total uploads size: ${(afterTotal / 1024 / 1024).toFixed(2)} MB`);
}

main();
