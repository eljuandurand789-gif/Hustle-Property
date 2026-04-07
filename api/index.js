/**
 * Vercel serverless entry: all HTTP traffic is rewritten here (see vercel.json).
 * Local development: run `npm start` (node server.js listens directly; this file is unused).
 */
const app = require("../server");

// Vercel Node Functions use (req, res). An Express app is already a compatible handler.
module.exports = (req, res) => {
  // Restore original path + query from rewrite.
  // vercel.json rewrites all routes to /api/index.js?path=<original>&<original query...>
  try {
    const u = new URL(req.url, "http://localhost");
    const p = u.searchParams.get("path") || "";
    if (p) {
      u.searchParams.delete("path");
      const rest = u.searchParams.toString();
      req.url = "/" + p + (rest ? "?" + rest : "");
    }
  } catch (_) {
    // ignore
  }
  return app(req, res);
};
