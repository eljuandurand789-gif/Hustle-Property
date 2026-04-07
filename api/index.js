/**
 * Vercel serverless entry: all HTTP traffic is rewritten here (see vercel.json).
 * Local development: run `npm start` (node server.js listens directly; this file is unused).
 */
const app = require("../server");

// Vercel Node Functions use (req, res). An Express app is already a compatible handler.
module.exports = (req, res) => {
  // Restore original path from rewrite.
  const p = req && req.query && typeof req.query.path === "string" ? req.query.path : "";
  if (p) {
    req.url = "/" + p;
  }
  return app(req, res);
};
