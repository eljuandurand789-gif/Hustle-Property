/**
 * Vercel serverless entry: all HTTP traffic is rewritten here (see vercel.json).
 * Local development: run `npm start` (node server.js listens directly; this file is unused).
 */
const app = require("../server");

// Vercel Node Functions use (req, res). An Express app is already a compatible handler.
module.exports = (req, res) => app(req, res);
