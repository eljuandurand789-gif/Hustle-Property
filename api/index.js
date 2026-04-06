/**
 * Vercel serverless entry: all HTTP traffic is rewritten here (see vercel.json).
 * Local development: run `npm start` (node server.js listens directly; this file is unused).
 */
const serverless = require("serverless-http");
const app = require("../server");

const handler = serverless(app, {
  binary: ["image/*", "application/pdf"]
});

module.exports = handler;
