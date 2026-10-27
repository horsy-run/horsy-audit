#!/usr/bin/env node
// Runs `horsy audit` with any extra flags (e.g. --plan max). Nothing leaves this machine.
process.argv.splice(2, 0, "audit");
await import("./horsy.mjs");
