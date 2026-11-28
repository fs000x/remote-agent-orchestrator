#!/usr/bin/env node
/**
 * Build step: ship the zero-dependency runtime (.mjs) into dist/runtime so
 * provision/runtimeDir() resolve correctly in the compiled package.
 * (tsconfig excludes src/runtime from compilation — the files are deployed
 * verbatim, never transpiled.)
 */
import { mkdirSync, copyFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const src = join(root, "src", "runtime");
const dst = join(root, "dist", "runtime");
mkdirSync(dst, { recursive: true });
for (const f of readdirSync(src).filter((n) => n.endsWith(".mjs"))) {
  copyFileSync(join(src, f), join(dst, f));
  console.log(`copied src/runtime/${f} -> dist/runtime/${f}`);
}
