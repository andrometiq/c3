// The Chromium binary the browser checks drive: $CHROMIUM, else the usual install paths.
import fs from "node:fs";

export function findChromium() {
  const candidates = [process.env.CHROMIUM, "/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome"];
  const found = candidates.find((candidate) => candidate && fs.existsSync(candidate));
  if (!found) throw new Error("Chromium not found; set CHROMIUM=/path/to/chromium");
  return found;
}
