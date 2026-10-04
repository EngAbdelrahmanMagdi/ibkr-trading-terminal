import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const output = path.resolve(process.env.SHOWCASE_OUTPUT ?? path.join(root, ".artifacts/showcase"));
const prefix = process.env.SHOWCASE_PREFIX ?? "/showcase";
if (!/^\/(?:[a-z0-9-]+\/)*[a-z0-9-]+$/.test(prefix)) throw new Error("Invalid mount prefix");
// Refuse publishing into the source tree or replacing repository files.
if (!output.startsWith(path.join(root, ".artifacts") + path.sep)) throw new Error("Output must be under .artifacts");
const directory = path.join(output, ...prefix.split("/").filter(Boolean));
await mkdir(directory, { recursive: true });
for (const name of ["styles.css", "mark.svg", "architecture.svg"]) await copyFile(path.join(here, name), path.join(directory, name));
await copyFile(path.join(root, "assets/marketpulse-terminal.png"), path.join(directory, "terminal.png"));
const escape = (value) => value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
let transcript = "The recording is not included in this source-only build. Publication requires a verified local MOCK recording.";
if (process.env.SHOWCASE_MEDIA) {
  const media = path.resolve(process.env.SHOWCASE_MEDIA);
  const { stat } = await import("node:fs/promises");
  const video = path.join(media, "terminal.mp4");
  const { createHash } = await import("node:crypto");
  const evidence = JSON.parse(await readFile(path.join(media, "verified-recording.json"), "utf8"));
  if (evidence.scenario_passed !== true || evidence.video_sha256 !== createHash("sha256").update(await readFile(video)).digest("hex")) throw new Error("Unverified recording");
  if ((await stat(video)).size >= 24 * 1024 * 1024) throw new Error("Recording exceeds publication bound");
  for (const name of ["terminal.mp4", "poster.png"]) await copyFile(path.join(media, name), path.join(directory, name));
  transcript = await readFile(path.join(media, "transcript.txt"), "utf8");
} else {
  await copyFile(path.join(root, "assets/marketpulse-terminal.png"), path.join(directory, "poster.png"));
}
let html = await readFile(path.join(here, "index.html"), "utf8");
html = html.replace("__TRANSCRIPT__", `<p>${escape(transcript).replaceAll("\n\n", "</p><p>")}</p>`);
// Absolute asset paths also work at the canonical entry URL without a trailing slash.
for (const name of ["styles.css", "mark.svg", "architecture.svg", "terminal.png", "terminal.mp4", "poster.png"]) html = html.replaceAll(`"${name}"`, `"${prefix}/${name}"`);
await writeFile(path.join(directory, "index.html"), html);
await writeFile(path.join(output, "_headers"), `${prefix}\n  Content-Security-Policy: default-src 'none'; style-src 'self'; img-src 'self'; media-src 'self'; script-src 'none'; connect-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'\n  X-Content-Type-Options: nosniff\n  Referrer-Policy: no-referrer\n  Permissions-Policy: camera=(), microphone=(), geolocation=(), payment=()\n${prefix}/*\n  X-Content-Type-Options: nosniff\n  Referrer-Policy: no-referrer\n`);
console.log("Destination-neutral static bundle built; media presence checked separately before publication.");
