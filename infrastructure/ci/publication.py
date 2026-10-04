"""Local static publication with destination separation and no backend bindings."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import uuid
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]


class PublicationError(Exception):
    """Messages must be fixed, destination-free descriptions."""


def validate(config: dict) -> None:
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", config["hostname"]):
        raise PublicationError("Invalid local hostname configuration")
    if not re.fullmatch(r"/(?:[a-z0-9-]+/)*[a-z0-9-]+", config["prefix"]):
        raise PublicationError("Invalid local mount prefix")
    for key in ["account_id", "zone_id"]:
        if not re.fullmatch(r"[0-9a-f]{32}", config[key]):
            raise PublicationError("Configure local account and zone identifiers")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{2,62}", config["worker_name"]):
        raise PublicationError("Configure a unique showcase-owned Worker name")
    if config.get("free_static_only_confirmed") is not True:
        raise PublicationError("Confirm free static-only publication locally")
    if config.get("scoped_token_permissions_confirmed") is not True:
        raise PublicationError("Confirm the token was created with only the documented account/zone permissions")


def reject_conflicts(config: dict, routes: list[dict]) -> None:
    entry = config["hostname"] + config["prefix"]
    patterns = {entry, entry + "/*"}
    for route in routes:
        pattern = route["pattern"].removeprefix("https://").removeprefix("http://")
        # Existing broad main-site routes remain intact; new routes only own this subtree.
        route_path = pattern[pattern.find("/"):] if "/" in pattern else ""
        touches_subtree = (pattern in patterns or route_path.startswith(config["prefix"] + "/")
                           or (fnmatch.fnmatchcase(entry, pattern) and route_path.startswith(config["prefix"])))
        if touches_subtree and route.get("script") != config["worker_name"]:
            raise PublicationError("Existing project routing conflicts; owner review required")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def get(url: str, token: str | None = None, limit: int = 2 * 1024 * 1024, method: str = "GET") -> tuple[bytes, dict]:
    headers = {"Authorization": "Bearer " + token} if token else {}
    request = urllib.request.Request(url, headers=headers, method=method)
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=15) as response:
        data = response.read(limit + 1)
        if len(data) > limit:
            raise PublicationError("Preflight response exceeds bound")
        return data, {name.lower(): value for name, value in response.headers.items()}


def api(path: str, token: str, method: str = "GET"):
    data, _ = get("https://api.cloudflare.com/client/v4" + path, token, method=method)
    result = json.loads(data)
    if not result.get("success"):
        raise PublicationError("Cloudflare preflight failed; verify scoped permissions")
    return result["result"]


def public_scan(config: dict) -> None:
    names = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT).decode().split("\0")
    values = [config[key].encode() for key in ["hostname", "prefix", "worker_name", "account_id", "zone_id"]]
    for name in names:
        file = ROOT / name
        if name and file.is_file() and any(value in file.read_bytes() for value in values):
            raise PublicationError("Destination detail found in public source; publication refused")


def wrangler(arguments: list[str], token: str) -> None:
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("CLOUDFLARE_", "CF_"))}
    environment["CLOUDFLARE_API_TOKEN"] = token
    command = "npx.cmd" if os.name == "nt" else "npx"
    result = subprocess.run([command, "--yes", "--package", "wrangler@4.147.0", "wrangler", *arguments],
                            cwd=ROOT, env=environment, capture_output=True, timeout=300)
    if result.returncode:
        raise PublicationError("Static operation failed; CLI output withheld to preserve destination separation")


def rollback_owned(config: dict, token: str, output: Path, routes: list[dict]) -> None:
    identity = {key: config[key] for key in ["account_id", "zone_id", "hostname", "prefix", "worker_name"]}
    if not (output / "ownership.json").is_file() or json.loads((output / "ownership.json").read_text()) != identity:
        raise PublicationError("Rollback requires exact local ownership evidence")
    before = json.loads((output / "before.json").read_text())
    if any(before[key] != value for key, value in identity.items()):
        raise PublicationError("Rollback snapshot identity mismatch")
    main, _ = get("https://" + config["hostname"] + "/")
    if before["existing"]:
        previous_assets = Path(before["previous_assets"]).resolve()
        if not previous_assets.is_relative_to(output.resolve()) or not previous_assets.is_dir():
            raise PublicationError("Previous static bundle missing")
        previous = json.loads((output / "wrangler.json").read_text())
        previous["assets"]["directory"] = str(previous_assets)
        previous["routes"] = [{"pattern": route["pattern"], "zone_id": config["zone_id"]}
                              for route in before["routes"] if route.get("script") == config["worker_name"]]
        target = output / "rollback.json"
        target.write_text(json.dumps(previous, indent=2))
        wrangler(["deploy", "--config", str(target)], token)
    else:
        for route in routes:
            if route.get("script") == config["worker_name"]:
                api("/zones/" + config["zone_id"] + "/workers/routes/" + route["id"], token, "DELETE")
        api("/accounts/" + config["account_id"] + "/workers/scripts/" + config["worker_name"], token, "DELETE")
    after, _ = get("https://" + config["hostname"] + "/")
    if main != after:
        raise PublicationError("Main-site response changed; owner review required")
    print("Owned showcase rollback completed; main-site response unchanged.")


def publish(config_file: Path, preflight_only: bool, rollback: bool = False) -> None:
    if os.environ.get("GITHUB_ACTIONS"):
        raise PublicationError("Publication is forbidden in GitHub Actions")
    local = ROOT / ".local"
    if not config_file.resolve().is_relative_to(local.resolve()):
        raise PublicationError("Publication configuration must remain under .local")
    config = json.loads(config_file.read_text())
    validate(config)
    token_file = Path(config["token_file"])
    if not token_file.is_absolute():
        token_file = ROOT / token_file
    if not token_file.resolve().is_relative_to(local.resolve()):
        raise PublicationError("Store the scoped publication token under .local")
    token = token_file.read_text().strip()
    if not 20 <= len(token) <= 512 or any(char.isspace() for char in token):
        raise PublicationError("Invalid scoped token file")
    public_scan(config)
    verification = api("/user/tokens/verify", token)
    if verification.get("status") != "active":
        raise PublicationError("Scoped token is inactive")
    zone = api("/zones/" + config["zone_id"], token)
    if zone["status"] != "active" or not (config["hostname"] == zone["name"] or config["hostname"].endswith("." + zone["name"])):
        raise PublicationError("Target does not belong to the configured active zone")
    routes = api("/zones/" + config["zone_id"] + "/workers/routes", token)
    reject_conflicts(config, routes)
    owned_patterns = {config["hostname"] + config["prefix"] + suffix for suffix in ["", "/*"]}
    if any(route.get("script") == config["worker_name"] and route["pattern"] not in owned_patterns for route in routes):
        raise PublicationError("Configured Worker owns unrelated routes; publication refused")
    output = ROOT / ".artifacts/publication"
    output.mkdir(parents=True, exist_ok=True)
    identity = {key: config[key] for key in ["account_id", "zone_id", "hostname", "prefix", "worker_name"]}
    ownership = output / "ownership.json"
    if rollback:
        if preflight_only:
            raise PublicationError("Choose preflight or rollback, not both")
        rollback_owned(config, token, output, routes)
        return
    try:
        api("/accounts/" + config["account_id"] + "/workers/scripts/" + config["worker_name"] + "/settings", token)
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        existing = False
    else:
        existing = True
    if existing and (not ownership.is_file() or json.loads(ownership.read_text()) != identity):
        raise PublicationError("Worker already exists without local ownership evidence; choose a new name")
    main, headers = get("https://" + config["hostname"] + "/")
    if not any(name.lower() == "cf-ray" for name in headers):
        raise PublicationError("Main-site proxy eligibility needs owner review")
    before = {key: config[key] for key in ["account_id", "zone_id", "hostname", "prefix", "worker_name"]}
    before["routes"] = routes
    before["existing"] = existing
    if existing:
        before["deployments"] = api("/accounts/" + config["account_id"] + "/workers/scripts/" + config["worker_name"] + "/deployments", token)
    if preflight_only:
        (output / "preflight.json").write_text(json.dumps(before, indent=2))
        print("Scoped-token, zone, proxy and route preflight completed; nothing published.")
        return
    media = (ROOT / config["media_directory"]).resolve()
    if not media.is_relative_to((ROOT / ".artifacts").resolve()):
        raise PublicationError("Verified recording must remain under .artifacts")
    environment = {**os.environ, "SHOWCASE_PREFIX": config["prefix"], "SHOWCASE_MEDIA": str(media),
                   "SHOWCASE_OUTPUT": str(output / "assets")}
    if existing:
        if not (output / "assets").is_dir():
            raise PublicationError("Previous local asset bundle missing; rollback prerequisite failed")
        previous_assets = output / ("previous-assets-" + uuid.uuid4().hex)
        shutil.copytree(output / "assets", previous_assets)
        before["previous_assets"] = str(previous_assets)
    (output / "before.json").write_text(json.dumps(before, indent=2))
    subprocess.run(["node", "apps/showcase/build.mjs"], cwd=ROOT, env=environment, check=True, timeout=30, capture_output=True)
    worker = {"name": config["worker_name"], "account_id": config["account_id"],
              "compatibility_date": "2026-10-04", "workers_dev": False, "preview_urls": False,
              "assets": {"directory": str(output / "assets"), "html_handling": "drop-trailing-slash", "not_found_handling": "none"},
              "routes": [{"pattern": config["hostname"] + config["prefix"] + suffix,
                          "zone_id": config["zone_id"]} for suffix in ["", "/*"]]}
    worker_config = output / "wrangler.json"
    worker_config.write_text(json.dumps(worker, indent=2))
    ownership.write_text(json.dumps(identity, indent=2))
    # Never fall back to ambient OAuth or global API keys; never print CLI output.
    wrangler(["deploy", "--config", str(worker_config)], token)
    (output / "deployment.json").write_text(json.dumps(api("/accounts/" + config["account_id"] + "/workers/scripts/" + config["worker_name"] + "/deployments", token), indent=2))
    _, showcase_headers = get("https://" + config["hostname"] + config["prefix"])
    if "script-src 'none'" not in showcase_headers.get("content-security-policy", ""):
        raise PublicationError("Published static security headers failed verification")
    video, video_headers = get("https://" + config["hostname"] + config["prefix"] + "/terminal.mp4", limit=24 * 1024 * 1024)
    if hashlib.sha256(video).digest() != hashlib.sha256((media / "terminal.mp4").read_bytes()).digest() or "video/mp4" not in video_headers.get("content-type", ""):
        raise PublicationError("Published video verification failed")
    after, _ = get("https://" + config["hostname"] + "/")
    if main != after:
        raise PublicationError("Main-site response changed; owner review required")
    (output / "verified.json").write_text(json.dumps({"static_verified": True, "main_site_unchanged": True}))
    print("Static publication verified; destination and configuration remain local.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / ".local/publication.json")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--rollback", action="store_true")
    args = parser.parse_args()
    try:
        publish(args.config, args.preflight_only, args.rollback)
    except PublicationError as error:
        raise SystemExit(str(error)) from None
    except (OSError, KeyError, ValueError, subprocess.SubprocessError):
        raise SystemExit("Publication stopped; local configuration, credential or network prerequisite failed. No sensitive details printed.") from None
