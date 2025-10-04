from __future__ import annotations
import argparse, json, os, time
from src.raidintel.wcl_client import WCLClient

GQL_REPORTS = """
query($guildName:String!, $guildServerSlug:String!, $guildServerRegion:String!, $page:Int){
  reportData {
    reports(guildName:$guildName, guildServerSlug:$guildServerSlug, guildServerRegion:$guildServerRegion, page:$page){
      data { code title startTime endTime }
      has_more_pages
    }
  }
}
"""

def _normalize(d: dict) -> dict:
    out = dict(d)
    out["site"] = str(out.get("site","www")).lower()
    out["server_region"] = str(out.get("server_region","")).lower()
    out["server_slug"] = str(out.get("server_slug","")).strip().lower().replace(" ", "-")
    return out

def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    # allow env overrides
    def env(key, default):
        return os.getenv(f"WCL_{key.upper()}", raw.get(key, default))
    return _normalize({
        "client_id": env("client_id",""),
        "client_secret": env("client_secret",""),
        "site": env("site","www"),
        "guild": env("guild",""),
        "server_slug": env("server_slug",""),
        "server_region": env("server_region",""),
    })

def fetch_all_reports(client: WCLClient, guild: str, slug: str, region: str):
    page, out = 1, []
    while True:
        block = client.gql(GQL_REPORTS, {
            "guildName": guild, "guildServerSlug": slug, "guildServerRegion": region, "page": page
        })["reportData"]["reports"]
        out.extend(block["data"])
        if not block.get("has_more_pages", False):
            return out
        page += 1
        time.sleep(0.2)

def main():
    ap = argparse.ArgumentParser(description="Dev runner using JSON (not in VC)")
    ap.add_argument("--json-config", default=os.getenv("WCL_JSON_CONFIG", "secrets/wcl.dev.json"))
    ap.add_argument("--list-reports", action="store_true")
    args = ap.parse_args()

    cfg = load_json(args.json_config)
    client = WCLClient(site=cfg["site"], client_id=cfg["client_id"], client_secret=cfg["client_secret"])

    reports = fetch_all_reports(client, cfg["guild"], cfg["server_slug"], cfg["server_region"])
    if args.list-reports:
        print(f"Found {len(reports)} reports")
        for r in reports:
            print(r["code"], r.get("title",""))
    else:
        latest = max(reports, key=lambda r: r.get("startTime", 0) or 0) if reports else None
        print("Latest:", latest["code"] if latest else "None")

if __name__ == "__main__":
    main()
