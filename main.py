from __future__ import annotations
import argparse, json, logging, os, sys, time
from typing import Any, Dict, List

from config import WCLConfig
from repository import WCLRepository
log = logging.getLogger("main")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)

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

def _normalize(d: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize site/region/slug values for WCL."""
    out = dict(d)
    out["site"] = str(out.get("site", "www")).lower()
    out["server_region"] = str(out.get("server_region", "")).lower()
    # WCL expects realm *slug* (lowercase, dashes). If user provided "Draenor", make it "draenor".
    out["server_slug"] = str(out.get("server_slug", "")).strip().lower().replace(" ", "-")
    return out

def load_config_from_json(path: str) -> WCLConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    def env_override(key: str, default):
        return os.getenv(f"WCL_{key.upper()}", raw.get(key, default))

    data = {
        "client_id": env_override("client_id", ""),
        "client_secret": env_override("client_secret", ""),
        "site": env_override("site", "www"),
        "guild": env_override("guild", ""),
        "server_slug": env_override("server_slug", ""),
        "server_region": env_override("server_region", ""),
        "output_dir": env_override("output_dir", "out"),
        "event_types": env_override(
            "event_types",
            ["Casts","DamageDone","Healing","Buffs","Debuffs","Deaths","Resources","Threat"],
        ),
    }
    data = _normalize(data)

    cfg = WCLConfig(
        client_id=data["client_id"],
        client_secret=data["client_secret"],
        site=data["site"],
        guild=data["guild"],
        server_slug=data["server_slug"],
        server_region=data["server_region"],
        output_dir=data["output_dir"],
        event_types=data["event_types"],
    )
    cfg.validate()
    return cfg


def main():
    ap = argparse.ArgumentParser(description="Dev entry. Will probably be nuked if I decide to actually make this an application.")
    ap.add_argument(
        "--json-config",
        default=os.getenv("WCL_JSON_CONFIG", "secrets/wcl.dev.json"),
        help="Path to JSON config with WCL creds and guild info (default: env or secrets/wcl.dev.json)",
    )
    args = ap.parse_args()

    cfg = load_config_from_json(args.json_config)

    wclrepository = WCLRepository(cfg)

    reports = wclrepository.list_guild_reports(cfg.guild, cfg.server_slug, cfg.server_region)
    print(f"Found {len(reports)} reports for {cfg.guild} ({cfg.server_slug}, {cfg.server_region}) on {cfg.site}.warcraftlogs.com")
    for r in reports:
        url = f"https://{cfg.site}.warcraftlogs.com/reports/{r['code']}"
        print(f"{r['code']}\t{r.get('title') or ''}\t{url}")

if __name__ == "__main__":
    main()
