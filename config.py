from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
import os, sys

try:
    import tomllib  # PY>=3.11
except ModuleNotFoundError:  # PY<3.11
    import tomli as tomllib  # type: ignore

@dataclass
class WCLConfig:
    client_id: str
    client_secret: str
    site: str = "www"
    guild: str = ""
    server_slug: str = ""
    server_region: str = ""
    output_dir: str = "out"
    event_types: List[str] = field(default_factory=lambda: ["Casts","DamageDone","Healing","Buffs","Debuffs","Deaths","Resources","Threat"])

    @staticmethod
    def from_toml(path: str) -> "WCLConfig":
        with open(path, "rb") as f:
            data = tomllib.load(f)
        # env overrides (useful in CI/secrets)
        def env_override(key: str, default):
            return os.getenv(f"WCL_{key.upper()}", data.get(key, default))
        return WCLConfig(
            client_id=env_override("client_id", ""),
            client_secret=env_override("client_secret", ""),
            site=env_override("site", "www"),
            guild=env_override("guild", ""),
            server_slug=env_override("server_slug", ""),
            server_region=env_override("server_region", ""),
            output_dir=env_override("output_dir", "out"),
            event_types=env_override("event_types", ["Casts","DamageDone","Healing","Buffs","Debuffs","Deaths","Resources","Threat"]),
        )

    def validate(self) -> None:
        missing = [k for k in ["client_id","client_secret","guild","server_slug","server_region"] if not getattr(self, k)]
        if missing:
            raise SystemExit(f"Config is missing required keys: {', '.join(missing)}")
