from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from ..storage import ArtifactKey
from ..orchestrator import Action, Context

@dataclass
class EnsureReportHeader(Action):
    code: str
    ttl: Optional[int] = 24 * 3600  # cache header for a day

    def artifact(self) -> ArtifactKey:
        return ArtifactKey("report-header", {"code": self.code})

    def requires(self, ctx: Context) -> List[Action]:
        return []

    def run(self, ctx: Context) -> None:
        rep = ctx.repo.get_report_header(self.code)  # raises if not found
        ctx.etl.write_report_header_json(rep)

    def ttl_seconds(self) -> Optional[int]:
        return self.ttl

    def version(self) -> str:
        return "v1"
    
@dataclass
class EnsureReportInGuild(Action):
    guild: str
    slug: str
    region: str
    code: str
    ttl: Optional[int] = 3600  # listing freshness

    def artifact(self) -> ArtifactKey:
        return ArtifactKey("report-in-guild", {"g": self.guild, "s": self.slug, "r": self.region, "code": self.code})

    def requires(self, ctx: Context) -> List[Action]:
        # Reuse the listing artifact for caching/freshness
        return [EnsureGuildReports(self.guild, self.slug, self.region)]

    def run(self, ctx: Context) -> None:
        reps = ctx.repo.list_guild_reports(self.guild, self.slug, self.region)
        if not any(r.code == self.code for r in reps):
            raise RuntimeError(f"Report {self.code} was not found for guild={self.guild} ({self.slug}, {self.region}).")

    def ttl_seconds(self) -> Optional[int]:
        return self.ttl

    def version(self) -> str:
        return "v1"

@dataclass
class EnsureGuildReports(Action):
    guild: str; slug: str; region: str
    ttl: Optional[int] = 3600
    def artifact(self) -> ArtifactKey:
        return ArtifactKey("guild-reports", {"g": self.guild, "s": self.slug, "r": self.region})
    def requires(self, ctx: Context) -> List[Action]:
        return []
    def run(self, ctx: Context) -> None:
        _ = ctx.repo.list_guild_reports(self.guild, self.slug, self.region)
    def ttl_seconds(self) -> Optional[int]:
        return self.ttl
    def version(self) -> str:
        return "v1"

@dataclass
class EnsureReportEventsDumped(Action):
    code: str
    event_types: List[str]
    ttl: Optional[int] = None
    def artifact(self) -> ArtifactKey:
        return ArtifactKey("report-events", {"code": self.code, "types": tuple(sorted(self.event_types))})
    def requires(self, ctx: Context) -> List[Action]:
        return [EnsureReportHeader(self.code)]
    def run(self, ctx: Context) -> None:
        fights = ctx.repo.get_fights(self.code)
        ctx.etl.write_fights_csv(self.code, fights)
        for ft in fights:
            for et in self.event_types:
                ev_it = ctx.repo.stream_events(self.code, ft.id, float(ft.startTime), float(ft.endTime), et)
                ctx.etl.dump_events_jsonl(self.code, ft, et, ev_it)
    def ttl_seconds(self) -> Optional[int]:
        return self.ttl
    def version(self) -> str:
        return "v1"

@dataclass
class EnsureDatasetBuilt(Action):
    code: str
    ttl: Optional[int] = None
    def artifact(self) -> ArtifactKey:
        return ArtifactKey("dataset-built", {"code": self.code})
    def requires(self, ctx: Context) -> List[Action]:
        return [EnsureReportEventsDumped(self.code, ctx.cfg.event_types)]
    def run(self, ctx: Context) -> None:
        # Minimal dataset: per-fight counts using events files (simple example)
        import os, csv, json, glob
        out_dir = os.path.join(ctx.cfg.output_dir, self.code)
        events_dir = os.path.join(out_dir, "events")
        fights_csv = os.path.join(out_dir, "fights.csv")
        idx = {}  # fight_id -> aggregates
        if os.path.exists(fights_csv):
            with open(fights_csv, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    fid = int(row["id"])
                    idx[fid] = {"fight_id": fid, "startTime": int(row["startTime"]), "endTime": int(row["endTime"])}
        for path in glob.glob(os.path.join(events_dir, "fight_*_*.jsonl")):
            base = os.path.basename(path)
            fid = int(base.split("_")[1])
            etype = base.rsplit("_", 1)[-1].split(".")[0]
            idx.setdefault(fid, {"fight_id": fid})
            cnt = 0
            with open(path, "r", encoding="utf-8") as fh:
                for _ in fh: cnt += 1
            idx[fid][f"n_{etype}"] = cnt
        keys = sorted({k for d in idx.values() for k in d.keys()})
        with open(os.path.join(out_dir, "dataset.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys); w.writeheader(); w.writerows(idx.values())
    def ttl_seconds(self) -> Optional[int]:
        return self.ttl
    def version(self) -> str:
        return "v1"

@dataclass
class AnalyzeGuildLatest(Action):
    guild: str; slug: str; region: str
    ttl: Optional[int] = 600
    def artifact(self) -> ArtifactKey:
        return ArtifactKey("analysis-latest", {"g": self.guild, "s": self.slug, "r": self.region})
    def requires(self, ctx: Context) -> List[Action]:
        # ensure we have listing to pick latest
        _ = ctx.repo.list_guild_reports(self.guild, self.slug, self.region)
        reps = ctx.repo.list_guild_reports(self.guild, self.slug, self.region)
        if not reps:
            return [EnsureGuildReports(self.guild, self.slug, self.region)]
        latest = max(reps, key=lambda r: r.startTime)
        return [EnsureGuildReports(self.guild, self.slug, self.region), EnsureDatasetBuilt(latest.code)]
    def run(self, ctx: Context) -> None:
        import os, json, pandas as pd
        reps = ctx.repo.list_guild_reports(self.guild, self.slug, self.region)
        latest = max(reps, key=lambda r: r.startTime)
        out_dir = os.path.join(ctx.cfg.output_dir, latest.code)
        ds = os.path.join(out_dir, "dataset.csv")
        if not os.path.exists(ds):
            raise RuntimeError("Dataset missing")
        df = pd.read_csv(ds).fillna(0)
        summary = {
            "report": latest.code,
            "fights": int(df["fight_id"].nunique()),
            "total_events": int(df[[c for c in df.columns if c.startswith("n_")]].sum().sum()) if any(c.startswith("n_") for c in df.columns) else 0,
        }
        with open(os.path.join(out_dir, "analysis.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    def ttl_seconds(self) -> Optional[int]:
        return self.ttl
    def version(self) -> str:
        return "v1"

@dataclass
class JustGetGuildData(Action):
    guild: str; slug: str; region: str
    ttl: Optional[int] = 3600
    def artifact(self) -> ArtifactKey:
        return ArtifactKey("guild-data-only", {"g": self.guild, "s": self.slug, "r": self.region})
    def requires(self, ctx: Context) -> List[Action]:
        return [EnsureGuildReports(self.guild, self.slug, self.region)]
    def run(self, ctx: Context) -> None:
        pass
    def ttl_seconds(self) -> Optional[int]:
        return self.ttl
    def version(self) -> str:
        return "v1"
