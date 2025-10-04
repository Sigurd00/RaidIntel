from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import os
from ..storage import ArtifactKey
from ..orchestrator import Action, Context
from ..analysis.player_features import build_player_features
from ..analysis.prescriptions import prescribe, write_coaching_md
# optional ML flag
try:
    from ..ml.anomaly import add_anomaly_scores
except Exception:
    add_anomaly_scores = None

@dataclass
class BuildPlayerFeatures(Action):
    code: str
    ttl: Optional[int] = None
    def artifact(self) -> ArtifactKey: return ArtifactKey("features-players", {"code": self.code})
    def requires(self, ctx: Context) -> List[Action]:
        from .core import EnsureReportEventsDumped
        return [EnsureReportEventsDumped(self.code, ctx.cfg.event_types)]
    def run(self, ctx: Context) -> None:
        df = build_player_features(self.code, ctx.cfg.output_dir)
        out = os.path.join(ctx.cfg.output_dir, self.code, "player_features.csv")
        df.to_csv(out, index=False)
    def ttl_seconds(self) -> Optional[int]: return self.ttl
    def version(self) -> str: return "v1"

@dataclass
class PrescribeImprovements(Action):
    code: str
    ttl: Optional[int] = 600
    def artifact(self) -> ArtifactKey: return ArtifactKey("coaching-notes", {"code": self.code})
    def requires(self, ctx: Context) -> List[Action]: return [BuildPlayerFeatures(self.code)]
    def run(self, ctx: Context) -> None:
        import pandas as pd
        feats_path = os.path.join(ctx.cfg.output_dir, self.code, "player_features.csv")
        df = pd.read_csv(feats_path).fillna(0)
        if add_anomaly_scores:
            df = add_anomaly_scores(df)
        sug = prescribe(df)
        write_coaching_md(sug, os.path.join(ctx.cfg.output_dir, self.code, "coaching.md"))
    def ttl_seconds(self) -> Optional[int]: return self.ttl
    def version(self) -> str: return "v1"
