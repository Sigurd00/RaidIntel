from __future__ import annotations
from dataclasses import dataclass
from typing import List, Protocol, Optional

from .storage import ArtifactKey, ArtifactStore

class Action(Protocol):
    def artifact(self) -> ArtifactKey: ...
    def requires(self, ctx: "Context") -> List["Action"]: ...
    def run(self, ctx: "Context") -> None: ...
    def ttl_seconds(self) -> Optional[int]: ...  # freshness policy

@dataclass
class Context:
    store: ArtifactStore
    repo: any
    etl: any
    cfg: any

class Orchestrator:
    def __init__(self, ctx: Context):
        self.ctx = ctx

    def ensure(self, action: Action) -> None:
        # Resolve dependencies first
        for dep in action.requires(self.ctx):
            self.ensure(dep)
        # Skip if artifact exists and is fresh
        key = action.artifact()
        ttl = action.ttl_seconds()
        if self.ctx.store.exists(key) and self.ctx.store.is_fresh(key, ttl):
            return
        # Execute and mark produced
        action.run(self.ctx)
        self.ctx.store.touch(key, {"name": key.name, "params": key.params})
