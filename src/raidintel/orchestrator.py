from __future__ import annotations
from dataclasses import dataclass
from typing import List, Protocol, Optional, Any
from .storage import ArtifactKey, ArtifactStore

class Action(Protocol):
    def artifact(self) -> ArtifactKey: ...
    def requires(self, ctx: "Context") -> List["Action"]: ...
    def run(self, ctx: "Context") -> None: ...
    def ttl_seconds(self) -> Optional[int]: ...
    def version(self) -> str: ...  # bump when logic/schema changes

@dataclass
class Context:
    store: ArtifactStore
    repo: Any
    etl: Any
    cfg: Any

class Orchestrator:
    def __init__(self, ctx: Context):
        self.ctx = ctx

    def ensure(self, action: Action) -> None:
        for dep in action.requires(self.ctx):
            self.ensure(dep)
        key = action.artifact()
        key.version = action.version()
        if self.ctx.store.exists(key) and self.ctx.store.is_fresh(key, action.ttl_seconds()):
            return
        action.run(self.ctx)
        self.ctx.store.touch(key, {"name": key.name, "params": key.params, "version": key.version})
