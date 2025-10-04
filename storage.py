from __future__ import annotations
import json, os, time, hashlib, pathlib
from typing import Any, Dict

class ArtifactKey:
    def __init__(self, name: str, params: Dict[str, Any]):
        self.name, self.params = name, params
    def id(self) -> str:
        raw = json.dumps([self.name, self.params], sort_keys=True).encode()
        return f"{self.name}-{hashlib.sha1(raw).hexdigest()[:12]}"

class ArtifactStore:
    def __init__(self, root: str = "out"):
        self.root = root
    def path(self, key: ArtifactKey) -> str:
        p = pathlib.Path(self.root) / "_artifacts" / key.id()
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p)

    def exists(self, key: ArtifactKey) -> bool:
        return os.path.exists(self.path(key))

    def touch(self, key: ArtifactKey, meta: Dict[str, Any]) -> None:
        with open(self.path(key), "w", encoding="utf-8") as f:
            json.dump({"meta": meta, "t": time.time()}, f)

    def is_fresh(self, key: ArtifactKey, ttl_seconds: int | None) -> bool:
        if ttl_seconds is None:
            return self.exists(key)  # treat existence as fresh
        try:
            with open(self.path(key), "r", encoding="utf-8") as f:
                data = json.load(f)
            return (time.time() - float(data.get("t", 0))) < ttl_seconds
        except Exception:
            return False
