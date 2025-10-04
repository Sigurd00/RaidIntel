from __future__ import annotations
import json, os, time, pathlib, hashlib
from typing import Any, Dict, Optional

class ArtifactKey:
    def __init__(self, name: str, params: Dict[str, Any], version: str = "v1") -> None:
        self.name, self.params, self.version = name, params, version

    def id(self) -> str:
        raw = json.dumps([self.name, self.params, self.version], sort_keys=True).encode()
        h = hashlib.sha1(raw).hexdigest()[:12]
        return f"{self.name}-{h}"

class ArtifactStore:
    def __init__(self, root: str = "out"):
        self.root = root

    def _path(self, key: ArtifactKey) -> str:
        p = pathlib.Path(self.root) / "_artifacts" / key.id()
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p)

    def exists(self, key: ArtifactKey) -> bool:
        return os.path.exists(self._path(key))

    def is_fresh(self, key: ArtifactKey, ttl_seconds: Optional[int]) -> bool:
        if not self.exists(key): return False
        if ttl_seconds is None: return True
        try:
            with open(self._path(key), "r", encoding="utf-8") as f:
                data = json.load(f)
            return (time.time() - float(data.get("t", 0))) < ttl_seconds
        except Exception:
            return False

    def touch(self, key: ArtifactKey, meta: Dict[str, Any]) -> None:
        with open(self._path(key), "w", encoding="utf-8") as f:
            json.dump({"meta": meta, "t": time.time()}, f)
