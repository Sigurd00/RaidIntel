from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, Any

@dataclass(frozen=True)
class Node:
    id: str
    label: str
    action_name: str
    artifact_name: str

Edge = Tuple[str, str]  # (from, to)

def _action_name(a: Any) -> str: return a.__class__.__name__

def _artifact_id(a: Any) -> Tuple[str, str, str]:
    key = a.artifact()
    key.version = getattr(a, "version", lambda: "v1")()
    params = getattr(key, "params", {})
    try:
        import json; p = json.dumps(params, sort_keys=True)
    except Exception:
        p = str(params)
    return key.id(), key.name, p

def build_graph(root_action: Any, ctx: Any, max_nodes: int = 500) -> Tuple[Dict[str, Node], Set[Edge], str]:
    nodes: Dict[str, Node] = {}
    edges: Set[Edge] = set()
    stack: List[Any] = [root_action]
    seen: Set[str] = set()
    root_id, root_art, _ = _artifact_id(root_action)
    while stack:
        act = stack.pop()
        nid, aname, params = _artifact_id(act)
        if nid in seen: continue
        seen.add(nid)
        nodes[nid] = Node(nid, f"{_action_name(act)}\\n[{aname}]\\n{params}", _action_name(act), aname)
        try:
            deps = act.requires(ctx)
        except Exception:
            deps = []
        for dep in deps:
            did, _, _ = _artifact_id(dep)
            edges.add((did, nid))
            if did not in seen:
                stack.append(dep)
        if len(nodes) >= max_nodes: break
    return nodes, edges, root_id

def render_ascii(nodes: Dict[str, Node], edges: Set[Edge], root_id: str) -> str:
    children: Dict[str, List[str]] = {}
    for a, b in edges:
        children.setdefault(a, []).append(b)
    lines: List[str] = []
    visited: Set[str] = set()
    def dfs(nid: str, depth: int):
        star = " *" if nid in visited else ""
        lines.append("  " * depth + f"{nodes[nid].action_name} -> [{nodes[nid].artifact_name}]{star}")
        if nid in visited: return
        visited.add(nid)
        for ch in sorted(children.get(nid, [])):
            dfs(ch, depth+1)
    dfs(root_id, 0)
    return "\n".join(lines)
