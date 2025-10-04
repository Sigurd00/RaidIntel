from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class Report:
    code: str
    title: str
    startTime: int
    endTime: int

@dataclass
class Fight:
    id: int
    startTime: int
    endTime: int

Event = Dict[str, Any]
