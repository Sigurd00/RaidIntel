from __future__ import annotations
import os, glob, json
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple
import pandas as pd
import csv

@dataclass
class PlayerFightRow:
    report_code: str
    fight_id: int
    sourceID: int
    n_casts: int = 0
    n_damage_events: int = 0
    n_heal_events: int = 0
    n_deaths: int = 0
    active_ms: int = 0
    duration_s: float = 0.0
    casts_per_s: float = 0.0
    dmg_events_per_s: float = 0.0
    heal_events_per_s: float = 0.0
    active_uptime_pct: float = 0.0

def _merge_intervals(ms_list: List[int], gap_ms: int = 1500) -> int:
    """Given a list of event timestamps (ms), merge close events into intervals and return total covered ms."""
    if not ms_list:
        return 0
    times = sorted(ms_list)
    total = 0
    st = times[0]; en = st + 1
    for t in times[1:]:
        if t - en <= gap_ms:
            en = max(en, t + 1)
        else:
            total += (en - st)
            st = t; en = t + 1
    total += (en - st)
    return max(0, int(total))

def build_player_features(report_code: str, out_dir: str) -> pd.DataFrame:
    base = os.path.join(out_dir, report_code)
    fights_csv = os.path.join(base, "fights.csv")
    if not os.path.exists(fights_csv):
        raise FileNotFoundError(f"Missing fights.csv at {fights_csv}")

    # Load fight durations
    fight_dur: Dict[int, Tuple[int,int,float]] = {}
    with open(fights_csv, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fid = int(row["id"]); st = int(row["startTime"]); en = int(row["endTime"])
            fight_dur[fid] = (st, en, max(1.0, (en - st)/1000.0))

    # Pre-scan event files
    events_dir = os.path.join(base, "events")
    casts_files     = glob.glob(os.path.join(events_dir, "fight_*_Casts.jsonl"))
    damage_files    = glob.glob(os.path.join(events_dir, "fight_*_DamageDone.jsonl"))
    healing_files   = glob.glob(os.path.join(events_dir, "fight_*_Healing.jsonl"))
    deaths_files    = glob.glob(os.path.join(events_dir, "fight_*_Deaths.jsonl"))

    # indexes
    rows: Dict[Tuple[int,int], PlayerFightRow] = {}  # (fid, src) -> row
    cast_times: Dict[Tuple[int,int], List[int]] = {}
    dmg_times: Dict[Tuple[int,int], List[int]] = {}

    def row(fid: int, sid: int) -> PlayerFightRow:
        if (fid, sid) not in rows:
            _, _, dur = fight_dur[fid]
            rows[(fid, sid)] = PlayerFightRow(report_code, fid, sid, duration_s=dur)
        return rows[(fid, sid)]

    def iter_jsonl(path: str):
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line: continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue

    # counts + timestamp collection
    for p in casts_files:
        fid = int(os.path.basename(p).split("_")[1])
        for ev in iter_jsonl(p):
            sid = ev.get("sourceID")
            if sid is None: continue
            row(fid, sid).n_casts += 1
            cast_times.setdefault((fid, sid), []).append(int(ev.get("timestamp", 0)))

    for p in damage_files:
        fid = int(os.path.basename(p).split("_")[1])
        for ev in iter_jsonl(p):
            sid = ev.get("sourceID")
            if sid is None: continue
            row(fid, sid).n_damage_events += 1
            dmg_times.setdefault((fid, sid), []).append(int(ev.get("timestamp", 0)))

    for p in healing_files:
        fid = int(os.path.basename(p).split("_")[1])
        for ev in iter_jsonl(p):
            sid = ev.get("sourceID")
            if sid is None: continue
            row(fid, sid).n_heal_events += 1

    for p in deaths_files:
        fid = int(os.path.basename(p).split("_")[1])
        for ev in iter_jsonl(p):
            sid = ev.get("sourceID")
            if sid is None: continue
            row(fid, sid).n_deaths += 1

    # finalize metrics
    out: List[dict] = []
    for (fid, sid), r in rows.items():
        act_ms = _merge_intervals(cast_times.get((fid, sid), []) + dmg_times.get((fid, sid), []))
        r.active_ms = act_ms
        r.casts_per_s = r.n_casts / r.duration_s
        r.dmg_events_per_s = r.n_damage_events / r.duration_s
        r.heal_events_per_s = r.n_heal_events / r.duration_s
        r.active_uptime_pct = min(100.0, 100.0 * (act_ms / (r.duration_s * 1000.0)))
        out.append(asdict(r))

    df = pd.DataFrame(out).fillna(0)
    return df
