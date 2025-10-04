from __future__ import annotations
import os
import pandas as pd

def prescribe(df: pd.DataFrame) -> dict:
    """
    Produce prescriptive suggestions per player and team from per-player, per-fight DF.
    Columns expected: report_code, fight_id, sourceID, duration_s,
                      n_deaths, casts_per_s, dmg_events_per_s, heal_events_per_s, active_uptime_pct
    """
    suggestions = {"team": [], "players": {}}
    if df.empty: return suggestions

    # team medians for context
    med_casts = df["casts_per_s"].median()
    med_dmg   = df["dmg_events_per_s"].median()
    med_heal  = df["heal_events_per_s"].median()

    # aggregate per player across the report
    g = df.groupby("sourceID", as_index=False).agg({
        "active_uptime_pct":"mean",
        "n_deaths":"sum",
        "casts_per_s":"mean",
        "dmg_events_per_s":"mean",
        "heal_events_per_s":"mean",
        "fight_id":"nunique"
    }).rename(columns={"fight_id":"fights"})

    # team-level prescriptions
    low_uptime_pct = (g["active_uptime_pct"] < 85).mean() * 100
    if low_uptime_pct > 0:
        suggestions["team"].append(f"{low_uptime_pct:.0f}% of players average <85% active uptime — emphasize 'always be casting' and minimizing downtime during movement.")

    high_deathers = (g["n_deaths"] > 0).sum()
    if high_deathers:
        suggestions["team"].append(f"{high_deathers} players died at least once — review defensive assignments and death contexts (last 10s of incoming).")

    # per-player prescriptions
    for _, r in g.iterrows():
        notes = []
        if r["active_uptime_pct"] < 90:
            notes.append(f"Increase active uptime to ≥90% (current {r['active_uptime_pct']:.1f}%). Plan movement casts/instants.")
        # below-team performance markers (15% gap)
        if r["dmg_events_per_s"] < 0.85 * med_dmg and r["dmg_events_per_s"] > 0:
            gap = 100*(1 - r["dmg_events_per_s"]/med_dmg)
            notes.append(f"Time-on-target: dmg events/s {gap:.0f}% below team median — reduce target swaps, pre-position for uptime.")
        if r["heal_events_per_s"] < 0.85 * med_heal and r["heal_events_per_s"] > 0:
            gap = 100*(1 - r["heal_events_per_s"]/med_heal)
            notes.append(f"Healing cadence: {gap:.0f}% below team median — anticipate spike windows, pre-cast more.")
        if r["n_deaths"] > 0:
            notes.append(f"{int(r['n_deaths'])} deaths — check defensives, personals, and healer externals usage on spikes.")
        if notes:
            suggestions["players"][int(r["sourceID"])] = notes

    return suggestions

def write_coaching_md(suggestions: dict, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    lines = ["# Coaching Notes\n"]
    if suggestions.get("team"):
        lines.append("## Team\n")
        for s in suggestions["team"]:
            lines.append(f"- {s}")
        lines.append("")
    if suggestions.get("players"):
        lines.append("## Players\n")
        for pid, notes in suggestions["players"].items():
            lines.append(f"### Player {pid}")
            for s in notes:
                lines.append(f"- {s}")
            lines.append("")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return out_path
