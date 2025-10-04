from __future__ import annotations
import os, csv
import pandas as pd

FEATURES = [
    "team_active_uptime_mean",
    "team_active_uptime_p25",
    "team_active_uptime_p75",
    "players_below_uptime_90_pct",
    "team_casts_per_s_mean",
    "team_dmg_events_per_s_mean",
    "team_heal_events_per_s_mean",
    "team_dmg_events_per_s_cv",
    "team_deaths_sum",
    "death_rate_per_player",
    "n_players",
]

def _read_fight_durations(base_dir: str):
    path = os.path.join(base_dir, "fights.csv")
    if not os.path.exists(path):
        return {}
    out = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fid = int(row["id"])
            dur_s = max(1.0, (int(row["endTime"]) - int(row["startTime"])) / 1000.0)
            out[fid] = dur_s
    return out

def build_pull_features(report_code: str, out_dir: str) -> pd.DataFrame:
    base = os.path.join(out_dir, report_code)
    pf_path = os.path.join(base, "player_features.csv")
    if not os.path.exists(pf_path):
        raise FileNotFoundError(f"Missing {pf_path}. Run BuildPlayerFeatures first.")
    df = pd.read_csv(pf_path).fillna(0)

    # fight durations
    fight_dur = _read_fight_durations(base)
    if "duration_s" not in df.columns or not fight_dur:
        # fallback: duration per fight from player rows (they all have same duration)
        dur_series = df.groupby("fight_id")["duration_s"].first()
    else:
        dur_series = pd.Series(fight_dur, name="duration_s")

    g = df.groupby("fight_id")
    agg = pd.DataFrame({
        "team_active_uptime_mean": g["active_uptime_pct"].mean(),
        "team_active_uptime_p25": g["active_uptime_pct"].quantile(0.25),
        "team_active_uptime_p75": g["active_uptime_pct"].quantile(0.75),
        "players_below_uptime_90_pct": g.apply(lambda x: (x["active_uptime_pct"] < 90).mean()*100),
        "team_casts_per_s_mean": g["casts_per_s"].mean(),
        "team_dmg_events_per_s_mean": g["dmg_events_per_s"].mean(),
        "team_heal_events_per_s_mean": g["heal_events_per_s"].mean(),
        "team_deaths_sum": g["n_deaths"].sum(),
        "n_players": g["sourceID"].nunique(),
    })

    # coefficient of variation for damage cadence
    dmg = g["dmg_events_per_s"].agg(["mean","std"]).rename(columns={"mean":"_mean","std":"_std"})
    agg["team_dmg_events_per_s_cv"] = (dmg["_std"] / (dmg["_mean"].replace(0, 1))).fillna(0)

    # death rate
    agg["death_rate_per_player"] = agg["team_deaths_sum"] / agg["n_players"].clip(lower=1)

    # label: normalized progress (duration / max_duration in this report)
    agg["duration_s"] = agg.index.map(dur_series.to_dict()).fillna(df["duration_s"].max())
    max_dur = float(agg["duration_s"].max()) or 1.0
    agg["progress_norm"] = (agg["duration_s"] / max_dur).clip(0, 1)

    # "good pull" label: top 25% by duration (or >= 0.8 of max)
    thresh = max(agg["progress_norm"].quantile(0.75), 0.8)
    agg["good_pull"] = (agg["progress_norm"] >= thresh).astype(int)

    agg.reset_index(inplace=True)  # fight_id back as a column
    agg.insert(0, "fight_id", agg.pop("fight_id"))
    return agg

def write_pull_features_csv(df: pd.DataFrame, path: str) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False)
    return path
