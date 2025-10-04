from __future__ import annotations
import pandas as pd
from sklearn.ensemble import IsolationForest

def add_anomaly_scores(df: pd.DataFrame) -> pd.DataFrame:
    feats = ["active_uptime_pct","casts_per_s","dmg_events_per_s","heal_events_per_s","n_deaths"]
    X = df[feats].fillna(0)
    mdl = IsolationForest(n_estimators=300, contamination=0.1, random_state=42)
    scores = mdl.score_samples(X)  # higher is more normal
    out = df.copy()
    out["anomaly_score"] = scores
    return out
