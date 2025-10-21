# src/monitoring/drift.py
import os, json, glob, math
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True); return p

def _psi(a, b, eps=1e-6):
    # a, b are arrays of bin proportions (sum to 1)
    return float(sum((ai - bi) * math.log((ai + eps) / (bi + eps)) for ai, bi in zip(a, b)))

def _proportions(series, bins):
    hist, _ = pd.cut(series, bins=bins, include_lowest=True, right=True, retbins=True)
    pr = hist.value_counts(normalize=True, sort=False)
    return pr.values.tolist()

def _load_day(clean_root, day):
    p = Path(clean_root) / f"date={day}/traffic.parquet"
    return pd.read_parquet(p) if p.exists() else None

def main(clean_root="data/clean", out_root="data/monitoring", ref_days=7):
    today = pd.Timestamp.now(tz="Asia/Kolkata").date()
    today_str = today.strftime("%Y-%m-%d")

    # collect reference window
    ref_frames = []
    for i in range(1, ref_days + 1):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        df = _load_day(clean_root, d)
        if df is not None: ref_frames.append(df)
    ref_df = pd.concat(ref_frames, ignore_index=True) if ref_frames else None

    cur_df = _load_day(clean_root, today_str)
    if cur_df is None or ref_df is None:
        print("[drift] Not enough data for drift check.")
        return 0

    # PSI on duration_sec
    # Build bins from reference quantiles (10 bins)
    qs = ref_df["duration_sec"].quantile([i/10 for i in range(11)]).values
    bins = sorted(set(qs))  # de-dup if small data
    if len(bins) < 3:
        print("[drift] Too few unique values for PSI.")
        return 0

    ref_p = _proportions(ref_df["duration_sec"], bins)
    cur_p = _proportions(cur_df["duration_sec"], bins)
    psi_duration = _psi(ref_p, cur_p)

    # Class balance shift
    ref_cls = ref_df["congestion"].value_counts(normalize=True)
    cur_cls = cur_df["congestion"].value_counts(normalize=True)
    classes = sorted(set(ref_cls.index) | set(cur_cls.index))
    cls_shift = {c: float(cur_cls.get(c, 0.0) - ref_cls.get(c, 0.0)) for c in classes}

    report = {
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
        "today": today_str,
        "reference_days": ref_days,
        "psi_duration_sec": psi_duration,
        "class_balance_shift": cls_shift,
    }

    out_dir = ensure_dir(Path(out_root) / f"date={today_str}")
    (out_dir / "drift_report.json").write_text(json.dumps(report, indent=2))
    print(f"[drift] wrote -> {out_dir / 'drift_report.json'} | PSI={psi_duration:.3f}")
    return 1

if __name__ == "__main__":
    main()
