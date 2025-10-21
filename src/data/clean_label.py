import json
import os
import sys
import glob
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------- helpers ----------
def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def label_row(duration_sec: int, free_flow: int) -> tuple[str, int]:
    free_flow = max(60, int(free_flow))  # guardrail
    if duration_sec <= 1.15 * free_flow:
        return "light", 0
    elif duration_sec <= 1.41 * free_flow:
        return "moderate", 1
    else:
        return "heavy", 2

# ---------- main ----------
def run_clean(
    config_path="configs/routes.yaml",
    raw_dir="data/raw",
    clean_root="data/clean",
    date_str=None
):
    """
    Reads raw JSONL files for the given date, attaches labels using baseline_duration_sec
    from routes.yaml, and writes a partitioned Parquet dataset under:
      data/clean/date=YYYY-MM-DD/traffic.parquet
    """
    cfg = load_yaml(config_path)

    # route_id -> static config (baseline, distance, origin, destination)
    route_cfg = {}
    for r in cfg["routes"]:
        route_cfg[r["route_id"]] = {
            "baseline_duration_sec": int(r["baseline_duration_sec"]),
            "distance_m": int(r.get("distance_m", 0)),
            "origin": r["origin"],
            "destination": r["destination"],
        }

    # Decide which date (YYYYMMDD) to process
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")
    pretty_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    # Find ALL raw files for that date regardless of route_id contents
    # Pattern example: *_20250909.jsonl — works even if route_id has underscores
    pattern = os.path.join(raw_dir, f"*_{date_str}.jsonl")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[clean_label] No raw files found for {date_str} in {raw_dir}", file=sys.stderr)
        return 0

    rows = []
    for fp in files:
        with open(fp, "r") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Read route_id directly from the JSON record (robust to underscores)
                route_id = rec.get("route_id")
                if not route_id or route_id not in route_cfg:
                    # Skip if route_id missing or not in config
                    print(f"[clean_label] Skipping record with unknown route_id: {route_id}", file=sys.stderr)
                    continue

                cfg_r = route_cfg[route_id]
                baseline = cfg_r["baseline_duration_sec"]
                duration = int(rec.get("duration_sec", 0))

                label_str, label_int = label_row(duration, baseline)

                rows.append({
                    "timestamp": rec.get("timestamp"),
                    "date": pretty_date,
                    "route_id": route_id,
                    "origin": cfg_r["origin"],
                    "destination": cfg_r["destination"],
                    "distance_m": cfg_r["distance_m"],
                    "duration_sec": duration,
                    "free_flow_sec": baseline,
                    "congestion": label_str,
                    "congestion_level": label_int,
                    "weekday": rec.get("weekday"),
                    "minute_of_day": rec.get("minute_of_day"),
                    "provider": rec.get("provider", "demo")
                })

    if not rows:
        print(f"[clean_label] No rows assembled for date {pretty_date}.", file=sys.stderr)
        return 0

    df = pd.DataFrame(rows).sort_values("timestamp")

    out_dir = ensure_dir(Path(clean_root) / f"date={pretty_date}")
    out_fp = out_dir / "traffic.parquet"
    df.to_parquet(out_fp, index=False)
    print(f"[clean_label] Wrote {len(df)} rows -> {out_fp}")
    return len(df)

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_clean(date_str=date_arg)

