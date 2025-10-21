import glob
from pathlib import Path
import pandas as pd

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def load_clean(clean_root="data/clean"):
    parts = sorted(glob.glob(f"{clean_root}/date=*/traffic.parquet"))
    if not parts:
        raise FileNotFoundError(f"No clean parquet found under {clean_root}")
    dfs = [pd.read_parquet(p) for p in parts]
    df = pd.concat(dfs, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values(["route_id", "timestamp"])

def add_lags(df: pd.DataFrame) -> pd.DataFrame:
    def per_route(g):
        g = g.sort_values("timestamp")
        g["lag1"] = g["duration_sec"].shift(1)
        g["lag3_mean"] = g["duration_sec"].rolling(3, min_periods=1).mean().shift(1)
        return g
    return df.groupby("route_id", group_keys=False).apply(per_route)

def time_split(df: pd.DataFrame, val_days: int = 1):
    # Use the most recent 'val_days' as validation
    last_date = df["timestamp"].dt.date.max()
    cutoff = pd.Timestamp(last_date) - pd.Timedelta(days=val_days-1)
    train = df[df["timestamp"].dt.date < cutoff.date()]
    val   = df[df["timestamp"].dt.date >= cutoff.date()]
    # Fallback if you only have one day of data
    if train.empty:
        n = len(df)
        cut = max(1, int(n*0.8))
        train, val = df.iloc[:cut], df.iloc[cut:]
    return train, val

def main(clean_root="data/clean", feat_root="data/features"):
    raw = load_clean(clean_root)
    raw = add_lags(raw)

    # Ensure numeric dtypes
    raw = raw.assign(
        minute_of_day=raw["minute_of_day"].astype(int),
        weekday=raw["weekday"].astype(int),
        distance_m=raw["distance_m"].astype(int),
        free_flow_sec=raw["free_flow_sec"].astype(int),
        congestion_level=raw["congestion_level"].astype(int),
    )

    train_df, val_df = time_split(raw, val_days=1)

    cols = ["minute_of_day","weekday","distance_m","free_flow_sec","lag1","lag3_mean","congestion_level"]
    train_out = train_df[cols].dropna()
    val_out   = val_df[cols].dropna()

    outdir = ensure_dir(Path(feat_root))
    train_out.to_parquet(outdir / "train.parquet", index=False)
    val_out.to_parquet(outdir / "val.parquet", index=False)
    print(f"[features] wrote {len(train_out)} train rows, {len(val_out)} val rows to {outdir}/")

if __name__ == "__main__":
    main()
