import os, json, yaml, datetime as dt
from pathlib import Path


try:
    import pytz
except ImportError:
    pytz = None

from .provider_demo import demo_duration_with_traffic, DemoRequest
from .provider_tomtom import tomtom_duration_with_traffic, TomTomRequest
from .quota import read_count, bump_count

def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def fetch_once(config_path="configs/routes.yaml", out_dir="data/raw", quota_root="data/quota"):
    cfg = load_yaml(config_path)
    tzname = cfg.get("timezone", "Asia/Kolkata")

    if pytz:
        import pytz as _p
        tz = _p.timezone(tzname)
        now_local = dt.datetime.now(tz)
    else:
        now_local = dt.datetime.now()

    provider = os.getenv("TRAFFIC_PROVIDER", "tomtom").lower()

    # Simple daily cap
    max_per_day = int(os.getenv("MAX_REQUESTS_PER_DAY", "1000000"))
    used = read_count(quota_root, provider)
    if used >= max_per_day:
        raise RuntimeError(f"Daily request cap reached for provider={provider} ({used}/{max_per_day}).")

    req_timeout = int(os.getenv("REQUEST_TIMEOUT_SEC", "10"))
    max_retries = int(os.getenv("MAX_RETRIES", "3"))

    wrote = 0
    for r in cfg["routes"]:
        route_id = r["route_id"]
        origin = r["origin"]
        destination = r["destination"]
        baseline = int(r["baseline_duration_sec"])
        distance_m = int(r.get("distance_m", 0))

        if provider == "demo":
            doc = demo_duration_with_traffic(DemoRequest(
                origin=origin, destination=destination,
                baseline_duration_sec=baseline, distance_m=distance_m, now=now_local
            ))
            duration_sec = doc["duration_in_traffic_sec"]
            distance_m_ = doc["distance_m"]

        elif provider == "tomtom":
            print("Using tomtom")
            api_key = os.getenv("TOMTOM_API_KEY", "BpOpNnNWxUyxLZlRTaPFFvwaqebdV1R3").strip()
            if not api_key:
                raise RuntimeError("TOMTOM_API_KEY is not set.")
            doc = tomtom_duration_with_traffic(TomTomRequest(
                origin=origin, destination=destination,
                api_key=api_key, timeout=req_timeout, max_retries=max_retries
            ))
            duration_sec = doc["duration_in_traffic_sec"]
            distance_m_ = doc["distance_m"]

        else:
            raise ValueError(f"Unsupported provider: {provider}")

        # Write record
        record = {
            "timestamp": now_local.isoformat(),
            "route_id": route_id,
            "origin": origin,
            "destination": destination,
            "duration_sec": duration_sec,
            "distance_m": distance_m_,
            "provider": provider,
            "weekday": now_local.weekday(),
            "minute_of_day": now_local.hour*60 + now_local.minute
        }

        day_file = ensure_dir(Path(out_dir)) / f"{route_id}_{now_local:%Y%m%d}.jsonl"
        with open(day_file, "a") as f:
            f.write(json.dumps(record) + "\n")
        wrote += 1

    # Bump quota counter by number of successful route calls
    bump_count(quota_root, provider, wrote)
    print(f"Wrote {wrote} record(s). Requests today for {provider}: {read_count(quota_root, provider)}")

if __name__ == "__main__":
    fetch_once()
