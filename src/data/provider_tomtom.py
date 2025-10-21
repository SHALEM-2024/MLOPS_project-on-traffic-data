# src/data/provider_tomtom.py
import time
import requests
from dataclasses import dataclass

@dataclass
class TomTomRequest:
    origin: str        # "lat,lon"
    destination: str   # "lat,lon"
    api_key: str
    timeout: int = 10
    max_retries: int = 3

BASE = "https://api.tomtom.com/maps/orbis/routing/calculateRoute"

def tomtom_duration_with_traffic(req: TomTomRequest) -> dict:
    """
    Returns:
      {"duration_in_traffic_sec": int, "distance_m": int}
    """
    # Build URL like: .../calculateRoute/lat1,lon1:lat2,lon2/json
    path = f"{req.origin}:{req.destination}"
    url = f"{BASE}/{path}/json"

    params = {
        "key": req.api_key,
        "apiVersion": 2,          # Orbis Routing v2
        "traffic": "live",        # include live traffic
        "routeType": "fast",   # sensible default
        "travelMode": "car"
    }

    retries, backoff = 0, 1.0
    while True:
        try:
            r = requests.get(url, params=params, timeout=req.timeout)
        except requests.RequestException as e:
            if retries >= req.max_retries:
                raise RuntimeError(f"TomTom request failed: {e}")
            time.sleep(backoff); retries += 1; backoff *= 2; continue

        if r.status_code == 200:
            data = r.json()
            try:
                summary = data["routes"][0]["summary"]
                travel = int(summary["travelTimeInSeconds"])     # ETA (traffic-aware)
                length = int(summary["lengthInMeters"])
                return {"duration_in_traffic_sec": travel, "distance_m": length}
            except Exception as e:
                raise RuntimeError(f"Unexpected TomTom response shape: {e} | payload={data}")

        if r.status_code in (429, 500, 502, 503, 504) and retries < req.max_retries:
            time.sleep(backoff); retries += 1; backoff *= 2; continue

        # Non-retryable failure
        try:
            payload = r.json()
        except Exception:
            payload = r.text
        raise RuntimeError(f"TomTom HTTP {r.status_code}: {payload}")
