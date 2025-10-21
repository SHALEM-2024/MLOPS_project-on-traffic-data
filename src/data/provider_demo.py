import math, random, datetime as dt
from dataclasses import dataclass

@dataclass
class DemoRequest:
    origin: str
    destination: str
    baseline_duration_sec: int
    distance_m: int
    now: dt.datetime  # timezone-aware

def demo_duration_with_traffic(req: DemoRequest) -> dict:
    minute_of_day = req.now.hour * 60 + req.now.minute

    def peak(minute, center, width):
        x = (minute - center) / width
        return max(0.0, math.cos(x * math.pi / 2)) ** 2  # smooth bump

    morning = peak(minute_of_day, 9*60, 160)
    evening = peak(minute_of_day, 18*60, 180)
    rush_factor = 1.0 + 0.45*morning + 0.55*evening

    is_weekend = req.now.weekday() >= 5
    weekend_factor = 0.9 if is_weekend else 1.0

    noise = random.uniform(-0.05, 0.05)
    duration = int(req.baseline_duration_sec * rush_factor * weekend_factor * (1.0 + noise))

    return {
        "duration_in_traffic_sec": max(60, duration),
        "distance_m": req.distance_m
    }
