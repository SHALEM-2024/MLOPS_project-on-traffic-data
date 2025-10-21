from pathlib import Path
from datetime import datetime

def _quota_file(root: str, provider: str) -> Path:
    d = Path(root) / provider
    d.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    return d / f"{provider}_{today}.count"

def read_count(root: str, provider: str) -> int:
    f = _quota_file(root, provider)
    if not f.exists(): return 0
    try:
        return int(f.read_text().strip())
    except Exception:
        return 0

def bump_count(root: str, provider: str, delta: int = 1) -> int:
    f = _quota_file(root, provider)
    cnt = read_count(root, provider) + delta
    f.write_text(str(cnt))
    return cnt
