# src/models/promote.py
import os, json, shutil
from pathlib import Path
from datetime import datetime

def load_json(p: Path, default=None):
    try:
        return json.loads(p.read_text())
    except Exception:
        return default

def main(model_root="data/models", min_improve_env="MIN_IMPROVEMENT"):
    model_root = Path(model_root)
    prod_dir = model_root / "production"
    prod_dir.mkdir(parents=True, exist_ok=True)

    last_metrics = load_json(model_root / "last_metrics.json")
    if not last_metrics:
        raise SystemExit("[promote] No last_metrics.json found. Train first.")

    new_f1 = float(last_metrics.get("f1_macro", 0.0))
    new_run = last_metrics.get("run_id")

    prod_meta_path = prod_dir / "metadata.json"
    prod_meta = load_json(prod_meta_path, default={"f1_macro": -1.0, "run_id": None})
    old_f1 = float(prod_meta.get("f1_macro", -1.0))

    min_improve = float(os.getenv(min_improve_env, "0.0"))

    print(f"[promote] old_f1={old_f1:.3f} new_f1={new_f1:.3f} min_improve={min_improve}")
    if new_f1 >= old_f1 + min_improve:
        src = model_root / "latest_logreg.joblib"
        dst = prod_dir / "model.joblib"
        if not src.exists():
            raise SystemExit(f"[promote] Missing model file: {src}")

        shutil.copy2(src, dst)
        meta = {
            "promoted_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            "f1_macro": new_f1,
            "run_id": new_run,
            "source_file": str(src),
        }
        prod_meta_path.write_text(json.dumps(meta, indent=2))
        print(f"[promote] ✅ promoted -> {dst}")
    else:
        print("[promote] ⏭️ not promoted (no improvement)")

if __name__ == "__main__":
    main()
