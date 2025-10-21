# src/models/train.py
import os
import mlflow, mlflow.sklearn  # import early, fine
from pathlib import Path
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, classification_report
import json
from datetime import datetime

# --- set tracking URI BEFORE any other mlflow call ---
# ...imports unchanged...

# --- set tracking URI BEFORE any other mlflow call ---
uri = os.getenv("MLFLOW_TRACKING_URI", "").strip()
print(f"[train] env MLFLOW_TRACKING_URI = {uri or '(empty)'}")
if not uri:
    uri = "http://mlflow:5000"
    print(f"[train] defaulting tracking URI -> {uri}")
mlflow.set_tracking_uri(uri)
print(f"[train] effective tracking URI = {mlflow.get_tracking_uri()}")

def load_xy(path: str):
    df = pd.read_parquet(path)
    if df.empty:
        raise ValueError(f"{path} is empty – upstream step produced no rows.")
    if "congestion_level" not in df.columns:
        raise ValueError(f"{path} has no 'congestion_level' column.")
    y = df["congestion_level"].astype(int)
    X = df.drop(columns=["congestion_level"])
    if X.empty or len(y) == 0:
        raise ValueError(f"{path} has no usable rows after separating X/y.")
    return X, y

def main(feat_root="data/features", model_root="data/models", experiment="traffic-congestion"):
    mlflow.set_experiment(experiment)

    X_train, y_train = load_xy(f"{feat_root}/train.parquet")
    X_val,   y_val   = load_xy(f"{feat_root}/val.parquet")

    for split in ["train", "val"]:
        df = pd.read_parquet(f"data/features/{split}.parquet")
        print(split, df.shape, list(df.columns)[:10])
        print("null rows in target:", df["congestion_level"].isna().sum() if "congestion_level" in df else "MISSING")


    with mlflow.start_run():
        params = {"max_iter": 300, "class_weight": "balanced", "multi_class": "auto"}
        clf = LogisticRegression(**params).fit(X_train, y_train)

        y_pred = clf.predict(X_val)
        f1_macro = f1_score(y_val, y_pred, average="macro")

        mlflow.log_params(params)
        mlflow.log_metric("f1_macro", float(f1_macro))
        metrics = {
            "run_id": mlflow.active_run().info.run_id if mlflow.active_run() else None,
            "f1_macro": float(f1_macro),
            "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds")
        }
        (Path(model_root) / "last_metrics.json").write_text(json.dumps(metrics, indent=2))
        print(f"[train] saved metrics -> {Path(model_root) / 'last_metrics.json'}")

        outdir = Path(model_root)
        outdir.mkdir(parents=True, exist_ok=True)

        # save model in a writable place
        local_model = outdir / "latest_logreg.joblib"
        print(local_model, "*********")
        joblib.dump(clf, local_model)
        mlflow.log_artifact(str(local_model), artifact_path="model_files")

        # save the text report in the SAME writable place
        report_path = outdir / "cls_report.txt"
        report = classification_report(y_val, y_pred, digits=3, zero_division=0)  # suppress warnings
        report_path.write_text(report)
        mlflow.log_artifact(str(report_path))

        print(f"[train] f1_macro={f1_macro:.3f} | model -> {local_model} | report -> {report_path}")

if __name__ == "__main__":
    main()
