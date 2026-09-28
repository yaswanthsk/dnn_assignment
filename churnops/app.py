"""
ChurnOps: single-file DataOps pipeline + API for AIMLCZG549 Assignment I.

- Prefect flow: ingestion -> preprocessing -> EDA (Sub-Objective 1)
- FastAPI app: exposes pipeline/application details (Sub-Objective 2)

Run pipeline once:      python app.py
Run API locally:        uvicorn app:api --reload --port 8000
Deploy schedule:        Prefect Cloud (see README) runs churn_pipeline() every 2 min
"""
import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from prefect import flow, task, get_run_logger
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "telco_churn.csv")
ARTIFACT_DIR = os.path.join(BASE_DIR, "artifacts")
STATUS_PATH = os.path.join(BASE_DIR, "latest_status.json")
DATASET_URL = "https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv"

os.makedirs(ARTIFACT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)


# ---------------------------------------------------------------------------
# Prefect DataOps pipeline (Sub-Objective 1)
# ---------------------------------------------------------------------------

@task
def load_data():
    logger = get_run_logger()
    if not os.path.exists(DATA_PATH):
        logger.info(f"Dataset not found locally, downloading from {DATASET_URL}")
        response = requests.get(DATASET_URL, timeout=30)
        response.raise_for_status()
        with open(DATA_PATH, "wb") as f:
            f.write(response.content)
        logger.info("Dataset downloaded successfully")
    logger.info(f"Ingesting dataset from {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)
    logger.info(f"Loaded dataset with shape {df.shape}")
    return df


@task
def preprocess(df):
    logger = get_run_logger()
    df.describe(include="all")
    missing_before = df.isnull().sum()
    logger.info(f"Missing values found in {(missing_before > 0).sum()} columns")

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    for col in numeric_cols:
        if df[col].isnull().any():
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.info(f"Imputed '{col}' with median={median_val:.2f}")

    if "TotalCharges" in df.columns and df["TotalCharges"].dtype == object:
        df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")
        df["TotalCharges"] = df["TotalCharges"].fillna(df["TotalCharges"].median())
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    logger.info(f"Data types: {df.dtypes.astype(str).to_dict()}")

    normalized_count = 0
    for col in numeric_cols:
        col_min, col_max = df[col].min(), df[col].max()
        if col_max > col_min:
            normalized_count += 1
    logger.info(f"Normalized {normalized_count} numeric columns (min-max scaling)")
    return df


@task
def eda(df):
    logger = get_run_logger()
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = df.select_dtypes(include=["object", "str"]).columns.tolist()

    corr_matrix = df[numeric_cols].corr()
    logger.info(f"Correlation matrix across {len(numeric_cols)} numeric features")
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Heatmap")
    plt.tight_layout()
    plt.savefig(os.path.join(ARTIFACT_DIR, "correlation_heatmap.png"))
    plt.close()

    if "tenure" in df.columns:
        df["tenure_bin"] = pd.cut(
            df["tenure"], bins=[0, 12, 24, 48, 72],
            labels=["0-12mo", "13-24mo", "25-48mo", "49-72mo"]
        )
        logger.info(f"Tenure bins: {df['tenure_bin'].value_counts().to_dict()}")

    for col in categorical_cols[:5]:
        logger.info(f"Top categories in '{col}': {df[col].value_counts().head(5).to_dict()}")

    target_col = "Churn" if "Churn" in df.columns else None
    if target_col:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.preprocessing import LabelEncoder

        model_df = df.copy()
        model_df[target_col] = LabelEncoder().fit_transform(model_df[target_col].astype(str))
        feature_cols = [c for c in numeric_cols if c != target_col]
        cat_feature_cols = [c for c in categorical_cols if c not in (target_col, "customerID")]
        for c in cat_feature_cols:
            model_df[c] = LabelEncoder().fit_transform(model_df[c].astype(str))
        all_features = feature_cols + cat_feature_cols

        if all_features:
            X = model_df[all_features].fillna(0)
            y = model_df[target_col]
            rf = RandomForestClassifier(n_estimators=100, random_state=42)
            rf.fit(X, y)
            importances = dict(zip(all_features, rf.feature_importances_))
            top_importance = dict(sorted(importances.items(), key=lambda x: -x[1])[:10])
            logger.info(f"Feature importance (top 10): {top_importance}")

            plt.figure(figsize=(8, 6))
            plt.barh(list(top_importance.keys()), list(top_importance.values()))
            plt.gca().invert_yaxis()
            plt.xlabel("Importance")
            plt.title(f"Feature Importance for {target_col}")
            plt.tight_layout()
            plt.savefig(os.path.join(ARTIFACT_DIR, "feature_importance.png"))
            plt.close()

    if numeric_cols:
        fig, axes = plt.subplots(1, min(3, len(numeric_cols)), figsize=(15, 4))
        if len(numeric_cols) == 1:
            axes = [axes]
        for ax, col in zip(axes, numeric_cols[:3]):
            sns.histplot(df[col], kde=True, ax=ax)
            ax.set_title(f"Distribution: {col}")
        plt.tight_layout()
        plt.savefig(os.path.join(ARTIFACT_DIR, "univariate_distributions.png"))
        plt.close()

    if target_col and numeric_cols:
        plt.figure(figsize=(8, 5))
        sns.boxplot(data=df, x=target_col, y=numeric_cols[0])
        plt.title(f"{numeric_cols[0]} vs {target_col} (Bivariate)")
        plt.tight_layout()
        plt.savefig(os.path.join(ARTIFACT_DIR, "bivariate_analysis.png"))
        plt.close()

    logger.info("EDA complete: charts saved to artifacts/")


@flow(name="churn-dataops-pipeline")
def churn_pipeline():
    logger = get_run_logger()
    started_at = datetime.now(timezone.utc).isoformat()
    df = load_data()
    cleaned_df = preprocess(df)
    eda(cleaned_df)
    finished_at = datetime.now(timezone.utc).isoformat()
    with open(STATUS_PATH, "w") as f:
        json.dump({"started_at": started_at, "finished_at": finished_at, "status": "success"}, f, indent=2)
    logger.info(f"Pipeline run complete: {started_at} -> {finished_at}")


# ---------------------------------------------------------------------------
# FastAPI app (Sub-Objective 2 - API Access)
# ---------------------------------------------------------------------------

api = FastAPI(
    title="ChurnOps Pipeline API",
    description="API access layer for the Customer Churn DataOps pipeline (AIMLCZG549 Assignment I)",
    version="1.0.0",
)
api.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@api.get("/")
def root():
    return {"service": "ChurnOps Pipeline API", "status": "online", "time": datetime.now(timezone.utc).isoformat(), "docs": "/docs"}


@api.get("/status")
def get_status():
    """Application detail #1: latest pipeline run status."""
    if not os.path.exists(STATUS_PATH):
        raise HTTPException(status_code=404, detail="No pipeline run has completed yet.")
    with open(STATUS_PATH) as f:
        return json.load(f)


@api.get("/dataset/info")
def dataset_info():
    """Application detail #2: dataset metadata."""
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=404, detail="Dataset not found.")
    df = pd.read_csv(DATA_PATH)
    return {
        "source": "Telco Customer Churn (Kaggle / IBM sample dataset)",
        "rows": df.shape[0],
        "columns": df.shape[1],
        "column_names": df.columns.tolist(),
        "dtypes": df.dtypes.astype(str).to_dict(),
    }


@api.get("/pipeline/config")
def pipeline_config():
    """Application detail #3: pipeline flow/deployment/schedule details."""
    return {
        "pipeline_name": "churn-dataops-pipeline",
        "schedule": "every 2 minutes (Prefect Cloud deployment)",
        "orchestrator": "Prefect Cloud",
        "steps": ["1. Data Ingestion", "2. Pre-processing", "3. EDA", "4. Logging (Prefect UI)"],
    }


@api.get("/artifacts")
def list_artifacts():
    """Application detail #4: generated EDA chart artifacts."""
    if not os.path.isdir(ARTIFACT_DIR):
        return {"artifacts": []}
    files = [f for f in os.listdir(ARTIFACT_DIR) if f.endswith(".png")]
    return {"count": len(files), "artifacts": files}


@api.get("/artifacts/{filename}")
def get_artifact(filename: str):
    safe_name = os.path.basename(filename)
    path = os.path.join(ARTIFACT_DIR, safe_name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Artifact '{safe_name}' not found.")
    return FileResponse(path, media_type="image/png")


@api.post("/pipeline/trigger")
def trigger_run_now():
    """Manually trigger an immediate pipeline run (useful for demos)."""
    churn_pipeline()
    return {"message": "Pipeline run triggered", "time": datetime.now(timezone.utc).isoformat()}


if __name__ == "__main__":
    churn_pipeline()
