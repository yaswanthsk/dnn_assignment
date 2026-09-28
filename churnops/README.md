# ChurnOps — Cloud DataOps Pipeline + API (AIMLCZG549 Assignment I)

Customer Churn prediction pipeline (ingestion -> preprocessing -> EDA),
scheduled every 2 minutes on **Prefect Cloud** (free), with a **FastAPI**
layer exposing application details, deployed on **Render** (free).

## Files (only 4 — everything else is auto-generated or your local venv)
```
app.py              Pipeline (Prefect flow) + API (FastAPI) in one file
requirements.txt    Dependencies
data/telco_churn.csv  Dataset (Telco Customer Churn, 7043 rows)
README.md           This file
```

## 1. Upload to GitHub (no git push needed — web UI works behind any firewall)
1. Go to https://github.com/new, create a new **empty** repo (e.g. `churnops`)
2. Click **"uploading an existing file"** on the new repo page
3. Drag in `app.py`, `requirements.txt`, `README.md`, and the `data` folder
   (drag the whole `data` folder — GitHub keeps the folder structure)
4. Commit directly in the browser

## 2. Deploy the API — Render (free)
1. https://render.com -> Sign in with GitHub -> **New +** -> **Web Service**
2. Connect your `churnops` repo
3. Settings:
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `uvicorn app:api --host 0.0.0.0 --port $PORT`
4. Click **Create Web Service** — Render gives you a public URL like
   `https://churnops.onrender.com`
5. Test it: open `https://churnops.onrender.com/docs` in your browser

## 3. Schedule the pipeline every 2 minutes — Prefect Cloud (free)
1. https://app.prefect.cloud -> sign up (free)
2. On your machine (or in Render's shell), run:
   ```
   pip install prefect
   prefect cloud login
   ```
3. Deploy the flow with a 2-minute schedule:
   ```
   prefect deploy app.py:churn_pipeline -n churn-run --cron "*/2 * * * *"
   ```
4. Start a worker to execute scheduled runs:
   ```
   prefect worker start --pool default-agent-pool
   ```
5. Open the Prefect Cloud dashboard — this **is** your "Cloud dashboard"
   (Assignment 1.5): shows every run, logs, duration, success/failure, on a
   timeline, automatically.

## 4. Test the API (Sub-Objective 2)
Use Postman or `curl` against your Render URL, screenshot each request +
response + status code:
```
curl -i https://churnops.onrender.com/status
curl -i https://churnops.onrender.com/dataset/info
curl -i https://churnops.onrender.com/pipeline/config
curl -i https://churnops.onrender.com/artifacts
```
At least 4 application details are exposed: `/status`, `/dataset/info`,
`/pipeline/config`, `/artifacts`.

## Run locally first (optional, to sanity check before deploying)
```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python app.py                              # runs the pipeline once
uvicorn app:api --reload --port 8000       # then in a 2nd terminal, run the API
```

## Group Contribution
| Member | Contribution |
|---|---|
| Member 1 | ... |
| Member 2 | ... |
| Member 3 | ... |
| Member 4 | ... |
