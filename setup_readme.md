# Setup & Run Guide

## 1. Install dependencies

```
pip install -r requirements.txt
```

## 2. Run the notebook

Open it in Jupyter / JupyterLab / VS Code and use **"Run All"**.

Or from the command line:

```
jupyter nbconvert --to notebook --execute --inplace <your-notebook-file>.ipynb --ExecutePreprocessor.timeout=1800
```

## 3. Convert to PDF

```
jupyter nbconvert --to pdf <your-notebook-file>.ipynb
```

If that fails because LaTeX isn't installed:

```
pip install "nbconvert[webpdf]"
playwright install chromium
jupyter nbconvert --to webpdf <your-notebook-file>.ipynb
```
