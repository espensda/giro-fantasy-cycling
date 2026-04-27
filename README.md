# Giro Fantasy Cycling

Fantasy cycling game for Giro d'Italia 2026, built with Streamlit.

## Run Locally

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Start the app:

```bash
streamlit run app.py
```

## Deploy To Streamlit Community Cloud

### 1. Push this repository to GitHub

Your Streamlit Cloud app deploys directly from a GitHub repository.

### 2. Create the app in Streamlit Cloud

1. Go to https://share.streamlit.io
2. Click `New app`
3. Select this repository and branch
4. Set `Main file path` to:

```text
app.py
```

5. Click `Deploy`

### 3. Verify first launch

After deployment, open the app URL and check:

1. Home page loads
2. Admin page is accessible
3. Database view renders

## Important Notes For Cloud Hosting

1. The default SQLite database is stored on the app container filesystem.
2. Streamlit Community Cloud containers can restart, which may reset local DB state.
3. For persistent multi-user play, migrate to a managed Postgres database later.

## Current Upload Workflow

In Admin -> Add Results:

1. Upload one Excel file (`.xlsx`) per stage
2. Required columns:

```text
name, points
```

3. `points` should be base points for that stage; game rules then apply winner-team bonus and category multiplier.
