# Invest Diary (Streamlit)

Personal investment portfolio tracker built with Streamlit.

## Run locally

```bash
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

## Main tabs

- Dashboard
- Record Input
- FX
- Company Info
- Company Comparison
- Company Score
- API Settings

## Security

- API keys are saved in local `portfolio.db` (not committed).
- `.gitignore` excludes:
  - `portfolio.db`, `*.db`, `*.sqlite*`
  - `.env*`, `.streamlit/secrets.toml`
  - personal workbook `내 주식자산.xlsx`

## Streamlit Community Cloud deployment

1. Open Streamlit Community Cloud and click `New app`.
2. Repository: `ssungjun83/invest_diary`
3. Branch: `main`
4. Main file path: `app.py`
5. Click `Deploy`

If you need API keys in deployed environment, configure them in Streamlit Cloud `Secrets`.
