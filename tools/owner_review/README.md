# Owner Review Tools

These Streamlit apps are local tools for inspecting parcel-owner match candidates, recording human review decisions, and browsing the owner mart output.

They are intentionally outside `dags/` and `include/` so they do not become part of the Airflow import path. Treat them as bootstrap/manual review tooling for this draft; the pipeline consumes the database tables they write, but the Astro runtime does not need Streamlit installed.

Run directly from the repo root after installing development dependencies:

```bash
pip install -r requirements-dev.txt
streamlit run tools/owner_review/owner_match_review.py
```

On Windows, the launchers in this directory install their own temporary Streamlit dependencies and start each app on localhost:

```powershell
.\tools\owner_review\start_owner_match_review.ps1
.\tools\owner_review\start_owner_bulk_match_review.ps1
.\tools\owner_review\start_owner_mart_browser.ps1
```

CSV exports produced while reviewing should stay local under the ignored `exports/` directory.
