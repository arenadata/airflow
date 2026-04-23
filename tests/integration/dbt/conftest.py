from __future__ import annotations

import subprocess
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).parents[3] / "dbt_projects" / "demo"


def pytest_configure(config):
    """Run dbt compile to generate manifest.json before test collection"""
    manifest = DBT_PROJECT_DIR / "target" / "manifest.json"
    if manifest.exists():
        return

    subprocess.run(
        [
            "dbt", "compile",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROJECT_DIR),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
