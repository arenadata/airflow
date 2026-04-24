"""
Example: dbt-af (Toloka) integration with Apache Airflow

dbt-af auto-generates Airflow DAGs from a dbt manifest.json
It parses the dbt project graph, creates domain-based DAGs with proper
scheduling, cross-DAG sensors, and test grouping

Requirements:
    pip install dbt-af dbt-postgres

Key dbt-af features demonstrated:
    - Domain-based DAG separation (staging, intermediate, marts - separate DAGs)
    - Cross-schedule dependencies (daily - hourly - daily via ExternalTaskSensor)
    - Singular test tasks (not_null checks as inline tasks)
    - Snapshot support (customers_snapshot_ts)
    - Backfill DAGs (auto-generated alongside scheduled DAGs)
    - Manual "run single model" DAG with Airflow UI params
    - Source freshness sensors (for sources with freshness config)
    - dry_run mode (skips actual dbt execution, disables catchup)

IMPORTANT — dbt-af project structure constraints:
    - Schedule values MUST use @ prefix: "@daily", "@hourly", "@weekly"
      (without @ prefix, dbt-af silently falls back to @daily)
    - Generic tests (unique/not_null in schema.yml) are INCOMPATIBLE with dbt-af
      due to fqn[3] IndexError. Use singular tests in tests/dbt/models/<domain>/ instead.
    - Every model config MUST have: sql_cluster, py_cluster, daily_sql_cluster, bf_cluster

Running tests:
    pip install dbt-af dbt-postgres
    pytest tests/integration/dbt/test_dbt_af_integration.py -v
"""

from pathlib import Path

from dbt_af.conf import Config, DbtDefaultTargetsConfig, DbtProjectConfig
from dbt_af.dags import compile_dbt_af_dags

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "dbt_projects" / "demo"

config = Config(
    dbt_project=DbtProjectConfig(
        dbt_project_name="dbt_af_demo",
        dbt_models_path=DBT_PROJECT_DIR / "models",
        dbt_project_path=DBT_PROJECT_DIR,
        dbt_profiles_path=DBT_PROJECT_DIR,
        dbt_target_path=DBT_PROJECT_DIR / "target",
        dbt_log_path=DBT_PROJECT_DIR / "logs",
        dbt_schema="public",
    ),
    dbt_default_targets=DbtDefaultTargetsConfig(
        default_target="dev",
    ),
    max_active_dag_runs=1,
    include_single_model_manual_dag=True,
    debug_mode_enabled=False,
    dry_run=True,
)

dags = compile_dbt_af_dags(
    manifest_path=str(DBT_PROJECT_DIR / "target" / "manifest.json"),
    config=config,
)

for dag_name, dag in dags.items():
    globals()[dag_name] = dag
