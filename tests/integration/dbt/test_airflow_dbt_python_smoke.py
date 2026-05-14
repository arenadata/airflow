"""
Smoke tests for airflow-dbt-python framework

airflow-dbt-python takes a fundamentally different approach from all other frameworks:
- It provides individual operators (DbtRunOperator, DbtTestOperator, etc)
- Users build DAGs manually by placing operators and setting dependencies
- No automatic DAG generation from manifest, user controls the structure
- dbt is invoked as a Python library (not CLI subprocess)
- Supports XCom push of dbt artifacts
- Can pull dbt projects from remote storage (S3, Git)
- Only supports Airflow 3.0+

Run inside Breeze (Airflow 3):
    pip install airflow-dbt-python dbt-postgres
    pytest tests/integration/dbt/test_airflow_dbt_python_smoke.py -v

Uses the same demo dbt project as other framework tests
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from airflow.models.dag import DAG

from airflow_dbt_python.operators.dbt import (
    DbtBuildOperator,
    DbtCompileOperator,
    DbtRunOperator,
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtSourceFreshnessOperator,
    DbtTestOperator,
)

DBT_PROJECT_DIR = Path(__file__).parent / "demo"


@pytest.fixture
def dag():
    return DAG(
        dag_id="test_airflow_dbt_python",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
    )


class TestOperatorsExist:
    """airflow-dbt-python provides one operator per dbt command"""

    def test_run_operator_instantiation(self, dag):
        """DbtRunOperator wraps 'dbt run' command"""
        op = DbtRunOperator(
            task_id="dbt_run",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            select=["stg_orders"],
            dag=dag,
        )
        assert op.command == "run"
        assert op.task_id == "dbt_run"

    def test_test_operator_instantiation(self, dag):
        """DbtTestOperator wraps 'dbt test' command"""
        op = DbtTestOperator(
            task_id="dbt_test",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            dag=dag,
        )
        assert op.command == "test"

    def test_seed_operator_instantiation(self, dag):
        """DbtSeedOperator wraps 'dbt seed' command"""
        op = DbtSeedOperator(
            task_id="dbt_seed",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            dag=dag,
        )
        assert op.command == "seed"

    def test_snapshot_operator_instantiation(self, dag):
        """DbtSnapshotOperator wraps 'dbt snapshot' command"""
        op = DbtSnapshotOperator(
            task_id="dbt_snapshot",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            dag=dag,
        )
        assert op.command == "snapshot"

    def test_build_operator_instantiation(self, dag):
        """DbtBuildOperator wraps 'dbt build' (run + test + seed + snapshot)"""
        op = DbtBuildOperator(
            task_id="dbt_build",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            dag=dag,
        )
        assert op.command == "build"

    def test_compile_operator_instantiation(self, dag):
        """DbtCompileOperator wraps 'dbt compile'"""
        op = DbtCompileOperator(
            task_id="dbt_compile",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            dag=dag,
        )
        assert op.command == "compile"

    def test_source_freshness_operator_instantiation(self, dag):
        """DbtSourceFreshnessOperator wraps 'dbt source freshness'"""
        op = DbtSourceFreshnessOperator(
            task_id="dbt_source",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            dag=dag,
        )
        assert op.command == "source"


class TestManualDagConstruction:
    """Unlike other frameworks, users build DAGs manually with operators"""

    def test_build_dag_with_dependencies(self, dag):
        """User defines task dependencies explicitly, no auto-generation from manifest"""
        with dag:
            seed = DbtSeedOperator(
                task_id="seed",
                project_dir=str(DBT_PROJECT_DIR),
                profiles_dir=str(DBT_PROJECT_DIR),
            )
            run = DbtRunOperator(
                task_id="run",
                project_dir=str(DBT_PROJECT_DIR),
                profiles_dir=str(DBT_PROJECT_DIR),
                select=["stg_orders", "stg_customers"],
            )
            test = DbtTestOperator(
                task_id="test",
                project_dir=str(DBT_PROJECT_DIR),
                profiles_dir=str(DBT_PROJECT_DIR),
            )
            seed >> run >> test

        assert "seed" in dag.task_ids
        assert "run" in dag.task_ids
        assert "test" in dag.task_ids
        # Verify dependency chain
        assert "run" in dag.get_task("seed").downstream_task_ids
        assert "test" in dag.get_task("run").downstream_task_ids


class TestOperatorFeatures:
    """airflow-dbt-python operators support dbt CLI arguments as parameters"""

    def test_select_parameter(self, dag):
        """Operators accept select parameter for model filtering"""
        op = DbtRunOperator(
            task_id="run_selected",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            select=["stg_orders", "stg_customers"],
            dag=dag,
        )
        assert op.select == ["stg_orders", "stg_customers"]

    def test_exclude_parameter(self, dag):
        """Operators accept exclude parameter"""
        op = DbtRunOperator(
            task_id="run_excluded",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            exclude=["stg_orders"],
            dag=dag,
        )
        assert op.exclude == ["stg_orders"]

    def test_full_refresh_parameter(self, dag):
        """Operators accept full_refresh for incremental model rebuilds"""
        op = DbtRunOperator(
            task_id="run_full_refresh",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            full_refresh=True,
            dag=dag,
        )
        assert op.full_refresh is True

    def test_vars_parameter(self, dag):
        """Operators accept vars for passing variables to dbt"""
        op = DbtRunOperator(
            task_id="run_with_vars",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            vars={"start_date": "2024-01-01", "end_date": "2024-01-31"},
            dag=dag,
        )
        assert op.vars == {"start_date": "2024-01-01", "end_date": "2024-01-31"}

    def test_target_override(self, dag):
        """Operators accept target to override profiles.yml default"""
        op = DbtRunOperator(
            task_id="run_prod",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            target="prod",
            dag=dag,
        )
        assert op.target == "prod"


class TestXComSupport:
    """airflow-dbt-python can push dbt artifacts to XCom"""

    def test_xcom_push_artifacts_config(self, dag):
        """do_xcom_push_artifacts specifies which artifacts to push to XCom."""
        op = DbtRunOperator(
            task_id="run_with_xcom",
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
            do_xcom_push_artifacts=["run_results.json", "manifest.json"],
            dag=dag,
        )
        assert op.do_xcom_push_artifacts == ["run_results.json", "manifest.json"]
        # write_json should be auto-enabled when pushing run_results.json
        assert op.write_json is True


class TestAirflowConnectionAsTarget:
    """airflow-dbt-python can use Airflow connections instead of profiles.yml"""

    def test_dbt_conn_id_parameter(self, dag):
        """dbt_conn_id allows using an Airflow connection as dbt target"""
        op = DbtRunOperator(
            task_id="run_with_conn",
            project_dir=str(DBT_PROJECT_DIR),
            dbt_conn_id="my_postgres_conn",
            dag=dag,
        )
        assert op.dbt_conn_id == "my_postgres_conn"
        # profiles_dir not needed when using connection
        assert op.profiles_dir is None


class TestRemoteProjectSupport:
    """airflow-dbt-python can pull dbt projects from remote storage"""

    def test_s3_project_dir(self, dag):
        """project_dir accepts S3 URLs, project is downloaded before execution"""
        op = DbtRunOperator(
            task_id="run_from_s3",
            project_dir="s3://my-bucket/dbt-project/",
            profiles_dir="s3://my-bucket/dbt-project/",
            dag=dag,
        )
        assert op.project_dir == "s3://my-bucket/dbt-project/"

    def test_git_project_dir(self, dag):
        """project_dir accepts Git URLs, project is cloned before execution"""
        op = DbtRunOperator(
            task_id="run_from_git",
            project_dir="https://github.com/org/dbt-project.git",
            dag=dag,
        )
        assert op.project_dir == "https://github.com/org/dbt-project.git"
