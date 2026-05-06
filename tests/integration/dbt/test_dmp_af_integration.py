"""
Integration tests for dmp-af framework

dmp-af is a fork of dbt-af (Toloka) by dmp-labs with Airflow 3.x support added
Uses the same demo dbt project as dbt-af tests

Run inside Breeze:
    pip install /path/to/dmp-af dbt-postgres
    pytest tests/integration/dbt/test_dmp_af_integration.py -v

dbt compile runs automatically via conftest.py if manifest.json is missing
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dmp_af.conf import Config, DbtDefaultTargetsConfig, DbtProjectConfig
from dmp_af.dags import compile_dmp_af_dags

DBT_PROJECT_DIR = Path(__file__).parent / "demo"
MANIFEST_PATH = str(DBT_PROJECT_DIR / "target" / "manifest.json")


@pytest.fixture(scope="module")
def dmp_af_config():
    return Config(
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


@pytest.fixture(scope="module")
def generated_dags(dmp_af_config):
    """Compile all DAGs from manifest, the core dmp-af entry point"""
    return compile_dmp_af_dags(
        manifest_path=MANIFEST_PATH,
        config=dmp_af_config,
    )


class TestDmpAfDagGeneration:
    """Verify compile_dmp_af_dags produces correct DAG set"""

    def test_dags_not_empty(self, generated_dags):
        """compile_dmp_af_dags must produce at least one DAG from the manifest"""
        assert len(generated_dags) > 0

    def test_all_dags_have_dbt_tag(self, generated_dags):
        """dmp-af tags every generated DAG with 'dbt' for filtering in Airflow UI"""
        for name, dag in generated_dags.items():
            assert "dbt" in dag.tags, f"DAG '{name}' missing 'dbt' tag. Tags: {dag.tags}"

    def test_manual_run_dag_exists(self, generated_dags):
        """When include_single_model_manual_dag=True, dmp-af creates a special DAG
        ('{project}_dbt_run_model') with no schedule. It allows running any single
        model on-demand from Airflow UI with custom params (model name, date range)"""
        expected = "dbt_af_demo_dbt_run_model"
        assert expected in generated_dags, (
            f"Manual run DAG '{expected}' not found. DAGs: {list(generated_dags.keys())}"
        )

    def test_manual_run_dag_has_no_schedule(self, generated_dags):
        """Manual run DAG must have no schedule, it's triggered manually from UI"""
        dag = generated_dags["dbt_af_demo_dbt_run_model"]
        # Airflow 3 removed schedule_interval in favor of schedule
        schedule = getattr(dag, 'schedule_interval', None) if hasattr(dag, 'schedule_interval') else dag.schedule
        assert schedule is None


class TestDmpAfDomainSeparation:
    """dmp-af groups models into DAGs by domain (fqn[1]) + schedule"""

    def test_expected_domain_dags_created(self, generated_dags):
        """Each domain + schedule combination produces a separate DAG
        Our demo has 3 domains: staging(@daily), intermediate(@hourly), marts(@daily)"""
        expected_dags = {"staging__daily", "intermediate__hourly", "marts__daily"}
        missing = expected_dags - set(generated_dags.keys())
        assert not missing, (
            f"Missing domain DAGs: {missing}. Got: {list(generated_dags.keys())}"
        )


class TestDmpAfTaskStructure:
    """Verify model/test/snapshot tasks inside DAGs"""

    def _all_task_ids(self, dags):
        ids = set()
        for dag in dags.values():
            ids.update(dag.task_ids)
        return ids

    def test_model_tasks_present(self, generated_dags):
        """Each dbt model produces at least one Airflow task (may appear in multiple
        DAGs, scheduled + backfill, and may be wrapped in a TaskGroup with tests)"""
        ids = self._all_task_ids(generated_dags)
        for model in ("stg_customers", "stg_orders", "int_orders_enriched"):
            assert any(model in t for t in ids), (
                f"Model '{model}' not found in tasks: {ids}"
            )

    def test_snapshot_task_present(self, generated_dags):
        """dbt snapshots (SCD Type 2) are mapped to DbtSnapshot operator tasks"""
        ids = self._all_task_ids(generated_dags)
        assert any("customers_snapshot_ts" in t for t in ids), (
            f"Snapshot task not found. Tasks: {ids}"
        )

    def test_small_test_tasks_present(self, generated_dags):
        """Singular tests (tests/dbt/models/<domain>/*.sql) appear as inline
        tasks within the model's task group, executed after the model runs"""
        ids = self._all_task_ids(generated_dags)
        test_tasks = [t for t in ids if "not_null_" in t]
        assert len(test_tasks) > 0, f"No test tasks found. Tasks: {ids}"


class TestDmpAfBackfillDags:
    """dmp-af auto-generates backfill DAGs for each domain"""

    def test_backfill_dags_exist(self, generated_dags):
        """dmp-af creates a backfill DAG for each domain to rerun historical data"""
        bf = [n for n in generated_dags if "backfill" in n]
        assert bf, f"No backfill DAGs. DAGs: {list(generated_dags.keys())}"

    def test_backfill_dags_tagged(self, generated_dags):
        """Backfill DAGs are tagged for easy filtering in Airflow UI"""
        for name, dag in generated_dags.items():
            if "backfill" in name:
                assert "backfill" in dag.tags, (
                    f"Backfill DAG '{name}' missing 'backfill' tag"
                )


class TestDmpAfSourceFreshness:
    """Sources with freshness config should produce DbtSourceFreshnessSensor tasks"""

    def test_freshness_sensors_created(self, generated_dags):
        """Sources with freshness config (warn_after/error_after in sources.yml)
        produce DbtSourceFreshnessSensor tasks that check loaded_at field
        before allowing downstream models to run"""
        staging = {k: v for k, v in generated_dags.items()
                   if "staging" in k and "backfill" not in k}
        ids = set()
        for dag in staging.values():
            ids.update(dag.task_ids)
        freshness = [t for t in ids if "freshness" in t.lower()]
        assert freshness, (
            f"Expected source freshness sensors in staging DAGs. Tasks: {ids}"
        )


class TestDmpAfDryRun:
    """dry_run=True should disable catchup on all scheduled DAGs"""

    def test_catchup_disabled(self, generated_dags):
        """dry_run=True skips actual dbt execution and disables catchup
        so Airflow won't try to backfill missed runs on first deploy"""
        for name, dag in generated_dags.items():
            # Airflow 3 removed schedule_interval in favor of schedule
            schedule = getattr(dag, 'schedule_interval', None) if hasattr(dag, 'schedule_interval') else dag.schedule
            if schedule is not None:
                assert dag.catchup is False, (
                    f"DAG '{name}' should have catchup=False in dry_run mode"
                )
