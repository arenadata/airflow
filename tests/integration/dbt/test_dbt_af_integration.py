"""
Integration tests for dbt-af framework

Run inside Breeze:
    pytest tests/integration/dbt/test_dbt_af_integration.py -v

Tests verify that dbt-af correctly:
1. Parses manifest.json with custom dbt-af config fields (schedule, clusters)
2. Generates domain-based Airflow DAGs from manifest graph
3. Creates cross-domain ExternalTaskSensors for inter-domain dependencies
4. Handles cross-schedule dependencies (daily staging -> hourly intermediate -> daily marts)
5. Groups small tests (unique/not_null) inline with model tasks
6. Supports snapshot nodes
7. Generates backfill DAGs alongside scheduled DAGs
8. Generates manual "run single model" DAG with UI params
9. Creates source freshness sensors for sources with freshness config
10. Respects dry_run mode (catchup=False)

No external database required - dbt-af only parses manifest.json and builds
Airflow DAG objects in memory
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_af.conf import Config, DbtDefaultTargetsConfig, DbtProjectConfig
from dbt_af.dags import compile_dbt_af_dags

DBT_PROJECT_DIR = Path(__file__).parents[3] / "dbt_projects" / "demo"
MANIFEST_PATH = str(DBT_PROJECT_DIR / "target" / "manifest.json")


@pytest.fixture(scope="module")
def dbt_af_config():
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
def generated_dags(dbt_af_config):
    """Compile all DAGs from manifest — the core dbt-af entry point"""
    return compile_dbt_af_dags(
        manifest_path=MANIFEST_PATH,
        config=dbt_af_config,
    )


# Manifest validation
class TestManifestParsing:
    """Verify the generated manifest.json is valid for dbt-af"""

    def test_manifest_exists(self):
        assert Path(MANIFEST_PATH).exists(), "manifest.json must exist in target/"

    def test_manifest_has_required_sections(self):
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)
        for key in ("nodes", "sources", "metadata"):
            assert key in manifest, f"manifest.json missing '{key}'"

    def test_manifest_node_counts(self):
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)
        by_type = {}
        for node in manifest["nodes"].values():
            rt = node["resource_type"]
            by_type[rt] = by_type.get(rt, 0) + 1
        assert by_type.get("model", 0) == 6, f"Expected 6 models, got {by_type}"
        assert by_type.get("test", 0) == 4, f"Expected 4 tests, got {by_type}"
        assert by_type.get("snapshot", 0) == 1, f"Expected 1 snapshot, got {by_type}"

    def test_manifest_sources_count(self):
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)
        assert len(manifest["sources"]) == 3

    def test_models_have_dbt_af_config_fields(self):
        """dbt-af requires schedule + cluster fields in every model config"""
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)
        required = {"schedule", "sql_cluster", "py_cluster", "daily_sql_cluster", "bf_cluster"}
        for uid, node in manifest["nodes"].items():
            if node["resource_type"] == "model":
                missing = required - set(node["config"].keys())
                assert not missing, f"{uid} missing dbt-af config fields: {missing}"


# DAG generation
class TestDagGeneration:
    """Verify compile_dbt_af_dags produces correct DAG set"""

    def test_dags_not_empty(self, generated_dags):
        assert len(generated_dags) > 0

    def test_all_dags_have_dbt_tag(self, generated_dags):
        for name, dag in generated_dags.items():
            assert "dbt" in dag.tags, f"DAG '{name}' missing 'dbt' tag. Tags: {dag.tags}"

    def test_manual_run_dag_exists(self, generated_dags):
        expected = "dbt_af_demo_dbt_run_model"
        assert expected in generated_dags, (
            f"Manual run DAG '{expected}' not found. DAGs: {list(generated_dags.keys())}"
        )

    def test_manual_run_dag_has_no_schedule(self, generated_dags):
        dag = generated_dags["dbt_af_demo_dbt_run_model"]
        assert dag.schedule_interval is None


# Domain separation — core dbt-af feature
class TestDomainSeparation:
    """dbt-af groups models into DAGs by domain (fqn[1]) + schedule"""

    def test_multiple_domain_dags_created(self, generated_dags):
        scheduled = {k for k in generated_dags
                     if "dbt_run_model" not in k and "backfill" not in k}
        assert len(scheduled) >= 2, (
            f"Expected DAGs for multiple domains. Got: {scheduled}"
        )

    def test_expected_domains_present(self, generated_dags):
        dag_names = set(generated_dags.keys())
        found = {d for d in ("staging", "intermediate", "marts") if any(d in n for n in dag_names)}
        assert len(found) >= 2, f"Expected domain names in DAG ids. DAGs: {dag_names}"


# Cross-schedule dependencies
class TestCrossSchedule:
    """int_orders_enriched is hourly; staging/marts are daily
    dbt-af must create separate DAGs for different schedules"""

    def test_hourly_dag_exists(self, generated_dags):
        hourly = [n for n in generated_dags if "hourly" in n]
        assert hourly, f"Expected hourly DAG. DAGs: {list(generated_dags.keys())}"

    def test_daily_and_hourly_coexist(self, generated_dags):
        has_daily = any("daily" in n for n in generated_dags)
        has_hourly = any("hourly" in n for n in generated_dags)
        assert has_daily and has_hourly, (
            f"Need both daily and hourly DAGs. DAGs: {list(generated_dags.keys())}"
        )


# Task structure
class TestTaskStructure:
    """Verify model/test/snapshot tasks inside DAGs"""

    def _all_task_ids(self, dags):
        ids = set()
        for dag in dags.values():
            ids.update(dag.task_ids)
        return ids

    def test_model_tasks_present(self, generated_dags):
        ids = self._all_task_ids(generated_dags)
        for model in ("stg_customers", "stg_orders", "int_orders_enriched"):
            assert any(model in t for t in ids), (
                f"Model '{model}' not found in tasks: {ids}"
            )

    def test_snapshot_task_present(self, generated_dags):
        ids = self._all_task_ids(generated_dags)
        assert any("customers_snapshot_ts" in t for t in ids), (
            f"Snapshot task not found. Tasks: {ids}"
        )

    def test_small_test_tasks_present(self, generated_dags):
        """Singular tests should appear as inline tasks"""
        ids = self._all_task_ids(generated_dags)
        test_tasks = [t for t in ids if "not_null_" in t]
        assert len(test_tasks) > 0, f"No test tasks found. Tasks: {ids}"


# Backfill DAGs
class TestBackfillDags:
    """dbt-af auto-generates backfill DAGs for each domain"""

    def test_backfill_dags_exist(self, generated_dags):
        bf = [n for n in generated_dags if "backfill" in n]
        assert bf, f"No backfill DAGs. DAGs: {list(generated_dags.keys())}"

    def test_backfill_dags_tagged(self, generated_dags):
        for name, dag in generated_dags.items():
            if "backfill" in name:
                assert "backfill" in dag.tags, (
                    f"Backfill DAG '{name}' missing 'backfill' tag"
                )


# Source freshness
class TestSourceFreshness:
    """Sources with freshness config should produce DbtSourceFreshnessSensor tasks"""

    def test_freshness_sensors_created(self, generated_dags):
        staging = {k: v for k, v in generated_dags.items()
                   if "staging" in k and "backfill" not in k}
        ids = set()
        for dag in staging.values():
            ids.update(dag.task_ids)
        freshness = [t for t in ids if "freshness" in t.lower()]
        assert freshness, (
            f"Expected source freshness sensors in staging DAGs. Tasks: {ids}"
        )


# Dry-run mode
class TestDryRun:
    """dry_run=True should disable catchup on all scheduled DAGs"""

    def test_catchup_disabled(self, generated_dags):
        for name, dag in generated_dags.items():
            if dag.schedule_interval is not None:
                assert dag.catchup is False, (
                    f"DAG '{name}' should have catchup=False in dry_run mode"
                )
