"""
Integration tests for astronomer-cosmos framework

Cosmos takes a fundamentally different approach from dbt-af/dmp-af:
- One DAG with all models as tasks (not domain-based splitting)
- Works with any standard dbt project (no custom config fields required)
- Multiple execution modes (local, docker, kubernetes, etc)
- Profile can be generated from Airflow connections (but profiles.yml using is also possible)
- Supports dbt build command (run + test together)
- Generic tests work out of the box (no fqn issues)
- Multiple ways to load the dbt graph (manifest, dbt ls, automatic)

Run inside Breeze:
    pip install astronomer-cosmos dbt-postgres
    pytest tests/integration/dbt/test_cosmos_integration.py -v

Uses the same demo dbt project as dbt-af/dmp-af tests
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from airflow.models.dag import DAG

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from cosmos.converter import DbtToAirflowConverter

DBT_PROJECT_DIR = Path(__file__).parent / "demo"
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


@pytest.fixture
def profile_config():
    """Profile config using profiles.yml from the demo project"""
    return ProfileConfig(
        profile_name="dbt_af_demo",
        target_name="dev",
        profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
    )


@pytest.fixture
def project_config():
    return ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)


@pytest.fixture
def project_config_with_manifest():
    return ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
    )


class TestCosmosDagGeneration:
    """Cosmos creates a DAG with all dbt models as Airflow tasks."""

    def test_dag_has_tasks(self, project_config_with_manifest, profile_config):
        """DbtToAirflowConverter populates a DAG with tasks from manifest."""
        with DAG("test_cosmos_dag", start_date=datetime(2024, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    test_behavior=TestBehavior.NONE,
                ),
            )
        assert len(dag.task_ids) > 0, f"DAG should have tasks. Got: {dag.task_ids}"

    def test_model_tasks_present(self, project_config_with_manifest, profile_config):
        """Each dbt model becomes an Airflow task within the DAG"""
        with DAG("test_cosmos_models", start_date=datetime(2024, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    test_behavior=TestBehavior.NONE,
                ),
            )
        task_ids = dag.task_ids
        for model in ("stg_customers", "stg_orders", "int_orders_enriched", "orders", "customers"):
            assert any(model in tid for tid in task_ids), (
                f"Model '{model}' not found in tasks: {task_ids}"
            )

    def test_snapshot_task_present(self, project_config_with_manifest, profile_config):
        """Snapshots are included as tasks in the DAG"""
        with DAG("test_cosmos_snapshot", start_date=datetime(2024, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    test_behavior=TestBehavior.NONE,
                ),
            )
        task_ids = dag.task_ids
        assert any("customers_snapshot_ts" in tid for tid in task_ids), (
            f"Snapshot task not found. Tasks: {task_ids}"
        )


class TestCosmosTestBehavior:
    """Cosmos supports different strategies for running dbt tests"""

    def test_after_each_creates_test_tasks(self, project_config_with_manifest, profile_config):
        """AFTER_EACH: test tasks run immediately after their parent model"""
        with DAG("test_cosmos_after_each", start_date=datetime(2024, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    test_behavior=TestBehavior.AFTER_EACH,
                ),
            )
        task_ids = dag.task_ids
        test_tasks = [tid for tid in task_ids if "test" in tid.lower()]
        assert len(test_tasks) > 0, f"Expected test tasks with AFTER_EACH. Tasks: {task_ids}"

    def test_none_skips_tests(self, project_config_with_manifest, profile_config):
        """NONE: no test tasks are created."""
        with DAG("test_cosmos_none", start_date=datetime(2024, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    test_behavior=TestBehavior.NONE,
                ),
            )
        task_ids = dag.task_ids
        test_tasks = [tid for tid in task_ids if "test" in tid.lower()]
        assert len(test_tasks) == 0, f"Expected no test tasks with NONE. Got: {test_tasks}"


class TestCosmosTaskGroup:
    """DbtTaskGroup embeds dbt models as a TaskGroup inside an existing DAG"""

    def test_task_group_in_existing_dag(self, project_config_with_manifest, profile_config):
        """dbt project can be embedded as a TaskGroup within a larger DAG"""
        with DAG("test_cosmos_task_group", start_date=datetime(2024, 1, 1)) as dag:
            DbtTaskGroup(
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    test_behavior=TestBehavior.NONE,
                ),
            )
        assert len(dag.task_ids) > 0, f"TaskGroup should add tasks to DAG. Got: {dag.task_ids}"


class TestCosmosModelSelection:
    """Cosmos supports dbt-style select/exclude for filtering models"""

    def test_select_specific_models(self, project_config_with_manifest, profile_config):
        """select parameter filters which models are included in the DAG"""
        with DAG("test_cosmos_select", start_date=datetime(2024, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    select=["stg_customers", "stg_orders"],
                    test_behavior=TestBehavior.NONE,
                ),
            )
        task_ids = dag.task_ids
        assert any("stg_" in tid for tid in task_ids), f"Expected staging models. Tasks: {task_ids}"
        assert not any("revenue_by_region" in tid for tid in task_ids), (
            f"Should not have marts models when selecting staging only. Tasks: {task_ids}"
        )

    def test_exclude_models(self, project_config_with_manifest, profile_config):
        """exclude parameter removes specific models from the DAG"""
        with DAG("test_cosmos_exclude", start_date=datetime(2024, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    exclude=["stg_customers", "stg_orders"],
                    test_behavior=TestBehavior.NONE,
                ),
            )
        task_ids = dag.task_ids
        assert not any("stg_" in tid for tid in task_ids), (
            f"Should not have staging models when excluding them. Tasks: {task_ids}"
        )
        # Other models should still be present
        assert any("int_orders_enriched" in tid for tid in task_ids), (
            f"Non-excluded models should be present. Tasks: {task_ids}"
        )


class TestCosmosNoCustomConfigRequired:
    """Unlike dbt-af/dmp-af, cosmos works with any standard dbt project"""

    def test_all_models_in_one_dag(self, project_config_with_manifest, profile_config):
        """Cosmos does not require 'schedule' in model config, all models go into one DAG
        regardless of any dbt-af-specific config fields presented in schema.yml"""
        with DAG("test_cosmos_no_config", start_date=datetime(2024, 1, 1)) as dag:
            DbtToAirflowConverter(
                dag=dag,
                project_config=project_config_with_manifest,
                profile_config=profile_config,
                execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
                render_config=RenderConfig(
                    load_method=LoadMode.DBT_MANIFEST,
                    test_behavior=TestBehavior.NONE,
                ),
            )
        task_ids = dag.task_ids
        # All models from all domains in one DAG
        assert any("stg_orders" in tid for tid in task_ids)
        assert any("int_orders_enriched" in tid for tid in task_ids)
        assert any("orders" in tid for tid in task_ids)
