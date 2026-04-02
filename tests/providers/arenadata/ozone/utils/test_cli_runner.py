from __future__ import annotations

import subprocess

import pytest

from airflow.providers.arenadata.ozone.utils.cli_runner import CliRunner, OzoneCliRunner
from airflow.providers.arenadata.ozone.utils.errors import OzoneCliError


def test_clirunner_run_process_retries_and_returns_result(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}

    def fake_run(
        _cls,
        command: list[str],
        *,
        env_overrides=None,
        timeout=300,
        input_text=None,
        cwd=None,
        check=True,
        log_output=False,
    ) -> subprocess.CompletedProcess[str]:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=command,
                output="",
                stderr="temporary failure",
            )
        return subprocess.CompletedProcess(args=command, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(CliRunner, "run", classmethod(fake_run))

    result = CliRunner.run_process(
        ["ozone", "sh", "volume", "list", "/"],
        retry_attempts=2,
    )
    assert result.returncode == 0
    assert result.stdout == "ok"
    assert attempts["count"] == 2


def test_ozoneclirunner_run_ozone_retries_retryable_and_not_non_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retryable_attempts = {"count": 0}

    def fake_run_retryable(
        _cls,
        command: list[str],
        *,
        env_overrides=None,
        timeout=300,
        input_text=None,
        cwd=None,
        check=True,
        log_output=False,
    ) -> subprocess.CompletedProcess[str]:
        retryable_attempts["count"] += 1
        if retryable_attempts["count"] == 1:
            raise subprocess.CalledProcessError(
                returncode=255,
                cmd=command,
                output="",
                stderr="connection reset by peer",
            )
        return subprocess.CompletedProcess(args=command, returncode=0, stdout="done", stderr="")

    monkeypatch.setattr(OzoneCliRunner, "run", classmethod(fake_run_retryable))
    result = OzoneCliRunner.run_ozone(["ozone", "sh", "bucket", "list", "/vol"], retry_attempts=2)
    assert result.returncode == 0
    assert retryable_attempts["count"] == 2

    non_retryable_attempts = {"count": 0}

    def fake_run_non_retryable(
        _cls,
        command: list[str],
        *,
        env_overrides=None,
        timeout=300,
        input_text=None,
        cwd=None,
        check=True,
        log_output=False,
    ) -> subprocess.CompletedProcess[str]:
        non_retryable_attempts["count"] += 1
        raise subprocess.CalledProcessError(
            returncode=1,
            cmd=command,
            output="",
            stderr="ACCESS_DENIED",
        )

    monkeypatch.setattr(OzoneCliRunner, "run", classmethod(fake_run_non_retryable))
    with pytest.raises(OzoneCliError, match="non-retryable"):
        OzoneCliRunner.run_ozone(
            ["ozone", "sh", "bucket", "create", "/vol/bkt"],
            retry_attempts=2,
        )
    assert non_retryable_attempts["count"] == 1
