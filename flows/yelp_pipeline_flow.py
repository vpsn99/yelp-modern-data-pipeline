from pathlib import Path
import subprocess
from prefect import flow, task
from prefect.logging import get_run_logger
import sys
import shutil
from prefect import flow, task
from prefect.logging import get_run_logger


def dbt_cmd() -> str:
    # Prefer venv dbt if available
    exe = shutil.which("dbt")
    if exe:
        return exe
    # Fallback to Scripts/dbt.exe next to the running interpreter
    return str((Path(sys.executable).parent / "dbt.exe"))


@task
def run_incremental_ingestion():
    logger = get_run_logger()
    logger.info("Running incremental ingestion...")
    result = subprocess.run(
        [sys.executable, "pipelines/ingest/ingest_incremental_from_incoming.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("Ingestion failed")
    logger.info("Ingestion complete.")


@task
def run_dbt():
    logger = get_run_logger()
    logger.info("Running dbt models...")
    result = subprocess.run(
        [
            dbt_cmd(),
            "run",
            "--profiles-dir",
            "..",
            "--vars",
            '{"data_root": "../../../data"}',
        ],
        cwd="warehouse/dbt/yelp_dbt",
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt run failed")
    logger.info("dbt run complete.")


@task
def run_dbt_tests():
    logger = get_run_logger()
    logger.info("Running dbt tests...")
    result = subprocess.run(
        [
            dbt_cmd(),
            "test",
            "--profiles-dir",
            "..",
            "--vars",
            '{"data_root": "../../../data"}',
        ],
        cwd="warehouse/dbt/yelp_dbt",
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt tests failed")
    logger.info("dbt tests passed.")


@flow(name="yelp-modern-data-pipeline")
def yelp_pipeline_flow():
    run_incremental_ingestion()
    run_dbt()
    run_dbt_tests()


if __name__ == "__main__":
    yelp_pipeline_flow()