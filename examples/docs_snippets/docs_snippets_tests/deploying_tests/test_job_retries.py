import os

from docs_snippets.deploying.job_retries import other_sample_sample_job, sample_job

from dagster._core.execution.plan.resume_retry import ReexecutionStrategy
from dagster._core.instance.ref import DagsterInstanceRef
from dagster._core.storage.tags import MAX_RETRIES_TAG, RETRY_STRATEGY_TAG


def test_tags():
    assert sample_job.tags[MAX_RETRIES_TAG] == "3"
    assert other_sample_sample_job.tags[MAX_RETRIES_TAG] == "3"
    assert (
        other_sample_sample_job.tags[RETRY_STRATEGY_TAG]
        == ReexecutionStrategy.ALL_STEPS.value
    )


def test_instance(docs_snippets_folder):
    ref = DagsterInstanceRef.from_dir(
        os.path.join(docs_snippets_folder, "deploying", "dagster_instance")
    )

    assert ref.settings["run_retries"]["enabled"] is True
    assert ref.settings["run_retries"]["max_retries"] == 3
