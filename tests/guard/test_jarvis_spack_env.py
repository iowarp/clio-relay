from __future__ import annotations

from clio_relay.jarvis_submit import (
    INSTALL_ENV_ENV,
    SITE_SCRIPT,
    SPACK_ENV_ENV,
    site_environment,
    steps_digest,
)
from clio_relay.jobs.spec import JobSpec


def test_every_pipeline_op_shares_one_spack_env_dir_per_set_of_archives() -> None:
    # JARVIS keeps its spack load capture in JARVIS_SPACK_ENV_DIR and reuses it in check
    # mode, so check, submit, install and run of the same packages must name one dir.
    package = {"name": "demo.hello", "version": "1", "sha256": "b" * 64}
    spec = JobSpec.model_validate({"package": package})
    other = JobSpec.model_validate({"package": {**package, "parameters": {"n": 3}}})
    elsewhere = JobSpec.model_validate({"package": {**package, "sha256": "d" * 64}})
    home = "/home/me/.clio-relay"
    kept = site_environment(spec, home)[SPACK_ENV_ENV]
    assert kept == f"{home}/jarvis-cd/pipelines/{steps_digest(spec)}/spack-env"
    assert site_environment(other, home)[SPACK_ENV_ENV] == kept
    assert site_environment(elsewhere, home)[SPACK_ENV_ENV] != kept
    assert site_environment(spec, None) == {}
    export = SITE_SCRIPT.index('export JARVIS_SPACK_ENV_DIR="$spack_env"')
    for op in ("ppl install yaml", "ppl run yaml", "ppl check yaml", "ppl submit"):
        assert SITE_SCRIPT.index(op) > export, op


def test_pip_and_conda_envs_outlive_the_per_job_jarvis_root() -> None:
    # Each job has its own JARVIS root, so the install job's venv or conda env is kept
    # beside the Spack env, where the run finds it.
    package = {"name": "demo.tool", "version": "1", "sha256": "c" * 64}
    spec = JobSpec.model_validate({"package": package})
    home = "/home/me/.clio-relay"
    envs = site_environment(spec, home)
    assert envs[INSTALL_ENV_ENV] == f"{home}/jarvis-cd/pipelines/{steps_digest(spec)}/install-envs"
    export = SITE_SCRIPT.index('export JARVIS_INSTALL_ENV_DIR="$install_env"')
    for op in ("ppl install yaml", "ppl run yaml", "ppl check yaml", "ppl submit"):
        assert SITE_SCRIPT.index(op) > export, op
