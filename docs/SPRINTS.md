# Sprint checkpoints and retrospectives

The project was completed in a pair of workstreams organized here as
one-week sprint plans. These notes summarize work already completed and
link to its evidence. They do not establish calendar sprint dates or
independent reviewers for this solo project.

## Sprint 1 · Pipeline and quality

[GitHub milestone](https://github.com/Bduc07/claims-pipeline/milestone/1)

| Issue | Acceptance criteria | Evidence |
| --- | --- | --- |
| [Package bronze, silver, and gold](https://github.com/Bduc07/claims-pipeline/issues/2) | Bronze skips completed files. Silver keeps resends out of reporting. The SQL builds and reports on both gold tables. | [Bronze and reconciliation CI](https://github.com/Bduc07/claims-pipeline/actions/runs/44545762426) |
| [Add configurable quality checks](https://github.com/Bduc07/claims-pipeline/issues/3) | YAML controls severity, eligibility checks coverage dates, and reconciliation and answer-key tests verify outputs. | [Claim rule CI](https://github.com/Bduc07/claims-pipeline/actions/runs/44551775595) |

**Retrospective.** The answer key helped confirm the right rows were
quarantined. Move verification into CI earlier to avoid repeating the
same Colab uploads.

## Sprint 2 · Operations and delivery

[GitHub milestone](https://github.com/Bduc07/claims-pipeline/milestone/2)

| Issue | Acceptance criteria | Evidence |
| --- | --- | --- |
| [Add support and security controls](https://github.com/Bduc07/claims-pipeline/issues/4) | Record run outcomes, exercise three failure drills, mask patient IDs, and define loader and reporter grants. | [Runbook](../RUNBOOK.md), [security checks](https://github.com/Bduc07/claims-pipeline/actions/runs/44555402759) |
| [Verify orchestration and document the demo](https://github.com/Bduc07/claims-pipeline/issues/5) | Run the Airflow DAG and CLI in Docker, confirm the weekly report, and document the Snowflake handoff and AWS plan. | [Airflow CI](https://github.com/Bduc07/claims-pipeline/actions/runs/44560630335), [demo guide](DEMO.md) |

**Retrospective.** Grouping changes into one CI checkpoint reduced manual
work. A future iteration could automate the Snowflake handoff and test
the AWS deployment.

The [portfolio pull request](https://github.com/Bduc07/claims-pipeline/pull/1)
contains the implementation and CI history.
