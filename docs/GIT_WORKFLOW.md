# Git Workflow for This Pipeline

This project uses a lightweight **trunk-based workflow with short-lived feature
branches**, adapted for a data pipeline rather than an application service.

## Branches

| Branch           | Purpose                                                            |
|-------------------|---------------------------------------------------------------------|
| `main`            | Always deployable. Every commit here has passed CI.                |
| `develop`         | Integration branch for the next release; optional for a small team, useful once multiple pipeline changes land in parallel. |
| `feature/<name>`  | One unit of pipeline work, e.g. `feature/add-rating-bucket`, `feature/kafka-retry-backoff`. Branched off `develop` (or `main`), merged back via PR. |
| `fix/<name>`      | Bug fixes, same lifecycle as `feature/*`.                          |
| `data/<name>`     | Changes that alter table schemas, bucketing thresholds, or category mappings — kept separate from code-only features so a schema change gets reviewed and rolled out deliberately (it affects data already loaded downstream). |

Why a dedicated `data/*` prefix: in application code, a bad merge is a bug fix
away. In a pipeline, a bad transformation rule (e.g. moving the "Premium"
price threshold) can silently reshape historical bronze/silver/gold rows or
require a backfill. Naming these branches distinctly makes them easy to spot
in PR lists and changelogs, and is a reminder to check whether a backfill or
migration note is needed before merging.

## Workflow

1. Branch from `develop` (or `main` for a single-branch setup):
   `git checkout -b feature/add-rating-bucket`
2. Commit small, focused changes. Keep transformation-logic changes and
   orchestration/config changes in separate commits where practical — it
   makes `git bisect` and rollback meaningful when a transformation starts
   producing unexpected buckets.
3. Push and open a pull request into `develop`/`main`.
4. CI (`.github/workflows/ci.yml`) runs the unit test suite automatically on
   the PR. **A PR may not merge with failing tests.**
5. Squash-merge once approved, delete the feature branch.
6. Periodically merge/fast-forward `develop` into `main` as a release; tag it
   (`git tag v1.2.0`) so a specific set of transformation rules can always be
   traced back to a commit — useful when someone asks "what rule classified
   this row as Premium last March?".

## Commit messages

Use a short imperative summary, e.g. `Fix rating bucket boundary at 4.5`, and
mention the affected stage (`extract`, `validate`, `transform`, `load`,
`kafka`) when it isn't obvious from the diff.

## Secrets

`.env` is gitignored. Never commit Databricks tokens or Kafka connection
strings — copy `.env.example` to `.env` locally and fill in real values, and
store the real values as CI/CD secrets (e.g. GitHub Actions repo secrets) for
any workflow that needs them.
