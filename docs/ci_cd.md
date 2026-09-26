# CI and DEV validation

## Workflow boundaries

GitHub Actions provides two workflows. `CI` runs on pushes to `main`, pull requests targeting `main`, and manual dispatch. All four CI jobs are credential-free and independent, so they can run in parallel, including on fork pull requests. A newer run for the same branch or pull request cancels an older CI run. Both workflows grant only `contents: read` to the GitHub token.

| Job | Runner | Validation |
| --- | --- | --- |
| `python-validation` | Windows, Python 3.13.7 | Install `requirements.lock`, check dependency health, run Ruff, and run the full pytest suite, including the Airflow DAG unit tests. |
| `dbt-parse` | Windows, Python 3.13.7 | Install the validated lock and parse the dbt project with a temporary DEV profile containing placeholder, non-secret connection values. |
| `docker-airflow-validation` | Ubuntu | Check Compose interpolation with placeholders, build the Airflow image, and load DAGs with Airflow's `DagBag` parser in the image. It does not start services. |
| `repository-secrets` | Ubuntu | Reject tracked local `.env` variants, private-key files, and private-key blocks while allowing `.env.example` templates. |

The Windows jobs use `setup-python`'s pip cache keyed by `requirements.lock`. This keeps the committed lock on the operating system and Python version for which it was validated. Job timeouts bound stalled installations and builds. A failed check fails its job; the other independent jobs still report their results. No CI job deploys or refreshes warehouse objects.

`dbt deps` is unnecessary because the dbt project has no `packages.yml` or external dbt packages. `dbt parse` validates project structure without a Snowflake connection. `dbt compile` still opens a Snowflake connection for this project, even with `--no-introspect`, and dbt tests query existing DEV relations. Both therefore belong to the separate live workflow.

## Manual Snowflake DEV checks

`dbt DEV validation` runs only by manual dispatch from `main`. Its single job uses the `supply-chain-dev` GitHub Environment. Configure that environment with required reviewers and restrict deployment branches to `main` before enabling live runs. The job validates required secrets and an approved DEV role, creates a temporary key file with owner-only permissions, fixes its profile to `SUPPLY_CHAIN_DEV`, `SUPPLY_CHAIN_DEV_WH`, and `STAGING`, then runs `dbt compile` and `dbt test`. It deletes the temporary profile and key afterward. It does not run `dbt run`, `dbt snapshot`, ingestion, or deployment. The dbt schema macro also rejects non-DEV database and warehouse targets.

Create these **environment-scoped GitHub Actions secrets** in `supply-chain-dev`:

| Secret | Purpose |
| --- | --- |
| `SNOWFLAKE_ACCOUNT` | DEV Snowflake account identifier. |
| `SNOWFLAKE_USER` | DEV service user. |
| `SNOWFLAKE_ROLE` | DEV Snowflake role with only the required read privileges. |
| `SNOWFLAKE_PRIVATE_KEY` | PEM private key content for that user. |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | Optional passphrase if the key is encrypted. |

Do not put these values in repository files, workflow variables, or command arguments. The job does not print secret values or upload dbt artifacts. If required secrets are absent, the job fails before connecting. Pull requests and ordinary pushes never receive Snowflake secrets. The manual job requires existing DEV models and sources; missing relations or failing data tests cause a failure and are not repaired by CI. This includes Phase 10 inventory-intelligence relations after their separately approved DEV build; the workflow does not create or refresh them.

## Local reproduction

On Windows with the validated Python 3.13.7 environment:

```powershell
python -m pip install -r requirements.lock
python -m pip check
python -m ruff check src scripts tests
python -m pytest -p no:cacheprovider
```

For offline dbt parse, copy `dbt/profiles.yml.example` to a temporary directory as `profiles.yml`, set placeholder `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, and `SNOWFLAKE_PRIVATE_KEY_FILE` values, and run:

```powershell
dbt parse --project-dir dbt --profiles-dir <temporary-profile-directory> --no-partial-parse
```

This parse command needs no real key file. With Docker available, validate the image and DAG imports from the repository root:

```powershell
docker build --file airflow/Dockerfile --tag supplysight-airflow:ci .
docker run --rm --entrypoint /usr/local/bin/python --volume "${PWD}:/opt/supplysight:ro" --env PYTHONPATH=/opt/supplysight/src --env AIRFLOW__CORE__DAGS_FOLDER=/opt/supplysight/airflow/dags supplysight-airflow:ci -c "from airflow.models.dagbag import DagBag; bag = DagBag('/opt/supplysight/airflow/dags', include_examples=False); print(sorted(bag.dag_ids)); assert not bag.import_errors, bag.import_errors; assert {'supplysight_dev_ingestion', 'supplysight_dev_warehouse'} <= set(bag.dag_ids)"
```

`docker compose --file airflow/docker-compose.yml config --quiet` also requires the placeholder Airflow and Snowflake variables referenced by Compose, including a placeholder key host path. It only validates configuration and does not start the stack. To reproduce live dbt compile and tests, use a DEV-only role and the existing profile setup in [the staging runbook](staging.md); never use a production target.
