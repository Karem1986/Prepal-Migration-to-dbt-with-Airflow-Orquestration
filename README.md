# Prepal Migration to dbt and orquestration through Airflow

![Airflow ELT Orquestration](docs/AIRFLOW_UI_Graph.png)
This project is based on a data engineering project I was part of whilst working by CGI as a data engineer.

Prepal (fictitous name) is a retailer company that makes PFAS environmental friendly food packages.

The client's data is in a SAP environment and on-premises.

*The Problem:*
This project demonstrates a production-grade, cost-effective Data Warehouse architecture designed to "Unify hybrid enterprise data streams (SAP and an On-Premises Warehouse Management System (WMS)) into a Kimball Star Schema optimized for Power BI reporting."

*Problem Breakdown:*
PROBLEM 1: classic SQL stored procedures cannot be tracked and monitored.

SOLUTION: Migrate to dbt, migration from  SQL store procedures to dbt models, this will enhance team collaboration, continuous monitoring and automated testing.

PROBLEM 2: Too much time in extracting the data for the reports than generating the actual reports, data analysts have to access the data fom the on-premises DWH but the data is not organized, data extraction is not automated to extract data from the SAP environment and on-premises sources.

SOLUTION: Apache Airflow for isolated ingestion and automated data pipelines orquestration, a SQL database engine for compute and dbt to manage the Medallion transformation layers, eliminating the need for expensive cloud warehouse licenses while maintaining enterprise data governance.

## Security best practices

I do not hardcode credentials into configuration files. I separated configuration from code by utilizing an external .env file that is strictly blacklisted in the .gitignore file.

In our docker-compose.yml, the environment keys are dynamically injected at startup via host substitution. This exactly mirrors how a senior engineer prepares infrastructure for a production CI/CD pipeline where these exact same variables would be injected by a secure environment controller, such as GitHub Actions secrets or Azure Key Vault, without modifying a single line of application configuration.

## Centralized Secrets Management (HashiCorp Vault)

Airflow's `prepal_postgres_conn` connection is no longer an environment variable, it's a secret stored in a self-hosted HashiCorp Vault container, resolved at runtime through Airflow's pluggable secrets backend.

**Why this matters over the env var approach:** an environment variable is a copy of the credential. A secrets backend is a live lookup. If the Postgres password rotates, an env-var setup requires manually updating it everywhere it's duplicated; with Vault, every consumer reads the current value on its next request.

**How it's wired (`stored_procedures_dwh-migration/docker-compose.yml`):**

- A `vault` service runs `hashicorp/vault` in dev mode — auto-unsealed, fixed root token, local-only.
- The Postgres connection is stored at `secret/connections/prepal_postgres_conn`.
- Airflow's `AIRFLOW__SECRETS__BACKEND` is set to `VaultBackend`, with connection details in `AIRFLOW__SECRETS__BACKEND_KWARGS`. No `AIRFLOW_CONN_*` env var exists anymore, so a successful DAG run is proof the connection is genuinely coming from Vault.

**Why HashiCorp Vault instead of Azure Key Vault, given this is an Azure-oriented project:** I don't have live Azure credentials for this portfolio project. Vault is cloud-agnostic self-hosted software, so it's the only option of the three (Vault, Key Vault, AWS Secrets Manager) that's fully runnable and testable locally, with zero cloud account required.

**What would change moving to Azure Key Vault or AWS Secrets Manager in production:** the backend class (`AzureKeyVaultBackend` / `SecretsManagerBackend`), the authentication method (Managed Identity or an IAM role instead of a Vault token — the same passwordless-identity pattern already used by the `azurerm_databricks_access_connector` pattern in my Terraform/Databricks project), and the secret naming convention. The DAG code itself does not change — only the secrets backend configuration does. This is a smaller change than a rewrite, but it is a real config and auth change, not merely swapping a URL.

In practice this looks like this:

AIRFLOW__SECRETS__BACKEND: airflow.providers.hashicorp.secrets.vault.VaultBackend

This is the single most important line in this whole feature. It tells Airflow: "before you check your own metastore for a connection, ask this class first." The value is a Python import path — Airflow instantiates that class at startup. This is the exact same config key you'd change to airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend for Azure.

## Simulation SQL Store Procedures

In order to do the demo of how store procedures work and how this approach is improved with dbt, I made a simulation implementing two store procedures, one for extracting the data from SAP and the other store procedure for loading the data that contains only price information on the DW On-Premises of the client.

## Orchestration Layer (Airflow DAG)

The DAG (`airflow/prepal_ingestion_DAG.py`) runs four tasks:

1. `start_pipeline` — an EmptyOperator marking the entry point.
2. `extract_sap_orders` and `sync_retail_transactions` — run in parallel via `SQLExecuteQueryOperator`, each calling one Bronze stored procedure (`usp_extract_sap_orders`, `usp_sync_retail_transactions`).
3. `transform_with_dbt` — once both Bronze loads finish, a `BashOperator` runs `dbt build` against the dbt project in `dbt/`. This single task is what builds the entire Silver and Gold layer: dbt reads the Bronze tables, builds the Silver staging views, then the Gold fact tables, then runs every schema test — all in dependency order, in one command.

Apache Airflow's Task SDK does not run natively on Windows, so it runs the same way it would in a real production deployment: containerized. The `airflow` service in `stored_procedures_dwh-migration/docker-compose.yml` runs Airflow in `standalone` mode (webserver + scheduler + SQLite metadata DB in one process). The `prepal_postgres_conn` connection is resolved automatically from HashiCorp Vault at runtime (see Centralized Secrets Management below), no manual setup through the Airflow UI required.

## Custom Airflow Image

The airflow container used to install dbt and the Vault provider every time it started up, through Airflow's `_PIP_ADDITIONAL_REQUIREMENTS` variable. It worked, but it meant reinstalling the same two packages on every single boot, which is slow and not really how you'd want to run this for real.

I built a small Dockerfile instead, based on `apache/airflow:3.3.0`, that installs `dbt-postgres` and `apache-airflow-providers-hashicorp` directly into the image with pip. `docker-compose.yml` now builds this image locally (`build: .`) rather than pulling the plain Airflow image and reinstalling packages at runtime. The container starts up instantly now instead of waiting on pip every time.

## DBT Medallion Layers

Bronze is the raw output of the stored procedures above: `bronze_sap.sap_sales_orders` and `bronze_onprem.retail_transactions`.

dbt owns everything from here on:

- **Silver** (`dbt/models/staging/`, materialized as views): `stg_sap_sales_orders` and `stg_retail_transactions` — typed, deduplicated, with `retail_transactions` gaining a computed `line_amount` column so downstream models never repeat that calculation.
- **Gold** (`dbt/models/marts/`, materialized as tables): `fct_sap_sales_orders`, `fct_retail_transactions`, and `fct_daily_revenue_summary` — the last one unions both revenue streams onto a single daily grain, so Power BI reads one trusted number per day instead of two disagreeing reports from two source systems. This is the project's Single Source of Truth.
- **Tests**: every primary key gets `unique` + `not_null`, and `fct_daily_revenue_summary.source_system` is constrained to `accepted_values`. `dbt build` fails fast on the first broken test, so bad data never reaches the Gold layer Power BI reads from.

## Data Reconciliation Testing

`fct_sap_sales_orders` and `fct_daily_revenue_summary` are **siblings**, not a dependency chain — both independently `ref()` the same Silver staging model, `stg_sap_sales_orders`, rather than one being built on top of the other (see DBT Medallion Layers above). That design keeps each Gold model simple and independently traceable back to Silver, but it also means nothing guarantees the two stay in agreement if one model's logic changes and the other doesn't.

`dbt/tests/reconciliation_sap_gold_tables.sql` closes that gap for the SAP side of the business. It's a dbt **singular test**: a plain SQL query that recomputes the daily SAP total directly from `fct_sap_sales_orders`, joins it against the SAP rows already sitting in `fct_daily_revenue_summary` on `revenue_date`, and returns any day where the two totals disagree. Per dbt's test contract, zero rows returned means the test passes; any row returned is a real discrepancy and fails the build.

This test runs automatically as part of `dbt build`, alongside the schema tests. The equivalent reconciliation test for the retail/WMS side (`fct_retail_transactions` vs. `fct_daily_revenue_summary`) is a natural next addition, following the same pattern.

## DBT vs SQL

The transformation of the data (Silver layer) occurs within dbt models, which is far way better than using SQL stored procedures in the DW. With dbt models it is possible to automate these transformations, incentivate collaboration, allow monitoring and testing. Data lineage, being able to see the entire process from extraction to final transformations, is also possible with dbt.

## Migration Phased Approach

[Phase 1: Baseline] ──> [Phase 2: Airflow] ──> [Phase 3: dbt Migration]
        (Done)                (Done)                   (Done)

## Decopupling Infrastructure from Code for easier debugging

If we look at enterprise migration frameworks—like 'Rehost-then-Refactor' model or Martin Fowler's Strangler Fig pattern—the safest path to modernizing a legacy pipeline is to decouple the infrastructure migration from the code refactoring.

By setting up Apache Airflow orchestration layer first, we establish a stable, containerized scheduling baseline using our existing stored procedures. We prove our connections, docker networks, and error-handling work perfectly.

Once the infrastructure proves is working seamlessly, the SQL stored procedures are migrated to dbt models in Phase 3. This one-variable-at-a-time approach minimizes deployment risk and makes debugging incredibly straightforward.

## Instruction to run this locally

1.Start the docker containers:

 ***.\.venv\Scripts\Activate.ps1***

2.Run docker-compose up -d and verify each is up after with:

 ***docker ps --format "table {{.Names}}\t{{.Status}}"***

3.Open the Airflow UI:

Navigate your browser to localhost:8085 and log in with admin / the password, run: "docker exec prepal_airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated" to get a password (Or refer to docs/AIRFLOW.md instructions)
