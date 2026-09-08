# Building a custom image to deploy this prokect live and to install dbt-postgres + apache-airflow-providers-hashicorp once,
#  permanently, no more -reinstall in every boot.
FROM apache/airflow:3.3.0

# Command that will run durint the image building process
RUN pip install --no-cache-dir dbt-postgres apache-airflow-providers-hashicorp
