from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.utils.email import send_email

from includes.etl_pipelines.vehicle_profile.main import run_once
from airflow.providers.smtp.notifications.smtp import send_smtp_notification



ALERT_EMAILS = ["subhajit.chatterjee@taabi.ai"]


failure_email = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email="airflow@taabi.ai",
    to=ALERT_EMAILS,
    subject="[ALERT] DAG Failed: {{ dag.dag_id }}",
    html_content="""
        <h3 style="color: red;">Airflow DAG Failed</h3>

        <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
        <p><strong>Task:</strong> {{ ti.task_id }}</p>
        <p><strong>Run ID:</strong> {{ run_id }}</p>
        <p><strong>Logical date:</strong> {{ logical_date }}</p>
        <p><strong>Try number:</strong> {{ ti.try_number }}</p>

        <p>
            <a href="{{ ti.log_url }}">Open Airflow task logs</a>
        </p>

        <p><strong>Error:</strong></p>
        <pre>{{ exception }}</pre>
    """,
)

success_email = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email="airflow@taabi.ai",
    to=ALERT_EMAILS,
    subject="[SUCCESS] DAG Completed: {{ dag.dag_id }}",
    html_content="""
        <h3 style="color: green;">Airflow DAG Completed Successfully</h3>

        <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
        <p><strong>Run ID:</strong> {{ run_id }}</p>
        <p><strong>Logical date:</strong> {{ logical_date }}</p>
    """,
)



default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "on_failure_callback": failure_email,
}


# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
@dag(
    dag_id="vehicle_profile_full_refresh_etl",
    start_date=datetime(2026, 7, 16, tzinfo=timezone.utc),
    schedule="0 0,12 * * *",
    catchup=False,
    default_args=default_args,
    on_failure_callback=[failure_email],
    on_success_callback=[success_email],
    tags=["full_refresh", "vehicle_profile"],
)
def pipeline():

    @task
    def step_process_and_load():
        """
        Fetch all source tables, build the vehicle profile,
        truncate the ClickHouse target table, and insert the full dataset.
        """
        run_once()
        return "Vehicle profile full refresh completed."

    step_process_and_load()


# Instantiate DAG
dag_object = pipeline()