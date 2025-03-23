from __future__ import annotations

import pendulum
import datetime
import os

from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator

v_date=datetime.today().strftime('%Y%m%d')

@dag(
    dag_id="ingest_remote_files",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2025, 3, 23, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def extract_sftp_files():
    
    return BashOperator(
        task_id="extract_sftp_files",
        bash_command=f"spark-submit --driver-memory 50G --local['*'] ../python_scripts/data_ingestion.py -d {v_date}",
    )

def send_success_email():
    EMAIL_RECIPIENTS = ["testemail@example.com"]  # Replace with the recipient email addresses
    EMAIL_SUBJECT_SUCCESS = v_date+ " SFTP Data Extraction and Load Successful"

    return EmailOperator(
        task_id="send_success_email",
        to=EMAIL_RECIPIENTS,
        subject=EMAIL_SUBJECT_SUCCESS,
        html_content=f"""
            <h3>Data extraction and load from SFTP to data warehouse completed successfully.</h3>
            <p>Execution Time: {{ execution_date }}</p>
        """,
    )


extract_sftp_files >> send_success_email
