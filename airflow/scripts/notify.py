import os

from dotenv import load_dotenv

from scripts.send_email import send_email

load_dotenv()

def dag_notify_failure(context):
    """
    Sends an email notification when the ingestion DAG fails.

    Parameters
    ----------
    context : dict
        Context from the DAG run that failed.

    Returns
    -------
    None
    """
    send_email(
        subject=f"DAG {context['task_instance'].task_id} Failure",
        to=os.environ['MAIL_TO_ADDRESS'],
        body=f"DAG {context['task_instance'].task_id} failed for the following reason: {context['exception']} with id : {context['run_id']} on date : {context['logical_date']}"
    )

def dag_notify_success(context):
    """
    Sends an email notification when the ingestion DAG completes successfully.

    Parameters
    ----------
    context : dict
        Context from the DAG run that completed successfully.

    Returns
    -------
    None
    """
    send_email(
        subject=f"DAG {context['task_instance'].task_id} Success",
        to=os.environ['MAIL_TO_ADDRESS'],
        body=f"DAG {context['task_instance'].task_id} completed successfully with id : {context['dag'].dag_id} on date : {context['logical_date']}",
    )