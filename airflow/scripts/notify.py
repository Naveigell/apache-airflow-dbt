import os

from dotenv import load_dotenv

from scripts.send_email import send_email

load_dotenv()

def notify_failure(context):
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
        subject="DAG Failure",
        to=os.environ['MAIL_TO_ADDRESS'],
        body="DAG failed for the following reason: " + context['reason'] + " with id : " + context['run_id'],
    )

def notify_success(context):
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
        subject="DAG Success",
        to=os.environ['MAIL_TO_ADDRESS'],
        body="DAG completed successfully with id : " + context['run_id'],
    )