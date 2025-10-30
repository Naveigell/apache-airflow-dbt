import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()

def send_email(to, subject, body):
    """
    Email the specified recipient with the given subject and body.

    Parameters:
        to (str): recipient's email address
        subject (str): subject of the email
        body (str): body of the email

    Returns:
        None

    Raises:
        Exception: if there is an error sending the email
    """
    email = MIMEMultipart()
    email['From']    = os.environ['MAIL_FROM_ADDRESS']
    email['To']      = to
    email['Subject'] = subject

    email.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(os.environ['MAIL_HOST'], int(os.environ['MAIL_PORT']))
        server.starttls()
        server.login(os.environ['MAIL_USERNAME'], os.environ['MAIL_PASSWORD'])

        text = email.as_string()

        server.sendmail(os.environ['MAIL_FROM_NAME'], to, text)
        server.quit()

        print("Email sent into : " + to)
    except Exception as e:
        print(f"Error: {e}")