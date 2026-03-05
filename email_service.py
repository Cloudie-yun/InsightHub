import os
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

def send_email(to_email: str, subject: str, text_body: str, html_body: str | None = None) -> None:
    username = os.getenv("MAIL_USERNAME")
    app_password = os.getenv("MAIL_APP_PASSWORD")
    from_name = os.getenv("MAIL_FROM_NAME", "InsightHub")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{username}>"
    msg["To"] = to_email

    # Plain text fallback
    msg.set_content(text_body)

    # HTML version
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(username, app_password)
        smtp.send_message(msg)