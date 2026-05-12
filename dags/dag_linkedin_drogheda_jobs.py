from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
import os
import smtplib

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from linkedin_operator import LinkedInFetchUnprocessedOperator, LinkedInMarkProcessedOperator, LinkedInToMongoOperator


TO_EMAIL = "ozolsvoz@gmail.com"
MONGO_DB = "airflow"
MONGO_COLLECTION = "linkedin_jobs_drogheda"


def load_env_file() -> None:
    candidates = [
        Path(__file__).resolve().parents[1] / ".env",
        Path("/opt/airflow/.env"),
    ]
    for path in candidates:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line or line.lstrip().startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        return


def has_jobs_to_process(jobs_docs: list[dict]) -> bool:
    return bool(jobs_docs)


def build_jobs_email_payload(jobs_docs: list[dict]) -> dict:
    items = []
    for job in jobs_docs:
        posted_label = job.get("posted_text") or "Unknown"
        timestamp = job.get("timestamp") or "Unknown"
        items.append(
            f"""
            <div style="margin-bottom:16px;padding:12px;border:1px solid #ddd;border-radius:10px;">
              <div style="font-size:16px;font-weight:700;">{job.get('title', 'Untitled')} — {job.get('company', 'Unknown company')}</div>
              <div style="margin-top:6px;">📍 {job.get('location') or "—"}</div>
              <div style="margin-top:6px;">🕒 Postado há: {posted_label} | Timestamp calculado: {timestamp} UTC</div>
              <div style="margin-top:6px;">🔗 <a href="{job.get('url', '#')}">{job.get('url', '#')}</a></div>
            </div>
            """
        )

    subject = f"Drogheda jobs alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    html = f"""
    <html>
      <body style="font-family:Arial,sans-serif;">
        <h2>New LinkedIn jobs in Drogheda</h2>
        <p>Encontradas {len(jobs_docs)} vagas novas em Drogheda, ordenadas das mais recentes para as mais antigas.</p>
        {''.join(items)}
      </body>
    </html>
    """
    return {"to": TO_EMAIL, "subject": subject, "html": html}


def send_email_smtp(payload: dict) -> None:
    load_env_file()
    smtp_user = os.getenv("BOT_MAIL")
    smtp_pass = os.getenv("BOT_MAIL_PASSWORD")
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    if not smtp_user or not smtp_pass:
        raise ValueError("BOT_MAIL/BOT_MAIL_PASSWORD não encontrados no .env ou env vars.")

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = payload["to"]
    msg["Subject"] = payload["subject"]
    msg.set_content("Seu cliente de e-mail não suporta HTML.")
    msg.add_alternative(payload["html"], subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


with DAG(
    dag_id="linkedin_drogheda_jobs",
    start_date=datetime(2024, 1, 1),
    schedule=MultipleCronTriggerTimetable(
        "10,40 8-19 * * *",
        "10 20 * * *",
        timezone="UTC",
    ),
    catchup=False,
    max_active_tasks=1,
    tags=["linkedin", "drogheda", "email_alerts"],
) as dag:
    scrape_drogheda_jobs = LinkedInToMongoOperator(
        task_id="scrape_drogheda_all_jobs",
        keyword="",
        location="Drogheda, County Louth, Ireland",
        geo_id=None,
        days_back=1,
        blacklist=[],
        remote_only=False,
        distance=10,
        mongo_conn_id="mongo_vitor_ozols",
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
    )

    fetch_unprocessed = LinkedInFetchUnprocessedOperator(
        task_id="fetch_unprocessed_drogheda_jobs",
        mongo_conn_id="mongo_vitor_ozols",
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
        limit=100,
    )

    has_jobs = ShortCircuitOperator(
        task_id="has_jobs_to_process",
        python_callable=has_jobs_to_process,
        op_kwargs={"jobs_docs": XComArg(fetch_unprocessed)},
    )

    build_payload = PythonOperator(
        task_id="build_jobs_email_payload",
        python_callable=build_jobs_email_payload,
        op_kwargs={"jobs_docs": XComArg(fetch_unprocessed)},
    )

    send_email = PythonOperator(
        task_id="send_jobs_email",
        python_callable=send_email_smtp,
        op_kwargs={"payload": XComArg(build_payload)},
    )

    mark_processed = LinkedInMarkProcessedOperator(
        task_id="mark_processed_drogheda_jobs",
        mongo_conn_id="mongo_vitor_ozols",
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
        ids=XComArg(fetch_unprocessed),
    )

    scrape_drogheda_jobs >> fetch_unprocessed >> has_jobs >> build_payload >> send_email >> mark_processed

