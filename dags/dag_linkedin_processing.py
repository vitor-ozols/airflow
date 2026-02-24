from airflow import DAG
from datetime import datetime, timezone
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models.xcom_arg import XComArg
from airflow.models import Variable
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from linkedin_operator import (
    LinkedInFetchUnprocessedOperator,
    LinkedInMarkProcessedOperator,
)
from ai.agents.job_matcher import Job, get_agent, build_user_prompt, AgentOutput
from pathlib import Path
import os
import smtplib
from email.message import EmailMessage

TO_EMAIL = "ozolsvoz@gmail.com"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_email_html(out: AgentOutput) -> str:

    items = []
    for r in out.top_recommendations:
        gaps = "".join([f"<li>{g}</li>" for g in r.gaps]) if r.gaps else "<li>No meaningful gaps.</li>"
        items.append(f"""
        <div style="margin-bottom:16px;padding:12px;border:1px solid #ddd;border-radius:10px;">
          <div style="font-size:16px;font-weight:700;">{r.title} — {r.company} (Score: {r.score:.1f}/100)</div>
          <div style="margin-top:6px;">📍 {r.location or "—"} | 🔗 <a href="{r.url}">{r.url}</a></div>
          <div style="margin-top:8px;"><b>Why it fits:</b> {r.reason}</div>
          <div style="margin-top:8px;"><b>Gaps / CV tweaks:</b><ul>{gaps}</ul></div>
        </div>
        """)
    return f"""
    <html>
      <body style="font-family:Arial,sans-serif;">
        <h2>Job recommendations</h2>
        <p>{out.summary}</p>
        {''.join(items) if items else "<p>No recommended jobs right now.</p>"}
      </body>
    </html>
    """


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


def run_agent(jobs_docs: list[dict]) -> dict:
    if not jobs_docs:
        return AgentOutput(top_recommendations=[], discard_ids=[], summary="No new jobs.").model_dump(mode="json")
    load_env_file()
    cv_path = Path("/opt/airflow/plugins/ai/agents/CV.md")
    cv_markdown = cv_path.read_text(encoding="utf-8")

    jobs = [Job.model_validate(d) for d in jobs_docs]

    prompt = build_user_prompt(cv_markdown=cv_markdown, jobs=jobs, top_k=8)

    result = get_agent().run_sync(prompt)
    out: AgentOutput = result.output
    return out.model_dump(mode="json")

def has_jobs_to_process(jobs_docs: list[dict]) -> bool:
    return bool(jobs_docs)


def build_email_payload(out_dict: dict) -> dict:
    out = AgentOutput.model_validate(out_dict)

    to_email = TO_EMAIL
    subject = f"Job recommendations — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    html = build_email_html(out)

    return {"to": to_email, "subject": subject, "html": html, "has_recs": bool(out.top_recommendations)}

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
    dag_id='linkedin_processing_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=MultipleCronTriggerTimetable(
        "15,45 8-19 * * *",
        "15 20 * * *",
        timezone="UTC",
    ),
    catchup=False,
    max_active_tasks=1,
    tags=['linkedin', 'processing'],
) as dag:

    fetch_unprocessed = LinkedInFetchUnprocessedOperator(
        task_id='fetch_unprocessed',
        mongo_conn_id='mongo_vitor_ozols',
        mongo_db='airflow',
        mongo_collection='linkedin_jobs',
        limit=50,
    )

    has_jobs = ShortCircuitOperator(
        task_id='has_jobs_to_process',
        python_callable=has_jobs_to_process,
        op_kwargs={'jobs_docs': XComArg(fetch_unprocessed)},
    )

    analyze_jobs = PythonOperator(
        task_id='analyze_jobs',
        python_callable=run_agent,
        op_kwargs={'jobs_docs': XComArg(fetch_unprocessed)},
    )

    build_payload = PythonOperator(
        task_id='build_email_payload',
        python_callable=build_email_payload,
        op_kwargs={'out_dict': XComArg(analyze_jobs)},
    )

    send_email = PythonOperator(
        task_id="send_recommendations_email",
        python_callable=send_email_smtp,
        op_kwargs={'payload': XComArg(build_payload)},
    )

    mark_processed = LinkedInMarkProcessedOperator(
        task_id='mark_processed',
        mongo_conn_id='mongo_vitor_ozols',
        mongo_db='airflow',
        mongo_collection='linkedin_jobs',
        ids=XComArg(fetch_unprocessed),
    )

    fetch_unprocessed >> has_jobs >> analyze_jobs >> build_payload >> send_email >> mark_processed
