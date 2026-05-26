from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
import os
import smtplib

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from pymongo import ASCENDING

from job_llm_operator import ResumeTagMatchOperator
from job_tags import build_job_tags


TO_EMAIL = "ozolsvoz@gmail.com"
MONGO_CONN_ID = "mongo_vitor_ozols"
MONGO_DB = "airflow"
JOBS_COLLECTION = "jobs_unified"
PROFILE_COLLECTION = "job_search_profiles"
PROFILE_ID = "vitor_ozols_cv"
RESUME_PATH = str(Path(__file__).resolve().parents[1] / "plugins/ai/agents/CV.md")
TOP_K = 10


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


def ensure_resume_tags() -> dict:
    resume_path = Path(RESUME_PATH)
    if not resume_path.exists():
        raise FileNotFoundError(f"CV não encontrado em {resume_path}")

    cv_markdown = resume_path.read_text(encoding="utf-8").strip()
    if not cv_markdown:
        raise ValueError(f"CV vazio em {resume_path}")

    cv_tags = build_job_tags({"description": cv_markdown})
    now = datetime.now(timezone.utc)

    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    collection = hook.get_collection(PROFILE_COLLECTION, MONGO_DB)
    collection.create_index([("profile_type", ASCENDING)])
    collection.create_index([("cv_tags", ASCENDING)])
    collection.update_one(
        {"_id": PROFILE_ID},
        {
            "$set": {
                "profile_type": "resume",
                "cv_markdown": cv_markdown,
                "cv_tags": cv_tags,
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )
    return {
        "profile_id": PROFILE_ID,
        "cv_tags": cv_tags,
        "tag_count": len(cv_tags),
    }


def has_matches_to_send(tag_output: dict) -> bool:
    tag_matches = tag_output.get("matches") or []
    tag_changed = bool(tag_output.get("changed"))
    return bool(tag_matches) and tag_changed


def build_tag_matches_html(matches: list[dict], cv_tags: list[str]) -> str:
    items = []
    for index, match in enumerate(matches, start=1):
        last_seen = match.get("last_seen_at") or match.get("scraped_at") or "Unknown"
        matched_tags = ", ".join(match.get("matched_tags") or []) or "No matched tags"
        all_tags = ", ".join(match.get("tags") or []) or "No tags"
        items.append(
            f"""
            <div style="margin-bottom:16px;padding:12px;border:1px solid #ddd;border-radius:10px;">
              <div style="font-size:16px;font-weight:700;">#{index} {match.get('title', 'Untitled')} — {match.get('company', 'Unknown company')}</div>
              <div style="margin-top:6px;">Tags em comum: {match.get('matched_tag_count', 0)} | Fonte: {match.get('source') or '—'} | Keyword: {match.get('keyword') or '—'}</div>
              <div style="margin-top:6px;">📍 {match.get('location') or '—'} | Tipo: {match.get('job_type') or '—'} | Postado: {match.get('posted_text') or '—'}</div>
              <div style="margin-top:6px;">🕒 Last seen: {last_seen}</div>
              <div style="margin-top:6px;">🎯 Matched tags: {matched_tags}</div>
              <div style="margin-top:6px;">🏷️ Job tags: {all_tags}</div>
              <div style="margin-top:6px;">🔗 <a href="{match.get('url', '#')}">{match.get('url', '#')}</a></div>
            </div>
            """
        )
    cv_tags_label = ", ".join(cv_tags or []) or "No CV tags"
    return f"""
    <h2>CV tag matches</h2>
    <p>Tags extraidas do CV: {cv_tags_label}</p>
    <p>Top {len(matches)} vagas ranqueadas por intersecao de tags entre seu CV e as tags cadastradas no MongoDB.</p>
    {''.join(items) if items else "<p>No tag matches right now.</p>"}
    """


def build_email_html(tag_matches: list[dict], cv_tags: list[str]) -> str:
    return f"""
    <html>
      <body style="font-family:Arial,sans-serif;">
        {build_tag_matches_html(tag_matches, cv_tags)}
      </body>
    </html>
    """


def build_email_payload(tag_output: dict) -> dict:
    tag_matches = tag_output.get("matches") or []
    cv_tags = tag_output.get("cv_tags") or []
    html = build_email_html(tag_matches, cv_tags)
    subject = f"CV job tag matches — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    return {
        "to": TO_EMAIL,
        "subject": subject,
        "html": html,
        "tag_match_count": len(tag_matches),
        "cv_tags": cv_tags,
    }


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
    msg.set_content(
        f"Tag matches: {payload.get('tag_match_count', 0)}. "
        f"CV tags: {', '.join(payload.get('cv_tags') or [])}."
    )
    msg.add_alternative(payload["html"], subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


with DAG(
    dag_id="linkedin_processing_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=MultipleCronTriggerTimetable(
        "15,45 8-19 * * *",
        "15 20 * * *",
        timezone="UTC",
    ),
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    tags=["linkedin", "processing", "tag-match", "resume"],
) as dag:
    ensure_resume_tags_task = PythonOperator(
        task_id="ensure_resume_tags",
        python_callable=ensure_resume_tags,
    )

    find_best_tag_matches = ResumeTagMatchOperator(
        task_id="find_best_tag_matches",
        mongo_conn_id=MONGO_CONN_ID,
        mongo_db=MONGO_DB,
        jobs_collection=JOBS_COLLECTION,
        profile_collection=PROFILE_COLLECTION,
        profile_id=PROFILE_ID,
        limit=TOP_K,
        recent_days=30,
        only_active=True,
    )

    has_matches = ShortCircuitOperator(
        task_id="has_matches_to_send",
        python_callable=has_matches_to_send,
        op_kwargs={
            "tag_output": XComArg(find_best_tag_matches),
        },
    )

    build_payload = PythonOperator(
        task_id="build_email_payload",
        python_callable=build_email_payload,
        op_kwargs={
            "tag_output": XComArg(find_best_tag_matches),
        },
    )

    send_email = PythonOperator(
        task_id="send_recommendations_email",
        python_callable=send_email_smtp,
        op_kwargs={"payload": XComArg(build_payload)},
    )

    ensure_resume_tags_task >> find_best_tag_matches >> has_matches >> build_payload >> send_email
