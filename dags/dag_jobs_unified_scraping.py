import re
from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from job_llm_operator import JobStaleCleanupOperator
from job_sources_config import (
    LINKEDIN_BLACKLIST_COMPANIES,
    LINKEDIN_KEYWORDS,
    LINKEDIN_SEARCH_SCOPES,
    VOLCANIC_SITEMAP_URLS,
)
from linkedin_operator import LinkedInToMongoOperator
from pymongo import ASCENDING, DESCENDING
from volcanic_operator import VolcanicSitemapToMongoOperator


MONGO_CONN_ID = "mongo_vitor_ozols"
MONGO_DB = "airflow"
MONGO_COLLECTION = "jobs_unified"
REPORT_COLLECTION = "jobs_unified_scraping_reports"
STALE_AFTER_DAYS = 2
LINKEDIN_DAYS_BACK = 1


def create_jobs_unified_indexes():
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    collection = hook.get_collection(MONGO_COLLECTION, MONGO_DB)
    collection.create_index([("url", ASCENDING)], unique=True)
    collection.create_index([("source", ASCENDING), ("scraped_at", DESCENDING)])
    collection.create_index([("company", ASCENDING), ("title", ASCENDING)])
    collection.create_index([("last_seen_at", DESCENDING)])
    collection.create_index([("active", ASCENDING)])
    collection.create_index([("tags", ASCENDING)])
    collection.create_index([("llm_tagging_status", ASCENDING)])
    collection.create_index([("llm_tags.enriched_at", DESCENDING)])
    collection.create_index([("job_embedding_status", ASCENDING)])
    collection.create_index([("job_embedding.enriched_at", DESCENDING)])


def task_slug(value):
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def build_final_report(scraping_task_ids, cleanup_task_id, **context):
    task_instance = context["ti"]

    scraping_totals = {
        "inserted": 0,
        "updated": 0,
        "matched": 0,
        "total_scraped": 0,
        "total_synced": 0,
    }
    scraping_by_task = {}
    for task_id in scraping_task_ids:
        result = task_instance.xcom_pull(task_ids=task_id) or {}
        scraping_by_task[task_id] = result
        scraping_totals["inserted"] += int(result.get("inserted", 0) or 0)
        scraping_totals["updated"] += int(result.get("updated", 0) or 0)
        scraping_totals["matched"] += int(result.get("matched", 0) or 0)
        scraping_totals["total_scraped"] += int(result.get("total_scraped", 0) or 0)
        scraping_totals["total_synced"] += int(result.get("total_synced", 0) or 0)

    cleanup_result = task_instance.xcom_pull(task_ids=cleanup_task_id) or {}

    report = {
        "scraping": {
            **scraping_totals,
            "tasks": scraping_by_task,
        },
        "cleanup": {
            "matched": int(cleanup_result.get("matched", 0) or 0),
            "deleted": int(cleanup_result.get("deleted", 0) or 0),
            "dry_run": bool(cleanup_result.get("dry_run", False)),
        },
        "embedding": {
            "enabled": False,
            "calls_attempted": 0,
            "embedded": 0,
            "failed": 0,
            "skipped": 0,
        },
    }
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    reports_collection = hook.get_collection(REPORT_COLLECTION, MONGO_DB)
    reports_collection.create_index([("dag_id", ASCENDING), ("run_id", ASCENDING)], unique=True)
    reports_collection.create_index([("inserted_at", DESCENDING)])
    report_doc = {
        "dag_id": context["dag"].dag_id,
        "task_id": task_instance.task_id,
        "run_id": context["run_id"],
        "logical_date": context.get("logical_date"),
        "inserted_at": datetime.now(timezone.utc),
        "source": "airflow_task",
        "report": report,
    }
    reports_collection.update_one(
        {"dag_id": report_doc["dag_id"], "run_id": report_doc["run_id"]},
        {"$set": report_doc},
        upsert=True,
    )
    print(f"jobs_unified final report: {report}")
    return report


with DAG(
    dag_id="jobs_unified_scraping",
    start_date=datetime(2024, 1, 1),
    schedule=MultipleCronTriggerTimetable(
        "25 8-20 * * *",
        timezone="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["jobs", "linkedin", "volcanic", "greenhouse", "mongo", "unified"],
) as dag:
    ensure_indexes = PythonOperator(
        task_id="ensure_jobs_unified_indexes",
        python_callable=create_jobs_unified_indexes,
    )

    scrape_volcanic_jobs = VolcanicSitemapToMongoOperator(
        task_id="scrape_volcanic_jobs_to_unified",
        sitemap_urls=VOLCANIC_SITEMAP_URLS,
        mongo_conn_id=MONGO_CONN_ID,
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
        request_delay=0.5,
        request_timeout=30,
    )

    ensure_indexes >> scrape_volcanic_jobs
    scraping_done_tasks = [scrape_volcanic_jobs]
    scraping_task_ids = [scrape_volcanic_jobs.task_id]

    cleanup_stale_jobs = JobStaleCleanupOperator(
        task_id="cleanup_stale_jobs",
        mongo_conn_id=MONGO_CONN_ID,
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
        stale_after_days=STALE_AFTER_DAYS,
        trigger_rule="all_done",
    )

    for keyword in LINKEDIN_KEYWORDS:
        normalized_keyword = task_slug(keyword)
        for scope in LINKEDIN_SEARCH_SCOPES:
            scrape_linkedin_jobs = LinkedInToMongoOperator(
                task_id=f"scrape_linkedin_{scope['name']}_{normalized_keyword}_to_unified",
                keyword=keyword,
                location=scope["location"],
                geo_id=scope["geo_id"],
                days_back=LINKEDIN_DAYS_BACK,
                blacklist=LINKEDIN_BLACKLIST_COMPANIES,
                remote_only=scope["remote_only"],
                distance=50,
                fetch_details=True,
                detail_request_delay=1.0,
                request_timeout=20,
                mongo_conn_id=MONGO_CONN_ID,
                mongo_db=MONGO_DB,
                mongo_collection=MONGO_COLLECTION,
            )
            ensure_indexes >> scrape_linkedin_jobs
            scraping_done_tasks.append(scrape_linkedin_jobs)
            scraping_task_ids.append(scrape_linkedin_jobs.task_id)

    for task in scraping_done_tasks:
        task >> cleanup_stale_jobs

    final_report = PythonOperator(
        task_id="jobs_unified_final_report",
        python_callable=build_final_report,
        op_kwargs={
            "scraping_task_ids": scraping_task_ids,
            "cleanup_task_id": cleanup_stale_jobs.task_id,
        },
        trigger_rule="all_done",
    )

    cleanup_stale_jobs >> final_report
