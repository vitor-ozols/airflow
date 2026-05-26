import re
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from job_tags import build_job_tags
from job_llm_operator import JobGoogleEmbeddingOperator, JobStaleCleanupOperator
from job_sources_config import (
    LINKEDIN_BLACKLIST_COMPANIES,
    LINKEDIN_KEYWORDS,
    LINKEDIN_SEARCH_SCOPES,
)
from linkedin_operator import LinkedInToMongoOperator
from pymongo import ASCENDING, DESCENDING, UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError


MONGO_CONN_ID = "mongo_vitor_ozols"
MONGO_DB = "airflow"
MONGO_COLLECTION = "jobs_unified"
REPORT_COLLECTION = "jobs_unified_scraping_reports"
VOLCANIC_SOURCE_COLLECTION = "volcanic_jobs"
STALE_AFTER_DAYS = 2
LINKEDIN_DAYS_BACK = 1
EMBEDDING_LIMIT = 500


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


def sync_volcanic_jobs_to_unified(batch_size=100, max_docs_per_run=None):
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    source_collection = hook.get_collection(VOLCANIC_SOURCE_COLLECTION, MONGO_DB)
    target_collection = hook.get_collection(MONGO_COLLECTION, MONGO_DB)

    cursor = (
        source_collection
        .find(
            {"url": {"$exists": True, "$ne": ""}}
        )
        .batch_size(int(batch_size))
    )
    if max_docs_per_run:
        cursor = cursor.limit(int(max_docs_per_run))

    totals = {"inserted": 0, "updated": 0, "matched": 0, "total_synced": 0}
    operations = []

    def flush_operations():
        if not operations:
            return

        try:
            result = target_collection.bulk_write(operations, ordered=False)
        except BulkWriteError as error:
            details = error.details or {}
            write_errors = details.get("writeErrors", [])
            sample_errors = []
            for write_error in write_errors[:5]:
                operation = write_error.get("op", {})
                sample_errors.append(
                    {
                        "index": write_error.get("index"),
                        "code": write_error.get("code"),
                        "errmsg": write_error.get("errmsg"),
                        "url": (operation.get("q") or {}).get("url"),
                    }
                )
            raise RuntimeError(
                "Mongo bulk write failed while syncing volcanic jobs to unified "
                f"collection. batch_size={len(operations)} errors={sample_errors}"
            ) from None

        totals["inserted"] += result.upserted_count
        totals["updated"] += result.modified_count
        totals["matched"] += result.matched_count
        totals["total_synced"] += len(operations)
        operations.clear()

    try:
        for source_doc in cursor:
            source_original_id = str(source_doc.pop("_id", ""))
            source_doc.pop("processed", None)
            source_doc.pop("processed_at", None)
            source_doc["source_collection"] = VOLCANIC_SOURCE_COLLECTION
            source_doc["source_original_id"] = source_original_id
            source_doc["unified_synced_at"] = datetime.now(timezone.utc)
            source_doc["tags"] = build_job_tags(source_doc)

            operations.append(
                UpdateOne(
                    {"url": source_doc["url"]},
                    {
                        "$set": source_doc,
                        "$setOnInsert": {
                            "processed": False,
                            "processed_at": "",
                        },
                    },
                    upsert=True,
                )
            )

            if len(operations) >= int(batch_size):
                flush_operations()

        flush_operations()
    except PyMongoError as error:
        totals["sync_failed"] = True
        totals["sync_error"] = str(error)[:1000]
        print(f"sync_volcanic_jobs_to_unified Mongo error: {totals['sync_error']}")
        return totals
    return totals


def task_slug(value):
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def build_final_report(scraping_task_ids, cleanup_task_id, embedding_task_id, **context):
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
    embedding_result = task_instance.xcom_pull(task_ids=embedding_task_id) or {}

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
            "calls_attempted": int(embedding_result.get("attempted", 0) or 0),
            "embedded": int(embedding_result.get("embedded", 0) or 0),
            "failed": int(embedding_result.get("failed", 0) or 0),
            "skipped": int(embedding_result.get("skipped", 0) or 0),
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

    sync_volcanic_jobs = PythonOperator(
        task_id="sync_volcanic_jobs_to_unified",
        python_callable=sync_volcanic_jobs_to_unified,
    )

    ensure_indexes >> sync_volcanic_jobs
    scraping_done_tasks = [sync_volcanic_jobs]
    scraping_task_ids = [sync_volcanic_jobs.task_id]

    embed_relevant_job_content = JobGoogleEmbeddingOperator(
        task_id="embed_relevant_job_content",
        mongo_conn_id=MONGO_CONN_ID,
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
        limit=EMBEDDING_LIMIT,
        max_attempts=1,
    )

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

    cleanup_stale_jobs >> embed_relevant_job_content

    final_report = PythonOperator(
        task_id="jobs_unified_final_report",
        python_callable=build_final_report,
        op_kwargs={
            "scraping_task_ids": scraping_task_ids,
            "cleanup_task_id": cleanup_stale_jobs.task_id,
            "embedding_task_id": embed_relevant_job_content.task_id,
        },
        trigger_rule="all_done",
    )

    embed_relevant_job_content >> final_report
