import re
from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from job_sources_config import (
    LINKEDIN_BLACKLIST_COMPANIES,
    LINKEDIN_KEYWORDS,
    LINKEDIN_SEARCH_SCOPES,
)
from linkedin_operator import LinkedInToMongoOperator
from pymongo import ASCENDING, DESCENDING, UpdateOne
from pymongo.errors import BulkWriteError


MONGO_CONN_ID = "mongo_vitor_ozols"
MONGO_DB = "airflow"
MONGO_COLLECTION = "jobs_unified"
VOLCANIC_SOURCE_COLLECTION = "volcanic_jobs"


def create_jobs_unified_indexes():
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    collection = hook.get_collection(MONGO_COLLECTION, MONGO_DB)
    collection.create_index([("url", ASCENDING)], unique=True)
    collection.create_index([("source", ASCENDING), ("scraped_at", DESCENDING)])
    collection.create_index([("company", ASCENDING), ("title", ASCENDING)])


def sync_volcanic_jobs_to_unified(batch_size=100, max_docs_per_run=5000):
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    source_collection = hook.get_collection(VOLCANIC_SOURCE_COLLECTION, MONGO_DB)
    target_collection = hook.get_collection(MONGO_COLLECTION, MONGO_DB)

    cursor = (
        source_collection
        .find({"url": {"$exists": True, "$ne": ""}})
        .batch_size(int(batch_size))
        .limit(int(max_docs_per_run))
    )

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

    for source_doc in cursor:
        source_original_id = str(source_doc.pop("_id", ""))
        source_doc.pop("processed", None)
        source_doc.pop("processed_at", None)
        source_doc["source_collection"] = VOLCANIC_SOURCE_COLLECTION
        source_doc["source_original_id"] = source_original_id
        source_doc["unified_synced_at"] = datetime.now(timezone.utc)

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
    return totals


def task_slug(value):
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


with DAG(
    dag_id="jobs_unified_scraping",
    start_date=datetime(2024, 1, 1),
    schedule=MultipleCronTriggerTimetable(
        "25 8,12,16,20 * * *",
        timezone="UTC",
    ),
    catchup=False,
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

    for keyword in LINKEDIN_KEYWORDS:
        normalized_keyword = task_slug(keyword)
        for scope in LINKEDIN_SEARCH_SCOPES:
            scrape_linkedin_jobs = LinkedInToMongoOperator(
                task_id=f"scrape_linkedin_{scope['name']}_{normalized_keyword}_to_unified",
                keyword=keyword,
                location=scope["location"],
                geo_id=scope["geo_id"],
                days_back=1,
                blacklist=LINKEDIN_BLACKLIST_COMPANIES,
                remote_only=scope["remote_only"],
                distance=50,
                fetch_details=True,
                detail_request_delay=1.0,
                request_timeout=20,
                max_pages=1,
                mongo_conn_id=MONGO_CONN_ID,
                mongo_db=MONGO_DB,
                mongo_collection=MONGO_COLLECTION,
            )
            ensure_indexes >> scrape_linkedin_jobs
