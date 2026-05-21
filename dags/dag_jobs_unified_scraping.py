import re
from datetime import datetime

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
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


def create_jobs_unified_indexes():
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    collection = hook.get_collection(MONGO_COLLECTION, MONGO_DB)
    collection.create_index([("url", ASCENDING)], unique=True)
    collection.create_index([("source", ASCENDING), ("scraped_at", DESCENDING)])
    collection.create_index([("company", ASCENDING), ("title", ASCENDING)])


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
