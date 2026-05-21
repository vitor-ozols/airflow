from airflow import DAG
from datetime import datetime
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from job_sources_config import LINKEDIN_BLACKLIST_COMPANIES, LINKEDIN_KEYWORDS, LINKEDIN_SEARCH_SCOPES
from linkedin_operator import LinkedInToMongoOperator


with DAG(
    'linkedin_scraping',
    start_date=datetime(2024, 1, 1),
    schedule=MultipleCronTriggerTimetable(
        "0,30 8-19 * * *",
        "0 20 * * *",
        timezone="UTC",
    ),
    catchup=False,
    max_active_tasks=10,
    tags=['linkedin', 'scraping', 'ia_analysis']
    ) as dag:

    for kw in LINKEDIN_KEYWORDS:
        normalized_kw = kw.lower().replace(" ", "_")
        for scope in LINKEDIN_SEARCH_SCOPES:
            LinkedInToMongoOperator(
                task_id=f"scrape_linkedin_{scope['name']}_{normalized_kw}",
                keyword=kw,
                location=scope["location"],
                geo_id=scope["geo_id"],
                days_back=1,
                blacklist=LINKEDIN_BLACKLIST_COMPANIES,
                remote_only=scope["remote_only"],
                distance=50,
                mongo_conn_id="mongo_vitor_ozols",
                mongo_db='airflow',
                mongo_collection='linkedin_jobs'
            )
