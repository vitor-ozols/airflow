from airflow import DAG
from datetime import datetime
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from linkedin_config import KEYWORDS
from linkedin_operator import LinkedInToMongoOperator

BLACKLIST_COMPANIES = [
    'Fruition Group Ireland', 
    'Brightwater Recruitment',
]

SEARCH_SCOPES = [
    {
        "name": "dublin",
        "location": "Dublin, Ireland",
        "geo_id": "105178154",
        "remote_only": False,
    },
    {
        "name": "europe_remote",
        "location": "Europe",
        "geo_id": "91000000",
        "remote_only": True,
    },
]


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

    for kw in KEYWORDS:
        normalized_kw = kw.lower().replace(" ", "_")
        for scope in SEARCH_SCOPES:
            LinkedInToMongoOperator(
                task_id=f"scrape_linkedin_{scope['name']}_{normalized_kw}",
                keyword=kw,
                location=scope["location"],
                geo_id=scope["geo_id"],
                days_back=1,
                blacklist=BLACKLIST_COMPANIES,
                remote_only=scope["remote_only"],
                distance=50,
                mongo_conn_id="mongo_vitor_ozols",
                mongo_db='airflow',
                mongo_collection='linkedin_jobs'
            )
