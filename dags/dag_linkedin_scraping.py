from airflow import DAG
from datetime import datetime
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from linkedin_operator import LinkedInToMongoOperator

KEYWORDS = ['Airflow', 'Python', 'Data Engineering', 'RPA', 'Scraping']

BLACKLIST_COMPANIES = [
    'Fruition Group Ireland', 
    'Brightwater Recruitment',
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
    max_active_tasks=1,
    tags=['linkedin', 'scraping', 'ia_analysis']
    ) as dag:

    previous_task = None
    for kw in KEYWORDS:
        task_id = f'scrape_linkedin_task_{kw.lower().replace(" ", "_")}'
        
        current_task = LinkedInToMongoOperator(
            task_id=task_id,
            keyword=kw,
            location="Dublin, Ireland",
            geo_id="105178154",
            days_back=1,
            blacklist=BLACKLIST_COMPANIES,
            mongo_conn_id="mongo_vitor_ozols",
            mongo_db='airflow',
            mongo_collection='linkedin_jobs'
        )

        if previous_task:
            previous_task >> current_task
        previous_task = current_task
