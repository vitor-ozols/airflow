from datetime import datetime

from airflow import DAG
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from volcanic_operator import VolcanicSitemapToMongoOperator


MONGO_CONN_ID = "mongo_vitor_ozols"
MONGO_DB = "airflow"
MONGO_COLLECTION = "volcanic_jobs"

SITEMAP_URLS = [
    "https://careers.buseireann.ie/job/sitemap.xml",
    "https://www.woodies-careers.ie/job/sitemap.xml",
    "https://www.chadwicksgroup-careers.ie/job/sitemap.xml",
    "https://www.collinsmcnicholas.ie/job/sitemap.xml",
    {
        "url": "https://www.cpl.com/job/sitemap.xml",
        "max_urls_per_run": 250,
    },
    "https://www.nextgeneration.ie/job/sitemap.xml",
    "https://www.matrixrecruitment.ie/job/sitemap.xml",
    "https://www.manpower.ie/job/sitemap.xml",
]


with DAG(
    dag_id="volcanic_sitemap_jobs",
    start_date=datetime(2024, 1, 1),
    schedule=MultipleCronTriggerTimetable(
        "20 8-20 * * *",
        timezone="UTC",
    ),
    catchup=False,
    max_active_tasks=1,
    tags=["volcanic", "jobs", "sitemap", "mongo"],
) as dag:
    scrape_volcanic_jobs = VolcanicSitemapToMongoOperator(
        task_id="scrape_volcanic_sitemap_jobs",
        sitemap_urls=SITEMAP_URLS,
        mongo_conn_id=MONGO_CONN_ID,
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
        request_delay=0.5,
        request_timeout=30,
    )
