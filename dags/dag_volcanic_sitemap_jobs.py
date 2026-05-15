from datetime import datetime

from airflow import DAG
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from volcanic_operator import VolcanicSitemapToMongoOperator


MONGO_CONN_ID = "mongo_vitor_ozols"
MONGO_DB = "airflow"
MONGO_COLLECTION = "volcanic_jobs"


def greenhouse_board(board_token, source=None, company=None, max_urls_per_run=None):
    config = {
        "url": f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true",
        "source": source or f"greenhouse_{board_token}",
        "type": "greenhouse_jobs_api",
    }
    if company:
        config["company"] = company
    if max_urls_per_run:
        config["max_urls_per_run"] = max_urls_per_run
    return config


SITEMAP_URLS = [
    "https://careers.buseireann.ie/job/sitemap.xml",
    "https://www.woodies-careers.ie/job/sitemap.xml",
    "https://www.chadwicksgroup-careers.ie/job/sitemap.xml",
    "https://www.collinsmcnicholas.ie/job/sitemap.xml",
    {
        "url": "https://www.cpl.com/job/sitemap.xml",
        "include_url_regex": r"^https://www\.cpl\.com/job/",
        "max_urls_per_run": 250,
    },
    "https://www.nextgeneration.ie/job/sitemap.xml",
    "https://www.matrixrecruitment.ie/job/sitemap.xml",
    "https://www.manpower.ie/job/sitemap.xml",
    {
        "url": "https://careers.toasttab.com/sitemap.xml",
        "source": "toast",
        "include_url_regex": r"^https://careers\.toasttab\.com/jobs/(?!search(?:/|$))",
    },
    {
        "url": "https://careers.bankofireland.com/sitemap.xml",
        "source": "bank_of_ireland",
        "include_url_regex": r"^https://careers\.bankofireland\.com/jobs/(?!search(?:/|$))",
    },
    {
        "url": "https://careers.sse.com/sitemap.xml",
        "source": "sse",
        "include_url_regex": r"^https://careers\.sse\.com/jobs/(?!search(?:/|$))",
    },
    {
        "url": "https://careers.procore.com/sitemap.xml",
        "source": "procore",
        "include_url_regex": r"^https://careers\.procore\.com/jobs/(?!search(?:/|$))",
    },
    {
        "url": "https://careers.equinix.com/sitemap.xml",
        "source": "equinix",
        "include_url_regex": r"^https://careers\.equinix\.com/jobs/(?!search(?:/|$))",
        "max_urls_per_run": 250,
    },
    greenhouse_board("gitlab"),
    greenhouse_board("intercom", company="Intercom"),
    greenhouse_board("tines"),
    greenhouse_board("learnupon"),
    greenhouse_board("vectranetworks"),
    greenhouse_board("sonatus"),
    greenhouse_board("datadog", max_urls_per_run=250),
    greenhouse_board("mongodb", max_urls_per_run=250),
    greenhouse_board("stripe", max_urls_per_run=250),
    greenhouse_board("anthropic", max_urls_per_run=250),
    greenhouse_board("okta", max_urls_per_run=250),
    greenhouse_board("cloudflare"),
    greenhouse_board("figma"),
    greenhouse_board("elastic"),
    greenhouse_board("twilio"),
    greenhouse_board("asana"),
    greenhouse_board("reddit"),
    greenhouse_board("vercel"),
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
    tags=["volcanic", "toast", "greenhouse", "jobs", "sitemap", "mongo"],
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
