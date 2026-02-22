from airflow import DAG
from datetime import datetime
from linkedin_operator import LinkedInToMongoOperator

KEYWORDS = ['Airflow', 'Python', 'Data Engineering']

BLACKLIST_COMPANIES = [
    'Fruition Group Ireland', 
    'Brightwater Recruitment',
]

with DAG(
    'linkedin_to_mongo',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
    ) as dag:

    for kw in KEYWORDS:
        task_id = f'scrape_{kw.lower().replace(" ", "_")}'
        
        LinkedInToMongoOperator(
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
