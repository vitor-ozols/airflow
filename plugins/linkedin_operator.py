import re
import time
import json
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse
from scrapy.selector import Selector
from pymongo import UpdateOne
from pymongo import DESCENDING
from bson import ObjectId

from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

class LinkedInToMongoOperator(BaseOperator):
    template_fields = ('keyword', 'location', 'days_back')

    def __init__(
        self,
        keyword,
        location,
        geo_id,
        mongo_conn_id,
        mongo_db,
        mongo_collection,
        days_back=1,
        blacklist=None,
        remote_only=False,
        distance=None,
        fetch_details=False,
        detail_request_delay=0.75,
        request_timeout=15,
        max_pages=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.keyword = keyword
        self.location = location
        self.geo_id = geo_id
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.days_back = days_back
        self.blacklist = blacklist or []
        self.remote_only = remote_only
        self.distance = distance
        self.fetch_details = fetch_details
        self.detail_request_delay = detail_request_delay
        self.request_timeout = request_timeout
        self.max_pages = max_pages
        self.session = None

    def _format_for_log(self, value, max_length=20000):
        if value is None:
            return ""

        if isinstance(value, (dict, list)):
            return json.dumps(value, indent=2, ensure_ascii=False, default=str)

        text = str(value).strip()
        if not text:
            return text

        if text[0] in "{[":
            try:
                parsed = json.loads(text)
                text = json.dumps(parsed, indent=2, ensure_ascii=False, default=str)
            except json.JSONDecodeError:
                pass

        if len(text) > max_length:
            return f"{text[:max_length]}... [truncated {len(text) - max_length} chars]"

        return text

    def _parse_to_seconds(self, text):
        if not text:
            return None

        normalized = text.lower().strip()
        if normalized in {"just now", "agora", "now"}:
            return 0

        patterns = [
            (r"(\d+)\s*(min|mins|minute|minutes|m)\b", 60),
            (r"(\d+)\s*(hour|hours|hora|horas|hr|hrs|h)\b", 3600),
            (r"(\d+)\s*(day|days|dia|dias|d)\b", 86400),
            (r"(\d+)\s*(week|weeks|semana|semanas|w)\b", 604800),
        ]

        for pattern, multiplier in patterns:
            match = re.search(pattern, normalized)
            if match:
                return int(match.group(1)) * multiplier

        return None

    def _get_session(self):
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
                }
            )
        return self.session

    def _request(self, url, **kwargs):
        timeout = kwargs.pop("timeout", int(self.request_timeout))
        return self._get_session().get(url, timeout=timeout, **kwargs)

    def _job_id_from_url(self, url):
        if not url:
            return ""

        parsed = urlparse(url)
        query_match = re.search(r"(?:currentJobId|jobId)=(\d+)", parsed.query)
        if query_match:
            return query_match.group(1)

        path_match = re.search(r"/jobs/view/(?:[^/]+-)?(\d+)", parsed.path)
        if path_match:
            return path_match.group(1)

        slug_match = re.search(r"-(\d{8,})(?:/)?$", parsed.path.rstrip("/"))
        return slug_match.group(1) if slug_match else ""

    def _detail_api_url(self, job_url):
        job_id = self._job_id_from_url(job_url)
        if not job_id:
            return ""
        return f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"

    def _fetch_job_details(self, job_url):
        detail_url = self._detail_api_url(job_url) or job_url
        self.log.info("LinkedIn detail request | url=%s", detail_url)
        started_at = time.perf_counter()
        response = self._request(detail_url)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        self.log.info(
            "LinkedIn detail response | status_code=%s | elapsed_ms=%s | response_size=%s | url=%s",
            response.status_code,
            elapsed_ms,
            len(response.text or ""),
            detail_url,
        )
        if response.status_code != 200:
            self.log.warning(
                "Falha ao buscar detalhe LinkedIn | status_code=%s | url=%s | response=\n%s",
                response.status_code,
                detail_url,
                self._format_for_log(response.text, max_length=4000),
            )
            return {
                "detail_status_code": response.status_code,
                "detail_url": detail_url,
                "detail_fetched_at": datetime.now(timezone.utc),
            }

        return {
            **self._parse_job_details(response.text, job_url),
            "detail_status_code": response.status_code,
            "detail_url": detail_url,
            "detail_fetched_at": datetime.now(timezone.utc),
        }

    def _parse_job_details(self, html, job_url):
        selector = Selector(text=html)

        description_html = self._first_html(
            selector,
            [
                ".show-more-less-html__markup",
                ".description__text",
                "section.description",
            ],
        )
        description = self._text_from_html(description_html)
        criteria = self._parse_job_criteria(selector)
        apply_url = self._first_attr(
            selector,
            [
                "a[data-tracking-control-name='public_jobs_apply-link-offsite']::attr(href)",
                "a[data-tracking-control-name='public_jobs_apply-link']::attr(href)",
                "a.apply-button::attr(href)",
                "a[href*='apply']::attr(href)",
            ],
        )

        details = {
            "apply_url": urljoin(job_url, apply_url) if apply_url else job_url,
            "raw_detail_fields": criteria,
        }
        if description:
            details["description"] = description
        if description_html:
            details["description_html"] = description_html

        for field_name, criteria_key in {
            "job_type": "employment type",
            "seniority_level": "seniority level",
            "industries": "industries",
            "job_function": "job function",
        }.items():
            value = self._criteria_value(criteria, criteria_key)
            if value:
                details[field_name] = value

        return details

    def _parse_job_criteria(self, selector):
        fields = {}
        for item in selector.css(".description__job-criteria-item"):
            key = self._clean_text(" ".join(item.css(".description__job-criteria-subheader::text").getall())).lower()
            value = self._clean_text(" ".join(item.css(".description__job-criteria-text::text").getall()))
            if key and value:
                fields[key] = value
        return fields

    def _criteria_value(self, criteria, key):
        return criteria.get(key, "")

    def _first_html(self, selector, css_paths):
        for css_path in css_paths:
            value = selector.css(css_path).get()
            if value:
                return value
        return ""

    def _first_attr(self, selector, css_paths):
        for css_path in css_paths:
            value = selector.css(css_path).get()
            if value:
                return value
        return ""

    def _text_from_html(self, value):
        if not value:
            return ""
        return self._clean_text(" ".join(Selector(text=value).css("::text").getall()))

    def _clean_text(self, value):
        if not value:
            return ""
        return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()

    def execute(self, context):
        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        all_jobs = []
        seen_urls = set()
        search_label = self.keyword or "ALL_JOBS"
        
        seconds_limit = int(self.days_back) * 86400
        f_tpr_value = f"r{seconds_limit}"
        
        self.log.info(f"Buscando: {search_label}. Blacklist: {self.blacklist}")

        start = 0
        while True:
            url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            params = {
                "location": self.location,
                "start": start,
                "sortBy": "DD",
                "f_TPR": f_tpr_value
            }
            if self.keyword:
                params["keywords"] = self.keyword
            if self.geo_id:
                params["geoId"] = self.geo_id
            if self.distance:
                params["distance"] = str(self.distance)
            if self.remote_only:
                params["f_WT"] = "2"

            try:
                self.log.info(
                    "LinkedIn request | start=%s | url=%s | params=%s",
                    start,
                    url,
                    self._format_for_log(params),
                )
                request_started_at = time.perf_counter()
                resp = self._request(url, params=params)
                elapsed_ms = int((time.perf_counter() - request_started_at) * 1000)
                self.log.info(
                    "LinkedIn response | start=%s | status_code=%s | elapsed_ms=%s | response_size=%s",
                    start,
                    resp.status_code,
                    elapsed_ms,
                    len(resp.text or ""),
                )
                if resp.status_code != 200:
                    self.log.error(
                        "Falha ao buscar vagas | start=%s | status_code=%s | params=%s | response=\n%s",
                        start,
                        resp.status_code,
                        self._format_for_log(params),
                        self._format_for_log(resp.text),
                    )
                    break

                sel = Selector(text=resp.text)
                cards = sel.css("li .base-search-card")
                cards_count = len(cards)
                self.log.info("LinkedIn parse | start=%s | cards_encontrados=%s", start, cards_count)
                if not cards:
                    self.log.info("Nenhuma vaga retornada pelo LinkedIn para start=%s. Encerrando paginação.", start)
                    break

                jobs_before_page = len(all_jobs)

                for card in cards:
                    # Captura o nome da empresa
                    company = (card.css('h4.base-search-card__subtitle a::text').get() or 
                               card.css('h4.base-search-card__subtitle::text').get() or "N/A").strip()

                    if any(b.lower() in company.lower() for b in self.blacklist):
                        self.log.info(f"Empresa na Blacklist ignorada: {company}")
                        continue

                    raw_url = card.css('a.base-card__full-link::attr(href)').get()
                    if raw_url:
                        clean_url = raw_url.split('?')[0]
                        if clean_url in seen_urls: continue
                        seen_urls.add(clean_url)

                        posted_text = (card.css('time::text').get() or "").strip()
                        seconds = self._parse_to_seconds(posted_text)
                        posted_at = (
                            datetime.now(timezone.utc) - timedelta(seconds=seconds)
                            if seconds is not None else None
                        )

                        scraped_at = datetime.now(timezone.utc)
                        job = {
                            "source": "linkedin",
                            "source_type": "linkedin_jobs_guest_api",
                            "site": "www.linkedin.com",
                            "title": self._clean_text(card.css('h3.base-search-card__title::text').get() or ""),
                            "company": company,
                            "location": self._clean_text(card.css('span.job-search-card__location::text').get() or ""),
                            "url": clean_url,
                            "apply_url": clean_url,
                            "keyword": self.keyword or "ALL_JOBS",
                            "posted_text": posted_text,
                            "posted_seconds": seconds,
                            "timestamp": posted_at.strftime("%Y-%m-%d %H:%M:%S") if posted_at else "",
                            "posted_at": posted_at,
                            "scraped_at": scraped_at,
                            "last_seen_at": scraped_at,
                        }

                        if self.fetch_details:
                            try:
                                job.update(self._fetch_job_details(clean_url))
                            except Exception:
                                self.log.exception("Falha ao enriquecer detalhe LinkedIn: %s", clean_url)

                            if self.detail_request_delay:
                                time.sleep(float(self.detail_request_delay))

                        all_jobs.append(job)

                jobs_added = len(all_jobs) - jobs_before_page
                self.log.info(
                    "LinkedIn page summary | start=%s | cards=%s | vagas_adicionadas=%s | total_acumulado=%s",
                    start,
                    cards_count,
                    jobs_added,
                    len(all_jobs),
                )
                start += 25
                if self.max_pages and start >= int(self.max_pages) * 25:
                    self.log.info("Limite de páginas atingido no LinkedIn: max_pages=%s", self.max_pages)
                    break
                time.sleep(2)

            except Exception as e:
                formatted_error = self._format_for_log(str(e))
                if formatted_error and formatted_error != str(e):
                    self.log.error("Detalhe do erro formatado:\n%s", formatted_error)

                self.log.exception(
                    "Erro na paginação para keyword=%s, start=%s",
                    search_label,
                    start,
                )
                raise

        self.log.info("LinkedIn scraping finalizado | keyword=%s | total_vagas=%s", search_label, len(all_jobs))
        if not all_jobs:
            self.log.info("Nenhuma vaga coletada para inserir no MongoDB.")
            return {"inserted": 0, "updated": 0, "matched": 0, "total_scraped": 0}

        try:
            collection = hook.get_collection(self.mongo_collection, self.mongo_db)
            operations = [
                UpdateOne(
                    {"url": job["url"]},
                    {
                        "$set": job,
                        "$setOnInsert": {
                            "first_seen_at": datetime.now(timezone.utc),
                            "processed": False,
                            "processed_at": "",
                        },
                    },
                    upsert=True,
                )
                for job in all_jobs
            ]

            self.log.info(
                "Mongo insert start | keyword=%s | collection=%s | operations=%s",
                search_label,
                self.mongo_collection,
                len(operations),
            )
            result = collection.bulk_write(operations, ordered=False)
            self.log.info(
                "Mongo insert result | keyword=%s | upserted=%s | modified=%s | matched=%s",
                search_label,
                result.upserted_count,
                result.modified_count,
                result.matched_count,
            )

            return {
                "inserted": result.upserted_count,
                "updated": result.modified_count,
                "matched": result.matched_count,
                "total_scraped": len(all_jobs),
            }
        except Exception:
            self.log.exception(
                "Erro ao salvar vagas no MongoDB | keyword=%s | total_vagas=%s",
                search_label,
                len(all_jobs),
            )
            raise


class LinkedInFetchUnprocessedOperator(BaseOperator):
    def __init__(
        self,
        mongo_conn_id,
        mongo_db,
        mongo_collection,
        limit=50,
        keywords=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.limit = limit
        self.keywords = keywords or []

    def execute(self, context):
        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        collection = hook.get_collection(self.mongo_collection, self.mongo_db)

        query = {"processed": False}
        if self.keywords:
            query["keyword"] = {"$in": self.keywords}

        cursor = (
            collection
            .find(query)
            .sort("timestamp", DESCENDING)
            .limit(int(self.limit))
        )
        docs = []
        for doc in cursor:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            docs.append(doc)

        self.log.info(
            "Encontrados %s registros com processed=false para keywords=%s, ordenados por timestamp desc.",
            len(docs),
            self.keywords or "todas",
        )
        return docs


class LinkedInMarkProcessedOperator(BaseOperator):
    template_fields = ("ids",)

    def __init__(
        self,
        mongo_conn_id,
        mongo_db,
        mongo_collection,
        ids=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.ids = ids or []

    def _coerce_ids(self, ids):
        if ids is None:
            return []

        if isinstance(ids, list) and ids and isinstance(ids[0], dict):
            extracted = []
            for item in ids:
                if "_id" in item:
                    extracted.append(item["_id"])
            ids = extracted

        if isinstance(ids, str):
            try:
                ids = json.loads(ids)
            except json.JSONDecodeError:
                ids = [ids]

        if isinstance(ids, (set, tuple)):
            ids = list(ids)

        if not isinstance(ids, list):
            ids = [ids]

        coerced = []
        for item in ids:
            if isinstance(item, ObjectId):
                coerced.append(item)
                continue
            if isinstance(item, str):
                try:
                    coerced.append(ObjectId(item))
                except Exception:
                    coerced.append(item)
            else:
                coerced.append(item)
        return coerced

    def execute(self, context):
        ids = self._coerce_ids(self.ids)
        if not ids:
            self.log.info("Nenhum id recebido para marcar como processed.")
            return 0

        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        collection = hook.get_collection(self.mongo_collection, self.mongo_db)

        result = collection.update_many(
            {"_id": {"$in": ids}},
            {"$set": {"processed": True, "processed_at": datetime.now()}},
        )

        self.log.info("Atualizados %s registros como processed=true.", result.modified_count)
        return result.modified_count
