import json
import html as html_lib
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree

import requests
from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import ASCENDING, UpdateOne
from scrapy.selector import Selector


class VolcanicSitemapToMongoOperator(BaseOperator):
    template_fields = ("sitemap_urls",)

    def __init__(
        self,
        sitemap_urls,
        mongo_conn_id,
        mongo_db,
        mongo_collection,
        request_delay=0.5,
        request_timeout=30,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sitemap_urls = sitemap_urls
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.request_delay = request_delay
        self.request_timeout = request_timeout
        self.session = None

    def execute(self, context):
        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        collection = hook.get_collection(self.mongo_collection, self.mongo_db)
        collection.create_index([("url", ASCENDING)], unique=True)
        collection.create_index([("site", ASCENDING), ("sitemap_lastmod", ASCENDING)])

        sitemap_entries = self._collect_sitemap_entries()
        self.log.info("Sitemap URLs coletadas: %s", len(sitemap_entries))
        sitemap_entries = self._filter_entries_to_fetch(collection, sitemap_entries)
        sitemap_entries = self._apply_sitemap_limits(sitemap_entries)
        self.log.info("Sitemap URLs para extrair nesta execução: %s", len(sitemap_entries))

        jobs = []
        seen_urls = set()
        for entry in sitemap_entries:
            url = entry["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)

            try:
                job = self._fetch_job(url, entry)
            except Exception:
                self.log.exception("Falha ao processar vaga: %s", url)
                continue

            if job:
                jobs.append(job)

            if self.request_delay:
                time.sleep(float(self.request_delay))

        self.log.info("Vagas extraídas: %s", len(jobs))
        if not jobs:
            return {"inserted": 0, "updated": 0, "matched": 0, "total_scraped": 0}

        scraped_at = datetime.now(timezone.utc)
        operations = [
            UpdateOne(
                {"url": job["url"]},
                {
                    "$set": {
                        **job,
                        "last_seen_at": scraped_at,
                        "active": True,
                    },
                    "$setOnInsert": {
                        "first_seen_at": scraped_at,
                        "processed": False,
                        "processed_at": "",
                    },
                },
                upsert=True,
            )
            for job in jobs
        ]

        result = collection.bulk_write(operations, ordered=False)
        self.log.info(
            "Mongo result | collection=%s | upserted=%s | modified=%s | matched=%s",
            self.mongo_collection,
            result.upserted_count,
            result.modified_count,
            result.matched_count,
        )
        return {
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "matched": result.matched_count,
            "total_scraped": len(jobs),
        }

    def _collect_sitemap_entries(self):
        entries = []
        visited = set()
        for sitemap_config in self._coerce_sitemap_configs():
            if sitemap_config.get("type") == "greenhouse_jobs_api":
                entries.extend(self._collect_entries_from_greenhouse_api(sitemap_config))
            else:
                entries.extend(self._collect_entries_from_sitemap(sitemap_config, visited))
        return entries

    def _collect_entries_from_greenhouse_api(self, sitemap_config):
        api_url = sitemap_config["url"]
        board_token = self._greenhouse_board_token(api_url)
        payload = json.loads(self._request_text(api_url))
        entries = []
        for job in payload.get("jobs", []):
            url = job.get("absolute_url")
            if not url or not self._matches_sitemap_filters(url, sitemap_config):
                continue
            job_api_url = self._greenhouse_job_api_url(board_token, job)
            entries.append(
                {
                    "url": url,
                    "source_api_url": job_api_url,
                    "sitemap_url": api_url,
                    "sitemap_lastmod": job.get("updated_at", ""),
                    "configured_sitemap_url": sitemap_config["configured_url"],
                    "max_urls_per_run": sitemap_config.get("max_urls_per_run"),
                    "source": sitemap_config.get("source", "greenhouse"),
                    "source_type": sitemap_config.get("type"),
                    "company": sitemap_config.get("company", ""),
                    "greenhouse_job": job,
                }
            )
        return entries

    def _collect_entries_from_sitemap(self, sitemap_config, visited):
        sitemap_url = sitemap_config["url"]
        if sitemap_url in visited:
            return []
        visited.add(sitemap_url)

        xml = self._request_text(sitemap_url)
        root = ElementTree.fromstring(xml)
        namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        sitemap_children = [
            loc.text.strip()
            for loc in root.findall(".//sm:sitemap/sm:loc", namespace)
            if loc.text
        ]
        if sitemap_children:
            self.log.info(
                "Sitemap index detectado em %s com %s filhos.",
                sitemap_url,
                len(sitemap_children),
            )
            entries = []
            for child_url in sitemap_children:
                child_config = {**sitemap_config, "url": child_url}
                entries.extend(self._collect_entries_from_sitemap(child_config, visited))
            return entries

        entries = []
        for url_node in root.findall(".//sm:url", namespace):
            loc = url_node.find("sm:loc", namespace)
            if loc is None or not loc.text:
                continue
            url = loc.text.strip()
            if not self._matches_sitemap_filters(url, sitemap_config):
                continue
            lastmod = url_node.find("sm:lastmod", namespace)
            entries.append(
                {
                    "url": url,
                    "sitemap_url": sitemap_url,
                    "sitemap_lastmod": lastmod.text.strip() if lastmod is not None and lastmod.text else "",
                    "configured_sitemap_url": sitemap_config["configured_url"],
                    "max_urls_per_run": sitemap_config.get("max_urls_per_run"),
                    "source": sitemap_config.get("source", "volcanic"),
                }
            )
        return entries

    def _fetch_job(self, url, sitemap_entry):
        if sitemap_entry.get("source_type") == "greenhouse_jobs_api":
            return self._greenhouse_job_to_document(sitemap_entry["greenhouse_job"], sitemap_entry)

        html = self._request_text(url)
        selector = Selector(text=html)

        title = self._first_text(
            selector,
            [
                "#template-job-100 header h1::text",
                "#template-job-100 header h2::text",
                ".template-job-page header h1::text",
                ".template-job-page header h2::text",
                ".block-job-description .job-title::text",
                ".job-title::text",
                "h1::text",
            ],
        )
        description_html = self._first_html(
            selector,
            [
                "#template-job-100 .desc article",
                ".template-job-page .desc article",
                ".block-job-description .job-description",
                ".job-description",
                "article",
            ],
        )
        description = self._text_from_html(description_html)
        table_fields = {
            **self._parse_table_fields(selector),
            **self._parse_job_component_fields(selector),
        }
        json_ld = self._parse_json_ld_job(selector)

        parsed_url = urlparse(url)
        apply_url = self._first_attr(
            selector,
            [
                "#template-job-100 a.apply-now::attr(href)",
                ".template-job-page a.apply-now::attr(href)",
                "a[href$='/apply']::attr(href)",
                "a[href='#applynow']::attr(href)",
                "a[href*='apply']::attr(href)",
            ],
        )

        publication_date = (
            self._field(table_fields, "date posted", "posted date", "publication date", "published")
            or json_ld.get("datePosted", "")
        )
        expiry_date = (
            self._field(table_fields, "expiry date", "closing date", "valid through")
            or json_ld.get("validThrough", "")
        )

        if not title:
            title = json_ld.get("title", "")
        if not description_html and json_ld.get("description"):
            description_html = json_ld.get("description", "")
        if not description:
            description = self._text_from_html(json_ld.get("description", ""))

        return {
            "source": sitemap_entry.get("source", "volcanic"),
            "source_sitemap_url": sitemap_entry["sitemap_url"],
            "source_api_url": sitemap_entry.get("source_api_url", ""),
            "site": parsed_url.netloc,
            "url": url,
            "title": title,
            "description": description,
            "description_html": description_html,
            "location": self._field(table_fields, "location") or self._json_ld_location(json_ld),
            "discipline": self._field(table_fields, "discipline", "external posting category", "category"),
            "job_type": self._field(table_fields, "job type", "employment type") or json_ld.get("employmentType", ""),
            "salary": self._field(table_fields, "salary"),
            "contact_name": self._field(table_fields, "contact name"),
            "contact_email": self._field(table_fields, "contact email"),
            "job_ref": self._field(table_fields, "job ref", "reference", "job reference", "requisition identifier")
            or self._json_ld_identifier(json_ld),
            "publication_date": publication_date,
            "publication_date_at": self._parse_datetime(publication_date),
            "expiry_date": expiry_date,
            "expiry_date_at": self._parse_datetime(expiry_date),
            "sitemap_lastmod": sitemap_entry.get("sitemap_lastmod", ""),
            "sitemap_lastmod_at": self._parse_datetime(sitemap_entry.get("sitemap_lastmod", "")),
            "apply_url": urljoin(url, apply_url) if apply_url else "",
            "company": self._json_ld_company(json_ld) or self._company_from_site(parsed_url.netloc),
            "raw_fields": table_fields,
            "scraped_at": datetime.now(timezone.utc),
        }

    def _greenhouse_job_to_document(self, greenhouse_job, sitemap_entry):
        url = greenhouse_job["absolute_url"]
        parsed_url = urlparse(url)
        description_html = html_lib.unescape(greenhouse_job.get("content", ""))
        publication_date = greenhouse_job.get("first_published", "")
        expiry_date = greenhouse_job.get("application_deadline") or ""
        metadata_fields = self._greenhouse_metadata_fields(greenhouse_job)

        return {
            "source": sitemap_entry.get("source", "greenhouse"),
            "source_sitemap_url": sitemap_entry["sitemap_url"],
            "source_api_url": sitemap_entry.get("source_api_url", ""),
            "site": parsed_url.netloc,
            "url": url,
            "title": greenhouse_job.get("title", ""),
            "description": self._text_from_html(description_html),
            "description_html": description_html,
            "location": self._greenhouse_location(greenhouse_job),
            "discipline": self._greenhouse_department(greenhouse_job),
            "job_type": metadata_fields.get("Employment Type", ""),
            "salary": metadata_fields.get("Salary", ""),
            "contact_name": "",
            "contact_email": "",
            "job_ref": str(greenhouse_job.get("requisition_id") or greenhouse_job.get("id") or ""),
            "publication_date": publication_date,
            "publication_date_at": self._parse_datetime(publication_date),
            "expiry_date": expiry_date,
            "expiry_date_at": self._parse_datetime(expiry_date),
            "sitemap_lastmod": sitemap_entry.get("sitemap_lastmod", ""),
            "sitemap_lastmod_at": self._parse_datetime(sitemap_entry.get("sitemap_lastmod", "")),
            "apply_url": url,
            "company": sitemap_entry.get("company")
            or greenhouse_job.get("company_name", "")
            or self._company_from_site(parsed_url.netloc),
            "raw_fields": {
                "greenhouse_id": greenhouse_job.get("id"),
                "internal_job_id": greenhouse_job.get("internal_job_id"),
                "language": greenhouse_job.get("language", ""),
                "metadata": metadata_fields,
                "departments": [department.get("name", "") for department in greenhouse_job.get("departments") or []],
                "offices": [office.get("name", "") for office in greenhouse_job.get("offices") or []],
            },
            "scraped_at": datetime.now(timezone.utc),
        }

    def _request_text(self, url):
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                }
            )
        self.log.info("Request: %s", url)
        response = self.session.get(url, timeout=int(self.request_timeout))
        response.raise_for_status()
        return response.text

    def _filter_entries_to_fetch(self, collection, entries):
        existing_lastmods = self._existing_lastmods(collection, [entry["url"] for entry in entries])
        filtered = []
        for entry in entries:
            existing_lastmod = existing_lastmods.get(entry["url"])
            if existing_lastmod is None or existing_lastmod != entry.get("sitemap_lastmod", ""):
                filtered.append(entry)
        self.log.info(
            "Sitemap URLs novas ou alteradas: %s de %s",
            len(filtered),
            len(entries),
        )
        return filtered

    def _existing_lastmods(self, collection, urls):
        existing = {}
        chunk_size = 1000
        for index in range(0, len(urls), chunk_size):
            chunk = urls[index : index + chunk_size]
            for document in collection.find(
                {"url": {"$in": chunk}},
                {"url": 1, "sitemap_lastmod": 1},
            ):
                existing[document["url"]] = document.get("sitemap_lastmod", "")
        return existing

    def _apply_sitemap_limits(self, entries):
        limited = []
        counters = {}
        for entry in entries:
            max_urls = entry.get("max_urls_per_run")
            if not max_urls:
                limited.append(entry)
                continue

            key = entry.get("configured_sitemap_url") or entry["sitemap_url"]
            count = counters.get(key, 0)
            if count >= int(max_urls):
                continue
            counters[key] = count + 1
            limited.append(entry)
        return limited

    def _coerce_sitemap_configs(self):
        configs = []
        if isinstance(self.sitemap_urls, str):
            sitemap_urls = [self.sitemap_urls]
        else:
            sitemap_urls = list(self.sitemap_urls or [])

        for item in sitemap_urls:
            if isinstance(item, str):
                configs.append({"url": item, "configured_url": item})
                continue
            config = dict(item)
            config["configured_url"] = config.get("configured_url") or config["url"]
            configs.append(config)
        return configs

    def _matches_sitemap_filters(self, url, sitemap_config):
        include_regex = sitemap_config.get("include_url_regex")
        if include_regex and not re.search(include_regex, url):
            return False

        exclude_regex = sitemap_config.get("exclude_url_regex")
        if exclude_regex and re.search(exclude_regex, url):
            return False

        return True

    def _parse_table_fields(self, selector):
        fields = {}
        for row in selector.css("#template-job-100 table tr, .template-job-page table tr"):
            cells = row.css("td")
            if len(cells) < 2:
                continue
            key = self._clean_text(" ".join(cells[0].css("::text").getall())).rstrip(":").lower()
            value = self._clean_text(" ".join(cells[1].css("::text").getall()))
            if key and value:
                fields[key] = value
        return fields

    def _parse_job_component_fields(self, selector):
        field_names = {
            "job-component-requisition-identifier": "requisition identifier",
            "job-component-location": "location",
            "job-component-dropdown-field-1": "external posting category",
            "job-component-employment-type": "employment type",
        }
        fields = {}
        for item in selector.css(".job-component-icon-and-text"):
            classes = item.attrib.get("class", "").split()
            key = next((field_names[class_name] for class_name in classes if class_name in field_names), "")
            if not key:
                continue
            value = self._clean_text(" ".join(item.css("span::text").getall()))
            if value:
                fields[key] = value
        return fields

    def _parse_json_ld_job(self, selector):
        for script in selector.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
            except json.JSONDecodeError:
                continue
            candidates = data if isinstance(data, list) else [data]
            for item in candidates:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    return item
        return {}

    def _json_ld_company(self, json_ld):
        organization = json_ld.get("hiringOrganization") if isinstance(json_ld, dict) else None
        if isinstance(organization, dict):
            return organization.get("name", "")
        return ""

    def _json_ld_identifier(self, json_ld):
        identifier = json_ld.get("identifier") if isinstance(json_ld, dict) else None
        if isinstance(identifier, dict):
            return identifier.get("value", "")
        if isinstance(identifier, str):
            return identifier
        return ""

    def _greenhouse_metadata_fields(self, greenhouse_job):
        fields = {}
        for item in greenhouse_job.get("metadata") or []:
            name = item.get("name")
            value = item.get("value")
            if name and value is not None:
                fields[name] = value
        return fields

    def _greenhouse_location(self, greenhouse_job):
        location = greenhouse_job.get("location")
        if isinstance(location, dict):
            return location.get("name", "")
        return ""

    def _greenhouse_department(self, greenhouse_job):
        departments = [
            department.get("name", "")
            for department in greenhouse_job.get("departments") or []
            if department.get("name")
        ]
        return ", ".join(departments)

    def _greenhouse_board_token(self, api_url):
        match = re.search(r"/boards/([^/]+)/jobs", api_url)
        return match.group(1) if match else ""

    def _greenhouse_job_api_url(self, board_token, greenhouse_job):
        job_id = greenhouse_job.get("id")
        if not board_token or not job_id:
            return ""
        return f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs/{job_id}?content=true"

    def _json_ld_location(self, json_ld):
        location = json_ld.get("jobLocation") if isinstance(json_ld, dict) else None
        if isinstance(location, list):
            location = location[0] if location else None
        if not isinstance(location, dict):
            return ""
        address = location.get("address")
        if isinstance(address, dict):
            parts = [
                address.get("addressLocality", ""),
                address.get("addressRegion", ""),
                address.get("addressCountry", ""),
            ]
            return self._clean_text(", ".join(part for part in parts if part))
        return ""

    def _field(self, fields, *names):
        for name in names:
            value = fields.get(name)
            if value:
                return value
        return ""

    def _first_text(self, selector, css_paths):
        for css_path in css_paths:
            value = self._clean_text(" ".join(selector.css(css_path).getall()))
            if value:
                return value
        return ""

    def _first_attr(self, selector, css_paths):
        for css_path in css_paths:
            value = selector.css(css_path).get()
            if value:
                return value
        return ""

    def _first_html(self, selector, css_paths):
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

    def _parse_datetime(self, value):
        if not value:
            return None
        relative = self._parse_relative_datetime(value)
        if relative is not None:
            return relative
        normalized = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            parsed = None
            for date_format in ("%Y-%m-%d", "%d %B %Y %H:%M", "%d %b %Y %H:%M", "%d/%m/%Y"):
                try:
                    parsed = datetime.strptime(value.strip(), date_format)
                    break
                except ValueError:
                    continue
            if parsed is None:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _parse_relative_datetime(self, value):
        normalized = value.strip().lower()
        if normalized in {"today", "just now", "now"}:
            return datetime.now(timezone.utc)
        if normalized == "yesterday":
            return datetime.now(timezone.utc) - timedelta(days=1)

        match = re.search(
            r"(?:over|about|approx\.?|approximately)?\s*(\d+)\s*"
            r"(minute|minutes|min|mins|hour|hours|day|days|week|weeks|month|months|year|years)\s*ago",
            normalized,
        )
        if not match:
            return None

        amount = int(match.group(1))
        unit = match.group(2)
        days_by_unit = {
            "minute": 1 / 1440,
            "minutes": 1 / 1440,
            "min": 1 / 1440,
            "mins": 1 / 1440,
            "hour": 1 / 24,
            "hours": 1 / 24,
            "day": 1,
            "days": 1,
            "week": 7,
            "weeks": 7,
            "month": 30,
            "months": 30,
            "year": 365,
            "years": 365,
        }
        return datetime.now(timezone.utc) - timedelta(days=amount * days_by_unit[unit])

    def _company_from_site(self, netloc):
        host = netloc.split(":")[0]
        if host.startswith("www."):
            host = host[len("www.") :]
        if host.startswith("careers."):
            host = host[len("careers.") :]
        return host.split(".")[0].replace("-", " ").title()
