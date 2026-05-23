import json
import os
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, request

from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from ai.agents.job_tagger import JobTaggingInput, JobTaggingOutput, build_user_prompt, get_agent, get_model_from_env
from pymongo import ASCENDING, DESCENDING


def load_env_file() -> None:
    candidates = [
        Path(__file__).resolve().parents[1] / ".env",
        Path("/opt/airflow/.env"),
    ]
    for path in candidates:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line or line.lstrip().startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        return


class JobLLMTaggingOperator(BaseOperator):
    template_fields = ("mongo_collection",)

    def __init__(
        self,
        mongo_conn_id,
        mongo_db,
        mongo_collection,
        limit=None,
        max_attempts=1,
        model=None,
        max_description_chars=6000,
        fresh_after_days=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.limit = limit
        self.max_attempts = max_attempts
        self.model = model
        self.max_description_chars = max_description_chars
        self.fresh_after_days = fresh_after_days

    def execute(self, context):
        load_env_file()
        env_model = get_model_from_env()
        model = self.model or env_model
        if not model:
            raise ValueError("Modelo não configurado. Defina PYDANTIC_AI_MODEL, OPENAI_MODEL ou AI_MODEL.")
        if self.model:
            os.environ["PYDANTIC_AI_MODEL"] = self.model
        agent = get_agent()

        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        collection = hook.get_collection(self.mongo_collection, self.mongo_db)
        self._ensure_indexes(collection)

        job_ids = self._fetch_unenriched_job_ids(collection)

        stats = {"attempted": 0, "enriched": 0, "failed": 0, "skipped": 0, "found": len(job_ids)}
        if not job_ids:
            self.log.info("Nenhuma vaga nova sem tags LLM em %s.", self.mongo_collection)
            return stats

        self.log.info("Vagas sem tags LLM encontradas em %s: %s", self.mongo_collection, len(job_ids))

        for job_id in job_ids:
            doc = collection.find_one({"_id": job_id})
            if not doc:
                stats["skipped"] += 1
                continue
            if doc.get("llm_tags", {}).get("enriched_at"):
                stats["skipped"] += 1
                continue

            self._tag_one_job(collection, doc, agent, model, stats)

        self.log.info("LLM tagging finalizado: %s", json.dumps(stats, ensure_ascii=False))
        return stats

    def _fetch_unenriched_job_ids(self, collection):
        cursor = (
            collection
            .find(self._query_unenriched_jobs(), {"_id": 1})
            .sort([("first_seen_at", DESCENDING), ("scraped_at", DESCENDING)])
        )
        if self.limit:
            cursor = cursor.limit(int(self.limit))
        return [doc["_id"] for doc in cursor]

    def _tag_one_job(self, collection, doc, agent, model, stats):
        stats["attempted"] += 1
        now = datetime.now(timezone.utc)
        collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "llm_tagging_status": "processing",
                    "llm_tagging_requested_at": now,
                    "llm_tagging_model": model,
                },
                "$inc": {"llm_tagging_attempts": 1},
            },
        )

        try:
            prompt = build_user_prompt(self._build_job_input(doc))
            result = agent.run_sync(prompt)
            output: JobTaggingOutput = result.output
            payload = output.model_dump(mode="json")
            payload["enriched_at"] = datetime.now(timezone.utc)
            payload["model"] = model

            collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "llm_tags": payload,
                        "llm_tagging_status": "completed",
                        "llm_tagging_completed_at": payload["enriched_at"],
                    },
                    "$unset": {"llm_tagging_last_error": ""},
                },
            )
            stats["enriched"] += 1
        except Exception as exc:
            self.log.exception("Falha ao gerar tags LLM para vaga url=%s", doc.get("url", ""))
            collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "llm_tagging_status": "failed",
                        "llm_tagging_failed_at": datetime.now(timezone.utc),
                        "llm_tagging_last_error": str(exc)[:1000],
                    }
                },
            )
            stats["failed"] += 1

    def _ensure_indexes(self, collection):
        collection.create_index([("llm_tagging_status", ASCENDING)])
        collection.create_index([("llm_tags.enriched_at", DESCENDING)])
        collection.create_index([("first_seen_at", DESCENDING)])

    def _query_unenriched_jobs(self):
        query = {
            "url": {"$exists": True, "$ne": ""},
            "$and": [
                {
                    "$or": [
                        {"llm_tags.enriched_at": {"$exists": False}},
                        {"llm_tags": {"$exists": False}},
                    ]
                },
                {
                    "$or": [
                        {"llm_tagging_attempts": {"$exists": False}},
                        {"llm_tagging_attempts": {"$lt": int(self.max_attempts)}},
                    ]
                },
                {
                    "$or": [
                        {"llm_tagging_status": {"$exists": False}},
                        {"llm_tagging_status": {"$in": ["pending", "failed"]}},
                    ]
                },
            ],
        }
        if self.fresh_after_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(self.fresh_after_days))
            query["$and"].append(
                {
                    "$or": [
                        {"last_seen_at": {"$gte": cutoff}},
                        {
                            "last_seen_at": {"$exists": False},
                            "scraped_at": {"$gte": cutoff},
                        },
                    ]
                }
            )
        return query

    def _build_job_input(self, doc):
        description = (doc.get("description") or "")[: int(self.max_description_chars)]
        return JobTaggingInput(
            title=doc.get("title", ""),
            company=doc.get("company", ""),
            location=doc.get("location", ""),
            url=doc.get("url", ""),
            source=doc.get("source", ""),
            keyword=doc.get("keyword", ""),
            job_type=doc.get("job_type", ""),
            discipline=doc.get("discipline", ""),
            salary=doc.get("salary", ""),
            publication_date=str(doc.get("publication_date", "")),
            posted_text=doc.get("posted_text", ""),
            description=description,
        )


class JobStaleCleanupOperator(BaseOperator):
    template_fields = ("mongo_collection",)

    def __init__(
        self,
        mongo_conn_id,
        mongo_db,
        mongo_collection,
        stale_after_days=2,
        dry_run=False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.stale_after_days = stale_after_days
        self.dry_run = dry_run

    def execute(self, context):
        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        collection = hook.get_collection(self.mongo_collection, self.mongo_db)
        collection.create_index([("last_seen_at", ASCENDING)])
        collection.create_index([("scraped_at", ASCENDING)])

        cutoff = datetime.now(timezone.utc) - timedelta(days=int(self.stale_after_days))
        query = {
            "$or": [
                {"last_seen_at": {"$lt": cutoff}},
                {
                    "last_seen_at": {"$exists": False},
                    "scraped_at": {"$lt": cutoff},
                },
            ]
        }
        matched = collection.count_documents(query)
        if self.dry_run:
            self.log.info(
                "Dry run cleanup | collection=%s | stale_after_days=%s | matched=%s",
                self.mongo_collection,
                self.stale_after_days,
                matched,
            )
            return {"matched": matched, "deleted": 0, "dry_run": True}

        result = collection.delete_many(query)
        self.log.info(
            "Cleanup executado | collection=%s | stale_after_days=%s | deleted=%s",
            self.mongo_collection,
            self.stale_after_days,
            result.deleted_count,
        )
        return {"matched": matched, "deleted": result.deleted_count, "dry_run": False}


class JobGoogleEmbeddingOperator(BaseOperator):
    template_fields = ("mongo_collection",)

    def __init__(
        self,
        mongo_conn_id,
        mongo_db,
        mongo_collection,
        limit=None,
        max_attempts=1,
        model=None,
        output_dimensionality=None,
        fresh_after_days=None,
        max_description_chars=12000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.limit = limit
        self.max_attempts = max_attempts
        self.model = model
        self.output_dimensionality = output_dimensionality
        self.fresh_after_days = fresh_after_days
        self.max_description_chars = max_description_chars

    def execute(self, context):
        load_env_file()
        api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("Google API key não configurada. Defina GOOGLE_API_KEY ou GEMINI_API_KEY.")
        model = self.model or os.getenv("GOOGLE_EMBEDDING_MODEL") or "gemini-embedding-001"
        output_dimensionality = self.output_dimensionality
        if output_dimensionality is None:
            output_dimensionality = int(os.getenv("GOOGLE_EMBEDDING_DIMENSIONALITY", "768"))

        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        collection = hook.get_collection(self.mongo_collection, self.mongo_db)
        self._ensure_indexes(collection)

        job_ids = self._fetch_jobs_for_embedding(collection)
        stats = {"attempted": 0, "embedded": 0, "failed": 0, "skipped": 0, "found": len(job_ids)}
        if not job_ids:
            self.log.info("Nenhuma vaga pendente de embedding em %s.", self.mongo_collection)
            return stats

        self.log.info("Vagas pendentes de embedding em %s: %s", self.mongo_collection, len(job_ids))

        for job_id in job_ids:
            doc = collection.find_one({"_id": job_id})
            if not doc:
                stats["skipped"] += 1
                continue

            embedding_text = self._build_embedding_text(doc)
            if not embedding_text:
                stats["skipped"] += 1
                continue

            self._embed_one_job(collection, doc, api_key, model, output_dimensionality, embedding_text, stats)

        self.log.info("Embeddings finalizados: %s", json.dumps(stats, ensure_ascii=False))
        return stats

    def _fetch_jobs_for_embedding(self, collection):
        cursor = (
            collection
            .find(self._query_jobs_for_embedding(), {"_id": 1})
            .sort([("first_seen_at", DESCENDING), ("scraped_at", DESCENDING)])
        )
        if self.limit:
            cursor = cursor.limit(int(self.limit))
        return [doc["_id"] for doc in cursor]

    def _embed_one_job(self, collection, doc, api_key, model, output_dimensionality, embedding_text, stats):
        stats["attempted"] += 1
        now = datetime.now(timezone.utc)
        collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "job_embedding_status": "processing",
                    "job_embedding_requested_at": now,
                    "job_embedding_model": model,
                },
                "$inc": {"job_embedding_attempts": 1},
            },
        )

        try:
            values = self._generate_embedding(
                api_key=api_key,
                model=model,
                output_dimensionality=output_dimensionality,
                content=embedding_text,
                title=doc.get("title", ""),
            )
            payload = {
                "values": values,
                "dimensions": len(values),
                "model": model,
                "task_type": "RETRIEVAL_DOCUMENT",
                "enriched_at": datetime.now(timezone.utc),
            }
            collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "job_embedding": payload,
                        "job_embedding_status": "completed",
                        "job_embedding_completed_at": payload["enriched_at"],
                    },
                    "$unset": {"job_embedding_last_error": ""},
                },
            )
            stats["embedded"] += 1
        except Exception as exc:
            self.log.exception("Falha ao gerar embedding para vaga url=%s", doc.get("url", ""))
            collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "job_embedding_status": "failed",
                        "job_embedding_failed_at": datetime.now(timezone.utc),
                        "job_embedding_last_error": str(exc)[:1000],
                    }
                },
            )
            stats["failed"] += 1

    def _generate_embedding(self, api_key, model, output_dimensionality, content, title=""):
        payload = {
            "task_type": "RETRIEVAL_DOCUMENT",
            "content": {"parts": [{"text": content}]},
        }
        if title:
            payload["title"] = title[:500]
        if output_dimensionality:
            payload["output_dimensionality"] = int(output_dimensionality)

        req = request.Request(
            url=f"https://generativelanguage.googleapis.com/v1beta/models/{model}:embedContent",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": api_key,
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=60) as response:
                raw = response.read().decode("utf-8")
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Google embedding request failed with HTTP {exc.code}: {raw[:500]}") from exc

        data = json.loads(raw)
        embeddings = data.get("embeddings") or ([data["embedding"]] if data.get("embedding") else [])
        if not embeddings or "values" not in embeddings[0]:
            raise RuntimeError(f"Resposta inesperada da API de embeddings: {raw[:500]}")

        values = [float(value) for value in embeddings[0]["values"]]
        if model == "gemini-embedding-001" and output_dimensionality and int(output_dimensionality) != 3072:
            values = self._normalize_vector(values)
        return values

    def _normalize_vector(self, values):
        norm = math.sqrt(sum(value * value for value in values))
        if norm == 0:
            return values
        return [value / norm for value in values]

    def _ensure_indexes(self, collection):
        collection.create_index([("job_embedding_status", ASCENDING)])
        collection.create_index([("job_embedding.enriched_at", DESCENDING)])
        collection.create_index([("first_seen_at", DESCENDING)])

    def _query_jobs_for_embedding(self):
        query = {
            "url": {"$exists": True, "$ne": ""},
            "$and": [
                {
                    "$or": [
                        {"llm_tags.enriched_at": {"$exists": True}},
                        {"llm_tags": {"$exists": True}},
                    ]
                },
                {
                    "$or": [
                        {"job_embedding_attempts": {"$exists": False}},
                        {"job_embedding_attempts": {"$lt": int(self.max_attempts)}},
                    ]
                },
                {
                    "$or": [
                        {"job_embedding.enriched_at": {"$exists": False}},
                        {"job_embedding_status": {"$exists": False}},
                        {"job_embedding_status": {"$in": ["pending", "failed"]}},
                    ]
                },
            ],
        }
        if self.fresh_after_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(self.fresh_after_days))
            query["$and"].append(
                {
                    "$or": [
                        {"last_seen_at": {"$gte": cutoff}},
                        {
                            "last_seen_at": {"$exists": False},
                            "scraped_at": {"$gte": cutoff},
                        },
                    ]
                }
            )
        return query

    def _build_embedding_text(self, doc):
        llm_tags = doc.get("llm_tags") or {}
        description = (doc.get("description") or "")[: int(self.max_description_chars)]
        chunks = [
            f"Title: {doc.get('title', '').strip()}",
            f"Company: {doc.get('company', '').strip()}",
            f"Location: {doc.get('location', '').strip()}",
            f"Source: {doc.get('source', '').strip()}",
            f"Keyword: {doc.get('keyword', '').strip()}",
            f"Job type: {doc.get('job_type', '').strip()}",
            f"Discipline: {doc.get('discipline', '').strip()}",
            f"Salary: {doc.get('salary', '').strip()}",
            f"Publication date: {str(doc.get('publication_date', '')).strip()}",
            f"Posted text: {doc.get('posted_text', '').strip()}",
            f"LLM summary: {llm_tags.get('summary', '').strip()}",
            f"Tags: {', '.join(llm_tags.get('tags') or [])}",
            f"Skills: {', '.join(llm_tags.get('skills') or [])}",
            f"Tools: {', '.join(llm_tags.get('tools') or [])}",
            f"Role family: {llm_tags.get('role_family', '').strip()}",
            f"Seniority: {llm_tags.get('seniority', '').strip()}",
            f"Work mode: {llm_tags.get('work_mode', '').strip()}",
            f"Regions: {', '.join(llm_tags.get('regions') or [])}",
            f"Countries: {', '.join(llm_tags.get('countries') or [])}",
            f"Cities: {', '.join(llm_tags.get('cities') or [])}",
            f"Languages: {', '.join(llm_tags.get('languages') or [])}",
            f"Contract type: {llm_tags.get('contract_type', '').strip()}",
            f"Description:\n{description.strip()}",
        ]
        text = "\n".join(part for part in chunks if not part.endswith(": "))
        return text.strip()
