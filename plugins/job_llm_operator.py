import json
import os
import math
import time
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, request

from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from ai.agents.job_tagger import JobTaggingInput, JobTaggingOutput, build_user_prompt, get_agent, get_model_from_env
from job_tags import build_job_tags
from pymongo import ASCENDING, DESCENDING


PROGRESS_LOG_EVERY = 25


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

        self.log.info(
            "LLM tagging iniciado | collection=%s | batch_size=%s | limit=%s | model=%s",
            self.mongo_collection,
            len(job_ids),
            self.limit or "none",
            model,
        )

        started_at = time.monotonic()
        for index, job_id in enumerate(job_ids, start=1):
            doc = collection.find_one({"_id": job_id})
            if not doc:
                stats["skipped"] += 1
                self._log_llm_progress(index, len(job_ids), stats, started_at)
                continue
            if doc.get("llm_tags", {}).get("enriched_at"):
                stats["skipped"] += 1
                self._log_llm_progress(index, len(job_ids), stats, started_at)
                continue

            self.log.info(
                "LLM tagging vaga %s/%s | source=%s | company=%s | title=%s | url=%s",
                index,
                len(job_ids),
                doc.get("source", ""),
                self._short(doc.get("company", "")),
                self._short(doc.get("title", "")),
                doc.get("url", ""),
            )
            self._tag_one_job(collection, doc, agent, model, stats)
            self._log_llm_progress(index, len(job_ids), stats, started_at)

        self.log.info("LLM tagging finalizado: %s", json.dumps(stats, ensure_ascii=False))
        return stats

    def _log_llm_progress(self, index, total, stats, started_at):
        if index == total or index % PROGRESS_LOG_EVERY == 0:
            elapsed = max(time.monotonic() - started_at, 0.001)
            self.log.info(
                "LLM tagging progresso | processed=%s/%s | enriched=%s | failed=%s | skipped=%s | elapsed_s=%.1f | avg_s_per_item=%.2f",
                index,
                total,
                stats["enriched"],
                stats["failed"],
                stats["skipped"],
                elapsed,
                elapsed / max(index, 1),
            )

    def _short(self, value, limit=120):
        value = str(value or "").strip()
        if len(value) <= limit:
            return value
        return value[: limit - 3] + "..."

    def _fetch_unenriched_job_ids(self, collection):
        cursor = (
            collection
            .find(self._query_unenriched_jobs(), {"_id": 1})
            .sort([("last_seen_at", DESCENDING), ("scraped_at", DESCENDING)])
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
        collection.create_index([("last_seen_at", DESCENDING), ("scraped_at", DESCENDING)])

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

        self.log.info(
            "Embedding iniciado | collection=%s | batch_size=%s | limit=%s | model=%s | dimensions=%s",
            self.mongo_collection,
            len(job_ids),
            self.limit or "none",
            model,
            output_dimensionality,
        )

        started_at = time.monotonic()
        for index, job_id in enumerate(job_ids, start=1):
            doc = collection.find_one({"_id": job_id})
            if not doc:
                stats["skipped"] += 1
                self._log_embedding_progress(index, len(job_ids), stats, started_at)
                continue

            embedding_text = self._build_embedding_text(doc)
            if not embedding_text:
                stats["skipped"] += 1
                self._log_embedding_progress(index, len(job_ids), stats, started_at)
                continue

            self.log.info(
                "Embedding vaga %s/%s | source=%s | company=%s | title=%s | chars=%s | url=%s",
                index,
                len(job_ids),
                doc.get("source", ""),
                self._short(doc.get("company", "")),
                self._short(doc.get("title", "")),
                len(embedding_text),
                doc.get("url", ""),
            )
            self._embed_one_job(collection, doc, api_key, model, output_dimensionality, embedding_text, stats)
            self._log_embedding_progress(index, len(job_ids), stats, started_at)

        self.log.info("Embeddings finalizados: %s", json.dumps(stats, ensure_ascii=False))
        return stats

    def _log_embedding_progress(self, index, total, stats, started_at):
        if index == total or index % PROGRESS_LOG_EVERY == 0:
            elapsed = max(time.monotonic() - started_at, 0.001)
            self.log.info(
                "Embedding progresso | processed=%s/%s | embedded=%s | failed=%s | skipped=%s | elapsed_s=%.1f | avg_s_per_item=%.2f",
                index,
                total,
                stats["embedded"],
                stats["failed"],
                stats["skipped"],
                elapsed,
                elapsed / max(index, 1),
            )

    def _short(self, value, limit=120):
        value = str(value or "").strip()
        if len(value) <= limit:
            return value
        return value[: limit - 3] + "..."

    def _fetch_jobs_for_embedding(self, collection):
        cursor = (
            collection
            .find(self._query_jobs_for_embedding(), {"_id": 1})
            .sort([("last_seen_at", DESCENDING), ("scraped_at", DESCENDING)])
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
            tags = doc.get("tags")
            if not isinstance(tags, list):
                tags = build_job_tags(doc)
                collection.update_one({"_id": doc["_id"]}, {"$set": {"tags": tags}})
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

    def _generate_embedding(self, api_key, model, output_dimensionality, content, title="", task_type="RETRIEVAL_DOCUMENT"):
        payload = {
            "task_type": task_type,
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
        collection.create_index([("last_seen_at", DESCENDING), ("scraped_at", DESCENDING)])

    def _query_jobs_for_embedding(self):
        query = {
            "url": {"$exists": True, "$ne": ""},
            "$and": [
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
        tags = doc.get("tags")
        if not isinstance(tags, list):
            tags = build_job_tags(doc)
        description = self._embedding_field_text(doc.get("description"))[: int(self.max_description_chars)]
        chunks = [
            f"Title: {self._embedding_field_text(doc.get('title'))}",
            f"Company: {self._embedding_field_text(doc.get('company'))}",
            f"Location: {self._embedding_field_text(doc.get('location'))}",
            f"Source: {self._embedding_field_text(doc.get('source'))}",
            f"Keyword: {self._embedding_field_text(doc.get('keyword'))}",
            f"Job type: {self._embedding_field_text(doc.get('job_type'))}",
            f"Discipline: {self._embedding_field_text(doc.get('discipline'))}",
            f"Salary: {self._embedding_field_text(doc.get('salary'))}",
            f"Publication date: {self._embedding_field_text(doc.get('publication_date'))}",
            f"Posted text: {self._embedding_field_text(doc.get('posted_text'))}",
            f"Tags: {', '.join(self._embedding_field_text(tag) for tag in (tags or []) if self._embedding_field_text(tag))}",
            f"Description:\n{description.strip()}",
        ]
        text = "\n".join(part for part in chunks if not part.endswith(": "))
        return text.strip()

    def _embedding_field_text(self, value):
        if value is None:
            return ""
        if isinstance(value, list):
            return ", ".join(
                text
                for text in (self._embedding_field_text(item) for item in value)
                if text
            )
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False, default=str)
        return str(value).strip()


class ResumeGoogleEmbeddingOperator(JobGoogleEmbeddingOperator):
    template_fields = ("resume_path", "profile_collection", "embedding_cache_path")

    def __init__(
        self,
        mongo_conn_id,
        mongo_db,
        profile_collection,
        profile_id,
        resume_path,
        embedding_cache_path=None,
        model=None,
        output_dimensionality=None,
        force_refresh=False,
        **kwargs,
    ):
        super().__init__(
            mongo_conn_id=mongo_conn_id,
            mongo_db=mongo_db,
            mongo_collection=profile_collection,
            limit=None,
            max_attempts=1,
            model=model,
            output_dimensionality=output_dimensionality,
            **kwargs,
        )
        self.profile_collection = profile_collection
        self.profile_id = profile_id
        self.resume_path = resume_path
        self.embedding_cache_path = embedding_cache_path
        self.force_refresh = force_refresh

    def execute(self, context):
        load_env_file()
        api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("Google API key não configurada. Defina GOOGLE_API_KEY ou GEMINI_API_KEY.")

        model = self.model or os.getenv("GOOGLE_EMBEDDING_MODEL") or "gemini-embedding-001"
        output_dimensionality = self.output_dimensionality
        if output_dimensionality is None:
            output_dimensionality = int(os.getenv("GOOGLE_EMBEDDING_DIMENSIONALITY", "768"))

        resume_path = Path(self.resume_path)
        if not resume_path.exists():
            raise FileNotFoundError(f"CV não encontrado em {resume_path}")

        cv_markdown = resume_path.read_text(encoding="utf-8").strip()
        if not cv_markdown:
            raise ValueError(f"CV vazio em {resume_path}")

        cv_sha256 = hashlib.sha256(cv_markdown.encode("utf-8")).hexdigest()
        cv_tags = build_job_tags({"description": cv_markdown})
        now = datetime.now(timezone.utc)

        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        collection = hook.get_collection(self.profile_collection, self.mongo_db)
        self._ensure_resume_indexes(collection)

        existing = collection.find_one({"_id": self.profile_id}) or {}
        existing_embedding = existing.get("resume_embedding") or {}
        cached_embedding = self._load_cached_embedding(cv_sha256, model, output_dimensionality)
        should_refresh = bool(self.force_refresh)

        if not should_refresh:
            should_refresh = (
                (
                    existing.get("cv_sha256") != cv_sha256
                    or existing_embedding.get("model") != model
                    or int(existing_embedding.get("dimensions") or 0) != int(output_dimensionality)
                    or not isinstance(existing_embedding.get("values"), list)
                    or not existing_embedding.get("values")
                    or existing.get("resume_embedding_status") != "completed"
                )
                and not cached_embedding
            )

        collection.update_one(
            {"_id": self.profile_id},
            {
                "$set": {
                    "profile_type": "resume",
                    "cv_markdown": cv_markdown,
                    "cv_sha256": cv_sha256,
                    "cv_tags": cv_tags,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

        if not should_refresh:
            self.log.info(
                "Embedding do CV já está atualizado | profile_id=%s | model=%s | dimensions=%s",
                self.profile_id,
                existing_embedding.get("model"),
                existing_embedding.get("dimensions"),
            )
            return {
                "profile_id": self.profile_id,
                "embedded": False,
                "skipped": True,
                "cv_sha256": cv_sha256,
                "cv_tags": cv_tags,
                "dimensions": int(existing_embedding.get("dimensions") or 0),
                "model": existing_embedding.get("model") or model,
            }

        if cached_embedding:
            payload = {
                "values": cached_embedding["values"],
                "dimensions": int(cached_embedding["dimensions"]),
                "model": cached_embedding["model"],
                "task_type": "RETRIEVAL_QUERY",
                "enriched_at": datetime.now(timezone.utc),
            }
            collection.update_one(
                {"_id": self.profile_id},
                {
                    "$set": {
                        "resume_embedding": payload,
                        "resume_embedding_status": "completed",
                        "resume_embedding_completed_at": payload["enriched_at"],
                    },
                    "$unset": {"resume_embedding_last_error": ""},
                },
            )
            self.log.info(
                "Embedding do CV carregado do cache local | profile_id=%s | model=%s | dimensions=%s",
                self.profile_id,
                payload["model"],
                payload["dimensions"],
            )
            return {
                "profile_id": self.profile_id,
                "embedded": False,
                "skipped": True,
                "loaded_from_cache": True,
                "cv_sha256": cv_sha256,
                "cv_tags": cv_tags,
                "dimensions": payload["dimensions"],
                "model": payload["model"],
            }

        collection.update_one(
            {"_id": self.profile_id},
            {
                "$set": {
                    "resume_embedding_status": "processing",
                    "resume_embedding_requested_at": now,
                    "resume_embedding_model": model,
                },
                "$inc": {"resume_embedding_attempts": 1},
            },
        )

        try:
            values = self._generate_embedding(
                api_key=api_key,
                model=model,
                output_dimensionality=output_dimensionality,
                content=cv_markdown,
                task_type="RETRIEVAL_QUERY",
            )
            payload = {
                "values": values,
                "dimensions": len(values),
                "model": model,
                "task_type": "RETRIEVAL_QUERY",
                "enriched_at": datetime.now(timezone.utc),
            }
            self._write_cached_embedding(cv_sha256=cv_sha256, payload=payload)
            collection.update_one(
                {"_id": self.profile_id},
                {
                    "$set": {
                        "resume_embedding": payload,
                        "resume_embedding_status": "completed",
                        "resume_embedding_completed_at": payload["enriched_at"],
                    },
                    "$unset": {"resume_embedding_last_error": ""},
                },
            )
            self.log.info(
                "Embedding do CV atualizado | profile_id=%s | model=%s | dimensions=%s",
                self.profile_id,
                model,
                len(values),
            )
            return {
                "profile_id": self.profile_id,
                "embedded": True,
                "skipped": False,
                "cv_sha256": cv_sha256,
                "cv_tags": cv_tags,
                "dimensions": len(values),
                "model": model,
            }
        except Exception as exc:
            collection.update_one(
                {"_id": self.profile_id},
                {
                    "$set": {
                        "resume_embedding_status": "failed",
                        "resume_embedding_failed_at": datetime.now(timezone.utc),
                        "resume_embedding_last_error": str(exc)[:1000],
                    }
                },
            )
            raise

    def _ensure_resume_indexes(self, collection):
        collection.create_index([("profile_type", ASCENDING)])
        collection.create_index([("cv_sha256", ASCENDING)])
        collection.create_index([("resume_embedding_status", ASCENDING)])
        collection.create_index([("resume_embedding.enriched_at", DESCENDING)])

    def _load_cached_embedding(self, cv_sha256, model, output_dimensionality):
        if not self.embedding_cache_path:
            return None
        cache_path = Path(self.embedding_cache_path)
        if not cache_path.exists():
            return None
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        values = payload.get("values")
        if (
            payload.get("cv_sha256") != cv_sha256
            or payload.get("model") != model
            or int(payload.get("dimensions") or 0) != int(output_dimensionality)
            or not isinstance(values, list)
            or not values
        ):
            return None
        return payload

    def _write_cached_embedding(self, cv_sha256, payload):
        if not self.embedding_cache_path:
            return
        cache_path = Path(self.embedding_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_payload = {
            "cv_sha256": cv_sha256,
            "model": payload["model"],
            "dimensions": int(payload["dimensions"]),
            "task_type": payload["task_type"],
            "values": payload["values"],
        }
        cache_path.write_text(json.dumps(cache_payload, ensure_ascii=False), encoding="utf-8")


class ResumeVectorMatchOperator(BaseOperator):
    template_fields = ("jobs_collection", "profile_collection")

    def __init__(
        self,
        mongo_conn_id,
        mongo_db,
        jobs_collection,
        profile_collection,
        profile_id,
        limit=10,
        num_candidates=100,
        search_index="job_embedding_vector",
        recent_days=30,
        only_active=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.jobs_collection = jobs_collection
        self.profile_collection = profile_collection
        self.profile_id = profile_id
        self.limit = limit
        self.num_candidates = num_candidates
        self.search_index = search_index
        self.recent_days = recent_days
        self.only_active = only_active

    def execute(self, context):
        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        profile_collection = hook.get_collection(self.profile_collection, self.mongo_db)
        jobs_collection = hook.get_collection(self.jobs_collection, self.mongo_db)

        profile_doc = profile_collection.find_one({"_id": self.profile_id}) or {}
        resume_embedding = (profile_doc.get("resume_embedding") or {}).get("values") or []
        if not resume_embedding:
            raise ValueError(f"Embedding do CV não encontrado para profile_id={self.profile_id}")

        profile_collection.create_index([("last_match_run_at", DESCENDING)])

        matches = self._run_vector_search(jobs_collection, resume_embedding)
        signature = self._build_signature(matches)
        previous_signature = profile_doc.get("last_match_signature")
        changed = bool(matches) and signature != previous_signature
        now = datetime.now(timezone.utc)

        profile_collection.update_one(
            {"_id": self.profile_id},
            {
                "$set": {
                    "last_match_run_at": now,
                    "last_match_results": matches,
                    "last_match_signature": signature,
                    "last_match_changed": changed,
                    "last_match_count": len(matches),
                }
            },
        )

        self.log.info(
            "Busca vetorial finalizada | matches=%s | changed=%s | search_index=%s",
            len(matches),
            changed,
            self.search_index,
        )
        return {
            "matches": matches,
            "changed": changed,
            "match_count": len(matches),
            "signature": signature,
        }

    def _run_vector_search(self, collection, query_vector):
        if not self._has_search_index(collection):
            self.log.warning(
                "Search index %s nao encontrado em %s. Usando fallback local por cosseno.",
                self.search_index,
                self.jobs_collection,
            )
            return self._run_local_cosine_search(collection, query_vector)
        try:
            results = self._run_vector_search_pipeline(collection, query_vector)
            if results:
                return results
            self.log.warning(
                "$vectorSearch retornou 0 resultados com index=%s. Usando fallback local por cosseno.",
                self.search_index,
            )
            return self._run_local_cosine_search(collection, query_vector)
        except Exception as exc:
            self.log.warning(
                "Falha no $vectorSearch com index=%s. Usando fallback local por cosseno. erro=%s",
                self.search_index,
                str(exc)[:500],
            )
            return self._run_local_cosine_search(collection, query_vector)

    def _has_search_index(self, collection):
        try:
            indexes = list(collection.list_search_indexes())
        except Exception as exc:
            self.log.warning(
                "Nao foi possivel listar search indexes em %s. Usando fallback local por cosseno. erro=%s",
                self.jobs_collection,
                str(exc)[:500],
            )
            return False
        for index in indexes:
            if index.get("name") == self.search_index:
                return True
        return False

    def _run_vector_search_pipeline(self, collection, query_vector):
        pipeline = [
            {
                "$vectorSearch": {
                    "index": self.search_index,
                    "path": "job_embedding.values",
                    "queryVector": query_vector,
                    "numCandidates": max(int(self.num_candidates), int(self.limit)),
                    "limit": int(self.limit),
                    "filter": self._build_vector_filter(),
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "title": 1,
                    "company": 1,
                    "location": 1,
                    "url": 1,
                    "source": 1,
                    "keyword": 1,
                    "tags": 1,
                    "active": 1,
                    "posted_text": 1,
                    "job_type": 1,
                    "salary": 1,
                    "publication_date": 1,
                    "last_seen_at": 1,
                    "scraped_at": 1,
                    "vector_score": {"$meta": "vectorSearchScore"},
                }
            },
            {"$sort": {"vector_score": -1, "last_seen_at": -1, "scraped_at": -1}},
        ]
        docs = list(collection.aggregate(pipeline))
        return [self._serialize_match(doc, score_key="vector_score", search_method="vectorSearch") for doc in docs]

    def _run_local_cosine_search(self, collection, query_vector):
        cursor = (
            collection
            .find(self._build_local_query())
            .sort([("last_seen_at", DESCENDING), ("scraped_at", DESCENDING)])
            .limit(max(int(self.num_candidates) * 5, 500))
        )

        scored = []
        query_norm = self._normalize_vector(query_vector)
        for doc in cursor:
            embedding = ((doc.get("job_embedding") or {}).get("values")) or []
            if not isinstance(embedding, list) or not embedding:
                continue
            score = self._cosine_similarity(query_norm, self._normalize_vector(embedding))
            if score is None:
                continue
            doc["vector_score"] = score
            scored.append(doc)

        scored.sort(
            key=lambda item: (
                float(item.get("vector_score") or 0.0),
                item.get("last_seen_at") or datetime.min.replace(tzinfo=timezone.utc),
                item.get("scraped_at") or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )
        top_docs = scored[: int(self.limit)]
        return [self._serialize_match(doc, score_key="vector_score", search_method="local_cosine") for doc in top_docs]

    def _build_vector_filter(self):
        query = {}
        if self.only_active:
            query["active"] = True
        if self.recent_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(self.recent_days))
            query["last_seen_at"] = {"$gte": cutoff}
        return query

    def _build_local_query(self):
        query = {
            "url": {"$exists": True, "$ne": ""},
            "job_embedding.values": {"$exists": True},
        }
        if self.only_active:
            query["active"] = True
        if self.recent_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(self.recent_days))
            query["$or"] = [
                {"last_seen_at": {"$gte": cutoff}},
                {
                    "last_seen_at": {"$exists": False},
                    "scraped_at": {"$gte": cutoff},
                },
            ]
        return query

    def _serialize_match(self, doc, score_key, search_method):
        return {
            "_id": str(doc.get("_id", "")),
            "title": str(doc.get("title", "")).strip(),
            "company": str(doc.get("company", "")).strip(),
            "location": str(doc.get("location", "")).strip(),
            "url": str(doc.get("url", "")).strip(),
            "source": str(doc.get("source", "")).strip(),
            "keyword": str(doc.get("keyword", "")).strip(),
            "job_type": str(doc.get("job_type", "")).strip(),
            "salary": str(doc.get("salary", "")).strip(),
            "posted_text": str(doc.get("posted_text", "")).strip(),
            "publication_date": self._serialize_value(doc.get("publication_date")),
            "last_seen_at": self._serialize_value(doc.get("last_seen_at")),
            "scraped_at": self._serialize_value(doc.get("scraped_at")),
            "tags": doc.get("tags") if isinstance(doc.get("tags"), list) else [],
            "active": bool(doc.get("active", False)),
            "score": round(float(doc.get(score_key) or 0.0), 6),
            "search_method": search_method,
        }

    def _serialize_value(self, value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, dict):
            return {key: self._serialize_value(item) for key, item in value.items()}
        return value

    def _normalize_vector(self, values):
        if not values:
            return values
        norm = math.sqrt(sum(float(value) * float(value) for value in values))
        if norm == 0:
            return [float(value) for value in values]
        return [float(value) / norm for value in values]

    def _cosine_similarity(self, left, right):
        if not left or not right or len(left) != len(right):
            return None
        return sum(float(a) * float(b) for a, b in zip(left, right))

    def _build_signature(self, matches):
        if not matches:
            return ""
        parts = [f"{item['_id']}:{item['score']:.4f}" for item in matches]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


class ResumeTagMatchOperator(BaseOperator):
    template_fields = ("jobs_collection", "profile_collection")

    def __init__(
        self,
        mongo_conn_id,
        mongo_db,
        jobs_collection,
        profile_collection,
        profile_id,
        limit=10,
        recent_days=30,
        only_active=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.jobs_collection = jobs_collection
        self.profile_collection = profile_collection
        self.profile_id = profile_id
        self.limit = limit
        self.recent_days = recent_days
        self.only_active = only_active

    def execute(self, context):
        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        profile_collection = hook.get_collection(self.profile_collection, self.mongo_db)
        jobs_collection = hook.get_collection(self.jobs_collection, self.mongo_db)

        profile_doc = profile_collection.find_one({"_id": self.profile_id}) or {}
        cv_tags = profile_doc.get("cv_tags") if isinstance(profile_doc.get("cv_tags"), list) else []
        cv_tags = [str(tag).strip() for tag in cv_tags if str(tag).strip()]
        if not cv_tags:
            raise ValueError(f"Tags do CV não encontradas para profile_id={self.profile_id}")

        jobs_collection.create_index([("tags", ASCENDING)])
        profile_collection.create_index([("last_tag_match_run_at", DESCENDING)])

        pipeline = [
            {"$match": self._build_match_query(cv_tags)},
            {
                "$project": {
                    "_id": 1,
                    "title": 1,
                    "company": 1,
                    "location": 1,
                    "url": 1,
                    "source": 1,
                    "keyword": 1,
                    "tags": 1,
                    "active": 1,
                    "posted_text": 1,
                    "job_type": 1,
                    "salary": 1,
                    "publication_date": 1,
                    "last_seen_at": 1,
                    "scraped_at": 1,
                    "matched_tags": {"$setIntersection": ["$tags", cv_tags]},
                }
            },
            {
                "$addFields": {
                    "tag_match_count": {"$size": "$matched_tags"},
                }
            },
            {"$sort": {"tag_match_count": -1, "last_seen_at": -1, "scraped_at": -1}},
            {"$limit": int(self.limit)},
        ]
        docs = list(jobs_collection.aggregate(pipeline))
        matches = [self._serialize_match(doc) for doc in docs]

        signature = self._build_signature(matches)
        previous_signature = profile_doc.get("last_tag_match_signature")
        changed = bool(matches) and signature != previous_signature
        now = datetime.now(timezone.utc)

        profile_collection.update_one(
            {"_id": self.profile_id},
            {
                "$set": {
                    "last_tag_match_run_at": now,
                    "last_tag_match_results": matches,
                    "last_tag_match_signature": signature,
                    "last_tag_match_changed": changed,
                    "last_tag_match_count": len(matches),
                }
            },
        )

        self.log.info(
            "Busca por tags finalizada | matches=%s | changed=%s | cv_tags=%s",
            len(matches),
            changed,
            len(cv_tags),
        )
        return {
            "cv_tags": cv_tags,
            "matches": matches,
            "changed": changed,
            "match_count": len(matches),
            "signature": signature,
        }

    def _build_match_query(self, cv_tags):
        query = {
            "url": {"$exists": True, "$ne": ""},
            "tags": {"$in": cv_tags},
        }
        if self.only_active:
            query["active"] = True
        if self.recent_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(self.recent_days))
            query["$or"] = [
                {"last_seen_at": {"$gte": cutoff}},
                {
                    "last_seen_at": {"$exists": False},
                    "scraped_at": {"$gte": cutoff},
                },
            ]
        return query

    def _serialize_match(self, doc):
        matched_tags = doc.get("matched_tags") if isinstance(doc.get("matched_tags"), list) else []
        return {
            "_id": str(doc.get("_id", "")),
            "title": str(doc.get("title", "")).strip(),
            "company": str(doc.get("company", "")).strip(),
            "location": str(doc.get("location", "")).strip(),
            "url": str(doc.get("url", "")).strip(),
            "source": str(doc.get("source", "")).strip(),
            "keyword": str(doc.get("keyword", "")).strip(),
            "job_type": str(doc.get("job_type", "")).strip(),
            "salary": str(doc.get("salary", "")).strip(),
            "posted_text": str(doc.get("posted_text", "")).strip(),
            "publication_date": self._serialize_value(doc.get("publication_date")),
            "last_seen_at": self._serialize_value(doc.get("last_seen_at")),
            "scraped_at": self._serialize_value(doc.get("scraped_at")),
            "tags": doc.get("tags") if isinstance(doc.get("tags"), list) else [],
            "matched_tags": matched_tags,
            "matched_tag_count": int(doc.get("tag_match_count") or len(matched_tags)),
            "active": bool(doc.get("active", False)),
            "search_method": "tag_overlap",
        }

    def _serialize_value(self, value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, dict):
            return {key: self._serialize_value(item) for key, item in value.items()}
        return value

    def _build_signature(self, matches):
        if not matches:
            return ""
        parts = [f"{item['_id']}:{item['matched_tag_count']}:{','.join(item['matched_tags'])}" for item in matches]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
