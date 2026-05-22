import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
