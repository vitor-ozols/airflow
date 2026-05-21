from __future__ import annotations

import json
import os
from typing import Optional

from pydantic import BaseModel, Field
from pydantic_ai import Agent


class JobTaggingInput(BaseModel):
    title: str = ""
    company: str = ""
    location: str = ""
    url: str = ""
    source: str = ""
    keyword: str = ""
    job_type: str = ""
    discipline: str = ""
    salary: str = ""
    publication_date: str = ""
    posted_text: str = ""
    description: str = ""


class JobTaggingOutput(BaseModel):
    tags: list[str] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list)
    tools: list[str] = Field(default_factory=list)
    role_family: str = ""
    seniority: str = ""
    work_mode: str = Field(default="", description="remote, hybrid, onsite, field, or unknown")
    regions: list[str] = Field(default_factory=list)
    countries: list[str] = Field(default_factory=list)
    cities: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=list)
    contract_type: str = ""
    salary_mentioned: bool = False
    visa_or_sponsorship_mentioned: bool = False
    security_clearance_mentioned: bool = False
    summary: str = ""
    confidence: float = Field(default=0, ge=0, le=1)


SYSTEM_PROMPT = """
You extract structured metadata from job postings.

Return concise, normalized metadata only. Do not evaluate a candidate.

Rules:
- Infer work_mode from title, location, description, and explicit remote/hybrid wording.
- Keep tags useful for filtering/searching, not generic filler.
- Prefer short lowercase tags such as "python", "data-engineering", "airflow", "remote", "ireland".
- Extract skills and tools explicitly mentioned or strongly implied by the posting.
- If the posting lacks evidence for a field, return an empty value or "unknown" for work_mode.
- Summary must be one short sentence in Portuguese.
- Output MUST conform to the provided result schema.
"""


_agent: Agent | None = None


def get_model_from_env() -> Optional[str]:
    return (
        os.getenv("PYDANTIC_AI_MODEL")
        or os.getenv("OPENAI_MODEL")
        or os.getenv("AI_MODEL")
    )


def get_agent() -> Agent:
    global _agent
    if _agent is None:
        model = get_model_from_env()
        if not model:
            raise ValueError(
                "Modelo não configurado. Defina PYDANTIC_AI_MODEL, OPENAI_MODEL ou AI_MODEL."
            )
        _agent = Agent(
            model=model,
            system_prompt=SYSTEM_PROMPT,
            output_type=JobTaggingOutput,
        )
    return _agent


def build_user_prompt(job: JobTaggingInput) -> str:
    return f"""
Analyze this job posting and return structured metadata.

Job posting JSON:
---
{json.dumps(job.model_dump(), ensure_ascii=False)}
---
"""
