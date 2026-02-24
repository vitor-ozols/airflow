# agents/job_matcher.py
from __future__ import annotations

from typing import List, Optional
import os
from pydantic import BaseModel, Field, HttpUrl
from pydantic_ai import Agent

class Job(BaseModel):
    _id: str
    title: str
    company: str
    location: Optional[str] = None
    url: HttpUrl
    keyword: Optional[str] = None
    timestamp: str
    processed: bool = False
    processed_at: Optional[str] = None

class Recommendation(BaseModel):
    _id: str
    score: float = Field(ge=0, le=100)
    title: str
    company: str
    location: Optional[str] = None
    url: HttpUrl
    reason: str = Field(description="1-2 frases objetivas ligando CV à vaga.")
    gaps: List[str] = Field(default_factory=list, description="Pontos que faltam/ajustes no CV para a vaga.")

class AgentOutput(BaseModel):
    top_recommendations: List[Recommendation] = Field(description="Ordenado do melhor para o pior.")
    discard_ids: List[str] = Field(default_factory=list, description="Ids que não valem enviar.")
    summary: str = Field(description="Resumo curto do porquê essas vagas são as melhores (3-5 linhas).")

SYSTEM_PROMPT = """
You are a senior technical recruiter for Data/Software roles.

You will receive:
1) A CV in Markdown
2) A list of job postings (JSON objects)

Your task:
- Rank the jobs by fit to the CV.
- Return up to TOP_K recommendations.
- For each recommendation, provide:
  - a score from 0 to 100
  - a short reason (1 - 2 sentences) grounded ONLY in the CV
  - a small list of gaps (missing skills/keywords), if any

Rules:
- Do not invent experience or skills not present in the CV.
- If a job is not a good fit, put its id into discard_ids.
- Be concise and pragmatic.
- Output MUST conform to the provided result schema.
"""

_agent: Agent | None = None


def _get_model_from_env() -> str | None:
    return (
        os.getenv("PYDANTIC_AI_MODEL")
        or os.getenv("OPENAI_MODEL")
        or os.getenv("AI_MODEL")
    )


def get_agent() -> Agent:
    global _agent
    if _agent is None:
        model = _get_model_from_env()
        if not model:
            raise ValueError(
                "Modelo não configurado. Defina PYDANTIC_AI_MODEL, OPENAI_MODEL ou AI_MODEL."
            )
        _agent = Agent(
            # aqui você configura seu provider/model (depende do seu runtime)
            # ex: model=\"openai:gpt-4.1-mini\"
            model=model,
            system_prompt=SYSTEM_PROMPT,
            output_type=AgentOutput,
        )
    return _agent


def build_user_prompt(cv_markdown: str, jobs: List[Job], top_k: int = 8) -> str:
    jobs_payload = [j.model_dump() for j in jobs]

    return f"""
    
    CV (Markdown):
    ---
    {cv_markdown}
    ---

    Job postings (JSON):
    ---
    {jobs_payload}
    ---

    Instructions:
    - Return at most {top_k} items in `top_recommendations`, ordered best to worst.
    - Use a 0-100 score with this weighting:
    - Technical stack match: 40%
    - Seniority/role scope match: 25%
    - Domain/context match: 20%
    - Location/remote/language constraints: 15%
    - Reasons must be grounded ONLY in the CV. Do not invent experience.
    - Keep `reason` to 1-2 sentences per job.
    - Put clearly bad fits into `discard_ids`.
    """
