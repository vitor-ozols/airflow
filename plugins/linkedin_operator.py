import re
import time
import json
import requests
from datetime import datetime, timedelta
from scrapy.selector import Selector
from pymongo import UpdateOne # <-- Importação adicionada para a operação de Upsert

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
        if not text: return 9999999
        text = text.lower().strip()
        patterns = [(r"(\d+)\s*(min|m)", 60), (r"(\d+)\s*(hour|h)", 3600), (r"(\d+)\s*(day|d)", 86400)]
        for pattern, mult in patterns:
            m = re.search(pattern, text)
            if m: return int(m.group(1)) * mult
        return 9999999

    def execute(self, context):
        hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        all_jobs = []
        seen_urls = set()
        
        seconds_limit = int(self.days_back) * 86400
        f_tpr_value = f"r{seconds_limit}"
        
        self.log.info(f"Buscando: {self.keyword}. Blacklist: {self.blacklist}")

        start = 0
        while True:
            url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            params = {
                "keywords": self.keyword,
                "location": self.location,
                "geo_id": self.geo_id,
                "start": start,
                "sortBy": "DD",
                "f_TPR": f_tpr_value
            }

            try:
                resp = requests.get(url, params=params, timeout=15)
                if resp.status_code != 200:
                    self.log.error(
                        "Falha ao buscar vagas (status=%s). Response:\n%s",
                        resp.status_code,
                        self._format_for_log(resp.text),
                    )
                    break

                sel = Selector(text=resp.text)
                cards = sel.css("li .base-search-card")
                if not cards: break

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

                        posted_text = (card.css('time::text').get() or "0 mins").strip()
                        seconds = self._parse_to_seconds(posted_text)

                        all_jobs.append({
                            "title": card.css('h3.base-search-card__title::text').get().strip(),
                            "company": company,
                            "location": card.css('span.job-search-card__location::text').get().strip(),
                            "url": clean_url,
                            "keyword": self.keyword,
                            "timestamp": (datetime.now() - timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
                        })

                start += 25
                time.sleep(2)

            except Exception as e:
                formatted_error = self._format_for_log(str(e))
                if formatted_error and formatted_error != str(e):
                    self.log.error("Detalhe do erro formatado:\n%s", formatted_error)

                self.log.exception(
                    "Erro na paginação para keyword=%s, start=%s",
                    self.keyword,
                    start,
                )
                break

        # --- NOVA LÓGICA DE INSERÇÃO (UPSERT) ---
        if all_jobs:
            try:
                # Pegamos a coleção nativa do PyMongo através do hook
                collection = hook.get_collection(self.mongo_collection, self.mongo_db)
                
                # Montamos as operações: Atualiza se existir, Insere se não existir
                operations = [
                    UpdateOne(
                        {"url": job["url"]},  # Chave de busca
                        {"$set": job},        # O que será salvo
                        upsert=True           # Permite criar um novo se não achar
                    )
                    for job in all_jobs
                ]
                
                # Executa tudo de uma vez
                if operations:
                    result = collection.bulk_write(operations)
                    self.log.info(
                        f"Sucesso! {result.upserted_count} novas vagas inseridas e "
                        f"{result.modified_count} vagas atualizadas no banco de dados."
                    )
            except Exception as e:
                self.log.error(f"Erro ao salvar no MongoDB: {e}")