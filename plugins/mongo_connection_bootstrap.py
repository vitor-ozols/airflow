import json
import os
import subprocess
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

from sqlalchemy import create_engine, text


CONN_ID = "mongo_vitor_ozols"


def _clean(value: str) -> str:
    return value.strip().strip('"').strip("'")


def _build_connection_settings() -> tuple[str, str, str]:
    mongo_uri = _clean(os.environ["MONGO_STRING"])
    cert_path = Path(_clean(os.environ["MONGO_X509_CERT"])).expanduser()
    parsed = urlparse(mongo_uri)

    if not parsed.hostname:
        raise ValueError(f"MONGO_STRING inválida: host ausente em {mongo_uri!r}")
    if not cert_path.exists():
        raise FileNotFoundError(f"Certificado X.509 não encontrado em {cert_path}")

    extras = dict(parse_qsl(parsed.query))
    extras["srv"] = parsed.scheme == "mongodb+srv"
    extras["tls"] = True
    extras["tlsCertificateKeyFile"] = str(cert_path)

    schema = parsed.path.lstrip("/") or "airflow"
    extra_json = json.dumps(extras, separators=(",", ":"))
    return parsed.hostname, schema, extra_json


def _delete_existing_connection(conn_id: str) -> None:
    engine = create_engine(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM connection WHERE conn_id = :conn_id"), {"conn_id": conn_id})


def main() -> None:
    host, schema, extra_json = _build_connection_settings()
    _delete_existing_connection(CONN_ID)
    subprocess.run(
        [
            "airflow",
            "connections",
            "add",
            CONN_ID,
            "--conn-type",
            "mongo",
            "--conn-host",
            host,
            "--conn-schema",
            schema,
            "--conn-extra",
            extra_json,
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
