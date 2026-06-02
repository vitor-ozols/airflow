import json
import os
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

from airflow.models.connection import Connection
from airflow.utils.session import create_session


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


def _upsert_connection(conn_id: str, host: str, schema: str, extra_json: str) -> None:
    with create_session() as session:
        session.query(Connection).filter(Connection.conn_id == conn_id).delete(synchronize_session=False)
        session.add(
            Connection(
                conn_id=conn_id,
                conn_type="mongo",
                host=host,
                schema=schema,
                extra=extra_json,
            )
        )


def main() -> None:
    host, schema, extra_json = _build_connection_settings()
    _upsert_connection(CONN_ID, host, schema, extra_json)


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
