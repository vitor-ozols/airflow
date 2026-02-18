# Airflow com Docker

Projeto mínimo para executar Apache Airflow com Docker.
As dependências Python são gerenciadas com Poetry (`pyproject.toml`).

## Pré-requisitos

- Docker
- Docker Compose (`docker compose`)
- Make

## Subir o Airflow

```bash
make up
```

Isso executa a inicialização do banco e cria usuário admin:

- Usuário: `admin`
- Senha: `admin`

Acesse: http://localhost:8080

## Parar o Airflow

```bash
make down
```

## Comandos úteis

```bash
make logs   # acompanha logs
make ps     # status dos containers
make clean  # derruba e remove volumes
```

## Dependências Python (Poetry)

Edite o arquivo `pyproject.toml` e adicione os pacotes em `[tool.poetry.dependencies]`.
Depois reconstrua a imagem:

```bash
docker compose build --no-cache
make up
```
