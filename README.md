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

Isso executa a inicialização do banco e cria o usuário admin:

- Usuário: `admin`
- Senha: veja com `make pass`

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

## Manutenção

Se o Docker falhar com `failed to register layer ... file exists`, copie o SHA do final do caminho `layerdb/sha256/<SHA>` e rode:

```bash
make repair-layer LAYER=<SHA>
make up
```

Se também houver erro de cache do BuildKit, use:

```bash
make repair-layer-prune LAYER=<SHA>
make up
```

Esses comandos movem os layers afetados para uma quarentena ao lado do diretório de dados do Docker; nada é apagado diretamente.

Para começar com banco limpo:

```bash
make reset-db
```

## Dependências Python (Poetry)

Edite o arquivo `pyproject.toml` e adicione os pacotes em `[tool.poetry.dependencies]`.
Depois reconstrua a imagem:

```bash
docker compose build --no-cache
make up
```

Pegar senha do webserver:
```
make pass
```
