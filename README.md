# bncc-match

Cruza o currículo da BNCC (da Educação Infantil ao Ensino Médio) com vídeos do
YouTube, usando classificação híbrida (embeddings → candidatos → LLM).

Este repositório entrega a **fundação** do projeto: ambiente reproduzível,
tooling de qualidade e CI. As features de negócio (ingestão do currículo,
descoberta de vídeos, classificação) são implementadas em changes posteriores.

## Requisitos

- **Nix** (com flakes habilitados) — toolchain de sistema pinado.
- **uv** — gerenciamento de dependências Python (instalado pelo flake).

Não é necessário instalar Python/uv manualmente: `nix develop` fornece Python
3.12, `uv` e `ffmpeg` com hashes pinados.

## Setup

```bash
# 1. Entrar no ambiente reproduzível (Python 3.12 + uv + ffmpeg)
nix develop

# 2. Instalar dependências e o pacote em modo editável
uv sync
```

O comando `uv sync` cria o ambiente `.venv/` e o lockfile `uv.lock`.

## Comandos

```bash
uv run ruff check            # lint
uv run ruff format --check   # verifica formatação
uv run mypy                  # type check estrito
uv run pytest                # testes (com cobertura)
uv run pytest --cov=bncc_match --cov-report=term --cov-fail-under=90
```

## Estrutura

```
bncc-match/
├── flake.nix                 # ambiente Nix (Python 3.12, uv, ffmpeg)
├── pyproject.toml            # projeto Python + ruff/mypy/pytest
├── src/bncc_match/           # pacote (src layout)
├── tests/                    # testes
└── .github/workflows/ci.yml  # CI (lint, typecheck, test)
```

## Segredos

Copie `.env.example` para `.env` e preencha os valores. O `.env` está no
`.gitignore` e nunca é versionado. O CI verifica (e falha) se um `.env` for
commitado acidentalmente.
