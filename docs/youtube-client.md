# Cliente YouTube (busca + metadados)

A capability `youtube` busca vídeos no YouTube Data API v3 para cada
tópico do currículo BNCC e persiste os metadados. Faz parte da change
`video-discovery` (fila e catálogo em `docs/job-queue.md` e
`docs/transcription.md`).

## Setup da chave

A chave vem **estritamente** da variável de ambiente `YOUTUBE_API_KEY`
(veja `.env.example`). Nunca é hardcoded, versionada ou impressa em log.

```sh
export YOUTUBE_API_KEY="..."   # YouTube Data API v3 do Google Cloud Console
uv run bncc queue enqueue      # popula a fila de tópicos
uv run bncc queue process      # executa a fila (busca + enriquecimento)
```

Sem a chave, `search_videos`/`enrich_videos` levantam `RuntimeError`
antes de qualquer chamada de rede.

## Quotas (YouTube Data API v3)

| chamada                    | custo (unidades) | uso                     |
|----------------------------|------------------|-------------------------|
| `search.list`              | 100              | busca por tópico        |
| `videos.list`              | 1                | enriquecimento (≤50 ids por chamada) |

O orçamento diário padrão é **10.000 unidades** (configurável por
parâmetro `daily_budget` / flag `--budget`). A fila debita 100 unidades
antes de cada `search.list` e **não executa a busca** se o saldo for
insuficiente — o tópico é reagendado para o dia seguinte (UTC). O
consumo fica persistido em `quota_state` (ver `docs/job-queue.md`).

Erros de quota da API (HTTP 403 `quotaExceeded`/`dailyLimitExceeded`)
viram `QuotaExceededError`; erros transitórios (429/5xx) viram
`TransientYouTubeError` — ambos tratados pela fila (retry/reagendamento),
nunca derrubam a execução.

## Fluxo bronze

Cada **item bruto** das respostas (não a resposta inteira) vira uma linha
JSONL em bronze — imutável, particionada por data, registrada no
manifesto da data lake:

| fonte            | conteúdo                          |
|------------------|-----------------------------------|
| `youtube_search` | itens de `search.list` (id, título, canal) |
| `youtube_videos` | itens de `videos.list` (metadados completos) |

A **silver** (domínio `video`, Parquet) nasce do transform de
`youtube_videos`: normaliza campos, converte duração ISO 8601
(`PT1H2M3S` → segundos), escolhe a thumbnail (medium → default) e
**deduplica por `youtube_id`** (o bronze é cumulativo; a silver de uma
execução é a união sem duplicatas). O **gold** (`video`) faz upsert por
`youtube_id` — reprocessar nunca duplica.

```
search.list (100 un.)  →  bronze youtube_search  (audit)
videos.list  (1 un.)   →  bronze youtube_videos
                            │
                            ▼
                     silver video/<data>-<uuid>.parquet
                            │
                            ▼
                     gold  video (upsert por youtube_id)
                     + video_topic (vídeo ↔ tópico)
```

## Comandos de teste

```sh
uv run pytest tests/test_youtube_client.py -v   # cliente (fixtures mockadas)
uv run data ls                                   # arquivos bronze/silver no manifesto
```
