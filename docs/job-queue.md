# Fila de jobs (job-queue)

Fila **persistente em SQLite** (`job_queue`) que orquestra a descoberta
de vídeos: um item por tópico de busca (`search_topic`) e um por vídeo a
transcrever (`transcribe_video`). Processo único, acionado por
scheduler (cron/systemd timer/GitHub Actions) — cada execução retoma de
onde a anterior parou.

## Tabelas

| tabela         | descrição                                                             |
|----------------|------------------------------------------------------------------------|
| `job_queue`    | item de trabalho: `job_type`, `reference_key` (ID do nó / do vídeo), `status`, `attempts`, `max_attempts`, `last_error`, `next_retry_at`, `quota_consumed`, timestamps |
| `quota_state`  | consumo diário de quota da YouTube Data API: `day` (UTC, PK) → `units_used` |

Deduplicação: `UNIQUE (job_type, reference_key)` — reenfileirar um tópico
**reativa** o item (status `pending`, tentativas zeradas), nunca duplica.

## Estados do job

```
pending ──► in_progress ──► completed
   ▲              │
   │              ├──► retry (next_retry_at = agora + backoff)
   │              │         │ (vencido)
   │              │         └──► in_progress (novo claim)
   │              └──► dead (attempts >= max_attempts | erro permanente)
   └── quota esgotada: pending + next_retry_at = início do dia UTC seguinte
```

- `attempts` conta **tentativas de processamento**; reagendamento por
  cota **não** consome tentativa.
- **Retomada após crash**: um `in_progress` com `updated_at` mais velho
  que o timeout (default 600 s) é considerado morto e reclamado. Tudo é
  idempotente (upsert por `youtube_id`, links por chave composta), então
  reprocessar não duplica dados.

## Controle de cota

Antes de cada `search.list`, a fila verifica o orçamento diário
(`quota_state`, janela = dia UTC; novo dia = orçamento zerado):

1. `used + 100 > daily_budget` → **não busca**; o job volta a `pending`
   com `next_retry_at` no início do dia seguinte e a execução para
   (`quota_exhausted`).
2. Busca concluída → `used += 100` (persistido; visível entre execuções).

Defaults: `daily_budget = 10000` (cota padrão do YouTube),
`search_cost = 100`. O enriquecimento (`videos.list`, 1 unidade por
chamada) não entra no orçamento.

## Retry / backoff

Erros transitórios da API (429/5xx) agendam retry com
`backoff_delay = base · 2^(attempts-1) + jitter[0, base]` (base 30 s,
teto 1 h). Esgotados os `max_attempts` (default 3), o job vai para
`dead` com `last_error` inspecionável. Erros permanentes (nó/vídeo
inexistente, tipo desconhecido) vão direto para `dead`.

## Comandos

```sh
# enfileirar tópicos (nós folha do gold; filtro opcional)
uv run bncc queue enqueue                      # todos os nós folha
uv run bncc queue enqueue --stage fundamental  # só um ano-etapa
uv run bncc queue enqueue --node-ids 4,10,14   # IDs explícitos

# processar (execução agendada; para em none_available/quota_exhausted)
uv run bncc queue process
uv run bncc queue process --max-items 50 --budget 8000

# inspecionar (contagem por status + cota do dia)
uv run bncc queue status --budget 8000

# reprocessar transcrições com falha
uv run bncc queue retry-failed
```

Sugestão de scheduler: `process` a cada 15–30 min; o consumo diário
limita o gasto de quota sozinho — execuções extras só processam
transcrições (sem quota) e itens reagendados.

## Comandos de teste

```sh
uv run pytest tests/test_job_queue.py -v
uv run pytest tests/test_cli_queue.py -v
```
