# Transcrição de vídeos (legendas)

A capability `catalog` captura a transcrição (legendas) dos vídeos do
catálogo via `youtube-transcript-api` (v1.x), com retry, backoff + jitter
e throttle configuráveis.

## Comportamento

Cada captura tenta, na ordem, os idiomas preferidos (`pt` antes de `en`)
e classifica o resultado:

| status         | quando                                                    | retry?  |
|----------------|-----------------------------------------------------------|---------|
| `transcribed`  | legenda encontrada (texto + idioma registrados)          | —       |
| `unavailable`  | condição **permanente**: sem legendas, legendas desativadas, vídeo indisponível, ID inválido, restrição por idade | não — terminal |
| `failed`       | **transitório** (IP bloqueado, bloqueio de requisição, falha HTTP) após esgotar as tentativas | sim — reprocessável |

- **Backoff + jitter**: erro transitório → espera
  `base · 2^(tentativa-1) + jitter uniforme em [0, base]` (teto 60 s).
  Parâmetros: `backoff_base` (default 1 s), `max_attempts` (default 3).
- **Throttle**: intervalo mínimo entre duas capturas consecutivas
  (`throttle_seconds`, default 0 = desligado). Protege contra bloqueio
  de IP pela captura de legendas (sem quota oficial).

```
fetch (youtube-transcript-api)
   │  retry com backoff+jitter (transitórios)
   ▼
bronze  data/bronze/youtube_transcript/<data>/<uuid>.jsonl
   │     {video_id, raw_text, language, status, captured_at}
   ▼
silver  data/silver/transcript/<data>-<uuid>.parquet
   │     (strip de whitespace; a captura mais recente vence por captured_at)
   ▼
gold    transcript (1:1 por vídeo) + status do vídeo:
        transcribed → 'transcribed' | failed → 'transcript_failed'
```

Vídeo com `transcript_failed` fica **listável e reprocessável**:
`bncc queue retry-failed` reenfileira sem duplicar jobs.

## Limitações

- **Sem STT**: vídeo sem legenda (ou com legendas desativadas) fica
  `unavailable` — não há fallback de Whisper (decisão de design da change
  `video-discovery`; reavaliar na change de classificação).
- A captura depende das legendas expostas pelo YouTube; contas/IPs com
  muitos pedidos podem ser bloqueados (`IpBlocked`) — use o throttle.
- O texto é o bruto das legendas (sem limpeza além do strip); a
  classificação semântica é da change `video-classification`.

## Configuração do throttle / retry

```python
from bncc_match.catalog.transcript import capture_transcript

result = capture_transcript(
    "VIDEO_ID",
    languages=("pt", "en"),  # ordem decrescente de preferência
    max_attempts=5,  # tentativas p/ erros transitórios
    backoff_base=2.0,  # base do backoff exponencial (s)
    throttle_seconds=1.5,  # intervalo mínimo entre capturas (s)
)
```

A fila (`bncc queue process`) usa os defaults; para um servidor
dedicado (RTX 3090), `throttle_seconds` entre 1 e 3 s é um ponto de
partida razoável.

## Comandos de teste

```sh
uv run pytest tests/test_transcript.py -v
uv run data ls   # bronze/silver de transcrição no manifesto
```
