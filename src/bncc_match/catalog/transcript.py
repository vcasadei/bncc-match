"""Captura de transcrições (legendas) via ``youtube-transcript-api``.

Condições permanentes (sem legendas, legendas desativadas, vídeo
indisponível, ID inválido, restrição por idade) resultam em status
``unavailable`` — terminal, sem retry. Erros transitórios (IP bloqueado,
bloqueio de requisição, falha HTTP) são retentados com backoff exponencial
+ jitter configurável; esgotadas as tentativas, o status é ``failed``
(reprocessável pela fila).

Cada captura grava um registro bruto em bronze JSONL
(``source="youtube_transcript"``) com carimbo ``captured_at``, para a
silver/gold decidirem a versão mais recente por vídeo.
"""

from __future__ import annotations

import json
import math
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from requests.exceptions import HTTPError
from youtube_transcript_api import (
    AgeRestricted,
    FailedToCreateConsentCookie,
    InvalidVideoId,
    IpBlocked,
    NoTranscriptFound,
    RequestBlocked,
    TranscriptsDisabled,
    VideoUnavailable,
    YouTubeRequestFailed,
    YouTubeTranscriptApi,
)

from bncc_match.data_lake.bronze import write as write_bronze
from bncc_match.models import (
    TRANSCRIPT_FAILED,
    TRANSCRIPT_TRANSCRIBED,
    TRANSCRIPT_UNAVAILABLE,
)

#: Erros permanentes — o vídeo não tem (ou nunca terá) legenda acessível.
PERMANENT_ERRORS: tuple[type[Exception], ...] = (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
    InvalidVideoId,
    AgeRestricted,
)

#: Erros transitórios — candidatos a retry com backoff + jitter.
TRANSIENT_ERRORS: tuple[type[Exception], ...] = (
    IpBlocked,
    RequestBlocked,
    HTTPError,
    YouTubeRequestFailed,
    FailedToCreateConsentCookie,
)

#: Idiomas preferidos na ordem (``pt`` antes de ``en``).
DEFAULT_LANGUAGES: tuple[str, ...] = ("pt", "en")

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_BASE = 1.0
MAX_BACKOFF_SECONDS = 60.0

_last_call_at: float = 0.0


@dataclass
class TranscriptionResult:
    """Resultado da captura de uma transcrição.

    Attributes:
        video_id: ID do vídeo no YouTube.
        status: ``transcribed`` | ``unavailable`` | ``failed``.
        text: Texto completo (vazio quando indisponível/falhou).
        language: Código do idioma da legenda (ou ``None``).
    """

    video_id: str
    status: str
    text: str
    language: str | None


def reset_transcript_throttle() -> None:
    """Zera o registro do throttle (para testes/isolamento)."""
    global _last_call_at
    _last_call_at = 0.0


def _throttle_wait(throttle_seconds: float, sleep: Callable[[float], None]) -> None:
    """Garante intervalo mínimo entre capturas consecutivas.

    Args:
        throttle_seconds: Intervalo mínimo (0 desabilita).
        sleep: Função de espera (injetável para testes).
    """
    global _last_call_at
    if throttle_seconds <= 0:
        return
    now = time.monotonic()
    wait = throttle_seconds - (now - _last_call_at)
    if wait > 0:
        sleep(wait)
    _last_call_at = time.monotonic()


def fetch_transcript(
    video_id: str,
    languages: tuple[str, ...] = DEFAULT_LANGUAGES,
    api: Any = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    throttle_seconds: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> TranscriptionResult:
    """Busca a legenda de um vídeo com retry, backoff + jitter e throttle.

    Args:
        video_id: ID do vídeo (não a URL).
        languages: Idiomas preferidos (ordem decrescente).
        api: Instância ``YouTubeTranscriptApi`` injetada (testes).
        max_attempts: Máximo de tentativas para erros transitórios.
        backoff_base: Base do backoff exponencial (segundos).
        throttle_seconds: Intervalo mínimo entre requisições (segundos).
        sleep: Função de espera (injetável para testes).
        rng: Gerador aleatório (injetável para jitter determinístico).

    Returns:
        :class:`TranscriptionResult` com o status da captura.
    """
    ytt = api if api is not None else YouTubeTranscriptApi()
    r: random.Random = rng if rng is not None else random.Random()
    for attempt in range(1, max_attempts + 1):
        _throttle_wait(throttle_seconds, sleep)
        try:
            fetched = ytt.fetch(video_id, languages=list(languages))
            text = " ".join(str(snippet.text) for snippet in fetched)
            return TranscriptionResult(
                video_id=video_id,
                status=TRANSCRIPT_TRANSCRIBED,
                text=text.strip(),
                language=getattr(fetched, "language_code", None),
            )
        except PERMANENT_ERRORS:
            return TranscriptionResult(
                video_id=video_id,
                status=TRANSCRIPT_UNAVAILABLE,
                text="",
                language=None,
            )
        except TRANSIENT_ERRORS:
            # IpBlocked, RequestBlocked, falha HTTP, etc.
            if attempt < max_attempts:
                delay = min(
                    math.ldexp(backoff_base, attempt - 1), MAX_BACKOFF_SECONDS
                ) + r.uniform(0, backoff_base)
                sleep(delay)
    return TranscriptionResult(
        video_id=video_id,
        status=TRANSCRIPT_FAILED,
        text="",
        language=None,
    )


def capture_transcript(
    video_id: str,
    languages: tuple[str, ...] = DEFAULT_LANGUAGES,
    api: Any = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    throttle_seconds: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    root: Path | None = None,
) -> TranscriptionResult:
    """Captura a transcrição e grava o registro bruto em bronze.

    Args:
        video_id: ID do vídeo.
        languages: Idiomas preferidos.
        api: Instância injetada (testes).
        max_attempts: Máximo de tentativas (erros transitórios).
        backoff_base: Base do backoff (segundos).
        throttle_seconds: Intervalo mínimo entre requisições.
        sleep: Função de espera (injetável para testes).
        rng: Gerador aleatório (injetável para testes).
        root: Raiz da lake; ``None`` usa a padrão.

    Returns:
        :class:`TranscriptionResult` (bronze já gravado).
    """
    result = fetch_transcript(
        video_id,
        languages=languages,
        api=api,
        max_attempts=max_attempts,
        backoff_base=backoff_base,
        throttle_seconds=throttle_seconds,
        sleep=sleep,
        rng=rng,
    )
    line = {
        "video_id": video_id,
        "raw_text": result.text,
        "language": result.language,
        "status": result.status,
        "captured_at": datetime.now(UTC).isoformat(),
    }
    write_bronze(
        "youtube_transcript",
        [json.dumps(line, ensure_ascii=False)],
        root=root,
    )
    return result
