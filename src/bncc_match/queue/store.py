"""Fila persistente de jobs (SQLite) com controle de quota e retry.

``job_queue`` guarda um item por tópico de busca (``search_topic``) e um
por vídeo a transcrever (``transcribe_video``), deduplicados por
``(job_type, reference_key)``. ``quota_state`` persiste o consumo diário
de unidades da YouTube Data API (a janela é o dia UTC; um novo dia
recomeça o orçamento).

Fluxo de busca: orçamentar (100 unidades por ``search.list``) → buscar →
enriquecer → bronze → silver → gold → associar ao tópico → enfileirar a
transcrição. Falhas transitórias (429/5xx) agendam retry com backoff
exponencial + jitter; esgotadas as tentativas, o job vai para ``dead``.
Cota esgotada reagenda o tópico para o início do dia seguinte (UTC).
"""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from bncc_match.catalog.store import (
    link_video_to_topic,
    load_transcript_to_gold,
    load_videos_to_gold,
)
from bncc_match.catalog.transcript import capture_transcript
from bncc_match.catalog.transform import (
    make_video_transform,
    normalize_transcript_record,
    validate_transcript_record,
    validate_video_item,
)
from bncc_match.data_lake import silver as lake_silver
from bncc_match.db import default_session_factory
from bncc_match.models import (
    JOB_COMPLETED,
    JOB_DEAD,
    JOB_IN_PROGRESS,
    JOB_PENDING,
    JOB_RETRY,
    JOB_SEARCH_TOPIC,
    JOB_TRANSCRIBE_VIDEO,
    NODE_SKILL,
    VIDEO_TRANSCRIPT_FAILED,
    CurriculumNode,
    JobQueue,
    QuotaState,
    Transcript,
    Video,
)
from bncc_match.youtube.client import (
    SEARCH_COST,
    QuotaExceededError,
    TransientYouTubeError,
    enrich_videos,
    search_videos,
)

#: Orçamento diário padrão de unidades da YouTube Data API.
DEFAULT_DAILY_BUDGET = 10000
DEFAULT_MAX_ATTEMPTS = 3
#: Jobs ``in_progress`` parados há mais que isso são retomados (crash).
DEFAULT_IN_PROGRESS_TIMEOUT = 600
BACKOFF_BASE_SECONDS = 30.0
BACKOFF_MAX_SECONDS = 3600.0
TOPIC_MAX_RESULTS = 10


class QuotaExhausted(Exception):
    """Orçamento diário de quota esgotado — reagendar próxima janela."""


class PermanentJobError(Exception):
    """Erro permanente do job — item vai para o estado terminal ``dead``."""


def backoff_delay(
    attempts: int,
    base_seconds: float = BACKOFF_BASE_SECONDS,
    max_seconds: float = BACKOFF_MAX_SECONDS,
    rng: random.Random | None = None,
) -> float:
    """Delay de retry: backoff exponencial + jitter uniforme em [0, base].

    Args:
        attempts: Número da tentativa que falhou (1-based).
        base_seconds: Base exponencial em segundos.
        max_seconds: Teto do delay.
        rng: Gerador aleatório (injetável para testes determinísticos).

    Returns:
        Delay em segundos.
    """
    r: random.Random = rng if rng is not None else random.Random()
    delay = math.ldexp(base_seconds, attempts - 1) + r.uniform(0, base_seconds)
    return min(delay, max_seconds)


def _find_job(session: Session, job_type: str, reference_key: str) -> JobQueue | None:
    """Busca o job pela chave natural (tipo + referência)."""
    return (
        session.execute(
            select(JobQueue).where(
                JobQueue.job_type == job_type,
                JobQueue.reference_key == reference_key,
            )
        )
        .scalars()
        .first()
    )


def enqueue_topics(
    curriculum_node_ids: Sequence[int],
    session_factory: sessionmaker[Session] | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> int:
    """Enfileira um job de busca por tópico, sem duplicar itens.

    Tópicos já na fila (em qualquer estado) são reativados como
    ``pending`` (tentativas zeradas), não duplicados.

    Args:
        curriculum_node_ids: IDs dos nós do currículo (um item por nó).
        session_factory: Fábrica de sessões; ``None`` usa a padrão.
        max_attempts: Máximo de tentativas do job.

    Returns:
        Quantidade de itens enfileirados (novos + reativados).
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    count = 0
    with factory() as session, session.begin():
        now = datetime.now(UTC)
        for node_id in curriculum_node_ids:
            key = str(node_id)
            job = _find_job(session, JOB_SEARCH_TOPIC, key)
            if job is None:
                session.add(
                    JobQueue(
                        job_type=JOB_SEARCH_TOPIC,
                        reference_key=key,
                        status=JOB_PENDING,
                        max_attempts=max_attempts,
                        created_at=now,
                        updated_at=now,
                    )
                )
            else:
                job.status = JOB_PENDING
                job.attempts = 0
                job.next_retry_at = None
                job.last_error = None
                job.updated_at = now
            count += 1
    return count


def leaf_node_ids(
    stage: str | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> list[int]:
    """IDs dos nós folha (sem filhos), opcionalmente por etapa.

    Args:
        stage: Etapa (``infantil``/``fundamental``/``medio``) ou ``None``.
        session_factory: Fábrica de sessões; ``None`` usa a padrão.

    Returns:
        Lista de IDs dos nós folha.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    with factory() as session:
        parent_ids = select(CurriculumNode.parent_id).where(
            CurriculumNode.parent_id.isnot(None)
        )
        stmt = select(CurriculumNode.id).where(CurriculumNode.id.notin_(parent_ids))
        if stage is not None:
            stmt = stmt.where(CurriculumNode.stage == stage)
        return list(session.execute(stmt).scalars().all())


def build_search_query(node: CurriculumNode, session: Session) -> str:
    """Monta o texto de busca da hierarquia do nó + sufixo ``BNCC``.

    Inclui os ancestrais (da etapa até o pai do nó) e o próprio nome,
    exceto para nós ``skill`` (o código da habilidade não agrega à busca).

    Args:
        node: Nó do currículo (tópico).
        session: Sessão de leitura dos ancestrais.

    Returns:
        Texto da busca.
    """
    names: list[str] = []
    parent_id = node.parent_id
    while parent_id is not None:
        parent = session.get(CurriculumNode, parent_id)
        if parent is None:
            break
        names.append(parent.name)
        parent_id = parent.parent_id
    names.reverse()
    parts = list(names)
    if node.type != NODE_SKILL:
        parts.append(node.name)
    return " ".join(part for part in parts if part) + " BNCC"


def _quota_used(session: Session, day: str) -> int:
    """Unidades consumidas no dia (UTC)."""
    state = session.get(QuotaState, day)
    return state.units_used if state is not None else 0


def _record_quota(session: Session, day: str, units: int) -> None:
    """Soma unidades ao consumo do dia (linha criada sob demanda)."""
    state = session.get(QuotaState, day)
    if state is None:
        state = QuotaState(day=day, units_used=0)
        session.add(state)
    state.units_used += units


def _next_day_start(now: datetime) -> datetime:
    """Início do dia UTC seguinte (janela de quota seguinte)."""
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def _claim(
    session: Session, now: datetime, in_progress_timeout: int
) -> JobQueue | None:
    """Seleciona o próximo job processável (ordem de criação).

    Elegíveis: ``pending`` (sem retry agendado ou já vencido),
    ``retry`` (vencido) e ``in_progress`` obsoleto (processo morto).

    Args:
        session: Sessão.
        now: Momento da seleção.
        in_progress_timeout: Idade (s) para considerar ``in_progress`` morto.

    Returns:
        Job selecionado ou ``None``.
    """
    stmt = (
        select(JobQueue)
        .where(
            or_(
                and_(
                    JobQueue.status == JOB_PENDING,
                    or_(
                        JobQueue.next_retry_at.is_(None),
                        JobQueue.next_retry_at <= now,
                    ),
                ),
                and_(
                    JobQueue.status == JOB_RETRY,
                    JobQueue.next_retry_at <= now,
                ),
                and_(
                    JobQueue.status == JOB_IN_PROGRESS,
                    JobQueue.updated_at <= now - timedelta(seconds=in_progress_timeout),
                ),
            )
        )
        .order_by(JobQueue.id)
        .limit(1)
    )
    return session.execute(stmt).scalars().first()


def _ensure_transcribe_job(session: Session, video: Video, now: datetime) -> None:
    """Garante um job de transcrição para o vídeo (se não tiver legenda).

    Jobs existentes são mantidos, exceto ``dead``, que é reativado.

    Args:
        session: Sessão.
        video: Vídeo do catálogo.
        now: Momento.
    """
    has_transcript = (
        session.execute(
            select(Transcript).where(Transcript.video_id == video.id)
        ).first()
        is not None
    )
    if has_transcript:
        return
    job = _find_job(session, JOB_TRANSCRIBE_VIDEO, video.youtube_id)
    if job is None:
        session.add(
            JobQueue(
                job_type=JOB_TRANSCRIBE_VIDEO,
                reference_key=video.youtube_id,
                status=JOB_PENDING,
                max_attempts=DEFAULT_MAX_ATTEMPTS,
                created_at=now,
                updated_at=now,
            )
        )
    elif job.status == JOB_DEAD:
        job.status = JOB_PENDING
        job.attempts = 0
        job.next_retry_at = None
        job.last_error = None
        job.updated_at = now


def _process_search(
    session: Session,
    job: JobQueue,
    now: datetime,
    daily_budget: int,
    search_cost: int,
    max_results: int,
    root: Path | None,
) -> dict[str, Any]:
    """Executa o fluxo de busca de um tópico (orçamento → bronze → gold).

    Args:
        session: Sessão.
        job: Job ``search_topic`` em execução.
        now: Momento.
        daily_budget: Orçamento diário de unidades.
        search_cost: Custo da busca (unidades).
        max_results: Máximo de resultados por busca.
        root: Raiz da lake.

    Returns:
        Detalhes do resultado (vídeos, associações, consulta).

    Raises:
        PermanentJobError: Nó inexistente.
        QuotaExhausted: Orçamento diário esgotado.
        QuotaExceededError / TransientYouTubeError: erros da API.
    """
    node_id = int(job.reference_key)
    node = session.get(CurriculumNode, node_id)
    if node is None:
        raise PermanentJobError(f"nó curricular {node_id} não existe no gold")
    query_text = build_search_query(node, session)
    day = now.date().isoformat()
    used = _quota_used(session, day)
    if used + search_cost > daily_budget:
        raise QuotaExhausted(
            f"orçamento diário esgotado ({used}/{daily_budget} unidades)"
        )
    items = search_videos(query_text, max_results=max_results)
    video_ids = [str(item["youtube_id"]) for item in items]
    if video_ids:
        enrich_videos(video_ids)
    lake_silver.transform(
        "video",
        "youtube_videos",
        make_video_transform(),
        validate_video_item,
        root=root,
    )
    load_videos_to_gold(session=session, root=root)
    linked = 0
    for youtube_id in video_ids:
        video = (
            session.execute(select(Video).where(Video.youtube_id == youtube_id))
            .scalars()
            .first()
        )
        if video is None:
            continue
        link_video_to_topic(video.id, node_id, session=session)
        _ensure_transcribe_job(session, video, now)
        linked += 1
    job.quota_consumed += search_cost
    _record_quota(session, day, search_cost)
    return {"videos": len(video_ids), "linked": linked, "query": query_text}


def _process_transcribe(
    session: Session, job: JobQueue, now: datetime, root: Path | None
) -> dict[str, Any]:
    """Captura a transcrição do vídeo e atualiza bronze → silver → gold.

    Args:
        session: Sessão.
        job: Job ``transcribe_video`` em execução.
        now: Momento.
        root: Raiz da lake.

    Returns:
        Detalhes do resultado.

    Raises:
        PermanentJobError: Vídeo ausente do catálogo.
    """
    youtube_id = job.reference_key
    video = (
        session.execute(select(Video).where(Video.youtube_id == youtube_id))
        .scalars()
        .first()
    )
    if video is None:
        raise PermanentJobError(f"vídeo {youtube_id} não está no catálogo")
    result = capture_transcript(youtube_id, root=root)
    lake_silver.transform(
        "transcript",
        "youtube_transcript",
        normalize_transcript_record,
        validate_transcript_record,
        root=root,
    )
    load_transcript_to_gold(session=session, root=root)
    return {"transcript_status": result.status, "video_id": youtube_id}


def _schedule_retry(
    session: Session, job: JobQueue, now: datetime, error: Exception
) -> dict[str, Any]:
    """Agenda retry (backoff + jitter) ou encerra em ``dead``.

    O ``attempts`` já foi incrementado no claim; aqui apenas decide
    retry (com backoff pela tentativa atual) ou esgotamento.

    Args:
        session: Sessão.
        job: Job em execução.
        now: Momento.
        error: Erro transitório capturado.

    Returns:
        Resultado da decisão de retry.
    """
    job.last_error = str(error)
    job.updated_at = now
    if job.attempts >= job.max_attempts:
        job.status = JOB_DEAD
        session.flush()
        return {"status": "dead", "job_id": job.id, "last_error": job.last_error}
    delay = backoff_delay(job.attempts)
    job.status = JOB_RETRY
    job.next_retry_at = now + timedelta(seconds=delay)
    session.flush()
    return {
        "status": "retry_scheduled",
        "job_id": job.id,
        "attempts": job.attempts,
        "next_retry_at": job.next_retry_at,
    }


def process_next(
    session_factory: sessionmaker[Session] | None = None,
    daily_budget: int = DEFAULT_DAILY_BUDGET,
    search_cost: int = SEARCH_COST,
    in_progress_timeout: int = DEFAULT_IN_PROGRESS_TIMEOUT,
    root: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Processa o próximo item da fila e devolve o resultado.

    O job é marcado ``in_progress`` no início; o commit (sucesso, retry,
    quota ou morte) ocorre no fim, de modo que uma interrupção deixa o
    item ``in_progress`` e ele é retomado após o timeout.

    Args:
        session_factory: Fábrica de sessões; ``None`` usa a padrão.
        daily_budget: Orçamento diário de unidades.
        search_cost: Custo da busca (unidades).
        in_progress_timeout: Idade (s) para retomar ``in_progress`` mortos.
        root: Raiz da lake.
        now: Momento (injetável para testes).

    Returns:
        Dict com ``status`` em ``completed`` | ``quota_exhausted`` |
        ``retry_scheduled`` | ``dead`` | ``none_available`` e ``job_id``.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    moment = now if now is not None else datetime.now(UTC)
    with factory() as session, session.begin():
        job = _claim(session, moment, in_progress_timeout)
        if job is None:
            return {"status": "none_available", "job_id": None}
        job.status = JOB_IN_PROGRESS
        job.attempts += 1
        job.updated_at = moment
        try:
            if job.job_type == JOB_SEARCH_TOPIC:
                detail = _process_search(
                    session,
                    job,
                    moment,
                    daily_budget,
                    search_cost,
                    TOPIC_MAX_RESULTS,
                    root,
                )
            elif job.job_type == JOB_TRANSCRIBE_VIDEO:
                detail = _process_transcribe(session, job, moment, root)
            else:
                raise PermanentJobError(f"tipo de job desconhecido: {job.job_type}")
        except QuotaExhausted as error:
            # Reagendamento por cota não consome tentativa.
            job.attempts = max(job.attempts - 1, 0)
            job.status = JOB_PENDING
            job.next_retry_at = _next_day_start(moment)
            job.last_error = str(error)
            job.updated_at = moment
            session.flush()
            return {
                "status": "quota_exhausted",
                "job_id": job.id,
                "next_retry_at": job.next_retry_at,
            }
        except (QuotaExceededError, TransientYouTubeError) as error:
            return _schedule_retry(session, job, moment, error)
        except PermanentJobError as error:
            job.status = JOB_DEAD
            job.last_error = str(error)
            job.updated_at = moment
            session.flush()
            return {
                "status": "dead",
                "job_id": job.id,
                "last_error": str(error),
            }
        except Exception as error:  # noqa: BLE001 - inesperado → terminal
            job.status = JOB_DEAD
            job.last_error = f"{type(error).__name__}: {error}"
            job.updated_at = moment
            session.flush()
            return {
                "status": "dead",
                "job_id": job.id,
                "last_error": job.last_error,
            }
        job.status = JOB_COMPLETED
        job.updated_at = moment
        session.flush()
        return {"status": "completed", "job_id": job.id, **detail}


def run_queue(
    max_items: int | None = None,
    daily_budget: int = DEFAULT_DAILY_BUDGET,
    search_cost: int = SEARCH_COST,
    in_progress_timeout: int = DEFAULT_IN_PROGRESS_TIMEOUT,
    root: Path | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> list[dict[str, Any]]:
    """Executa a fila até esvaziar os itens ou a cota esgotar.

    Args:
        max_items: Limite de itens por execução (``None`` = todos).
        daily_budget: Orçamento diário de unidades.
        search_cost: Custo da busca (unidades).
        in_progress_timeout: Idade (s) para retomar ``in_progress`` mortos.
        root: Raiz da lake.
        session_factory: Fábrica de sessões; ``None`` usa a padrão.

    Returns:
        Lista de resultados de :func:`process_next`.
    """
    results: list[dict[str, Any]] = []
    limit = max_items if max_items is not None else 10**9
    for _ in range(limit):
        result = process_next(
            session_factory=session_factory,
            daily_budget=daily_budget,
            search_cost=search_cost,
            in_progress_timeout=in_progress_timeout,
            root=root,
        )
        results.append(result)
        if result["status"] in ("none_available", "quota_exhausted"):
            break
    return results


def list_failed_transcripts(
    session_factory: sessionmaker[Session] | None = None,
) -> list[str]:
    """``youtube_id`` dos vídeos com falha de transcrição (reprocessáveis).

    Args:
        session_factory: Fábrica de sessões; ``None`` usa a padrão.

    Returns:
        Lista de IDs de vídeo com falha.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    with factory() as session:
        return list(
            session.execute(
                select(Video.youtube_id).where(Video.status == VIDEO_TRANSCRIPT_FAILED)
            ).scalars()
        )


def enqueue_failed_transcripts(
    session_factory: sessionmaker[Session] | None = None,
) -> int:
    """Reenfileira a transcrição dos vídeos com falha, sem duplicar jobs.

    Jobs existentes (exceto os concluídos) são reativados como
    ``pending`` com tentativas zeradas.

    Args:
        session_factory: Fábrica de sessões; ``None`` usa a padrão.

    Returns:
        Quantidade de jobs reprocessáveis reativados/criados.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    count = 0
    with factory() as session, session.begin():
        now = datetime.now(UTC)
        videos = (
            session.execute(
                select(Video).where(Video.status == VIDEO_TRANSCRIPT_FAILED)
            )
            .scalars()
            .all()
        )
        for video in videos:
            job = _find_job(session, JOB_TRANSCRIBE_VIDEO, video.youtube_id)
            if job is None:
                session.add(
                    JobQueue(
                        job_type=JOB_TRANSCRIBE_VIDEO,
                        reference_key=video.youtube_id,
                        status=JOB_PENDING,
                        max_attempts=DEFAULT_MAX_ATTEMPTS,
                        created_at=now,
                        updated_at=now,
                    )
                )
            else:
                # Vídeo com falha: trabalho incompleto, mesmo que o job
                # anterior tenha "concluído" a captura falha.
                job.status = JOB_PENDING
                job.attempts = 0
                job.next_retry_at = None
                job.last_error = None
                job.updated_at = now
            count += 1
    return count


def quota_status(
    daily_budget: int = DEFAULT_DAILY_BUDGET,
    session_factory: sessionmaker[Session] | None = None,
) -> dict[str, Any]:
    """Consumo de quota do dia atual (UTC) e saldo do orçamento.

    Args:
        daily_budget: Orçamento diário de unidades.
        session_factory: Fábrica de sessões; ``None`` usa a padrão.

    Returns:
        Dict com ``day``, ``units_used``, ``daily_budget``,
        ``units_remaining``.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    with factory() as session:
        day = datetime.now(UTC).date().isoformat()
        used = _quota_used(session, day)
        return {
            "day": day,
            "units_used": used,
            "daily_budget": daily_budget,
            "units_remaining": max(daily_budget - used, 0),
        }


def queue_status(
    session_factory: sessionmaker[Session] | None = None,
) -> dict[str, int]:
    """Contagem de jobs por status na fila.

    Args:
        session_factory: Fábrica de sessões; ``None`` usa a padrão.

    Returns:
        Mapa status → contagem.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    with factory() as session:
        rows = session.execute(
            select(JobQueue.status, func.count(JobQueue.id)).group_by(JobQueue.status)
        ).all()
        return {status: count for status, count in rows}
