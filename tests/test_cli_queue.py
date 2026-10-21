"""Testes da CLI ``bncc queue`` (enqueue/process/status/retry-failed)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from util import job_field, node_ids_by_type, write_payload

from bncc_match.curriculum import cli, pipeline
from bncc_match.data_lake import write_bronze
from bncc_match.queue import enqueue_topics


@pytest.fixture
def curriculum_nodes(
    lake_root: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> list[int]:
    """Ingesta o currículo mínimo e retorna os IDs dos nós folha."""
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload)
    return node_ids_by_type(gold_db, "skill")


def test_cli_queue_enqueue(
    gold_db: Path, curriculum_nodes: list[int], capsys: pytest.CaptureFixture[str]
) -> None:
    """``bncc queue enqueue``: nós folha vitem itens, com resumo."""
    rc = cli.main(["queue", "enqueue"])

    out = capsys.readouterr().out
    assert rc == 0
    assert f"{len(curriculum_nodes)} tópicos enfileirados" in out
    assert (
        job_field(gold_db, "search_topic", str(curriculum_nodes[0]), "status")
        == "pending"
    )


def test_cli_queue_enqueue_node_ids(
    gold_db: Path, curriculum_nodes: list[int], capsys: pytest.CaptureFixture[str]
) -> None:
    """``--node-ids`` explicitos ignoram o padrão de nós folha."""
    rc = cli.main(["queue", "enqueue", "--node-ids", str(curriculum_nodes[0])])

    out = capsys.readouterr().out
    assert rc == 0
    assert "1 tópicos enfileirados" in out


def test_cli_queue_enqueue_vazio(
    gold_db: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Sem nós folha → erro 1 e mensagem no stderr."""
    monkeypatch.setattr(cli, "leaf_node_ids", lambda stage=None: [])
    rc = cli.main(["queue", "enqueue"])

    err = capsys.readouterr().err
    assert rc == 1
    assert "nenhum tópico para enfileirar" in err


def test_cli_queue_process(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``bncc queue process``: executa a fila e imprime cada resultado."""
    import bncc_match.queue.store as qs

    def _search(
        query: str,
        max_results: int = 10,
        api_key: str | None = None,
        service: Any = None,
    ) -> list[dict[str, Any]]:
        return [
            {
                "youtube_id": "abc123",
                "title": "vídeo abc123",
                "channel_name": "Canal",
            }
        ]

    def _enrich(
        video_ids: list[str],
        api_key: str | None = None,
        service: Any = None,
    ) -> list[dict[str, Any]]:
        items = [
            {
                "id": vid,
                "snippet": {
                    "title": f"vídeo {vid}",
                    "channelTitle": "Canal",
                },
            }
            for vid in video_ids
        ]
        if items:
            write_bronze(
                "youtube_videos",
                [json.dumps(i, ensure_ascii=False) for i in items],
            )
        return []

    monkeypatch.setattr(qs, "search_videos", _search)
    monkeypatch.setattr(qs, "enrich_videos", _enrich)
    enqueue_topics(curriculum_nodes[:1])

    rc = cli.main(["queue", "process", "--budget", "1000"])

    out = capsys.readouterr().out
    assert rc == 0
    assert "completed" in out
    assert "vídeos=1" in out


def test_cli_queue_status(
    gold_db: Path,
    curriculum_nodes: list[int],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``bncc queue status``: contagem por status + cota do dia."""
    enqueue_topics(curriculum_nodes)

    rc = cli.main(["queue", "status", "--budget", "1000"])

    out = capsys.readouterr().out
    assert rc == 0
    assert f"pending: {len(curriculum_nodes)}" in out
    assert "cota" in out
    assert "1000" in out


def test_cli_queue_status_fila_vazia(
    gold_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Fila vazia → mensagem amigável (sem traceback)."""
    rc = cli.main(["queue", "status"])

    out = capsys.readouterr().out
    assert rc == 0
    assert "fila vazia" in out


def test_cli_queue_retry_failed(
    gold_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``bncc queue retry-failed``: sem falhas → 0 reenfileiradas."""
    rc = cli.main(["queue", "retry-failed"])

    out = capsys.readouterr().out
    assert rc == 0
    assert "0 transcrições com falha reenfileiradas" in out
