"""Testes do armazenamento do catálogo (silver → gold, dedup, associações)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from util import (
    count_video_topics,
    count_videos,
    node_ids_by_type,
    video_field,
    video_pk,
    write_payload,
)

from bncc_match.catalog.store import (
    link_video_to_topic,
    load_videos_to_gold,
)
from bncc_match.catalog.transform import (
    make_video_transform,
    normalize_transcript_record,
    normalize_video_item,
    validate_video_item,
    video_item_youtube_id,
)
from bncc_match.curriculum import pipeline
from bncc_match.data_lake import silver, write_bronze


def _video_item(video_id: str) -> dict[str, Any]:
    """Item bruto de ``videos.list`` (forma do bronze)."""
    return {
        "id": video_id,
        "snippet": {
            "title": f"vídeo {video_id}",
            "channelTitle": "Canal Teste",
            "description": f"descrição {video_id}",
            "publishedAt": "2024-01-15T08:30:00Z",
            "thumbnails": {"default": {"url": f"https://thumb/{video_id}.jpg"}},
        },
        "contentDetails": {"duration": "PT10M"},
    }


def _transform_videos(root: Path) -> dict[str, Any]:
    """Executa a transformação bronze → silver do domínio ``video``."""
    return silver.transform(
        "video",
        "youtube_videos",
        make_video_transform(),
        validate_video_item,
        root=root,
    )


def test_silver_sem_duplicatas_youtube_id(lake_root: Path, gold_db: Path) -> None:
    """Task 3.1: bronze cumulativo → silver sem duplicatas de id."""
    write_bronze(
        "youtube_videos",
        [json.dumps(_video_item("abc123")), json.dumps(_video_item("def456"))],
        root=lake_root,
    )
    # Segunda resposta (cumulativa) repete o abc123 e traz um novo.
    write_bronze(
        "youtube_videos",
        [json.dumps(_video_item("abc123")), json.dumps(_video_item("xyz789"))],
        root=lake_root,
    )

    result = _transform_videos(lake_root)
    assert result["row_count"] == 3
    assert result["rejected"] == 1  # duplicata descartada
    assert "youtube_id" in result["columns"]

    rows = silver.read("video", root=lake_root).fetchall()
    assert len(rows) == 3


def test_load_videos_upsert_idempotente(lake_root: Path, gold_db: Path) -> None:
    """Task 3.2: load duas vezes com o mesmo id não duplica."""
    write_bronze("youtube_videos", [json.dumps(_video_item("abc123"))], root=lake_root)
    _transform_videos(lake_root)
    load_videos_to_gold(root=lake_root)
    load_videos_to_gold(root=lake_root)

    assert count_videos(gold_db) == 1


def test_video_novo_persistido_integralmente(lake_root: Path, gold_db: Path) -> None:
    """Cenário 'Vídeo novo é persistido integralmente' (todas as colunas)."""
    write_bronze("youtube_videos", [json.dumps(_video_item("abc123"))], root=lake_root)
    _transform_videos(lake_root)
    load_videos_to_gold(root=lake_root)

    assert video_field(gold_db, "abc123", "title") == "vídeo abc123"
    assert video_field(gold_db, "abc123", "channel_name") == "Canal Teste"
    assert video_field(gold_db, "abc123", "description") == "descrição abc123"
    assert video_field(gold_db, "abc123", "duration_seconds") == 600
    published = video_field(gold_db, "abc123", "published_at")
    assert published is not None and str(published).startswith("2024-01-15 08:30")
    assert (
        video_field(gold_db, "abc123", "url")
        == "https://www.youtube.com/watch?v=abc123"
    )
    assert video_field(gold_db, "abc123", "thumbnail_url") == "https://thumb/abc123.jpg"
    assert video_field(gold_db, "abc123", "status") == "discovered"


def test_mesmo_video_duas_buscas(
    lake_root: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Cenário 'Mesmo vídeo em duas buscas': 1 video, 2 video_topic."""
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload)
    node_ids = node_ids_by_type(gold_db, "area")[:2]

    write_bronze("youtube_videos", [json.dumps(_video_item("abc123"))], root=lake_root)
    _transform_videos(lake_root)
    load_videos_to_gold(root=lake_root)
    video_id = video_pk(gold_db, "abc123")
    assert video_id is not None

    link_video_to_topic(video_id, node_ids[0])
    link_video_to_topic(video_id, node_ids[1])
    link_video_to_topic(video_id, node_ids[0])  # duplicada: idempotente

    assert count_videos(gold_db) == 1
    assert count_video_topics(gold_db) == 2


def test_normalize_item_formato_busca() -> None:
    """Itens de ``search.list`` (``id`` como objeto) normalizam igual."""
    item = {
        "id": {"videoId": "abc123"},
        "snippet": {"title": "título", "channelTitle": "canal"},
    }
    row = normalize_video_item(item)
    assert row is not None
    assert row["youtube_id"] == "abc123"
    assert row["duration_seconds"] is None  # busca não traz duração
    assert row["published_at"] is None
    assert row["thumbnail_url"] is None


def test_video_item_youtube_id_inexistente() -> None:
    """Sem ``videoId``/``id`` → ``None`` (rejeitado na validação)."""
    assert video_item_youtube_id({"id": {"kind": "channel"}}) is None
    assert video_item_youtube_id({"id": ""}) is None
    assert video_item_youtube_id({}) is None


def test_normalize_video_com_campos_nao_dicts() -> None:
    """``snippet``/``contentDetails`` com tipo inesperado não quebra."""
    row = normalize_video_item({"id": "abc", "snippet": "x", "contentDetails": 1})
    assert row is not None
    assert row["title"] == ""
    assert row["duration_seconds"] is None


def test_normalize_transcript_sem_video_id() -> None:
    assert normalize_transcript_record({}) is None
    assert normalize_transcript_record({"video_id": "   "}) is None
    assert normalize_transcript_record({"video_id": 42}) is None
