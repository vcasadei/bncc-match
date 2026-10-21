"""Pipeline completo de ingestão da BNCC: fetch → bronze → silver → gold.

O fetch ocorre antes de qualquer escrita: se a fonte estiver
indisponível (erro HTTP/rede) ou o payload inválido/vazio, o erro é
propagado e nem a lake nem o gold são alterados.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from bncc_match.curriculum import source, transform
from bncc_match.curriculum.load import load_to_gold
from bncc_match.data_lake import bronze, silver


def run_ingest(
    url: str = source.DEFAULT_SOURCE_URL,
    source_version: str | None = None,
    timeout: float = 30.0,
    raw_file: str | Path | None = None,
    root: Path | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> dict[str, Any]:
    """Executa a ingestão completa da BNCC.

    Args:
        url: Endpoint da fonte (usado quando ``raw_file`` não é dado).
        source_version: Versão da fonte (proveniência em ``ingestion_run``).
        timeout: Timeout do fetch em segundos.
        raw_file: Arquivo JSON local com o payload (no lugar do fetch);
            útil quando o site exige superação de proteção anti-bot.
        root: Raiz da lake; ``None`` usa o raiz padrão.
        session_factory: Fábrica de sessões do gold; ``None`` usa o padrão.

    Returns:
        Dicionário com ``bronze_path``, ``silver`` (resultado do transform)
        e ``gold`` (contagens do load).

    Raises:
        httpx.HTTPError: Fonte indisponível (erro HTTP ou de rede).
        ValueError: Payload inválido ou vazio.
        OSError: ``raw_file`` não pode ser lido.
    """
    if raw_file is not None:
        raw = Path(raw_file).read_text(encoding="utf-8")
        source_url = f"file://{Path(raw_file).resolve()}"
    else:
        raw = source.fetch_raw(url, timeout=timeout)
        source_url = url

    items = source.parse_raw(raw)
    # Chave da ingestão: carimba cada item bruto com o timestamp do fetch.
    # O load usa a maior chave para definir a "visão atual" do gold,
    # pois o bronze é imutável/acumulativo (a silver reprocessa o histórico).
    ingest_run = datetime.now(UTC).isoformat()
    for item in items:
        item["ingest_run"] = ingest_run
    bronze_path = bronze.write(
        "bncc", [json.dumps(item, ensure_ascii=False) for item in items], root=root
    )
    silver_result = silver.transform(
        "curriculum",
        "bncc",
        transform.normalize_raw,
        transform.validate_raw,
        root=root,
    )
    gold_result = load_to_gold(
        source_url=source_url,
        source_version=source_version,
        root=root,
        session_factory=session_factory,
    )
    return {
        "bronze_path": str(bronze_path),
        "silver": silver_result,
        "gold": gold_result,
    }
