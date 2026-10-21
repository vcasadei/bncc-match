"""Camada de data lake (bronze JSONL + silver Parquet) via DuckDB.

Fornece primitivas reutilizáveis para persistência de dados brutos
(bronze) e limpos (silver), um manifesto rastreável e uma CLI de
inspeção.
"""

from bncc_match.data_lake import bronze, manifest, silver
from bncc_match.data_lake.bronze import read as read_bronze
from bncc_match.data_lake.bronze import write as write_bronze
from bncc_match.data_lake.silver import read as read_silver
from bncc_match.data_lake.silver import transform as transform_silver

__all__ = [
    "bronze",
    "manifest",
    "read_bronze",
    "read_silver",
    "silver",
    "transform_silver",
    "write_bronze",
]
