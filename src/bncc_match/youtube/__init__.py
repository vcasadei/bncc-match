"""Integração com o YouTube: cliente da Data API v3 (busca + metadados)."""

from bncc_match.youtube.client import (
    SEARCH_COST,
    VIDEOS_LIST_COST,
    QuotaExceededError,
    TransientYouTubeError,
    YouTubeClientError,
    classify_http_error,
    enrich_videos,
    iso8601_to_seconds,
    pick_thumbnail,
    search_videos,
)

__all__ = [
    "SEARCH_COST",
    "VIDEOS_LIST_COST",
    "QuotaExceededError",
    "TransientYouTubeError",
    "YouTubeClientError",
    "classify_http_error",
    "enrich_videos",
    "iso8601_to_seconds",
    "pick_thumbnail",
    "search_videos",
]
