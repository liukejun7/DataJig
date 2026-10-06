from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Self


@dataclass(frozen=True, slots=True)
class CachedMedia:
    perceptual_hash: str
    width: int
    height: int
    media_format: str
    channels: int


class AnalysisCache:
    def __init__(self, connection: sqlite3.Connection | None) -> None:
        self._connection = connection
        self._lock = RLock()

    @classmethod
    def open(cls, path: str | Path) -> Self:
        cache_path = Path(path).expanduser()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(cache_path, check_same_thread=False)
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS media (
                digest TEXT PRIMARY KEY,
                perceptual_hash TEXT NOT NULL,
                width INTEGER NOT NULL,
                height INTEGER NOT NULL,
                media_format TEXT NOT NULL,
                channels INTEGER NOT NULL
            )
            """
        )
        connection.commit()
        return cls(connection)

    @classmethod
    def disabled(cls) -> Self:
        return cls(None)

    def get_by_digest(self, digest: str) -> CachedMedia | None:
        if self._connection is None:
            return None
        with self._lock:
            row = self._connection.execute(
                """
                SELECT perceptual_hash, width, height, media_format, channels
                FROM media WHERE digest = ?
                """,
                (digest,),
            ).fetchone()
        if row is None:
            return None
        return CachedMedia(str(row[0]), int(row[1]), int(row[2]), str(row[3]), int(row[4]))

    def put(self, digest: str, media: CachedMedia) -> None:
        if self._connection is None:
            return
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO media
                    (digest, perceptual_hash, width, height, media_format, channels)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    digest,
                    media.perceptual_hash,
                    media.width,
                    media.height,
                    media.media_format,
                    media.channels,
                ),
            )
            self._connection.commit()

    def close(self) -> None:
        if self._connection is not None:
            with self._lock:
                self._connection.close()
                self._connection = None

