from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import suppress
from dataclasses import dataclass

from PIL import Image

from datajig.cache import AnalysisCache, CachedMedia
from datajig.hashing import compute_blake3, compute_phash
from datajig.layout import ImageFolderLayout, LayoutError, ParsedPath
from datajig.models import Finding, SampleRecord, Severity
from datajig.source import DirectorySource, SnapshotEntry, SnapshotSource


@dataclass(frozen=True, slots=True)
class ProbeResult:
    relative_path: str
    record: SampleRecord | None
    findings: tuple[Finding, ...] = ()


@dataclass(frozen=True, slots=True)
class InventoryResult:
    records: tuple[SampleRecord, ...]
    findings: tuple[Finding, ...]
    unsupported_paths: tuple[str, ...]
    enumerated_count: int


def _snapshot_identity(source: SnapshotSource) -> str:
    if isinstance(source, DirectorySource):
        return str(source.root)
    return type(source).__name__


def _record_from_media(
    source: SnapshotSource,
    entry: SnapshotEntry,
    parsed: ParsedPath,
    digest: str,
    media: CachedMedia | None,
    decode_error: str | None = None,
) -> SampleRecord:
    return SampleRecord(
        snapshot=_snapshot_identity(source),
        relative_path=entry.relative_path.as_posix(),
        split=parsed.split,
        label=parsed.label,
        logical_path=parsed.logical_path.as_posix(),
        size=entry.size,
        mtime_ns=entry.mtime_ns,
        content_hash=digest,
        perceptual_hash=media.perceptual_hash if media else None,
        width=media.width if media else None,
        height=media.height if media else None,
        media_format=media.media_format if media else None,
        channels=media.channels if media else None,
        decode_error=decode_error,
    )


def probe_entry(
    source: SnapshotSource,
    entry: SnapshotEntry,
    parsed: ParsedPath,
    cache: AnalysisCache,
) -> ProbeResult:
    with source.open(entry) as stream:
        digest = compute_blake3(stream)
        try:
            cached = cache.get_by_digest(digest)
        except Exception:
            cached = None
        if cached is not None and not _valid_cached_media(cached):
            cached = None
        if cached is not None:
            record = _record_from_media(source, entry, parsed, digest, cached)
            return ProbeResult(entry.relative_path.as_posix(), record, _channel_findings(record))
        try:
            stream.seek(0)
            with Image.open(stream) as image:
                image.load()
                media = CachedMedia(
                    perceptual_hash=compute_phash(image),
                    width=image.width,
                    height=image.height,
                    media_format=image.format
                    or parsed.logical_path.suffix.removeprefix(".").upper(),
                    channels=len(image.getbands()),
                )
        except Exception as exc:  # Pillow exposes several format-specific exception types.
            message = (
                f"cannot decode {entry.relative_path.as_posix()} "
                f"({type(exc).__name__})"
            )
            record = _record_from_media(source, entry, parsed, digest, None, message)
            finding = Finding(
                "IMAGE_DECODE_FAILED",
                Severity.ERROR,
                message,
                (entry.relative_path.as_posix(),),
            )
            return ProbeResult(entry.relative_path.as_posix(), record, (finding,))

    with suppress(Exception):
        cache.put(digest, media)
    record = _record_from_media(source, entry, parsed, digest, media)
    return ProbeResult(entry.relative_path.as_posix(), record, _channel_findings(record))


def _channel_findings(record: SampleRecord) -> tuple[Finding, ...]:
    if record.channels == 3:
        return ()
    return (
        Finding(
            "UNEXPECTED_CHANNELS",
            Severity.WARNING,
            f"expected 3 channels but found {record.channels}",
            (record.relative_path,),
            (("channels", record.channels),),
        ),
    )


def scan_snapshot(
    source: SnapshotSource,
    layout: ImageFolderLayout,
    cache: AnalysisCache,
    workers: int = 1,
) -> InventoryResult:
    if workers < 1:
        raise ValueError("workers must be at least 1")
    entries = list(source.entries())
    supported: list[tuple[SnapshotEntry, ParsedPath]] = []
    unsupported: list[str] = []
    findings: list[Finding] = []
    for entry in entries:
        if entry.relative_path.suffix.lower() not in layout.supported_suffixes:
            unsupported.append(entry.relative_path.as_posix())
            continue
        try:
            parsed = layout.parse(entry.relative_path)
        except LayoutError as exc:
            findings.append(
                Finding(
                    "INVALID_LAYOUT",
                    Severity.ERROR,
                    str(exc),
                    (entry.relative_path.as_posix(),),
                )
            )
            continue
        if parsed.supported:
            supported.append((entry, parsed))
        else:
            unsupported.append(entry.relative_path.as_posix())

    results: list[ProbeResult] = []
    if workers == 1:
        for entry, parsed in supported:
            results.append(_safe_probe(source, entry, parsed, cache))
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending: dict[Future[ProbeResult], SnapshotEntry] = {
                executor.submit(_safe_probe, source, entry, parsed, cache): entry
                for entry, parsed in supported
            }
            for future in as_completed(pending):
                results.append(future.result())

    results.sort(key=lambda result: result.relative_path)
    records = tuple(result.record for result in results if result.record is not None)
    for result in results:
        findings.extend(result.findings)
    findings.sort(key=lambda item: (item.code, item.sample_ids))
    return InventoryResult(records, tuple(findings), tuple(sorted(unsupported)), len(entries))


def _safe_probe(
    source: SnapshotSource,
    entry: SnapshotEntry,
    parsed: ParsedPath,
    cache: AnalysisCache,
) -> ProbeResult:
    try:
        return probe_entry(source, entry, parsed, cache)
    except Exception as exc:
        path = entry.relative_path.as_posix()
        finding = Finding(
            "PROBE_INTERNAL_ERROR",
            Severity.ERROR,
            f"probe failed for {path}: {exc}",
            (path,),
        )
    return ProbeResult(path, None, (finding,))


def _valid_cached_media(media: CachedMedia) -> bool:
    try:
        int(media.perceptual_hash, 16)
    except ValueError:
        return False
    return (
        len(media.perceptual_hash) == 16
        and media.width > 0
        and media.height > 0
        and media.channels > 0
        and bool(media.media_format)
    )
