from __future__ import annotations

from importlib import import_module
from typing import Any

from datajig.consumption import ConsumptionRun
from datajig.training import VerifiedBundle


def iterable_dataset(
    source: VerifiedBundle | ConsumptionRun, *, split: str | None = None
) -> Any:
    """Return a Hugging Face IterableDataset without caching or transforms."""

    try:
        datasets = import_module("datasets")
    except ModuleNotFoundError as exc:
        raise ImportError(
            "Hugging Face Datasets is optional; install datasets before creating this adapter"
        ) from exc
    if isinstance(source, ConsumptionRun):
        if source.consumer != "huggingface":
            raise ValueError(
                "training consumption plan does not name the Hugging Face adapter"
            )
        if split is not None and split != source.split:
            raise ValueError("training split does not match the consumption plan")
        selected_split = source.split
    else:
        if split is None or split not in source.splits:
            raise ValueError("training split was not found")
        selected_split = split

    def records() -> Any:
        if isinstance(source, ConsumptionRun):
            yield from source._iter_records(worker_index=0, worker_count=1)
        else:
            yield from source.iter_records(selected_split)

    return datasets.IterableDataset.from_generator(records)
