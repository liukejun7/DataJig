from __future__ import annotations

from importlib import import_module
from typing import Any

from datajig.consumption import ConsumptionRun
from datajig.training import VerifiedBundle


def _restore_iterable_dataset(
    source: VerifiedBundle | ConsumptionRun, split: str | None
) -> Any:
    return iterable_dataset(source, split=split)


def iterable_dataset(
    source: VerifiedBundle | ConsumptionRun, *, split: str | None = None
) -> Any:
    """Return a lazily imported PyTorch IterableDataset without changing records."""

    try:
        torch: Any = import_module("torch")
    except ModuleNotFoundError as exc:
        raise ImportError(
            "PyTorch is optional; install torch before creating this adapter"
        ) from exc
    if isinstance(source, ConsumptionRun):
        if source.consumer != "pytorch":
            raise ValueError("training consumption plan does not name the PyTorch adapter")
        if split is not None and split != source.split:
            raise ValueError("training split does not match the consumption plan")
        selected_split = source.split
    else:
        if split is None or split not in source.splits:
            raise ValueError("training split was not found")
        selected_split = split

    class _DataJigIterableDataset(torch.utils.data.IterableDataset):  # type: ignore[misc]
        def __init__(
            self, source: VerifiedBundle | ConsumptionRun, split: str
        ) -> None:
            super().__init__()
            self.source = source
            self.split = split

        def __iter__(self) -> Any:
            worker = torch.utils.data.get_worker_info()
            if isinstance(self.source, ConsumptionRun):
                if worker is None:
                    return self.source._iter_records(worker_index=0, worker_count=1)
                return self.source._iter_records(
                    worker_index=worker.id, worker_count=worker.num_workers
                )
            if worker is None:
                return self.source._iter_records(
                    self.split, worker_index=0, worker_count=1
                )
            return self.source._iter_records(
                self.split,
                worker_index=worker.id,
                worker_count=worker.num_workers,
            )

        def __reduce__(self) -> Any:
            return (_restore_iterable_dataset, (self.source, self.split))

    return _DataJigIterableDataset(source, selected_split)
