"""Public offline readings warehouse API (Python 3.11 standard library)."""
from pathlib import Path
from tempfile import TemporaryDirectory
from .catalog import Catalog
from .derived import derive, refresh as refresh_derived
from .intake import ingest, iter_input
from .model import (integer, ValidationError, WarehouseError, BatchConflict,
                    NotFound, NotPublished)
from .sort import external_sort, batches
from .window import materialize


class Warehouse:
    """Single worker; close every read iterator before closing this object."""
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.work = self.root / 'work'
        self.work.mkdir(exist_ok=True)
        self.catalog = Catalog(self.root / 'warehouse.sqlite')

    def ingest(self, batch_id, rows):
        # Normal admission must never overwrite a derived child batch.
        if self.catalog.derivation(batch_id) is not None:
            raise BatchConflict(batch_id)
        return ingest(self.catalog, batch_id, rows)

    def derive(self, child_id, parent_publication_id, changes):
        return derive(self.catalog, child_id, parent_publication_id, changes)

    def refresh(self, child_id, memory_budget, event_port=None):
        return refresh_derived(self, child_id, memory_budget, event_port)

    def status(self, batch_id):
        return self.catalog.status(batch_id)

    def iter_read(self, batch_id):
        return self.catalog.iter_read(batch_id)

    def read(self, batch_id):
        return list(self.iter_read(batch_id))

    def compute(self, batch_id, radius, memory_budget, *, fan_in=4, delivery_rows=64,
                event_port=None):
        integer(radius, 'radius', 0)
        integer(memory_budget, 'memory_budget', 1024, (1 << 63) - 1)
        integer(fan_in, 'fan_in', 2, 32)
        integer(delivery_rows, 'delivery_rows', 1, 10000)
        _, count, maximum, _ = self.catalog.batch(batch_id)
        if self.catalog.derivation(batch_id) is not None:
            raise ValidationError('compute requires a normal batch; use refresh for derived batches')
        required = max(1024, (fan_in + 4) * maximum)
        if memory_budget < required:
            raise ValidationError('memory_budget below required minimum ' + str(required))
        attempt_id = self.catalog.start(batch_id, radius)
        stats = {'memory_budget': memory_budget, 'max_record_bytes': maximum,
                 'sqlite_cache_kib_per_schema': 1024,
                 'fan_in': fan_in, 'delivery_rows': delivery_rows}
        workspace = None
        candidate = None
        released = False
        try:
            workspace = TemporaryDirectory(prefix='job-', dir=self.work)
            directory = Path(workspace.name)
            source = iter_input(self.catalog, batch_id)
            try:
                ordered = external_sort(source, directory, memory_budget, maximum,
                                        fan_in, stats, event_port)
            finally:
                source.close()
            deliveries = batches(ordered, delivery_rows, memory_budget, maximum, stats)
            try:
                candidate = materialize(deliveries, directory / 'candidate.sqlite',
                                        radius, count, memory_budget, maximum, stats,
                                        event_port)
            finally:
                deliveries.close()

            def release():
                nonlocal candidate, released
                candidate.close()
                candidate = None
                workspace.cleanup()
                released = True

            return self.catalog.publish(batch_id, radius, attempt_id, candidate,
                                        stats, event_port, release)
        except BaseException as error:
            self.catalog.fail(attempt_id, error, stats)
            raise
        finally:
            if candidate is not None:
                candidate.close()
            if workspace is not None and not released:
                workspace.cleanup()

    def close(self):
        self.catalog.close()

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.close()
