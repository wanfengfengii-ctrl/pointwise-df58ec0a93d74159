"""Bounded run generation, disk manifest and finite-fan-in merge."""
import heapq
from contextlib import ExitStack
from .model import connect, pack, unpack, notify


def order(row):
    return row[2], row[3], row[0]


def external_sort(rows, directory, budget, maximum, fan_in, stats, port=None):
    manifest = connect(directory / 'runs.sqlite')
    try:
        manifest.execute('CREATE TABLE runs(id INTEGER PRIMARY KEY,level INTEGER,path TEXT)')
        stats.update(initial_runs=0, merge_passes=0, sort_bytes_written=0, peak_payload=0)
        manifest.execute('BEGIN')
        counter = 0

        def destination():
            nonlocal counter
            counter += 1
            return directory / ('run-%08d.jsonl' % counter)

        def record(path, level):
            stats['sort_bytes_written'] += path.stat().st_size
            manifest.execute('INSERT INTO runs(level,path) VALUES(?,?)', (level, path.name))

        def peak(value):
            stats['peak_payload'] = max(stats['peak_payload'], value)
            if value > budget:
                raise AssertionError('payload budget exceeded')

        def spill(chunk):
            chunk.sort(key=order)
            path = destination()
            with path.open('wb') as sink:
                for row in chunk:
                    sink.write(pack(row))
            record(path, 0)
            stats['initial_runs'] += 1
            notify(port, 'sort_run', path=str(path), rows=len(chunk))

        chunk, used = [], 0
        capacity = budget - 3 * maximum
        for row in rows:
            length = len(pack(row))
            peak(used + length + 2 * maximum)
            if length > maximum:
                raise ValueError('input exceeds admitted record size')
            if chunk and used + length > capacity:
                spill(chunk)
                chunk, used = [], 0
            chunk.append(row)
            used += length
            peak(used + 2 * maximum)
        if chunk:
            spill(chunk)
        del chunk
        level = 0
        while manifest.execute('SELECT count(*) FROM runs WHERE level=?', (level,)).fetchone()[0] > 1:
            while True:
                runs = manifest.execute(
                    'SELECT id,path FROM runs WHERE level=? ORDER BY id LIMIT ?',
                    (level, fan_in)).fetchall()
                if not runs:
                    break
                target = destination()
                with ExitStack() as stack:
                    sources = [stack.enter_context((directory / path).open('rb'))
                               for _, path in runs]
                    sink = stack.enter_context(target.open('wb'))
                    heap, resident = [], 0
                    for index, source in enumerate(sources):
                        line = source.readline()
                        if line:
                            row = unpack(line)
                            heapq.heappush(heap, (order(row), index, row, len(line)))
                            resident += len(line)
                    peak(resident + 3 * maximum)
                    while heap:
                        _, index, row, length = heapq.heappop(heap)
                        sink.write(pack(row))
                        resident -= length
                        line = sources[index].readline()
                        if line:
                            following = unpack(line)
                            peak(resident + 3 * maximum)
                            heapq.heappush(heap, (order(following), index, following, len(line)))
                            resident += len(line)
                record(target, level + 1)
                for run_id, path in runs:
                    manifest.execute('DELETE FROM runs WHERE id=?', (run_id,))
                    (directory / path).unlink()
            level += 1
            stats['merge_passes'] += 1
        final = manifest.execute('SELECT path FROM runs').fetchone()
        if final is None:
            path = destination()
            path.touch()
            manifest.commit()
            return path
        manifest.commit()
        return directory / final[0]
    finally:
        manifest.close()


def batches(path, delivery_rows, budget, maximum, stats):
    """Transport batches have no partition or peer-group significance."""
    chunk, used = [], 0
    with path.open('rb') as source:
        for line in source:
            row = unpack(line)
            length = len(line)
            stats['peak_payload'] = max(stats['peak_payload'], used + length + maximum)
            if chunk and (len(chunk) >= delivery_rows or used + length > budget - 2 * maximum):
                yield chunk
                chunk, used = [], 0
            chunk.append(row)
            used += length
            if stats['peak_payload'] > budget:
                raise AssertionError('delivery budget exceeded')
        if chunk:
            yield chunk
