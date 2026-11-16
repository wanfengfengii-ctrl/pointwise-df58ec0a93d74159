"""Disk materialization and exact RANGE / EXCLUDE TIES evaluation."""
from .model import connect, pack, notify
from .sort import order


def materialize(deliveries, path, radius, expected, budget, maximum, stats, port=None):
    db = connect(path)
    cursor = None
    try:
        db.executescript("""
        CREATE TABLE sorted_rows(
         pos INTEGER PRIMARY KEY,seq INTEGER UNIQUE,row_id TEXT,
         part TEXT,k TEXT COLLATE HEXINT,amount TEXT);
        CREATE INDEX range_rows ON sorted_rows(part,k,seq);
        CREATE TABLE results(seq INTEGER PRIMARY KEY,row_id TEXT,total TEXT);
        """)
        previous, count = None, 0
        db.execute('BEGIN')
        for batch in deliveries:
            # The consumer owns this batch only until the next delivery.
            for row in batch:
                current = order(row)
                if previous is not None and current < previous:
                    raise ValueError('unsorted delivery')
                previous = current
                seq, rid, part, key, amount = row
                db.execute('INSERT INTO sorted_rows VALUES(?,?,?,?,?,?)',
                           (count, seq, rid, part, hex(key), hex(amount)))
                count += 1
            # Release the consumer reference before requesting the next batch.
            del row, batch
        if count != expected:
            raise ValueError('sort cardinality mismatch')
        db.commit()
        stats['staged_rows'] = count
        stats['staged_bytes'] = path.stat().st_size
        stats['range_rows_visited'] = 0
        notify(port, 'window_staged', rows=count, bytes=stats['staged_bytes'])
        db.execute('BEGIN')
        cursor = db.execute('SELECT seq,row_id,part,k,amount FROM sorted_rows ORDER BY pos')
        for seq, rid, part, raw_key, raw_amount in cursor:
            key = int(raw_key, 16)
            total = 0
            neighbors = db.execute(
                'SELECT seq,k,amount FROM sorted_rows '
                'WHERE part=? AND k>=? AND k<=? ORDER BY k,seq',
                (part, hex(key - radius), hex(key + radius)))
            try:
                for other_seq, other_key, amount in neighbors:
                    stats['range_rows_visited'] += 1
                    if int(other_key, 16) != key or other_seq == seq:
                        total += int(amount, 16)
            finally:
                neighbors.close()
            db.execute('INSERT INTO results VALUES(?,?,?)', (seq, rid, hex(total)))
        db.commit()
        # Range evaluation holds at most a current row and a neighbor, independent of N.
        stats['peak_payload'] = max(stats['peak_payload'], 2 * maximum)
        if stats['peak_payload'] > budget:
            raise AssertionError('window payload budget exceeded')
        stats['candidate_bytes'] = path.stat().st_size
        notify(port, 'window_complete', rows=count, bytes=stats['candidate_bytes'])
        return db
    except BaseException:
        if db.in_transaction:
            db.rollback()
        if cursor is not None:
            cursor.close()
            cursor = None
        db.close()
        raise
    finally:
        if cursor is not None:
            cursor.close()
