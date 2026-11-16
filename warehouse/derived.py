"""Derived correction batches and local window refresh.

Admission streams corrections into SQLite temp tables (no whole-batch or
peer-group Python containers), verifies every before-image against the same
parent input, and constructs the child input with new continuous sequence
numbers. Refresh reuses the fixed parent publication outside the allowed
re-evaluation set A and only evaluates rows inside A; the full child input and
the parent output mapping may be copied, and a parent range index is built
once and reused.
"""
import hashlib
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

from .model import (connect, integer, name, notify, pack,
                    BatchConflict, NotFound, ValidationError)

_CHANGE_FIELDS = {'row_id', 'before', 'after'}
_IMAGE_FIELDS = {'partition', 'order_key', 'amount'}


def _image(value):
    """Validate one non-None before/after image and return hex wire values."""
    if type(value) is not dict or set(value) != _IMAGE_FIELDS:
        raise ValidationError('change image must have exactly three documented fields')
    part = name(value['partition'], 'partition')
    key = integer(value['order_key'], 'order_key')
    amount = integer(value['amount'], 'amount')
    return part, hex(key), hex(amount)


def derive(catalog, child_id, parent_publication_id, changes):
    name(child_id, 'batch_id', 128)
    integer(parent_publication_id, 'parent_publication_id', 1)
    parent = catalog.parent_publication(parent_publication_id)
    if parent is None:
        # Either unknown, or a publication belonging to a derived batch.
        known = catalog.db.execute(
            'SELECT 1 FROM publications WHERE publication_id=?',
            (parent_publication_id,)).fetchone()
        if known is None:
            raise NotFound(parent_publication_id)
        raise ValidationError('parent publication must belong to a normal batch')
    parent_batch, parent_radius_hex = parent
    radius = int(parent_radius_hex, 16)
    db = catalog.db
    db.execute('CREATE TEMP TABLE IF NOT EXISTS derive_incoming('
               'change_seq INTEGER PRIMARY KEY,row_id TEXT UNIQUE,'
               'before_part TEXT,before_k TEXT,before_amount TEXT,'
               'after_part TEXT,after_k TEXT,after_amount TEXT)')
    db.execute('CREATE TEMP TABLE IF NOT EXISTS derive_child('
               'position INTEGER PRIMARY KEY AUTOINCREMENT,'
               'row_id TEXT UNIQUE,part TEXT,k TEXT,amount TEXT)')
    db.execute('BEGIN IMMEDIATE')
    try:
        db.execute('DELETE FROM derive_incoming')
        db.execute('DELETE FROM derive_child')
        change_count = 0
        for raw in changes:  # source exceptions propagate and roll back
            if type(raw) is not dict or set(raw) != _CHANGE_FIELDS:
                raise ValidationError('each change must have exactly row_id, before, after')
            rid = name(raw['row_id'], 'row_id')
            before, after = raw['before'], raw['after']
            if before is None and after is None:
                raise ValidationError('at least one change side must be an image')
            b_part = b_key = b_amount = None
            a_part = a_key = a_amount = None
            if before is not None:
                b_part, b_key, b_amount = _image(before)
            if after is not None:
                a_part, a_key, a_amount = _image(after)
            try:
                db.execute('INSERT INTO derive_incoming VALUES(?,?,?,?,?,?,?,?)',
                           (change_count, rid, b_part, b_key, b_amount,
                            a_part, a_key, a_amount))
            except sqlite3.IntegrityError as error:
                raise ValidationError('duplicate row_id in changes') from error
            change_count += 1

        # All before-images are checked against the same parent input, never
        # against an already-patched state.
        checks = (
            ('declared before-image does not match the parent input',
             """SELECT i.row_id FROM derive_incoming c JOIN inputs i
                  ON i.batch_id=? AND i.row_id=c.row_id
                 WHERE c.before_part IS NOT NULL
                   AND (i.part<>c.before_part OR i.k<>c.before_k
                        OR i.amount<>c.before_amount)"""),
            ('before-image given for an identity absent from the parent input',
             """SELECT c.row_id FROM derive_incoming c
                 WHERE c.before_part IS NOT NULL AND NOT EXISTS(
                   SELECT 1 FROM inputs i
                    WHERE i.batch_id=? AND i.row_id=c.row_id)"""),
            ('new identity must declare before=None',
             """SELECT c.row_id FROM derive_incoming c
                 WHERE c.before_part IS NULL AND EXISTS(
                   SELECT 1 FROM inputs i
                    WHERE i.batch_id=? AND i.row_id=c.row_id)"""),
            ('delete or patch requires the identity in the parent input',
             """SELECT c.row_id FROM derive_incoming c
                 WHERE c.before_part IS NULL AND c.after_part IS NULL
                   AND NOT EXISTS(SELECT 1 FROM inputs i
                    WHERE i.batch_id=? AND i.row_id=c.row_id)"""),
        )
        for message, query in checks:
            if db.execute(query, (parent_batch,)).fetchone() is not None:
                raise ValidationError(message)

        # Surviving rows keep parent relative order; new identities append in
        # change sequence. DELETE (after NULL) rows simply do not appear.
        db.execute(
            """INSERT INTO derive_child(row_id,part,k,amount)
               SELECT i.row_id,
                      COALESCE(c.after_part, i.part),
                      COALESCE(c.after_k, i.k),
                      COALESCE(c.after_amount, i.amount)
                 FROM inputs i
                 LEFT JOIN derive_incoming c ON c.row_id=i.row_id
                WHERE i.batch_id=?
                  AND (c.row_id IS NULL OR c.after_part IS NOT NULL)
                ORDER BY i.seq""",
            (parent_batch,))
        db.execute(
            """INSERT INTO derive_child(row_id,part,k,amount)
               SELECT c.row_id,c.after_part,c.after_k,c.after_amount
                 FROM derive_incoming c
                WHERE c.after_part IS NOT NULL AND c.before_part IS NULL
                ORDER BY c.change_seq""")

        # New continuous sequence numbers; digest over canonical packed rows.
        digest = hashlib.sha256()
        maximum, child_count = 0, 0
        cursor = db.execute(
            'SELECT position,row_id,part,k,amount FROM derive_child ORDER BY position')
        try:
            for position, rid, part, key_hex, amount_hex in cursor:
                if position - 1 != child_count:
                    raise ValueError('child sequence gap')
                encoded = pack((child_count, rid, part,
                                int(key_hex, 16), int(amount_hex, 16)))
                digest.update(encoded)
                maximum = max(maximum, len(encoded))
                child_count += 1
        finally:
            cursor.close()
        hexdigest = digest.hexdigest()

        existing = db.execute(
            'SELECT digest,n FROM batches WHERE batch_id=?', (child_id,)).fetchone()
        if existing is not None:
            if not catalog.is_derived(child_id):
                raise BatchConflict(child_id)
            binding = catalog.derivation(child_id)
            if binding['parent_publication_id'] != parent_publication_id:
                raise BatchConflict(child_id)
            change_mismatch = db.execute(
                'SELECT row_id,before_part,before_k,before_amount,'
                'after_part,after_k,after_amount FROM derive_incoming EXCEPT '
                'SELECT row_id,before_part,before_k,before_amount,'
                'after_part,after_k,after_amount FROM derived_changes '
                'WHERE child_id=? LIMIT 1', (child_id,)).fetchone()
            reverse_mismatch = db.execute(
                'SELECT row_id,before_part,before_k,before_amount,'
                'after_part,after_k,after_amount FROM derived_changes '
                'WHERE child_id=? EXCEPT '
                'SELECT row_id,before_part,before_k,before_amount,'
                'after_part,after_k,after_amount FROM derive_incoming LIMIT 1',
                (child_id,)).fetchone()
            input_mismatch = db.execute(
                'SELECT row_id,part,k,amount FROM derive_child EXCEPT '
                'SELECT row_id,part,k,amount FROM inputs WHERE batch_id=? LIMIT 1',
                (child_id,)).fetchone()
            stored_count = db.execute(
                'SELECT count(*) FROM inputs WHERE batch_id=?', (child_id,)).fetchone()[0]
            if (existing != (hexdigest, child_count) or change_mismatch is not None
                    or reverse_mismatch is not None or input_mismatch is not None
                    or stored_count != child_count):
                raise BatchConflict(child_id)
            receipt = {'child_id': child_id,
                       'parent_publication_id': parent_publication_id,
                       'radius': radius, 'digest': hexdigest, 'rows': child_count}
            db.commit()
            return receipt

        db.execute('INSERT INTO batches VALUES(?,?,?,?,NULL)',
                   (child_id, hexdigest, child_count, maximum))
        db.execute(
            'INSERT INTO inputs(batch_id,seq,row_id,part,k,amount) '
            'SELECT ?,new_seq,row_id,part,k,amount FROM ('
            'SELECT row_id,part,k,amount,row_number() OVER (ORDER BY position)-1 AS new_seq '
            'FROM derive_child)', (child_id,))
        db.execute('INSERT INTO derivations VALUES(?,?,?)',
                   (child_id, parent_publication_id, parent_radius_hex))
        db.execute(
            'INSERT INTO derived_changes(child_id,change_seq,row_id,'
            'before_part,before_k,before_amount,after_part,after_k,after_amount) '
            'SELECT ?,change_seq,row_id,before_part,before_k,before_amount,'
            'after_part,after_k,after_amount FROM derive_incoming', (child_id,))
        db.commit()
        return {'child_id': child_id,
                'parent_publication_id': parent_publication_id,
                'radius': radius, 'digest': hexdigest, 'rows': child_count}
    except BaseException:
        db.rollback()
        raise
    finally:
        # Drop rather than DELETE: fresh rowids make rowid-1 the continuous
        # sequence and the temp tables stay empty for the next admission.
        db.execute('DROP TABLE IF EXISTS derive_incoming')
        db.execute('DROP TABLE IF EXISTS derive_child')


def _ensure_parent_index(db, parent_batch, stats):
    """Build the reusable parent range index once; later refreshes reuse it."""
    exists = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name='parent_range'").fetchone()
    if exists is not None:
        return 0
    db.execute('CREATE INDEX parent_range ON inputs(batch_id,part,k COLLATE HEXINT,seq)')
    # Every parent input row is touched while building the index entries.
    indexed_rows = db.execute(
        'SELECT count(*) FROM inputs WHERE batch_id=?', (parent_batch,)).fetchone()[0]
    stats['parent_index_built'] = True
    return indexed_rows


def _build_candidate(db, workspace, child_id, parent_batch, parent_publication_id,
                     radius, budget, maximum, stats, port):
    """Stage the full child copy, set A, then evaluate locally or reuse."""
    path = workspace / 'candidate.sqlite'
    candidate = connect(path)
    try:
        candidate.executescript("""
        CREATE TABLE child_input(
         seq INTEGER PRIMARY KEY,row_id TEXT UNIQUE,
         part TEXT,k TEXT COLLATE HEXINT,amount TEXT);
        CREATE TABLE affected(row_id TEXT PRIMARY KEY);
        CREATE TABLE parent_totals(row_id TEXT PRIMARY KEY,total TEXT);
        CREATE TABLE results(seq INTEGER PRIMARY KEY,row_id TEXT,total TEXT);
        """)
        candidate.execute('BEGIN')
        copied_rows = 0
        cursor = db.execute(
            'SELECT seq,row_id,part,k,amount FROM inputs WHERE batch_id=? ORDER BY seq',
            (child_id,))
        try:
            for seq, rid, part, key_hex, amount_hex in cursor:
                encoded = pack((seq, rid, part, int(key_hex, 16), int(amount_hex, 16)))
                stats['peak_payload'] = max(stats['peak_payload'], len(encoded) + 2 * maximum)
                candidate.execute('INSERT INTO child_input VALUES(?,?,?,?,?)',
                                  (seq, rid, part, key_hex, amount_hex))
                copied_rows += 1
        finally:
            cursor.close()
        candidate.commit()
        stats['copied_rows'] = copied_rows

        # Index preparation: the reusable parent index may scan and sort the
        # whole parent input once; it is never a child-wide re-sort.
        indexed_rows = _ensure_parent_index(db, parent_batch, stats)

        candidate.execute('BEGIN')
        positions = db.execute(
            'SELECT row_id,before_part,before_k,after_part,after_k '
            'FROM derived_changes WHERE child_id=? ORDER BY change_seq', (child_id,))
        try:
            for rid, b_part, b_key, a_part, a_key in positions:
                # Corrected rows that still exist belong to A outright.
                if a_part is not None:
                    candidate.execute('INSERT OR IGNORE INTO affected VALUES(?)', (rid,))
                for part, key_hex in ((b_part, b_key), (a_part, a_key)):
                    if part is None:
                        continue
                    low, high = hex(int(key_hex, 16) - radius), hex(int(key_hex, 16) + radius)
                    neighbors = db.execute(
                        'SELECT row_id FROM inputs WHERE batch_id=? AND part=? '
                        'AND k>=? COLLATE HEXINT AND k<=? COLLATE HEXINT',
                        (parent_batch, part, low, high))
                    try:
                        for (other_id,) in neighbors:
                            candidate.execute(
                                'INSERT OR IGNORE INTO affected VALUES(?)', (other_id,))
                    finally:
                        neighbors.close()
        finally:
            positions.close()

        # Parent outputs of the fixed publication, available for direct reuse.
        outputs = db.execute(
            'SELECT i.row_id,o.total FROM outputs o JOIN inputs i '
            'ON i.batch_id=? AND i.seq=o.seq WHERE o.publication_id=?',
            (parent_batch, parent_publication_id))
        try:
            for rid, total in outputs:
                candidate.execute('INSERT INTO parent_totals VALUES(?,?)', (rid, total))
        finally:
            outputs.close()
        candidate.commit()
        stats['indexed_rows'] = indexed_rows
        notify(port, 'refresh_prepared', copied_rows=copied_rows,
               indexed_rows=indexed_rows)

        # Parent-range positions can name identities the child deleted;
        # evaluation only covers rows that actually exist in the child.
        candidate.execute(
            'DELETE FROM affected WHERE row_id NOT IN (SELECT row_id FROM child_input)')

        # Evaluate set A with exact closed RANGE / EXCLUDE TIES arithmetic.
        candidate.execute('BEGIN')
        affected = candidate.execute('SELECT row_id FROM affected')
        evaluated = 0
        try:
            for (rid,) in affected:
                seq, part, key_hex = candidate.execute(
                    'SELECT seq,part,k FROM child_input WHERE row_id=?', (rid,)).fetchone()
                low, high = hex(int(key_hex, 16) - radius), hex(int(key_hex, 16) + radius)
                neighbors = candidate.execute(
                    'SELECT seq,k,amount FROM child_input '
                    'WHERE part=? AND k>=? COLLATE HEXINT AND k<=? COLLATE HEXINT',
                    (part, low, high))
                total = 0
                visited = 0
                try:
                    for other_seq, other_key, amount_hex in neighbors:
                        visited += 1
                        if int(other_key, 16) != int(key_hex, 16) or other_seq == seq:
                            total += int(amount_hex, 16)
                finally:
                    neighbors.close()
                stats['range_rows_visited'] += visited
                candidate.execute('INSERT INTO results VALUES(?,?,?)',
                                  (seq, rid, hex(total)))
                evaluated += 1
                notify(port, 'refresh_work', kind='evaluate', row_id=rid)
        finally:
            affected.close()
        candidate.commit()
        stats['evaluated_rows'] = evaluated

        # Everything outside A reuses the bound parent result by identity,
        # emitted in child input order. No re-sort, no window query.
        candidate.execute('BEGIN')
        reused = 0
        cursor = candidate.execute(
            'SELECT c.seq,c.row_id FROM child_input c ORDER BY c.seq')
        try:
            for seq, rid in cursor:
                if candidate.execute(
                        'SELECT 1 FROM results WHERE row_id=?', (rid,)).fetchone() is None:
                    total = candidate.execute(
                        'SELECT total FROM parent_totals WHERE row_id=?', (rid,)).fetchone()
                    if total is None:
                        raise ValueError('missing reusable parent result for ' + rid)
                    candidate.execute('INSERT INTO results(seq,row_id,total) VALUES(?,?,?)',
                                       (seq, rid, total[0]))
                    reused += 1
                    notify(port, 'refresh_work', kind='reuse', row_id=rid)
        finally:
            cursor.close()
        candidate.commit()
        stats['reused_rows'] = reused
        stats['peak_payload'] = max(stats['peak_payload'], 2 * maximum)
        if stats['peak_payload'] > budget:
            raise AssertionError('refresh payload budget exceeded')
        stats['candidate_bytes'] = path.stat().st_size
        return candidate
    except BaseException:
        candidate.close()
        raise


def refresh(warehouse, child_id, memory_budget, event_port=None):
    integer(memory_budget, 'memory_budget', 1024, (1 << 63) - 1)
    catalog = warehouse.catalog
    digest, child_count, child_maximum, _head = catalog.batch(child_id)
    binding = catalog.derivation(child_id)
    if binding is None:
        raise ValidationError('refresh requires a derived batch')
    parent_publication_id = binding['parent_publication_id']
    radius = binding['radius']
    parent = catalog.parent_publication(parent_publication_id)
    if parent is None:
        raise NotFound(parent_publication_id)
    parent_batch, _ = parent
    parent_maximum = catalog.batch(parent_batch)[2]
    maximum = max(child_maximum, parent_maximum)
    required = max(1024, 8 * maximum)
    if memory_budget < required:
        raise ValidationError('memory_budget below required minimum ' + str(required))

    attempt_id = catalog.start(child_id, radius)
    stats = {'memory_budget': memory_budget, 'max_record_bytes': maximum,
             'sqlite_cache_kib_per_schema': 1024, 'peak_payload': 0,
             'range_rows_visited': 0, 'parent_index_built': False}
    workspace = None
    candidate = None
    released = False
    try:
        workspace = TemporaryDirectory(prefix='derive-', dir=warehouse.work)
        directory = Path(workspace.name)
        candidate = _build_candidate(
            catalog.db, directory, child_id, parent_batch, parent_publication_id,
            radius, memory_budget, maximum, stats, event_port)

        def release():
            nonlocal candidate, released
            candidate.close()
            candidate = None
            workspace.cleanup()
            released = True

        return catalog.publish(
            child_id, radius, attempt_id, candidate, stats, event_port, release,
            extra={'parent_publication_id': parent_publication_id})
    except BaseException as error:
        catalog.fail(attempt_id, error, stats)
        raise
    finally:
        if candidate is not None:
            candidate.close()
        if workspace is not None and not released:
            workspace.cleanup()
