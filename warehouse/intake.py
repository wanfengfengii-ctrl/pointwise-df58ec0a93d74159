"""Streaming, all-or-nothing admission of immutable batches."""
import hashlib
import sqlite3
from .model import name, integer, pack, ValidationError, BatchConflict


def ingest(catalog, batch_id, rows):
    name(batch_id, 'batch_id', 128)
    db = catalog.db
    db.execute('CREATE TEMP TABLE IF NOT EXISTS incoming('
               'seq INTEGER PRIMARY KEY,row_id TEXT UNIQUE,part TEXT,k TEXT,amount TEXT)')
    db.execute('BEGIN IMMEDIATE')
    try:
        db.execute('DELETE FROM incoming')
        digest = hashlib.sha256()
        count, maximum = 0, 0
        for raw in rows:
            if type(raw) is not dict or set(raw) != {'row_id', 'partition', 'order_key', 'amount'}:
                raise ValidationError('each row must have exactly four documented fields')
            row = (count, name(raw['row_id'], 'row_id'),
                   name(raw['partition'], 'partition'),
                   integer(raw['order_key'], 'order_key'),
                   integer(raw['amount'], 'amount'))
            encoded = pack(row)
            digest.update(encoded)
            maximum = max(maximum, len(encoded))
            try:
                db.execute('INSERT INTO incoming VALUES(?,?,?,?,?)',
                           (count, row[1], row[2], hex(row[3]), hex(row[4])))
            except sqlite3.IntegrityError as error:
                raise ValidationError('duplicate row_id') from error
            count += 1
        hexdigest = digest.hexdigest()
        old = db.execute('SELECT digest,n FROM batches WHERE batch_id=?', (batch_id,)).fetchone()
        if old is not None:
            difference = db.execute(
                'SELECT seq,row_id,part,k,amount FROM incoming EXCEPT '
                'SELECT seq,row_id,part,k,amount FROM inputs WHERE batch_id=? LIMIT 1',
                (batch_id,)).fetchone()
            if old != (hexdigest, count) or difference is not None:
                raise BatchConflict(batch_id)
        else:
            db.execute('INSERT INTO batches VALUES(?,?,?,?,NULL)',
                       (batch_id, hexdigest, count, maximum))
            db.execute('INSERT INTO inputs SELECT ?,seq,row_id,part,k,amount FROM incoming',
                       (batch_id,))
        db.execute('DELETE FROM incoming')
        db.commit()
        return {'batch_id': batch_id, 'digest': hexdigest, 'rows': count}
    except BaseException:
        db.rollback()
        raise


def iter_input(catalog, batch_id):
    cursor = catalog.db.execute(
        'SELECT seq,row_id,part,k,amount FROM inputs WHERE batch_id=? ORDER BY seq', (batch_id,))
    try:
        for seq, rid, part, key, amount in cursor:
            yield seq, rid, part, int(key, 16), int(amount, 16)
    finally:
        cursor.close()
