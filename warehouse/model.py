"""Shared wire representation. Integers on disk use signed hexadecimal."""
import json
import sqlite3


class WarehouseError(Exception):
    code = 'WAREHOUSE_ERROR'


class ValidationError(WarehouseError):
    code = 'INVALID_INPUT'


class BatchConflict(WarehouseError):
    code = 'BATCH_CONFLICT'


class NotFound(WarehouseError):
    code = 'BATCH_NOT_FOUND'


class NotPublished(WarehouseError):
    code = 'NOT_PUBLISHED'


def integer(value, name, minimum=None, maximum=None):
    if type(value) is not int or (minimum is not None and value < minimum):
        raise ValidationError(name + ' must be an integer in range')
    if maximum is not None and value > maximum:
        raise ValidationError(name + ' must be an integer in range')
    return value


def name(value, field, limit=256):
    try:
        valid = type(value) is str and 0 < len(value.encode('utf-8')) <= limit
    except UnicodeError:
        valid = False
    if not valid:
        raise ValidationError(field + ' must be nonempty UTF-8 text within byte limit')
    return value


def pack(row):
    seq, rid, part, key, amount = row
    return (json.dumps([seq, rid, part, hex(key), hex(amount)],
                       ensure_ascii=True, separators=(',', ':')) + '\n').encode('ascii')


def unpack(data):
    seq, rid, part, key, amount = json.loads(data)
    return seq, rid, part, int(key, 16), int(amount, 16)


def compare_hex(left, right):
    a, b = int(left, 16), int(right, 16)
    return (a > b) - (a < b)


def connect(path):
    db = sqlite3.connect(str(path), isolation_level=None)
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA journal_mode=DELETE')
    db.execute('PRAGMA synchronous=FULL')
    db.execute('PRAGMA temp_store=FILE')
    db.execute('PRAGMA cache_size=-1024')
    db.execute('PRAGMA temp.cache_size=-1024')
    db.create_collation('HEXINT', compare_hex)
    return db


def notify(port, event, **details):
    if port is not None:
        port(event, details)
