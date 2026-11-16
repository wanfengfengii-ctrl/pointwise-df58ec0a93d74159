"""Version-one durable catalog and atomic publication."""
import json
from .model import connect, name, NotFound, NotPublished, notify

SCHEMA = """
CREATE TABLE batches(
 batch_id TEXT PRIMARY KEY, digest TEXT NOT NULL, n INTEGER NOT NULL,
 max_payload INTEGER NOT NULL, head INTEGER);
CREATE TABLE inputs(
 batch_id TEXT NOT NULL REFERENCES batches(batch_id), seq INTEGER NOT NULL,
 row_id TEXT NOT NULL, part TEXT NOT NULL, k TEXT NOT NULL, amount TEXT NOT NULL,
 PRIMARY KEY(batch_id,seq), UNIQUE(batch_id,row_id));
CREATE TABLE attempts(
 attempt_id INTEGER PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batches(batch_id),
 radius TEXT NOT NULL, state TEXT NOT NULL, publication_id INTEGER,
 error_type TEXT, error_message TEXT, stats TEXT NOT NULL);
CREATE TABLE publications(
 publication_id INTEGER PRIMARY KEY,
 batch_id TEXT NOT NULL REFERENCES batches(batch_id), radius TEXT NOT NULL,
 stats TEXT NOT NULL);
CREATE TABLE outputs(
 publication_id INTEGER NOT NULL REFERENCES publications(publication_id),
 seq INTEGER NOT NULL, row_id TEXT NOT NULL, total TEXT NOT NULL,
 PRIMARY KEY(publication_id,seq));
CREATE TABLE derivations(
 child_id TEXT PRIMARY KEY REFERENCES batches(batch_id),
 parent_publication_id INTEGER NOT NULL, radius TEXT NOT NULL);
CREATE TABLE derived_changes(
 child_id TEXT NOT NULL REFERENCES batches(batch_id), change_seq INTEGER NOT NULL,
 row_id TEXT NOT NULL,
 before_part TEXT, before_k TEXT, before_amount TEXT,
 after_part TEXT, after_k TEXT, after_amount TEXT,
 PRIMARY KEY(child_id,change_seq));
PRAGMA user_version=1;
"""

# Derived-batch tables added for the refresh contract. The catalog on-disk
# layout version stays 1; DDL is idempotent so older catalogs gain the objects
# on first reopen without touching existing data. The reusable parent range
# index is deliberately absent here: refresh builds it on first use.
DERIVED_DDL = """
CREATE TABLE IF NOT EXISTS derivations(
 child_id TEXT PRIMARY KEY REFERENCES batches(batch_id),
 parent_publication_id INTEGER NOT NULL, radius TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS derived_changes(
 child_id TEXT NOT NULL REFERENCES batches(batch_id), change_seq INTEGER NOT NULL,
 row_id TEXT NOT NULL,
 before_part TEXT, before_k TEXT, before_amount TEXT,
 after_part TEXT, after_k TEXT, after_amount TEXT,
 PRIMARY KEY(child_id,change_seq));
"""


class Catalog:
    def __init__(self, path):
        self.db = connect(path)
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        if version == 0:
            try:
                self.db.executescript('BEGIN IMMEDIATE;\n' + SCHEMA + '\nCOMMIT;')
            except BaseException:
                if self.db.in_transaction:
                    self.db.rollback()
                self.db.close()
                raise
        elif version != 1:
            self.db.close()
            raise ValueError('unsupported catalog version')
        # Idempotent upgrade for catalogs created before derived batches.
        self.db.executescript(DERIVED_DDL)

    def derivation(self, child_id):
        row = self.db.execute(
            'SELECT parent_publication_id,radius FROM derivations WHERE child_id=?',
            (child_id,)).fetchone()
        if row is None:
            return None
        return {'parent_publication_id': row[0], 'radius': int(row[1], 16)}

    def is_derived(self, batch_id):
        return self.db.execute(
            'SELECT 1 FROM derivations WHERE child_id=?', (batch_id,)).fetchone() is not None

    def parent_publication(self, publication_id):
        return self.db.execute(
            'SELECT p.batch_id,p.radius FROM publications p '
            'WHERE p.publication_id=? AND NOT EXISTS('
            'SELECT 1 FROM derivations d WHERE d.child_id=p.batch_id)',
            (publication_id,)).fetchone()

    def batch(self, batch_id):
        name(batch_id, 'batch_id', 128)
        row = self.db.execute(
            'SELECT digest,n,max_payload,head FROM batches WHERE batch_id=?',
            (batch_id,)).fetchone()
        if row is None:
            raise NotFound(batch_id)
        return row

    def start(self, batch_id, radius):
        return self.db.execute(
            "INSERT INTO attempts(batch_id,radius,state,stats) VALUES(?,?,'running','{}')",
            (batch_id, hex(radius))).lastrowid

    def fail(self, attempt_id, error, stats):
        self.db.execute(
            "UPDATE attempts SET state='failed',error_type=?,error_message=?,stats=? "
            "WHERE attempt_id=?",
            (type(error).__name__, str(error), json.dumps(stats, sort_keys=True), attempt_id))

    def publish(self, batch_id, radius, attempt_id, candidate, stats, port, release, extra=None):
        expected = self.batch(batch_id)[1]
        db = self.db
        db.execute('BEGIN IMMEDIATE')
        cursor = None
        try:
            publication_id = db.execute(
                'INSERT INTO publications(batch_id,radius,stats) VALUES(?,?,?)',
                (batch_id, hex(radius), json.dumps(stats, sort_keys=True))).lastrowid
            cursor = candidate.execute('SELECT seq,row_id,total FROM results ORDER BY seq')
            count = 0
            for seq, rid, total in cursor:
                source = db.execute(
                    'SELECT row_id FROM inputs WHERE batch_id=? AND seq=?',
                    (batch_id, seq)).fetchone()
                if seq != count or source is None or source[0] != rid:
                    raise ValueError('candidate identity mismatch')
                int(total, 16)
                db.execute('INSERT INTO outputs VALUES(?,?,?,?)',
                           (publication_id, seq, rid, total))
                count += 1
            if count != expected:
                raise ValueError('candidate cardinality mismatch')
            cursor.close()
            cursor = None
            release()
            notify(port, 'publish_ready', rows=count, publication_id=publication_id)
            db.execute('UPDATE batches SET head=? WHERE batch_id=?',
                       (publication_id, batch_id))
            db.execute(
                "UPDATE attempts SET state='published',publication_id=?,stats=? "
                "WHERE attempt_id=?",
                (publication_id, json.dumps(stats, sort_keys=True), attempt_id))
            db.commit()
            receipt = {'batch_id': batch_id, 'publication_id': publication_id,
                       'attempt_id': attempt_id, 'radius': radius, 'rows': count,
                       'stats': dict(stats)}
            if extra:
                receipt.update(extra)
            return receipt
        except BaseException:
            db.rollback()
            raise
        finally:
            if cursor is not None:
                cursor.close()

    def iter_read(self, batch_id):
        head = self.batch(batch_id)[3]
        if head is None:
            raise NotPublished(batch_id)
        cursor = self.db.execute(
            'SELECT row_id,total FROM outputs WHERE publication_id=? ORDER BY seq', (head,))
        def consume():
            try:
                for rid, total in cursor:
                    yield {'row_id': rid, 'window_sum': int(total, 16)}
            finally:
                cursor.close()
        return consume()

    def status(self, batch_id):
        digest, n, maximum, head = self.batch(batch_id)
        attempt = self.db.execute(
            'SELECT attempt_id,radius,state,publication_id,error_type,error_message,stats '
            'FROM attempts WHERE batch_id=? ORDER BY attempt_id DESC LIMIT 1',
            (batch_id,)).fetchone()
        latest = None
        if attempt:
            latest = dict(zip(('attempt_id', 'radius', 'state', 'publication_id',
                               'error_type', 'error_message', 'stats'), attempt))
            latest['radius'] = int(latest['radius'], 16)
            latest['stats'] = json.loads(latest['stats'])
        publication = None
        if head is not None:
            radius, stats = self.db.execute(
                'SELECT radius,stats FROM publications WHERE publication_id=?',
                (head,)).fetchone()
            publication = {'publication_id': head, 'radius': int(radius, 16),
                           'rows': n, 'stats': json.loads(stats)}
        result = {'batch_id': batch_id, 'rows': n, 'digest': digest,
                  'max_payload': maximum, 'publication_id': head,
                  'publication': publication, 'latest_attempt': latest}
        binding = self.derivation(batch_id)
        if binding is not None:
            result['parent_publication_id'] = binding['parent_publication_id']
            result['radius'] = binding['radius']
        return result

    def close(self):
        self.db.close()
