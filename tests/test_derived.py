"""Contract tests for derived correction batches and local refresh.

Independent relation expectations are enumerated from the public RANGE /
EXCLUDE TIES definition; these tests supplement derived_check.py without
replacing it.
"""
import random
import sqlite3
import tracemalloc
from warehouse import (Warehouse, ValidationError, BatchConflict,
                       NotFound, NotPublished)
from tests.support import BusinessCase, FailAt, relation, row


def image(value):
    return {key: value[key] for key in ('partition', 'order_key', 'amount')}


def change(before, after):
    value = before if before is not None else after
    return dict(row_id=value['row_id'],
                before=None if before is None else image(before),
                after=None if after is None else image(after))


def child_rows(parent, changes):
    patches = {value['row_id']: value for value in changes}
    old_ids = {value['row_id'] for value in parent}
    result = []
    for value in parent:
        patch = patches.get(value['row_id'])
        if patch is None:
            result.append(dict(value))
        elif patch['after'] is not None:
            result.append(dict(row_id=value['row_id'], **patch['after']))
    for patch in changes:
        if patch['row_id'] not in old_ids and patch['after'] is not None:
            result.append(dict(row_id=patch['row_id'], **patch['after']))
    return result


class Recorder:
    def __init__(self):
        self.events = []

    def __call__(self, event, details):
        self.events.append((event, dict(details)))


class DerivedContractTests(BusinessCase):
    def prepare_parent(self, rows, radius, name='parent', budget=131072):
        self.w.ingest(name, iter(rows))
        return self.w.compute(name, radius, budget)['publication_id']

    def refresh_and_audit(self, name, rows, changes, radius, budget=131072):
        recorder = Recorder()
        receipt = self.w.refresh(name, budget, event_port=recorder)
        kinds = [d['kind'] for e, d in recorder.events if e == 'refresh_work']
        # Every child row has exactly one terminal provenance event.
        work_ids = [d['row_id'] for e, d in recorder.events if e == 'refresh_work']
        reuse = [d['row_id'] for e, d in recorder.events
                 if e == 'refresh_work' and d['kind'] == 'reuse']
        self.assertEqual(len(reuse), len(set(reuse)))
        self.assertEqual(sorted(work_ids), sorted(value['row_id'] for value in rows))
        prepared = [d for e, d in recorder.events if e == 'refresh_prepared']
        self.assertEqual(len(prepared), 1)
        self.assertGreaterEqual(prepared[0]['copied_rows'], 0)
        self.assertGreaterEqual(prepared[0]['indexed_rows'], 0)
        self.assertEqual(prepared[0]['copied_rows'], len(rows))
        self.assertEqual(self.w.read(name), relation(rows, radius),
                         'DERIVED_ROW_MISMATCH')
        self.assertEqual(receipt['rows'], len(rows))
        self.assertEqual(receipt['radius'], radius)
        self.assertLessEqual(receipt['stats']['peak_payload'], budget)
        self.assertNotIn('sort', kinds)
        self.assert_clean()
        return receipt, recorder

    def test_hand_computed_sample_and_fixed_binding(self):
        parent = [row('a', 'p', 10, 3), row('b', 'p', 10, 8),
                  row('c', 'p', 11, 5), row('d', 'q', 100, 7),
                  row('e', 'untouched', 0, 2), row('f', 'untouched', 1, 9)]
        pub = self.prepare_parent(parent, 1)
        changes = [change(parent[0], row('a', 'q', 100, 4))]
        accepted = self.w.derive('child', pub, iter(changes))
        self.assertEqual(set(accepted),
                         {'child_id', 'parent_publication_id', 'radius', 'digest', 'rows'})
        self.assertEqual(accepted['radius'], 1)
        with self.assertRaises(NotPublished):
            self.w.read('child')
        # Parent republished at a different radius must not move the binding.
        self.w.compute('parent', 0, 131072)
        self.assertEqual(self.w.read('parent'), relation(parent, 0))
        child = child_rows(parent, changes)
        receipt, recorder = self.refresh_and_audit('child', child, changes, 1)
        self.assertEqual(receipt['parent_publication_id'], pub)
        self.assertEqual(self.w.read('child'), relation(child, 1))
        # The untouched partition keeps radius=1 results, not parent head's.
        by_id = {value['row_id']: value['window_sum'] for value in self.w.read('child')}
        self.assertEqual(by_id['e'], 11)
        self.assertEqual(by_id['f'], 11)
        self.assertEqual(by_id['a'], 4)
        self.assertEqual(by_id['d'], 7)
        status = self.w.status('child')
        self.assertEqual(status['parent_publication_id'], pub)
        self.assertEqual(status['radius'], 1)
        self.assertIn('digest', status)
        # Repeated refresh yields a new publication on the same fixed binding.
        again = self.w.refresh('child', 131072)
        self.assertNotEqual(again['publication_id'], receipt['publication_id'])
        self.assertEqual(again['parent_publication_id'], pub)
        self.assertEqual(self.w.read('child'), relation(child, 1))

    def test_cross_partition_move_amount_change_insert_delete(self):
        parent = [row('a', 'p', 10, 3), row('b', 'p', 11, 8),
                  row('c', 'q', 5, 5), row('d', 'q', 6, -2)]
        pub = self.prepare_parent(parent, 2)
        changes = [
            change(parent[2], None),                                  # delete
            change(parent[0], row('a', 'r', 0, 9)),                  # move + amount
            change(None, row('z', 'p', 12, 4)),                      # insert
            change(parent[1], parent[1]),                            # no-op
        ]
        child = child_rows(parent, changes)
        self.assertEqual([value['row_id'] for value in child],
                         ['a', 'b', 'd', 'z'])
        self.w.derive('child', pub, changes)
        self.refresh_and_audit('child', child, changes, 2)

    def test_zero_radius_and_unbounded_integers(self):
        huge = 1 << 5000
        parent = [row('a', 'p', -huge, huge), row('b', 'p', -huge, -huge + 1),
                  row('c', 'p', huge, 7), row('d', 'q', 0, huge * 3)]
        pub = self.prepare_parent(parent, 0)
        changes = [change(parent[1], dict(parent[1], amount=0)),
                   change(None, row('new', 'q', 1, -huge))]
        child = child_rows(parent, changes)
        self.w.derive('child', pub, iter(changes))
        receipt, _ = self.refresh_and_audit('child', child, changes, 0)
        # radius=0 means each row is exactly its own amount.
        self.assertEqual(self.w.read('child'),
                         [{'row_id': r['row_id'], 'window_sum': r['amount']}
                          for r in child])
        for value in self.w.read('child'):
            self.assertEqual(type(value['window_sum']), int)
        self.assertGreater(receipt['stats']['max_record_bytes'], 1000)

    def test_same_key_exclusion_after_move(self):
        parent = [row('a', 'p', 10, 3), row('b', 'p', 10, 8),
                  row('d', 'q', 100, 7)]
        pub = self.prepare_parent(parent, 1)
        changes = [change(parent[0], row('a', 'q', 100, 4))]
        child = child_rows(parent, changes)
        self.w.derive('child', pub, changes)
        self.refresh_and_audit('child', child, changes, 1)
        totals = {r['row_id']: r['window_sum'] for r in self.w.read('child')}
        # a and d share a key in q: each excludes the other tie.
        self.assertEqual(totals['a'], 4)
        self.assertEqual(totals['d'], 7)

    def test_idempotent_retry_and_conflicts(self):
        parent = [row('a', 'p', 1, 2), row('b', 'p', 2, 5)]
        pub = self.prepare_parent(parent, 1)
        changes = [change(None, row('z', 'p', 0, 1)),
                   change(None, row('y', 'p', 3, 2))]
        accepted = self.w.derive('child', pub, iter(changes))
        self.assertEqual(self.w.derive('child', pub, iter(changes)), accepted)
        # Same resulting input but a different correction sequence conflicts.
        with self.assertRaises(BatchConflict):
            self.w.derive('child', pub, list(reversed(changes)))
        with self.assertRaises(BatchConflict):
            self.w.derive('child', pub, [])
        # A child id colliding with a normal batch conflicts.
        with self.assertRaises(BatchConflict):
            self.w.derive('parent', pub, [])
        with self.assertRaises(BatchConflict):
            self.w.ingest('child', parent)
        other = self.prepare_parent([row('x', 'p', 1, 1)], 1, name='other')
        with self.assertRaises(BatchConflict):
            self.w.derive('child', other, iter(changes))

    def test_bad_inputs_and_source_failure_leave_nothing(self):
        parent = [row('a', 'p', 1, 2)]
        pub = self.prepare_parent(parent, 1)
        valid = change(parent[0], parent[0])
        invalid_sequences = [
            [dict(valid, before=dict(valid['before'], amount=999))],   # stale before
            [valid, valid],                                            # duplicate id
            [dict(valid, after=dict(valid['after'], amount=True))],    # bool amount
            [dict(row_id='a', before=None, after=image(parent[0]))],   # existing, None before
            [dict(row_id='ghost', before=None, after=None)],           # both None
            [dict(row_id='a', before=image(parent[0]))],               # missing after key
            [{'row_id': 'a', 'before': image(parent[0])}],             # missing field
            'not-a-dict',
        ]
        for values in invalid_sequences:
            with self.assertRaises(ValidationError):
                self.w.derive('bad', pub, [values] if isinstance(values, str) else values)
            with self.assertRaises(NotFound):
                self.w.status('bad')

        def broken():
            yield valid
            raise OSError('source interruption')

        with self.assertRaisesRegex(OSError, 'source interruption'):
            self.w.derive('bad', pub, broken())
        with self.assertRaises(NotFound):
            self.w.status('bad')
        # Unknown publication and derived-parent bindings.
        with self.assertRaises(NotFound):
            self.w.derive('bad', 999999, [])
        self.w.derive('kid', pub, [])
        derived_pub = self.w.refresh('kid', 131072)['publication_id']
        with self.assertRaises(ValidationError):
            self.w.derive('grandchild', derived_pub, [])
        with self.assertRaises(ValidationError):
            self.w.derive('x', True, [])

    def test_cross_api_guards_start_no_attempt(self):
        parent = [row('a', 'p', 1, 2)]
        pub = self.prepare_parent(parent, 1)
        self.w.derive('child', pub, [])
        before = self.w.status('child')
        with self.assertRaises(ValidationError):
            self.w.compute('child', 0, 4096)
        with self.assertRaises(ValidationError):
            self.w.refresh('parent', 4096)
        with self.assertRaises(ValidationError):
            self.w.refresh('child', 100)
        self.assertEqual(self.w.status('child'), before)
        self.assertEqual(before['latest_attempt'], None)
        with self.assertRaises(NotFound):
            self.w.refresh('missing', 4096)

    def test_publish_failure_preserves_old_head_and_reopens(self):
        parent = [row('a', 'p', 1, 2), row('b', 'p', 2, 3)]
        pub = self.prepare_parent(parent, 1)
        changes = [change(parent[0], dict(parent[0], amount=9))]
        child = child_rows(parent, changes)
        self.w.derive('child', pub, changes)
        port = FailAt('publish_ready')
        with self.assertRaisesRegex(OSError, 'injected publish_ready'):
            self.w.refresh('child', 131072, event_port=port)
        with self.assertRaises(NotPublished):
            self.w.read('child')
        self.assertEqual(self.w.status('child')['latest_attempt']['state'], 'failed')
        self.assert_clean()
        # Retry in a reopened worker.
        self.w.close()
        self.w = Warehouse(self.root)
        receipt, _ = self.refresh_and_audit('child', child, changes, 1)
        self.assertEqual(self.w.status('child')['latest_attempt']['state'], 'published')
        old = self.w.read('child')
        head = receipt['publication_id']
        with self.assertRaisesRegex(OSError, 'injected publish_ready'):
            self.w.refresh('child', 131072, event_port=FailAt('publish_ready'))
        self.assertEqual(self.w.read('child'), old)
        self.assertEqual(self.w.status('child')['publication_id'], head)

    def test_unrelated_partitions_are_not_evaluated(self):
        parent = [row('a', 'p', 10, 3), row('b', 'p', 11, 5)]
        parent += [row('u' + str(i), 'unused' + str(i), i, i) for i in range(300)]
        pub = self.prepare_parent(parent, 1)
        changes = [change(parent[0], dict(parent[0], amount=4))]
        child = child_rows(parent, changes)
        self.w.derive('child', pub, changes)
        recorder = Recorder()
        self.w.refresh('child', 4096, event_port=recorder)
        evaluated = {d['row_id'] for e, d in recorder.events
                     if e == 'refresh_work' and d['kind'] == 'evaluate'}
        reused = {d['row_id'] for e, d in recorder.events
                  if e == 'refresh_work' and d['kind'] == 'reuse'}
        self.assertEqual(evaluated, {'a', 'b'})
        self.assertNotIn('a', reused)
        self.assertTrue({r['row_id'] for r in child[2:]}.issubset(reused))
        self.assertEqual(evaluated.union(reused),
                         {r['row_id'] for r in child})

    def test_fixed_budget_working_set_is_flat(self):
        peaks = []
        for n in (200, 2000):
            parent = [row('a', 'p', 10, 3), row('b', 'p', 11, 5)]
            parent += [row('u' + str(i), 'unused' + str(i), i, i) for i in range(n)]
            pub = self.prepare_parent(parent, 1, name='parent' + str(n))
            changes = [change(parent[0], dict(parent[0], amount=4))]
            child = child_rows(parent, changes)
            name = 'child' + str(n)
            self.w.derive(name, pub, changes)
            tracemalloc.start()
            try:
                receipt = self.w.refresh(name, 4096)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            peaks.append(peak)
            self.assertLessEqual(receipt['stats']['peak_payload'], 4096)
            self.assertEqual(self.w.read(name), relation(child, 1))
        self.assertLess(max(peaks), 8 * 1024 * 1024)
        self.assertLessEqual(peaks[1] - peaks[0], 2 * 1024 * 1024)

    def test_oversized_peer_group_respects_payload_budget(self):
        parent = [row('r' + str(i) + 'x' * 80, 'p', 10, i) for i in range(180)]
        parent.append(row('neighbor', 'p', 11, 5))
        pub = self.prepare_parent(parent, 1)
        changes = [change(parent[0], dict(parent[0], amount=-999))]
        child = child_rows(parent, changes)
        self.w.derive('child', pub, iter(changes))
        receipt, _ = self.refresh_and_audit('child', child, changes, 1, budget=4096)
        self.assertGreater(len(parent) * self.w.status('child')['max_payload'], 4096)
        self.assertLessEqual(receipt['stats']['peak_payload'], 4096)

    def test_parent_index_build_once_and_reused(self):
        parent = [row('a', 'p', 1, 2), row('b', 'p', 2, 3), row('c', 'q', 1, 1)]
        pub = self.prepare_parent(parent, 1)
        db = self.w.catalog.db
        self.assertIsNone(db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='parent_range'").fetchone())
        self.w.derive('c1', pub, [change(parent[0], dict(parent[0], amount=9))])
        first = self.w.refresh('c1', 131072)
        self.assertTrue(first['stats']['parent_index_built'])
        self.assertGreater(first['stats']['indexed_rows'], 0)
        self.assertIsNotNone(db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='parent_range'").fetchone())
        # A second child on the same parent reuses the persistent index.
        self.w.derive('c2', pub, [change(parent[1], dict(parent[1], amount=0))])
        second = self.w.refresh('c2', 131072)
        self.assertFalse(second['stats']['parent_index_built'])
        self.assertEqual(second['stats']['indexed_rows'], 0)
        # Parent data and all parent publications remain intact.
        self.assertEqual(self.w.read('parent'), relation(parent, 1))
        self.assertEqual(db.execute(
            'SELECT count(*) FROM publications WHERE batch_id=?', ('parent',)).fetchone()[0], 1)

    def test_empty_and_all_deleted_children(self):
        parent = [row('a', 'p', 1, 2), row('b', 'p', 2, 3)]
        pub = self.prepare_parent(parent, 1)
        self.w.derive('same', pub, [])
        receipt, _ = self.refresh_and_audit('same', parent, [], 1)
        self.assertEqual(receipt['rows'], 2)
        deleted = [change(value, None) for value in parent]
        self.w.derive('empty', pub, deleted)
        recorder = Recorder()
        receipt = self.w.refresh('empty', 131072, event_port=recorder)
        self.assertEqual(receipt['rows'], 0)
        self.assertEqual(self.w.read('empty'), [])
        prepared = [d for e, d in recorder.events if e == 'refresh_prepared'][0]
        self.assertEqual(prepared, {'copied_rows': 0, 'indexed_rows': 0})
        work = [d for e, d in recorder.events if e == 'refresh_work']
        self.assertEqual(work, [])

    def test_differential_random_relations(self):
        rng = random.Random(2026)
        for trial in range(8):
            rows = [row('r' + str(i), rng.choice(['p', 'q', '区']),
                        rng.randrange(-4, 5), rng.randrange(-20, 21))
                    for i in range(rng.randrange(1, 25))]
            rng.shuffle(rows)
            radius = rng.choice([0, 1, 3])
            pub = self.prepare_parent(rows, radius, name='p' + str(trial))
            if not rows:
                continue
            changes = [change(rows[0], None)]
            if len(rows) > 1:
                changes.append(change(rows[1], row(rows[1]['row_id'], 'q', 7, 1)))
            changes.append(change(None, row('new' + str(trial), 'p', 0, 4)))
            child = child_rows(rows, changes)
            name = 'c' + str(trial)
            self.w.derive(name, pub, iter(changes))
            self.refresh_and_audit(name, child, changes, radius)

    def test_reopen_preserves_binding_and_inputs(self):
        parent = [row('a', 'p', 1, 2), row('b', 'p', 2, 3)]
        pub = self.prepare_parent(parent, 1)
        changes = [change(parent[0], dict(parent[0], amount=7))]
        child = child_rows(parent, changes)
        self.w.derive('child', pub, changes)
        self.w.close()
        self.w = Warehouse(self.root)
        status = self.w.status('child')
        self.assertEqual(status['parent_publication_id'], pub)
        self.assertEqual(status['radius'], 1)
        self.refresh_and_audit('child', child, changes, 1)
        # Idempotent re-derivation still works after reopen.
        self.assertEqual(self.w.derive('child', pub, iter(changes))['digest'],
                         status['digest'])
