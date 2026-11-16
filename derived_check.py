"""Independent full-relation oracle and public integration fixtures; no refresh implementation."""
import argparse
import json
import random
import tempfile
import tracemalloc
import unittest
from pathlib import Path

from warehouse import Warehouse, ValidationError, BatchConflict, NotFound, NotPublished


def row(rid, part, key, amount):
    return dict(row_id=rid, partition=part, order_key=key, amount=amount)


def image(value):
    return {key: value[key] for key in ('partition', 'order_key', 'amount')}


def change(before, after):
    value = before if before is not None else after
    return dict(row_id=value['row_id'],
                before=None if before is None else image(before),
                after=None if after is None else image(after))


def child_rows(parent, changes):
    # Test-only full materialization; deliberately not a production algorithm.
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


def relation(rows, radius):
    return [dict(row_id=current['row_id'], window_sum=sum(
        other['amount'] for other in rows
        if other['partition'] == current['partition']
        and abs(other['order_key'] - current['order_key']) <= radius
        and (other['order_key'] != current['order_key']
             or other['row_id'] == current['row_id']))) for current in rows]


def allowed(rows, changes, radius):
    ids = {patch['row_id'] for patch in changes if patch['after'] is not None}
    positions = [patch[side] for patch in changes for side in ('before', 'after')
                 if patch[side] is not None]
    return {value['row_id'] for value in rows if value['row_id'] in ids or any(
        value['partition'] == pos['partition']
        and abs(value['order_key'] - pos['order_key']) <= radius for pos in positions)}


class Audit:
    def __init__(self, rows, changes, radius):
        self.ids = {value['row_id'] for value in rows}
        self.allowed = allowed(rows, changes, radius)
        self.evaluated, self.sorted, self.reused = set(), set(), set()
        self.prepared = None
        self.events = dict(evaluate=0, sort=0, reuse=0)

    def __call__(self, event, details):
        if event == 'refresh_work':
            kind, rid = details['kind'], details['row_id']
            assert kind in self.events, 'unknown work kind'
            assert rid in self.ids, 'work refers to absent child identity'
            self.events[kind] += 1
            if kind in ('evaluate', 'sort'):
                assert rid in self.allowed, 'NONLOCAL_WINDOW_WORK'
                (self.evaluated if kind == 'evaluate' else self.sorted).add(rid)
            else:
                assert rid not in self.reused, 'duplicate reuse'
                self.reused.add(rid)
        elif event == 'refresh_prepared':
            assert self.prepared is None, 'duplicate preparation accounting'
            assert all(type(details[key]) is int and details[key] >= 0
                       for key in ('copied_rows', 'indexed_rows'))
            self.prepared = dict(details)

    def finish(self):
        assert self.prepared is not None, 'missing preparation accounting'
        assert not self.evaluated.intersection(self.reused), 'mixed reuse and evaluation'
        assert self.ids == self.evaluated.union(self.reused), 'missing output provenance'


def sample():
    parent = [row('a', 'p', 10, 3), row('b', 'p', 10, 8),
              row('c', 'p', 11, 5), row('d', 'q', 100, 7)]
    changes = [change(parent[0], row('a', 'q', 100, 4))]
    return parent, changes


def reference():
    parent, changes = sample()
    child = child_rows(parent, changes)
    assert [r['window_sum'] for r in relation(parent, 1)] == [8, 13, 16, 7]
    assert [r['window_sum'] for r in relation(child, 1)] == [4, 13, 13, 7]
    assert [r['window_sum'] for r in relation(parent, 0)] == [3, 8, 5, 7]
    assert allowed(child, changes, 1) == {'a', 'b', 'c', 'd'}
    empty = child_rows(parent, [change(value, None) for value in parent])
    assert empty == [] and relation(empty, 1) == []
    print('DERIVED_REFERENCE_OK')


def scenario(root, fail_once=True):
    parent, changes = sample()
    # Unaffected rows whose radius=1 results differ from the later parent head.
    parent += [row('e', 'untouched', 0, 2), row('f', 'untouched', 1, 9)]
    with Warehouse(root) as w:
        w.ingest('parent', parent)
        pub = w.compute('parent', 1, 4096)['publication_id']
        accepted = w.derive('child', pub, iter(changes))
        w.compute('parent', 0, 4096)
        if fail_once:
            fired = []
            def port(event, details):
                if event == 'publish_ready':
                    fired.append(event)
                    raise OSError('derived publication interruption')
            try:
                w.refresh('child', 4096, event_port=port)
            except OSError as error:
                assert str(error) == 'derived publication interruption'
            else:
                raise AssertionError('failure port did not interrupt publication')
            assert fired == ['publish_ready']
            try:
                w.read('child')
            except NotPublished:
                pass
            else:
                raise AssertionError('partial first publication is visible')
            assert w.status('child')['latest_attempt']['state'] == 'failed'
    with Warehouse(root) as w:
        audit = Audit(child_rows(parent, changes), changes, 1)
        receipt = w.refresh('child', 4096, event_port=audit)
        audit.finish()
        result = w.read('child')
        assert result == relation(child_rows(parent, changes), 1), 'DERIVED_ROW_MISMATCH'
        assert w.read('parent') == relation(parent, 0)
        assert receipt['parent_publication_id'] == pub and receipt['radius'] == 1
        assert w.derive('child', pub, iter(changes)) == accepted
        status = w.status('child')
        assert status['parent_publication_id'] == pub and status['radius'] == 1
        return dict(accepted=accepted, receipt=receipt, results=result,
                    parent_current=w.read('parent'), work=audit.events,
                    preparation=audit.prepared)


class DerivedTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='derived-check-')
        self.root = Path(self.temp.name) / 'data'
        self.w = Warehouse(self.root)

    def tearDown(self):
        self.w.close()
        self.temp.cleanup()

    def prepare(self, rows, radius, name='p'):
        self.w.ingest(name, iter(rows))
        return self.w.compute(name, radius, 131072)['publication_id']

    def check_refresh(self, name, rows, changes, radius, expected=None):
        audit = Audit(rows, changes, radius)
        receipt = self.w.refresh(name, 131072, event_port=audit)
        audit.finish()
        self.assertEqual(self.w.read(name), relation(rows, radius) if expected is None else expected,
                         'DERIVED_ROW_MISMATCH')
        self.assertEqual(receipt['rows'], len(rows))
        self.assertEqual(receipt['radius'], radius)
        self.assertLessEqual(receipt['stats']['peak_payload'], 131072)
        self.assertEqual(list(self.w.work.iterdir()), [])
        return receipt, audit

    def test_joint_binding_failure_reopen(self):
        scenario(self.root)

    def test_rejection_and_retry(self):
        parent, changes = sample()
        pub = self.prepare(parent, 1)
        accepted = self.w.derive('child', pub, iter(changes))
        self.assertEqual(accepted, self.w.derive('child', pub, iter(changes)))
        with self.assertRaises(BatchConflict):
            self.w.derive('child', pub, [])
        with self.assertRaises(BatchConflict):
            self.w.derive('p', pub, [])
        invalid = [
            [dict(changes[0], before=dict(changes[0]['before'], amount=999))],
            [changes[0], changes[0]],
            [dict(changes[0], after=dict(changes[0]['after'], amount=True))],
            [dict(changes[0], before=None)],
            [dict(row_id='missing', before=None, after=None)],
        ]
        for values in invalid:
            with self.assertRaises(ValidationError):
                self.w.derive('bad', pub, values)
            with self.assertRaises(NotFound):
                self.w.status('bad')
        def broken():
            yield changes[0]
            raise OSError('source interruption')
        with self.assertRaisesRegex(OSError, 'source interruption'):
            self.w.derive('bad', pub, broken())
        with self.assertRaises(NotFound):
            self.w.status('bad')
        before = self.w.status('child')
        with self.assertRaises(ValidationError):
            self.w.refresh('child', 100)
        self.assertEqual(self.w.status('child'), before)
        with self.assertRaises(ValidationError):
            self.w.compute('child', 0, 4096)
        with self.assertRaises(ValidationError):
            self.w.refresh('p', 4096)
        receipt, _ = self.check_refresh('child', child_rows(parent, changes), changes, 1)
        with self.assertRaises(ValidationError):
            self.w.derive('grandchild', receipt['publication_id'], [])
        old = self.w.read('child')
        head = receipt['publication_id']
        def fail(event, details):
            if event == 'publish_ready':
                raise OSError('replacement interruption')
        with self.assertRaisesRegex(OSError, 'replacement interruption'):
            self.w.refresh('child', 131072, event_port=fail)
        self.assertEqual(self.w.read('child'), old)
        self.assertEqual(self.w.status('child')['publication_id'], head)
        self.w.close()
        self.w = Warehouse(self.root)
        self.check_refresh('child', child_rows(parent, changes), changes, 1)

    def test_order_insert_delete_noop_empty(self):
        parent, _ = sample()
        pub = self.prepare(parent, 1)
        changes = [change(None, row('z', 'p', 9, 0)), change(parent[1], None),
                   change(parent[0], parent[0]), change(None, row('y', 'p', 11, -4))]
        child = child_rows(parent, changes)
        self.assertEqual([value['row_id'] for value in child], ['a', 'c', 'd', 'z', 'y'])
        self.w.derive('child', pub, changes)
        self.check_refresh('child', child, changes, 1)
        with self.assertRaises(BatchConflict):
            self.w.derive('child', pub, list(reversed(changes)))
        self.w.derive('same', pub, [])
        self.check_refresh('same', parent, [], 1)
        deleted = [change(value, None) for value in parent]
        self.w.derive('empty', pub, deleted)
        self.check_refresh('empty', [], deleted, 1)

    def test_full_relation_differential(self):
        rng = random.Random(6841)
        for trial in range(10):
            huge = 1 << (20000 if trial == 0 else 90)
            parent = [row('r' + str(i), rng.choice(['p', 'q', '区']),
                          rng.randrange(-3, 4) + huge, rng.randrange(-9, 10) * huge)
                      for i in range(18)]
            rng.shuffle(parent)
            radius = trial % 3
            pub = self.prepare(parent, radius, 'p' + str(trial))
            changes = [change(parent[0], None),
                       change(parent[1], row(parent[1]['row_id'], 'q', huge + 2, -huge)),
                       change(parent[2], dict(parent[2], amount=0)),
                       change(None, row('new', 'p', huge, 8))]
            name = 'c' + str(trial)
            self.w.derive(name, pub, iter(changes))
            self.check_refresh(name, child_rows(parent, changes), changes, radius)

    def test_locality_and_working_set(self):
        peaks, evaluated = [], []
        for n in (200, 2000):
            parent = [row('a', 'p', 10, 3), row('b', 'p', 11, 5)]
            parent += [row('u' + str(i), 'unused' + str(i), i, i) for i in range(n)]
            pub = self.prepare(parent, 1, 'p' + str(n))
            changes = [change(parent[0], dict(parent[0], amount=4))]
            child = child_rows(parent, changes)
            name = 'c' + str(n)
            self.w.derive(name, pub, changes)
            audit = Audit(child, changes, 1)
            tracemalloc.start()
            try:
                receipt = self.w.refresh(name, 4096, event_port=audit)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            audit.finish()
            peaks.append(peak)
            evaluated.append(audit.evaluated)
            expected = relation(child[:2], 1) + [
                dict(row_id=value['row_id'], window_sum=value['amount']) for value in child[2:]]
            self.assertEqual(self.w.read(name), expected, 'DERIVED_ROW_MISMATCH')
            self.assertLessEqual(receipt['stats']['peak_payload'], 4096)
        self.assertEqual(evaluated[0], evaluated[1])
        # Same broad Python-allocation guard as the original external regression.
        self.assertLess(max(peaks), 8 * 1024 * 1024)
        self.assertLessEqual(peaks[1] - peaks[0], 2 * 1024 * 1024)

    def test_oversized_peer_group(self):
        parent = [row('r' + str(i) + 'x' * 80, 'p', 10, i) for i in range(180)]
        parent += [row('neighbor', 'p', 11, 5)]
        pub = self.prepare(parent, 1)
        changes = [change(parent[0], dict(parent[0], amount=-999))]
        child = child_rows(parent, changes)
        self.w.derive('child', pub, iter(changes))
        audit = Audit(child, changes, 1)
        receipt = self.w.refresh('child', 4096, event_port=audit)
        audit.finish()
        self.assertGreater(len(parent) * self.w.status('child')['max_payload'], 4096)
        self.assertLessEqual(receipt['stats']['peak_payload'], 4096)
        self.assertEqual(self.w.read('child'), relation(child, 1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['reference', 'verify', 'demo'])
    args = parser.parse_args()
    if args.command == 'reference':
        reference()
    elif args.command == 'verify':
        suite = unittest.defaultTestLoader.discover('tests')
        suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(DerivedTests))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        if not result.wasSuccessful():
            raise SystemExit(1)
    else:
        with tempfile.TemporaryDirectory(prefix='derived-demo-') as directory:
            print(json.dumps(scenario(Path(directory) / 'data'), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
