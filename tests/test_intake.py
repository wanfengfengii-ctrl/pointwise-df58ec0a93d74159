from warehouse import ValidationError, BatchConflict, NotFound
from tests.support import BusinessCase, row


class IntakeTests(BusinessCase):
    def test_retry(self):
        rows = [row('one', 'p', 1, 2), row('two', 'p', 1, 2)]
        receipt = self.w.ingest('b', iter(rows))
        self.w.compute('b', 0, 2048)
        before = self.w.status('b')
        self.assertEqual(self.w.ingest('b', iter(rows)), receipt)
        self.assertEqual(self.w.status('b'), before)
        self.assert_relation('b', rows, 0)

    def test_duplicate(self):
        with self.assertRaises(ValidationError):
            self.w.ingest('bad', [row('same', 'p', 1, 2), row('same', 'q', 2, 3)])
        with self.assertRaises(NotFound):
            self.w.status('bad')
        self.assertEqual(self.w.catalog.db.execute('SELECT count(*) FROM inputs').fetchone()[0], 0)

    def test_conflict(self):
        original = [row('a', 'p', 1, 3), row('b', 'p', 2, 5)]
        self.w.ingest('b', original)
        self.w.compute('b', 1, 2048)
        before = self.w.status('b')
        for changed in ([row('a', 'p', 1, 4), original[1]], list(reversed(original))):
            with self.assertRaises(BatchConflict):
                self.w.ingest('b', changed)
            self.assertEqual(self.w.status('b'), before)
            self.assert_relation('b', original, 1)

    def test_fields_and_source_failure(self):
        valid = row('x', 'p', 1, 2)
        invalid = [dict(valid, amount=True), dict(valid, order_key=1.0),
                   dict(valid, row_id=''), dict(valid, partition='x' * 257),
                   dict(valid, extra=2), dict(valid, row_id='\ud800')]
        for raw in invalid:
            with self.assertRaises(ValidationError):
                self.w.ingest('invalid', [valid, raw])
            with self.assertRaises(NotFound):
                self.w.status('invalid')

        def source():
            yield valid
            raise OSError('source stopped')
        with self.assertRaisesRegex(OSError, 'source stopped'):
            self.w.ingest('partial', source())
        with self.assertRaises(NotFound):
            self.w.status('partial')
