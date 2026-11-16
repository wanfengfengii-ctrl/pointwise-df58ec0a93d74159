from warehouse import NotPublished, NotFound, Warehouse
from tests.support import BusinessCase, row


class BusinessTests(BusinessCase):
    def test_process_replace_reopen(self):
        rows = [row('a', 'north', -2, 17), row('b', 'south', -2, -8),
                row('c', 'north', -1, -5), row('d', 'north', -2, 17),
                row('e', 'north', 0, 9)]
        self.w.ingest('readings', iter(rows))
        with self.assertRaises(NotPublished):
            self.w.read('readings')
        first = self.w.compute('readings', 1, 4096, fan_in=2, delivery_rows=1)
        self.assert_relation('readings', rows, 1)
        second = self.w.compute('readings', 2, 4096, delivery_rows=3)
        self.assertNotEqual(first['publication_id'], second['publication_id'])
        self.assert_relation('readings', rows, 2)
        self.assertEqual(self.w.status('readings')['latest_attempt']['state'], 'published')
        self.w.close()
        self.w = Warehouse(self.root)
        self.assert_relation('readings', rows, 2)
        iterator = self.w.iter_read('readings')
        self.assertEqual(next(iterator), self.w.read('readings')[0])
        iterator.close()
        self.assert_clean()

    def test_empty_and_unknown(self):
        self.w.ingest('empty', iter(()))
        receipt = self.w.compute('empty', 0, 1024)
        self.assertEqual(receipt['rows'], 0)
        self.assertEqual(self.w.read('empty'), [])
        for operation in (self.w.read, self.w.status):
            with self.assertRaises(NotFound):
                operation('missing')
        with self.assertRaises(NotFound):
            self.w.compute('missing', 0, 1024)
        self.assert_clean()
