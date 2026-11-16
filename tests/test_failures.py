from warehouse import Warehouse
from tests.support import BusinessCase, FailAt, row


class FailureTests(BusinessCase):
    def prepare(self):
        rows = [row('r' + str(i), 'p', i % 3, i - 20) for i in range(50)]
        self.w.ingest('b', rows)
        self.w.compute('b', 0, 2048, fan_in=2)
        return rows, self.w.read('b'), self.w.status('b')['publication_id']

    def test_sort_write_failure(self):
        rows, old, head = self.prepare()
        def inspect(details):
            from pathlib import Path
            self.assertGreater(Path(details['path']).stat().st_size, 0)
        port = FailAt('sort_run', inspect)
        with self.assertRaisesRegex(OSError, 'injected sort_run'):
            self.w.compute('b', 2, 2048, fan_in=2, event_port=port)
        self.assertTrue(port.fired)
        self.assertEqual(self.w.read('b'), old)
        status = self.w.status('b')
        self.assertEqual(status['publication_id'], head)
        self.assertEqual(status['latest_attempt']['state'], 'failed')
        self.assertEqual(status['latest_attempt']['error_type'], 'OSError')
        self.assert_clean()
        self.w.compute('b', 2, 2048, fan_in=2)
        self.assert_relation('b', rows, 2)

    def test_publish_failure_and_retry(self):
        rows, old, head = self.prepare()
        def inspect(details):
            self.assertEqual(details['rows'], len(rows))
            self.assertEqual(self.w.read('b'), old)
            # Workspace release precedes the publication switch.
            self.assert_clean()
        port = FailAt('publish_ready', inspect)
        with self.assertRaisesRegex(OSError, 'injected publish_ready'):
            self.w.compute('b', 2, 2048, fan_in=2, event_port=port)
        self.assertTrue(port.fired)
        self.assertEqual(self.w.read('b'), old)
        self.assertEqual(self.w.status('b')['publication_id'], head)
        db = self.w.catalog.db
        self.assertEqual(db.execute('SELECT count(*) FROM publications').fetchone()[0], 1)
        self.assertEqual(db.execute('SELECT count(*) FROM outputs').fetchone()[0], len(rows))
        self.assertEqual(self.w.status('b')['latest_attempt']['state'], 'failed')
        self.assert_clean()
        self.w.close()
        self.w = Warehouse(self.root)
        self.assertEqual(self.w.read('b'), old)
        self.w.compute('b', 2, 2048, fan_in=2, delivery_rows=1)
        self.assert_relation('b', rows, 2)

    def test_window_stage_failure(self):
        _, old, head = self.prepare()
        port = FailAt('window_staged')
        with self.assertRaisesRegex(OSError, 'window_staged'):
            self.w.compute('b', 1, 2048, fan_in=2, event_port=port)
        self.assertTrue(port.fired)
        self.assertEqual(self.w.read('b'), old)
        self.assertEqual(self.w.status('b')['publication_id'], head)
        self.assert_clean()
