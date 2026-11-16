import random
import tracemalloc
from pathlib import Path
from tests.support import BusinessCase, row


class ExternalTests(BusinessCase):
    def test_oversized_peers_and_transport_invariance(self):
        huge = 1 << 200
        rows = [row(('r%04d-' % i) + 'x' * 80, 'p', 10,
                    (huge if i % 2 else -huge) + i) for i in range(160)]
        rows += [row('left', 'p', 9, 5), row('right', 'p', 11, 7),
                 row('outside', 'p', 12, 999), row('isolated', 'q', 10, -3)]
        random.Random(41).shuffle(rows)
        for cut in (1, 17):
            def chunked():
                for offset in range(0, len(rows), cut):
                    yield from rows[offset:offset + cut]
            batch = 'source' + str(cut)
            self.w.ingest(batch, chunked())
            maximum = self.w.status(batch)['max_payload']
            baseline = None
            for fan_in, delivery in ((2, 1), (3, 7), (7, 11)):
                observed = {'runs': 0, 'staged': False}

                def port(event, details):
                    if event == 'sort_run':
                        observed['runs'] += 1
                        self.assertGreater(Path(details['path']).stat().st_size, 0)
                    if event == 'window_staged':
                        paths = list(self.w.work.glob('job-*/candidate.sqlite'))
                        self.assertEqual(len(paths), 1)
                        self.assertEqual(paths[0].stat().st_size, details['bytes'])
                        observed['staged'] = True

                budget = max(2048, (fan_in + 4) * maximum + 128)
                receipt = self.w.compute(batch, 1, budget, fan_in=fan_in,
                                         delivery_rows=delivery, event_port=port)
                self.assert_relation(batch, rows, 1)
                output = self.w.read(batch)
                if baseline is not None:
                    self.assertEqual(output, baseline, 'WINDOW_ROW_MISMATCH')
                baseline = output
                stats = receipt['stats']
                self.assertGreater(stats['initial_runs'], 1)
                self.assertGreater(stats['merge_passes'], 0)
                self.assertEqual(observed['runs'], stats['initial_runs'])
                self.assertTrue(observed['staged'])
                self.assertEqual(stats['staged_rows'], len(rows))
                self.assertGreater(stats['sort_bytes_written'], budget)
                self.assertGreater(stats['candidate_bytes'], 0)
                self.assertLessEqual(stats['peak_payload'], budget)
                self.assertGreater(160 * maximum, budget)
                self.assert_clean()

    def test_fixed_budget_python_working_set(self):
        peaks = []
        for count in (800, 4000):
            batch = 'scale' + str(count)
            self.w.ingest(batch, (row('r%06d' % i + 'y' * 100, 'p', i, i - 50)
                                  for i in range(count)))
            maximum = self.w.status(batch)['max_payload']
            budget = max(2048, 6 * maximum + 128)
            tracemalloc.start()
            try:
                receipt = self.w.compute(batch, 0, budget, fan_in=2, delivery_rows=3)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            peaks.append(peak)
            self.assertLessEqual(receipt['stats']['peak_payload'], budget)
            self.assertEqual(receipt['stats']['staged_rows'], count)
            self.assert_clean()
        self.assertLess(max(peaks), 8 * 1024 * 1024)
        self.assertLessEqual(peaks[1] - peaks[0], 2 * 1024 * 1024)
