import tempfile
import unittest
from pathlib import Path
from warehouse import Warehouse


def row(rid, part, key, amount):
    return {'row_id': rid, 'partition': part, 'order_key': key, 'amount': amount}


def relation(rows, radius):
    return [
        {'row_id': current['row_id'],
         'window_sum': sum(other['amount'] for other in rows
                           if other['partition'] == current['partition']
                           and abs(other['order_key'] - current['order_key']) <= radius
                           and (other['order_key'] != current['order_key']
                                or other['row_id'] == current['row_id']))}
        for current in rows
    ]


class BusinessCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='readings-test-')
        self.root = Path(self.temp.name) / 'warehouse'
        self.w = Warehouse(self.root)

    def tearDown(self):
        self.w.close()
        self.temp.cleanup()

    def assert_relation(self, batch, rows, radius):
        self.assertEqual(self.w.read(batch), relation(rows, radius), 'WINDOW_ROW_MISMATCH')

    def assert_clean(self):
        self.assertEqual(list(self.w.work.iterdir()), [])


class FailAt:
    """A real lifecycle port; raises after the named operation has happened."""
    def __init__(self, event, inspect=None):
        self.event, self.inspect, self.fired = event, inspect, False

    def __call__(self, event, details):
        if event == self.event:
            if self.inspect is not None:
                self.inspect(details)
            self.fired = True
            raise OSError('injected ' + event)
