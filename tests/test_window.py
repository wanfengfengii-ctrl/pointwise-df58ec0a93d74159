import random
from warehouse import ValidationError
from warehouse.model import pack, unpack, compare_hex
from tests.support import BusinessCase, row


class WindowTests(BusinessCase):
    def test_peers_and_unbounded_integers(self):
        huge = 1 << 20000
        rows = [row('a', 'p', -huge, huge), row('b', 'p', -huge, -huge + 3),
                row('c', 'p', -huge, huge), row('d', 'q', -huge, 7),
                row('e', 'p', -huge + 1, 11)]
        self.w.ingest('big', rows)
        maximum = self.w.status('big')['max_payload']
        for radius in (0, 1, huge * 3):
            self.w.compute('big', radius, max(1024, maximum * 6 + 128),
                           fan_in=2, delivery_rows=1)
            self.assert_relation('big', rows, radius)
        self.assert_clean()

    def test_deterministic_relational_differential(self):
        rng = random.Random(7301)
        for trial in range(12):
            rows = [row('r' + str(i), rng.choice(['p', 'q', '区']),
                        rng.randrange(-4, 5), rng.randrange(-100, 101) * (1 << 80))
                    for i in range(rng.randrange(1, 30))]
            rng.shuffle(rows)
            batch = 'case' + str(trial)
            self.w.ingest(batch, rows)
            for radius in (0, 1, 3):
                self.w.compute(batch, radius, 4096, fan_in=2, delivery_rows=2)
                self.assert_relation(batch, rows, radius)

    def test_preflight_does_not_start_attempt(self):
        self.w.ingest('b', [row('a', 'p', 1, 2)])
        before = self.w.status('b')
        for radius, budget, options in [
            (-1, 2048, {}), (True, 2048, {}), (0, 100, {}),
            (0, 2048, {'fan_in': 1}), (0, 2048, {'delivery_rows': 0})]:
            with self.assertRaises(ValidationError):
                self.w.compute('b', radius, budget, **options)
        self.assertEqual(self.w.status('b'), before)
        self.w.ingest('large', [row('x', 'p', 0, 1 << 20000)])
        before = self.w.status('large')
        with self.assertRaises(ValidationError):
            self.w.compute('large', 0, 1024)
        self.assertEqual(self.w.status('large'), before)
        self.assert_clean()

    def test_wire_and_numeric_order(self):
        huge = 1 << 20000
        values = [-huge, -2, -1, 0, 1, 2, huge]
        for a in values:
            original = (3, 'identity', '区', a, -a)
            self.assertEqual(unpack(pack(original)), original)
            for b in values:
                self.assertEqual(compare_hex(hex(a), hex(b)), (a > b) - (a < b))
