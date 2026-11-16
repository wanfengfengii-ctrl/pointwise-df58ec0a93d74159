import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from . import Warehouse


def demo():
    source = Path(__file__).resolve().parent.parent / 'samples' / 'readings.jsonl'
    with TemporaryDirectory(prefix='readings-demo-') as directory:
        with Warehouse(directory) as warehouse:
            with source.open(encoding='utf-8') as stream:
                accepted = warehouse.ingest('meter-day', (json.loads(line) for line in stream))
            first = warehouse.compute('meter-day', 0, 2048, fan_in=2, delivery_rows=1)
            before = warehouse.read('meter-day')
            def fail(event, details):
                if event == 'publish_ready':
                    raise OSError('demonstration publication failure')
            try:
                warehouse.compute('meter-day', 1, 2048, fan_in=2, event_port=fail)
            except OSError:
                pass
            failed = warehouse.status('meter-day')
            if warehouse.read('meter-day') != before or failed['latest_attempt']['state'] != 'failed':
                raise AssertionError('failed replacement changed publication')
            final = warehouse.compute('meter-day', 1, 2048, fan_in=2, delivery_rows=2)
            result = warehouse.read('meter-day')
            expected = [{'row_id': 'm1', 'window_sum': 13}, {'row_id': 'm2', 'window_sum': 16},
                        {'row_id': 'm3', 'window_sum': 18}, {'row_id': 'm4', 'window_sum': -4},
                        {'row_id': 'm5', 'window_sum': 16}]
            if result != expected:
                raise AssertionError('WINDOW_ROW_MISMATCH')
            print(json.dumps({'accepted': accepted, 'radius_zero': before,
                              'old_publication': first['publication_id'],
                              'failed_attempt': failed['latest_attempt'],
                              'receipt': final, 'results': result},
                             ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description='Offline readings warehouse')
    parser.add_argument('command', choices=['demo'])
    parser.parse_args()
    demo()


if __name__ == '__main__':
    main()
