# SPDX-License-Identifier: Apache-2.0
"""CPU-only observation contracts for the actual outbound transport method."""
import importlib.util
import os
from pathlib import Path
import types
import unittest
from unittest.mock import patch

from test_pp_mq_progress_contract import Client, client, fill, methods


PATH = Path(__file__).resolve().parents[2] / 'lmcache/v1/multiprocess/pp_wait_observer.py'
SPEC = importlib.util.spec_from_file_location('pp_wait_observer_local', PATH)
observer_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(observer_module)


class ObservationContracts(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = patch.dict(Client.process_outbound_task.__globals__, {
            'time': types.SimpleNamespace(monotonic_ns=iter([100, 500]).__next__)
        })
        self.clock.start()
        self.addCleanup(self.clock.stop)

    def test_constructor_uses_explicit_gate_for_zero_and_positive_budget(self) -> None:
        actual = methods('MessageQueueClient', ['__init__'])
        socket = types.SimpleNamespace(
            setsockopt=lambda *args: None,
            get_monitor_socket=lambda **kwargs: object(), connect=lambda url: None)
        registered = []
        loop = types.SimpleNamespace(register=registered.append)
        actual.__init__.__globals__.update(
            zmq=types.SimpleNamespace(DEALER=1, CONNECT_TIMEOUT=2,
                                     RECONNECT_IVL_MAX=3, EVENT_DISCONNECTED=4),
            CONNECT_TIMEOUT_MS=10, RECONNECT_IVL_MAX_MS=10,
            ClientPollingLoop=types.SimpleNamespace(get_instance=lambda: loop),
            create_pp_wait_observer=observer_module.create_pp_wait_observer)
        for enabled in ['0', '1']:
            for budget in [0, 32]:
                with patch.dict(os.environ, {'LMCACHE_PP_WAIT_OBSERVE': enabled}):
                    obj = actual('unused', types.SimpleNamespace(socket=lambda kind: socket),
                                 outbound_batch_limit=budget)
                self.assertEqual(obj._pp_queue_observer is not None, enabled == '1')
                self.assertIs(registered[-1], obj)
                if enabled == '1':
                    self.assertEqual(obj._pp_queue_observer.scope, 'client_queue')

    def test_default_disabled_has_no_clock_or_qsize(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(observer_module.create_pp_wait_observer(
                True, 'client_queue', 'unused'))
        obj = client(32)
        obj._pp_queue_observer = None
        fill(obj, 32)
        def forbidden() -> None:
            raise AssertionError('disabled observation touched clock or qsize')
        Client.process_outbound_task.__globals__['time'].monotonic_ns = forbidden
        obj.input_queue.qsize = forbidden
        obj.process_outbound_task()
        self.assertEqual(len(obj.sent), 32)
        self.assertEqual(len(obj.notifications), 1)

    def test_budget_exhausted_with_backlog_and_exact_empty(self) -> None:
        for count, remaining in [(33, 1), (32, 0)]:
            with self.subTest(count=count):
                Client.process_outbound_task.__globals__['time'].monotonic_ns = iter([100, 500]).__next__
                obj = client(32)
                observer = observer_module.PPWaitObserver('client_queue', 'unused')
                obj._pp_queue_observer = observer
                fill(obj, count)
                obj.process_outbound_task()
                metrics = observer.snapshot()
                self.assertEqual(metrics['outbound_processed'], (1, 32, 32))
                self.assertEqual(metrics['outbound_wall_ns'], (1, 400, 400))
                self.assertEqual(metrics['outbound_remaining'], (1, remaining, remaining))
                self.assertEqual(metrics['outbound_budget_exhausted'], (1, 1, 1))
                self.assertEqual(metrics['outbound_budget_exhausted_remaining'], (1, remaining, remaining))
                self.assertEqual(len(obj.notifications), 1)

    def test_zero_budget_observed_and_no_budget_event(self) -> None:
        obj = client(0)
        observer = observer_module.PPWaitObserver('client_queue', 'unused')
        obj._pp_queue_observer = observer
        fill(obj, 33)
        obj.process_outbound_task()
        self.assertEqual(observer.snapshot()['outbound_processed'], (1, 33, 33))
        self.assertEqual(observer.snapshot()['outbound_remaining'], (1, 0, 0))
        self.assertNotIn('outbound_budget_exhausted', observer.snapshot())
        self.assertEqual(obj.notifications, [])

    def test_processed_includes_original_encoding_failure(self) -> None:
        obj = client(1)
        observer = observer_module.PPWaitObserver('client_queue', 'unused')
        obj._pp_queue_observer = observer
        failures = []
        obj.input_queue.put(types.SimpleNamespace(
            request_uid=1, rpc_spec=types.SimpleNamespace(operation='test', payload_types=[int]),
            request_payloads=[], future=types.SimpleNamespace(set_exception=failures.append)))
        obj.process_outbound_task()
        self.assertEqual(len(failures), 1)
        self.assertEqual(observer.snapshot()['outbound_processed'], (1, 1, 1))
        self.assertEqual(len(obj.notifications), 1)

    def test_control_send_exception_is_unchanged(self) -> None:
        for enabled in [False, True]:
            obj = client(1)
            observer = observer_module.PPWaitObserver('client_queue', 'unused')
            obj._pp_queue_observer = observer if enabled else None
            fill(obj, 1)
            def fail(frame: list[bytes]) -> None:
                raise RuntimeError('send failed')
            obj.socket.send_multipart = fail
            with self.assertRaisesRegex(RuntimeError, 'send failed'):
                obj.process_outbound_task()
            self.assertEqual(obj.notifications, [])
            self.assertEqual(observer.snapshot(), {})

    def test_qsize_sampling_does_not_control_rewake(self) -> None:
        obj = client(1)
        obj._pp_queue_observer = observer_module.PPWaitObserver('client_queue', 'unused')
        fill(obj, 2)
        obj.input_queue.qsize = lambda: 0
        obj.process_outbound_task()
        self.assertEqual(len(obj.notifications), 1)
        self.assertEqual(len(obj.sent), 1)
        self.assertEqual(obj._pp_queue_observer.snapshot()['outbound_remaining'], (1, 0, 0))


if __name__ == '__main__':
    unittest.main()
