# -*- coding: utf-8 -*-
import unittest
from mock import patch, call, MagicMock
# from time import sleep
from openprocurement.bridge.contracting.databridge import ContractingDataBridge
from openprocurement.bridge.contracting.journal_msg_ids import (
    DATABRIDGE_INFO, DATABRIDGE_START
)


class TestDatabridge(unittest.TestCase):
    def setUp(self):
        self.config = {
            'main': {

            }
        }

    def _get_calls_count(self, calls_list, call_obj):
        count = 0
        for c in calls_list:
            if c == call_obj:
                count += 1
        return count

    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch('openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch('openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_run_with_dead_all_jobs_and_workers(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):

        # Prepare context
        true_list = [True for i in xrange(0, 21)]
        true_list.append(False)
        mocked_loop.__nonzero__.side_effect = true_list
        mocked_db()._backend = 'redis'
        mocked_db()._db_name = 'cache_db_name'
        mocked_db()._port = 6379
        mocked_db()._host = 'localhost'

        cb = ContractingDataBridge({'main': {}})
        # Check initialization
        msg = "Caching backend: '{}', db name: '{}', host: '{}', " \
            "port: '{}'".format(
                cb.cache_db._backend, cb.cache_db._db_name, cb.cache_db._host,
                cb.cache_db._port
        )
        calls = [
            call(msg, extra={'MESSAGE_ID': DATABRIDGE_INFO}),
            call('Initialization contracting clients.',
                 extra={'MESSAGE_ID': DATABRIDGE_INFO})
        ]
        mocked_logger.info.assert_has_calls(calls)

        # Run: while loop has 22 iterations with dead workers and jobs
        cb.run()
        logger_calls = mocked_logger.info.call_args_list
        start_bridge = call('Start Contracting Data Bridge',
                                        extra={'MESSAGE_ID': 'c_bridge_start'})
        current_stage = call(
            'Current state: Tenders to process 0; Unhandled contracts 0; '
            'Contracts to create 0; Retrying to create 0',
            extra={'handicap_contracts_queue_size': 0,
                   'contracts_queue_size': 0,
                   'tenders_queue_size': 0,
                   'contracts_retry_queue': 0}
        )
        starting_sync_workers = call(
            'Starting forward and backward sync workers')
        init_clients = call('Initialization contracting clients.',
                            extra={'MESSAGE_ID': 'c_bridge_info'})

        self.assertEqual(self._get_calls_count(logger_calls, start_bridge), 1)
        self.assertEqual(self._get_calls_count(logger_calls, current_stage), 1)
        self.assertEqual(
            self._get_calls_count(logger_calls, starting_sync_workers), 22)
        self.assertEqual(self._get_calls_count(logger_calls, init_clients), 43)

        warn_calls = mocked_logger.warn.call_args_list
        restart_sync = call('Restarting synchronization',
             extra={'MESSAGE_ID': 'c_bridge_restart'})
        restart_tender_worker = call('Restarting get_tender_contracts worker')
        restart_retry_worker = call('Restarting retry_put_contracts worker')
        restart_prepare_data = call('Restarting prepare_contract_data worker')
        restart_put = call('Restarting put_contracts worker')
        restart_prepare_data_retry = call(
            'Restarting prepare_contract_data_retry worker')
        self.assertEqual(self._get_calls_count(warn_calls, restart_sync), 21)
        self.assertEqual(
            self._get_calls_count(warn_calls, restart_tender_worker), 21)
        self.assertEqual(
            self._get_calls_count(warn_calls, restart_retry_worker), 21)
        self.assertEqual(
            self._get_calls_count(warn_calls, restart_prepare_data), 21)
        self.assertEqual(
            self._get_calls_count(warn_calls, restart_put), 21)
        self.assertEqual(
            self._get_calls_count(warn_calls, restart_prepare_data_retry), 21)
        # TODO: calculate spawn calls with different args

        spawn_calls = mocked_gevent.spawn.call_args_list

        self.assertEqual(len(spawn_calls), 154)
        self.assertEqual(
            self._get_calls_count(spawn_calls, call(cb.get_tender_contracts)), 22)
        self.assertEqual(
            self._get_calls_count(spawn_calls, call(cb.prepare_contract_data)), 22)
        self.assertEqual(
            self._get_calls_count(spawn_calls, call(cb.prepare_contract_data_retry)), 22)
        self.assertEqual(
            self._get_calls_count(spawn_calls, call(cb.put_contracts)), 22)
        self.assertEqual(
            self._get_calls_count(spawn_calls, call(cb.retry_put_contracts)), 22)
        self.assertEqual(
            self._get_calls_count(spawn_calls, call(cb.get_tender_contracts_backward)), 22)
        self.assertEqual(
            self._get_calls_count(spawn_calls, call(cb.get_tender_contracts_forward)), 22)

    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_run_with_all_jobs_and_workers(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):
        cb = ContractingDataBridge({'main': {}})

        true_list = [True, False]
        mocked_loop.__nonzero__.side_effect = true_list

        def _start_conrtact_sculptors(cb):
            get_tender_contracts = MagicMock()
            prepare_contract_data = MagicMock()
            prepare_contract_data_retry = MagicMock()
            put_contracts = MagicMock()
            retry_put_contracts = MagicMock()

            get_tender_contracts.dead = False
            prepare_contract_data.dead = False
            prepare_contract_data_retry.dead = False
            put_contracts.dead = False
            retry_put_contracts.dead = False

            cb.immortal_jobs = {
                'get_tender_contracts': get_tender_contracts,
                'prepare_contract_data': prepare_contract_data,
                'prepare_contract_data_retry': prepare_contract_data_retry,
                'put_contracts': put_contracts,
                'retry_put_contracts': retry_put_contracts,
            }

        def _start_synchronization_workers(cb):
            get_tender_contracts_backward = MagicMock()
            get_tender_contracts_forward = MagicMock()
            get_tender_contracts_backward.dead = False
            get_tender_contracts_forward.dead = False
            cb.jobs = [get_tender_contracts_backward, get_tender_contracts_forward]

        cb._start_contract_sculptors = MagicMock(
            side_effect=_start_conrtact_sculptors(cb)
        )

        cb._start_synchronization_workers = MagicMock(
            side_effect=_start_synchronization_workers(cb)
        )
        _restart_synchronization_workers = MagicMock()
        cb._restart_synchronization_workers = _restart_synchronization_workers
        cb.run()

        logger_calls = mocked_logger.info.call_args_list

        first_log = call("Caching backend: '{}', db name: '{}', host: '{}', port: '{}'".format(cb.cache_db._backend,
                                                                                               cb.cache_db._db_name,
                                                                                               cb.cache_db._host,
                                                                                               cb.cache_db._port),
                         extra={"MESSAGE_ID": DATABRIDGE_INFO})
        second_log = call('Initialization contracting clients.', extra={"MESSAGE_ID": DATABRIDGE_INFO})
        thread_log = call('Start Contracting Data Bridge', extra=({'MESSAGE_ID': DATABRIDGE_START}))

        self.assertEqual(mocked_logger.info.call_count, 3)
        self.assertEqual(self._get_calls_count(logger_calls, first_log), 1)
        self.assertEqual(self._get_calls_count(logger_calls, second_log), 1)
        self.assertEqual(self._get_calls_count(logger_calls, thread_log), 1)
        self.assertEqual(mocked_logger.warn.call_count, 0)
        self.assertEqual(mocked_gevent.spawn.call_count, 0)
        self.assertEqual(cb._restart_synchronization_workers.call_count, 0)


def suite():
    suite = unittest.TestSuite()
    # TODO -add tests
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
