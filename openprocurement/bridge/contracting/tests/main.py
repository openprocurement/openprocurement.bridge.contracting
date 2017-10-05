# -*- coding: utf-8 -*-
import unittest
from mock import patch, call, MagicMock
# from time import sleep
from openprocurement.bridge.contracting.databridge import ContractingDataBridge
from openprocurement.bridge.contracting.journal_msg_ids import (
    DATABRIDGE_INFO
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
        # TODO: test when all jobs and workers run successful

    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch('openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch('openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_run_with_Exception(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):
        cb = ContractingDataBridge({'main': {}})
        true_list = [True for i in xrange(0, 21)]
        true_list.append(False)
        mocked_loop.__nonzero__.side_effect = true_list

        def _start_synchronization_workers(cb):
            cb.jobs = [False, False]

        cb._start_synchronization_workers = MagicMock(side_effect=_start_synchronization_workers(cb))
        cb.run()

        logger_calls = mocked_logger.exception.call_args_list
        error = 'call(AttributeError("\'bool\' object has no attribute \'dead\'",))'

        self.assertEqual(logger_calls[0].__repr__(), error)


def suite():
    suite = unittest.TestSuite()
    # TODO -add tests
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
