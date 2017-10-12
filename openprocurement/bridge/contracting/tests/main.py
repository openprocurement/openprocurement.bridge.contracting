# -*- coding: utf-8 -*-
import munch
import unittest

import exceptions
from mock import patch, call, MagicMock
from munch import munchify
from Queue import Queue
# from time import sleep
from openprocurement_client.client import ResourceNotFound
from openprocurement.bridge.contracting.databridge import (
    ContractingDataBridge,
    journal_context,
    ResourceNotFound,
)

from openprocurement.bridge.contracting.journal_msg_ids import (
    DATABRIDGE_INFO,
    DATABRIDGE_EXCEPTION,
    DATABRIDGE_CACHED,
    DATABRIDGE_CONTRACT_EXISTS,
    DATABRIDGE_CONTRACT_TO_SYNC,
    DATABRIDGE_COPY_CONTRACT_ITEMS,
    DATABRIDGE_MISSING_CONTRACT_ITEMS,
    DATABRIDGE_START,
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


    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch('openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch('openprocurement.bridge.contracting.databridge.ContractingClient')
    def test_get_tender_credentials(
            self, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db):

        cb = ContractingDataBridge({'main': {}})
        cb.client = MagicMock()
        tender_id = '42'
        cb.client.extract_credentials.side_effect = (Exception(),
                                                     Exception(),
                                                     Exception(),
                                                     tender_id)
        with self.assertRaises(Exception):
            cb.get_tender_credentials(tender_id)

        extract_credentials_calls = cb.client.extract_credentials.call_args_list
        self.assertEqual(
            self._get_calls_count(extract_credentials_calls, call(tender_id)), 3)
        self.assertEqual(len(extract_credentials_calls), 3)

        cb.client = MagicMock()
        cb.client.extract_credentials.return_value = tender_id

        data = cb.get_tender_credentials(tender_id)
        self.assertEqual(data, tender_id)
        cb.client.extract_credentials.assert_called_once_with(tender_id)

        cb.client = MagicMock()
        cb.client.extract_credentials.side_effect = (Exception('Boom!'),
                                                     Exception('Boom!'),
                                                     tender_id)
        data = cb.get_tender_credentials(tender_id)

        extract_credentials_calls = cb.client.extract_credentials.call_args_list
        self.assertEqual(
            self._get_calls_count(extract_credentials_calls, call(tender_id)), 3)
        self.assertEqual(len(extract_credentials_calls), 3)
        self.assertEqual(data, tender_id)

    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch('openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch('openprocurement.bridge.contracting.databridge.ContractingClient')
    def test_put_tender_in_cache_by_contract(self, mocked_contract_client,
                                             mocked_tender_client,
                                             mocked_sync_client, mocked_db):
        cb = ContractingDataBridge({'main': {}})
        tender_id = '2001'
        cb.basket = {'1': 'one', '2': 'two', '42': 'why'}
        cb.cache_db = MagicMock()

        cb._put_tender_in_cache_by_contract({'id': '1984'}, tender_id)
        self.assertEqual(cb.basket.get('42', None), 'why')
        self.assertEqual(cb.cache_db.put.called, False)

        cb._put_tender_in_cache_by_contract({'id': '42'}, tender_id)
        self.assertEqual(cb.basket.get('42', None), None)
        cb.cache_db.put.assert_called_once_with('2001', 'why')

    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch('openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch('openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    def test_restart_synchronization_workers(self, mocked_logger,
                                             mocked_contract_client,
                                             mocked_tender_client,
                                             mocked_sync_client, mocked_db):

        cb = ContractingDataBridge({'main': {}})
        cb.clients_initialize = MagicMock()
        job_1 = MagicMock()
        job_2 = MagicMock()
        jobs_list = [job_1, job_2]
        cb.jobs = jobs_list
        cb._start_synchronization_workers = MagicMock()

        cb._restart_synchronization_workers()

        self.assertEqual(job_1.kill.call_count, 1)
        self.assertEqual(job_2.kill.call_count, 1)
        cb._start_synchronization_workers.assert_called_once_with()
        cb.clients_initialize.assert_called_once_with()
        mocked_logger.warn.assert_called_once_with('Restarting synchronization',
                                                   extra={'MESSAGE_ID': 'c_bridge_restart'})


    #
    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_run_with_KeyboardInterrupt(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):

        true_list = [True for i in xrange(0, 21)]
        true_list.append(False)
        mocked_loop.__nonzero__.side_effect = true_list

        cb = ContractingDataBridge({'main': {}})

        cb._restart_synchronization_workers = MagicMock(side_effect=KeyboardInterrupt)
        cb.run()

        gevent_calls = mocked_gevent.killall.call_args_list
        logger_calls = mocked_logger.info.call_args_list

        keyboard_interrut_log = call('Exiting...')
        kill_all_jobs = call(cb.jobs, timeout=5)
        kill_all_immortal_jobs = call(cb.immortal_jobs, timeout=5)

        self.assertEqual(self._get_calls_count(logger_calls, keyboard_interrut_log), 1)
        self.assertEqual(self._get_calls_count(gevent_calls, kill_all_jobs), 1)
        self.assertEqual(self._get_calls_count(gevent_calls, kill_all_immortal_jobs), 1)

        cb._start_contract_sculptors = MagicMock(side_effect=KeyboardInterrupt)
        with self.assertRaises(KeyboardInterrupt) as e:
            cb.run()
        isinstance(e.exception, exceptions.KeyboardInterrupt)


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

        e = Exception('Error!')
        cb._restart_synchronization_workers = MagicMock(side_effect=e)
        cb.run()

        mocked_logger.exception.assert_called_once_with(e)


    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_sync_single_tender(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):
        cb = ContractingDataBridge({'main': {}})
        tender_id = 33
        cb.tenders_sync_client.get_tender = MagicMock(
            return_value={'data': {'contracts': [munchify({'status': 'no_active', 'id': 1})],
                                   'id': 2, 'status': 'active', 'procuringEntity': 'procuringEntity',
                                   'owner': 'owner', 'tender_token': 'tender_token'}})

        cb.sync_single_tender(tender_id)

        calls_logs = mocked_logger.info.call_args_list
        self.assertEqual(self._get_calls_count(calls_logs, call("Skip contract 1 in status no_active")), 1)
        self.assertEqual(self._get_calls_count(calls_logs, call("Tender 33 does not contain contracts to transfer")), 1)

        cb.tenders_sync_client.get_tender = MagicMock(
            return_value={'data': {'contracts': [munchify({'status': 'active', 'id': 1})],
                                   'id': 2, 'status': 'active', 'procuringEntity': 'procuringEntity',
                                   'owner': 'owner', 'tender_token': 'tender_token'}})

        cb.get_tender_credentials = MagicMock(return_value={'data': {'procuringEntity': 'procuringEntity',
                                                                     'tender_token': 'tender_token'}})

        cb.contracting_client.get_contract = MagicMock(side_effect=ResourceNotFound)
        cb.contracting_client.create_contract = MagicMock(return_value={'data': ['test1', 'test2']})
        cb.sync_single_tender(tender_id)

        calls_logs = mocked_logger.info.call_args_list

        self.assertEqual(self._get_calls_count(calls_logs, call("Getting tender {}".format(tender_id))), 2)
        self.assertEqual(self._get_calls_count(calls_logs, call("Got tender 2 in status active")), 2)
        self.assertEqual(self._get_calls_count(calls_logs, call('Getting tender 33 credentials')), 2)
        self.assertEqual(self._get_calls_count(calls_logs, call('Got tender 33 credentials')), 2)
        self.assertEqual(self._get_calls_count(calls_logs, call("Checking if contract 1 already exists")), 1)
        self.assertEqual(
            self._get_calls_count(calls_logs, call('Contract 1 does not exists. Prepare contract for creation.')), 1)
        self.assertEqual(self._get_calls_count(calls_logs, call('Extending contract 1 with extra data')), 1)
        self.assertEqual(self._get_calls_count(calls_logs, call('Creating contract 1')), 1)
        self.assertEqual(self._get_calls_count(calls_logs, call('Contract 1 created')), 1)
        self.assertEqual(self._get_calls_count(calls_logs, call('Successfully transfered contracts: [1]')), 1)

    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_sync_single_tender_Exception(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):
        cb = ContractingDataBridge({'main': {}})
        tender_id = 33
        cb.tenders_sync_client.get_tender = MagicMock(
            return_value={'data': {'contracts': [munchify({'status': 'active', 'id': 1})],
                                   'id': 2, 'status': 'active', 'procuringEntity': 'procuringEntity',
                                   'owner': 'owner', 'tender_token': 'tender_token'}})
        cb.get_tender_credentials = MagicMock(return_value={'data': {'procuringEntity': 'procuringEntity',
                                                                     'tender_token': 'tender_token'}})

        cb.sync_single_tender(tender_id)
        calls_logs = mocked_logger.info.call_args_list

        self.assertEqual(self._get_calls_count(calls_logs, call('Contract exists 1')), 1)

        error = Exception('Error!')
        cb.contracting_client.get_contract = MagicMock(side_effect=error)

        with self.assertRaises(Exception) as e:
            cb.sync_single_tender(tender_id)
        mocked_logger.exception.assert_called_once_with(e.exception)


    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch('openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch('openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_retry_put_contracts(
            self, mocked_loop, mocked_gevent, mocked_logger, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db):

        true_list = [True, False]
        mocked_loop.__nonzero__.side_effect = true_list

        contract = {'id': '42', 'tender_id': '1984'}

        bridge = ContractingDataBridge({'main': {}})
        remember_put_with_retry = bridge._put_with_retry
        bridge.contracts_retry_put_queue = MagicMock()
        bridge._put_with_retry = MagicMock()
        bridge.cache_db = MagicMock()
        bridge._put_tender_in_cache_by_contract = MagicMock()
        bridge.contracts_retry_put_queue.get.return_value = contract

        bridge.retry_put_contracts()

        bridge.contracts_retry_put_queue.get.assert_called_once_with()
        bridge._put_with_retry.assert_called_once_with(contract)
        bridge.cache_db.put.assert_called_once_with(contract['id'], True)
        bridge._put_tender_in_cache_by_contract.assert_called_once_with(contract,
                                                                        contract['tender_id'])
        mocked_gevent.sleep.assert_called_once_with(0)

        bridge._put_with_retry = remember_put_with_retry
        mocked_loop.__nonzero__.side_effect = true_list
        e = Exception('Boom!')
        bridge.contracting_client.create_contract = MagicMock(side_effect=[e, True])
        contract = munch.munchify(contract)
        bridge.contracts_retry_put_queue.get.return_value = contract
        bridge.retry_put_contracts()

        mocked_logger.exception.assert_called_once_with(e)

    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch('openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch('openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_put_contracts(
            self, mocked_loop, mocked_gevent, mocked_logger, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db):

        list_loop = [True, False]
        mocked_loop.__nonzero__.side_effect = list_loop
        contract = munch.munchify({'id': '42', 'tender_id': '1984'})

        bridge = ContractingDataBridge({'main': {}})
        bridge.contracts_put_queue = MagicMock()
        bridge.contracts_put_queue.get.return_value = contract
        bridge.contracting_client = MagicMock()
        bridge.contracts_retry_put_queue = MagicMock()
        bridge.contracting_client_init = MagicMock()
        bridge.cache_db = MagicMock()
        bridge._put_tender_in_cache_by_contract = MagicMock()

        bridge.put_contracts()

        bridge.contracts_put_queue.get.assert_called_once_with()
        bridge.contracting_client.create_contract.assert_called_once_with({'data':contract.toDict()})
        bridge.cache_db.put.assert_called_once_with(contract.id, True)
        bridge._put_tender_in_cache_by_contract.assert_called_once_with(contract.toDict(), contract.tender_id)
        mocked_gevent.sleep.assert_called_once_with(0)

        list_contracts = []
        for i in range(0, 10):
            list_contracts.append(dict(id=i, tender_id=(i+100)))
        bridge.contracts_put_queue = MagicMock()
        bridge.contracts_put_queue.get.side_effect = list_contracts
        list_loop = [True for i in range(0, 10)]
        list_loop.append(False)
        mocked_loop.__nonzero__.side_effect = list_loop

        bridge.put_contracts()

        extract_calls = [data[0] for data, call in bridge.contracts_retry_put_queue.put.call_args_list]
        for i in range(0, 10):
            assert extract_calls[i]['id'] == i
        self.assertEqual(len(extract_calls), 10)
        bridge.contracting_client_init.assert_called_once_with()


    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_prepare_contract_data_retry(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):
        true_list = [True, False]
        mocked_loop.__nonzero__.side_effect = true_list

        cb = ContractingDataBridge({'main': {}})
        contract = {'id': 0, 'tender_id': 1111}
        tender_data = MagicMock()
        tender_data.data = {'owner': 'owner', 'tender_token': 'tender_token'}
        cb.handicap_contracts_queue_retry.get = MagicMock(return_value=contract)
        cb.get_tender_data_with_retry = MagicMock(return_value=tender_data)
        cb.prepare_contract_data_retry()
        self.assertEquals(cb.contracts_put_queue.qsize(), 1)
        self.assertEquals(cb.contracts_put_queue.get(), contract)

    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_prepare_contract_data_retry_with_exception(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):
        true_list = [True, False]
        mocked_loop.__nonzero__.side_effect = true_list

        cb = ContractingDataBridge({'main': {}})
        contract = {'id': 0, 'tender_id': 1111}
        cb.handicap_contracts_queue_retry.get = MagicMock(return_value=contract)
        e = Exception("Error!!! prepare_contract_data_retry")
        cb.get_tender_data_with_retry = MagicMock(side_effect=e)
        cb.prepare_contract_data_retry()
        mocked_logger.exception.assert_called_with(e)


    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_prepare_contract_data(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):
        true_list = [True, False]
        mocked_loop.__nonzero__.side_effect = true_list
        cb = ContractingDataBridge({'main': {}})
        contract = {'id': 0, 'tender_id': 1111}

        tender_data = MagicMock()
        tender_data.data = {'owner': 'owner', 'tender_token': 'tender_token'}

        cb.handicap_contracts_queue.get = MagicMock(
            return_value=contract)
        cb.get_tender_credentials = MagicMock(
            return_value=tender_data)
        cb.prepare_contract_data()
        self.assertEquals(cb.contracts_put_queue.qsize(), 1)
        self.assertEquals(cb.contracts_put_queue.get(), contract)

    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test_prepare_contract_data_with_exception(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):

        static_number = 12
        true_list = [True for i in xrange(0, static_number)]
        true_list.append(False)
        mocked_loop.__nonzero__.side_effect = true_list
        cb = ContractingDataBridge({'main': {}})

        for i in range(static_number):
            cb.handicap_contracts_queue.put({'id': i, 'tender_id': i+1111})

        tender_data = MagicMock()
        tender_data.data = {'no_owner': '', 'no_tender_token': ''}

        cb.get_tender_credentials = MagicMock(
            return_value=tender_data)

        cb.prepare_contract_data()
        list_calls = mocked_gevent.sleep.call_args_list
        calls_logs = mocked_logger.info.call_args_list

        calls_with_error_delay = call(cb.on_error_delay)
        self.assertEqual(self._get_calls_count(list_calls, calls_with_error_delay), static_number)

        reconnecting_log = call('Reconnecting tenders client',
                                extra={'JOURNAL_TENDER_ID': 1120, 'MESSAGE_ID': 'c_bridge_reconnect', 'JOURNAL_CONTRACT_ID': 9})
        self.assertEqual(self._get_calls_count(calls_logs, reconnecting_log), 1)

    @patch('openprocurement.bridge.contracting.databridge.gevent')
    @patch('openprocurement.bridge.contracting.databridge.logger')
    @patch('openprocurement.bridge.contracting.databridge.Db')
    @patch(
        'openprocurement.bridge.contracting.databridge.TendersClientSync')
    @patch('openprocurement.bridge.contracting.databridge.TendersClient')
    @patch(
        'openprocurement.bridge.contracting.databridge.ContractingClient')
    @patch('openprocurement.bridge.contracting.databridge.INFINITY_LOOP')
    def test__get_tender_contracts(
            self, mocked_loop, mocked_contract_client, mocked_tender_client,
            mocked_sync_client, mocked_db, mocked_logger, mocked_gevent):
        mocked_db()._backend = 'redis'
        mocked_db()._db_name = 'cache_db_name'
        mocked_db()._port = 6379
        mocked_db()._host = 'localhost'

        info_calls = []
        warn_calls = []
        debug_calls = []

        cb = ContractingDataBridge({'main': {}})
        # Check initialization
        msg = "Caching backend: '{}', db name: '{}', host: '{}', " \
              "port: '{}'".format(
            cb.cache_db._backend, cb.cache_db._db_name, cb.cache_db._host,
            cb.cache_db._port
        )
        info_calls += [
            call(msg, extra={'MESSAGE_ID': DATABRIDGE_INFO}),
            call('Initialization contracting clients.',
                 extra={'MESSAGE_ID': DATABRIDGE_INFO})
        ]

        empty_tender = {'id': 'some_id'}

        cb.tenders_queue.put(empty_tender)
        cb.tenders_sync_client = MagicMock()
        cb.tenders_sync_client.get_tender = MagicMock(side_effect=[Exception()])
        cb._get_tender_contracts()
        warn_calls += [
            call('Fail to get tender info {}'.format('some_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                       params={"TENDER_ID": 'some_id'}))
        ]
        info_calls += [
            call('Put tender {} back to tenders queue'.format('some_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                       params={"TENDER_ID": 'some_id'}))
        ]

        cb.tenders_queue.put(empty_tender)
        cb.tenders_sync_client.get_tender = MagicMock(return_value={'data': empty_tender})
        cb._get_tender_contracts()
        warn_calls += [
            call('!!!No contracts found in tender {}'.format('some_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                       params={"TENDER_ID": 'some_id'}))
        ]

        tender = {
            'id': 'some_id',
            'contracts': [
                {
                    'id': 'some_contract_id',
                    'status': 'active',
                    'items': []
                }
            ],
            'dateModified': 'some_date'
        }
        empty_tender['dateModified'] = 'some_date'

        cb.tenders_queue.put(empty_tender)
        cb.tenders_sync_client.get_tender.return_value = {'data': tender}
        cb._get_tender_contracts()
        info_calls += [
            call('Contract {} exists in local db'.format('some_contract_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_CACHED},
                                       params={"CONTRACT_ID": 'some_contract_id'}))
        ]

        cb.tenders_queue.put(empty_tender)
        cb.cache_db.has.return_value = False
        cb._get_tender_contracts()
        info_calls += [
            call('Contract exists {}'.format('some_contract_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_CONTRACT_EXISTS},
                                       {"TENDER_ID": empty_tender['id'], "CONTRACT_ID": 'some_contract_id'}))
        ]

        cb.tenders_queue.put(empty_tender)
        cb.cache_db.has.return_value = True
        cb._put_tender_in_cache_by_contract = MagicMock(side_effect=[Exception()])
        with self.assertRaises(Exception):
            cb._get_tender_contracts()
        info_calls += [
            call('Contract {} exists in local db'.format('some_contract_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_CACHED},
                                       params={"CONTRACT_ID": 'some_contract_id'})),
            call('Put tender {} back to tenders queue'.format('some_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                       params={"TENDER_ID": 'some_id',
                                               "CONTRACT_ID": 'some_contract_id'})),

        ]
        warn_calls += [
            call('Fail to contract existance {}'.format('some_contract_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                       params={"TENDER_ID": 'some_id', "CONTRACT_ID": 'some_contract_id'}))
        ]

        tender = {
            'id': 'some_id',
            'contracts': [
                {
                    'id': 'some_contract_id',
                    'status': 'active',
                    'items': [{}]
                }
            ],
            'dateModified': 'some_date',
            'procuringEntity': 'some_entity',
            'mode': 'some_mode',
            'items': []
        }
        cb.tenders_queue.put(empty_tender)
        cb.tenders_sync_client.get_tender.return_value = {'data': tender}
        cb._put_tender_in_cache_by_contract.side_effect = [ResourceNotFound()] * 20
        cb._get_tender_contracts()
        info_calls += [
            call('Contract {} exists in local db'.format('some_contract_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_CACHED},
                                       params={"CONTRACT_ID": 'some_contract_id'})),
            call('Sync contract {} of tender {}'.format('some_contract_id', tender['id']),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
                                       {"CONTRACT_ID": 'some_contract_id', "TENDER_ID": tender['id']}))
        ]

        cb.tenders_queue.put(empty_tender)
        tender['contracts'][0]['items'] = False
        tender['items'] = [{}]
        cb.tenders_sync_client.get_tender.return_value = {'data': tender}
        cb._get_tender_contracts()
        info_calls += info_calls[-2:] + [
            call('Copying contract {} items'.format('some_contract_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS},
                                       {"CONTRACT_ID": 'some_contract_id', "TENDER_ID": 'some_id'}))
        ]
        debug_calls += [
            call('Copying all tender {} items into contract {}'.format(tender['id'], 'some_contract_id'),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS},
                                       params={"CONTRACT_ID": 'some_contract_id', "TENDER_ID": tender['id']}))
        ]

        cb.tenders_queue.put(empty_tender)
        tender['lots'] = [None]
        tender['awards'] = [{
                'id': 'some_award_id'
        }]
        tender['contracts'][0]['awardID'] = 'some_other_award_id'
        if 'items' in tender['contracts'][0]:
            del tender['contracts'][0]['items']
        cb.tenders_sync_client.get_tender.return_value = {'data': tender}
        cb._get_tender_contracts()
        info_calls += info_calls[-3:]
        warn_calls += [
            call('Not found related award for contact {} of tender {}'.format('some_contract_id', tender['id']),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                       params={"CONTRACT_ID": 'some_contract_id', "TENDER_ID": tender['id']})),
            call('Contact {} of tender {} does not contain items info'.format('some_contract_id', tender['id']),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_MISSING_CONTRACT_ITEMS},
                                       {"CONTRACT_ID": 'some_contract_id', "TENDER_ID": tender['id']}))
        ]

        cb.tenders_queue.put(empty_tender)
        tender['contracts'][0]['awardID'] = 'some_award_id'
        tender['awards'][0]['lotID'] = 'some_lot_id'
        cb.tenders_sync_client.get_tender.return_value = {'data': tender}
        if 'items' in tender['contracts'][0]:
            del tender['contracts'][0]['items']
        cb._get_tender_contracts()
        info_calls += info_calls[-3:] + [
            call("Clearing 'items' key for contract with empty 'items' list",
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS},
                                       {"CONTRACT_ID": 'some_contract_id', "TENDER_ID": 'some_id'}))
        ]
        debug_calls += [
            call('Copying items matching related lot {}'.format('some_lot_id'))
        ]
        warn_calls += warn_calls[-1:]

        cb.tenders_queue.put(empty_tender)
        tender['awards'][0]['items'] = [{
            'deliveryDate': {
                'startDate': 2,
                'endDate': 1
            }
        }]
        if 'items' in tender['contracts'][0]:
            del tender['contracts'][0]['items']
        cb.tenders_sync_client.get_tender.return_value = {'data': tender}
        cb._get_tender_contracts()
        info_calls += info_calls[-4:-1] + [
            call("Found dates missmatch {} and {}".format(2, 1),
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                       params={"CONTRACT_ID": 'some_contract_id', "TENDER_ID": tender['id']})),
            call("startDate value cleaned.",
                 extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                       params={"CONTRACT_ID": 'some_contract_id', "TENDER_ID": tender['id']}))
        ]
        debug_calls += [
            call('Copying items from related award {}'.format('some_award_id'))
        ]

        self.assertEqual(mocked_logger.warn.mock_calls, warn_calls)
        self.assertEqual(mocked_logger.info.mock_calls, info_calls)
        self.assertEqual(mocked_logger.debug.mock_calls, debug_calls)


def suite():
    suite = unittest.TestSuite()
    # TODO -add tests
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
