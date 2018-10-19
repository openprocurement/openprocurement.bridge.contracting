import json
import os
import unittest

from copy import deepcopy
from datetime import datetime, timedelta
from iso8601 import parse_date
from mock import patch, MagicMock, call
from munch import munchify
from uuid import uuid4

from openprocurement_client.exceptions import (
    RequestFailed,
    ResourceNotFound,
    ResourceGone
)

from openprocurement.bridge.basic.tests.base import AdaptiveCache
from openprocurement.bridge.contracting.handlers import (
    CommonObjectMaker, EscoObjectMaker
)
from openprocurement.bridge.contracting.constants import ACCELERATOR_RE, DAYS_PER_YEAR
from openprocurement.bridge.contracting.journal_msg_ids import (
    DATABRIDGE_RESTART, DATABRIDGE_GET_CREDENTIALS, DATABRIDGE_GOT_CREDENTIALS,
    DATABRIDGE_FOUND_MULTILOT_COMPLETE, DATABRIDGE_FOUND_NOLOT_COMPLETE,
    DATABRIDGE_CONTRACT_TO_SYNC, DATABRIDGE_CONTRACT_EXISTS,
    DATABRIDGE_COPY_CONTRACT_ITEMS, DATABRIDGE_MISSING_CONTRACT_ITEMS,
    DATABRIDGE_GET_EXTRA_INFO, DATABRIDGE_WORKER_DIED, DATABRIDGE_START,
    DATABRIDGE_GOT_EXTRA_INFO, DATABRIDGE_CREATE_CONTRACT, DATABRIDGE_EXCEPTION,
    DATABRIDGE_CONTRACT_CREATED, DATABRIDGE_RETRY_CREATE, DATABRIDGE_INFO,
    DATABRIDGE_TENDER_PROCESS, DATABRIDGE_SKIP_NOT_MODIFIED,
    DATABRIDGE_SYNC_SLEEP, DATABRIDGE_SYNC_RESUME, DATABRIDGE_CACHED,
    DATABRIDGE_RECONNECT)
from openprocurement.bridge.contracting.utils import journal_context

PWD = os.path.dirname(os.path.realpath(__file__))


class TestCommonObjectMaker(unittest.TestCase):
    config = {'worker_config': {'handler_common_contracting': {
        'input_resources_api_token': 'resources_api_token',
        'output_resources_api_token': 'resources_api_token',
        'resources_api_version': 'resources_api_version',
        'input_resources_api_server': 'resources_api_server',
        'input_public_resources_api_server': 'public_resources_api_server',
        'input_resource': 'resource',
        'output_resources_api_server': 'resources_api_server',
        'output_public_resources_api_server': 'public_resources_api_server',
        'output_resource': 'output_resource'
    }}}

    def setUp(self):
        with open(PWD + '/data/tender.json', 'r') as json_file:
            self.tender = json.load(json_file)['data']
        with open(PWD + '/data/not_accelerated_tender.json', 'r') as json_file:
            self.not_accelerated_tender = json.load(json_file)

        self.contract = self.tender['contracts'][0]

        self.assertEquals(self.contract['status'], 'active')

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_init(self, mocked_logger, mocked_client):
        handler = CommonObjectMaker(self.config, 'cache_db')
        self.assertEquals(handler.cache_db, 'cache_db')
        self.assertEquals(handler.handler_config, self.config['worker_config']['handler_common_contracting'])
        self.assertEquals(handler.main_config, self.config)
        self.assertEquals(handler.config_keys,
                          ('input_resources_api_token', 'output_resources_api_token', 'resources_api_version',
                           'input_resources_api_server',
                           'input_public_resources_api_server', 'input_resource', 'output_resources_api_server',
                           'output_public_resources_api_server', 'output_resource')
                          )
        self.assertEquals(handler.keys_from_tender, ('procuringEntity',))
        mocked_logger.info.assert_called_once_with('Init Common Contracting Handler.')

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_fill_base_contract_data(self, mocked_logger, mocked_client):
        handler = CommonObjectMaker(self.config, 'cache_db')

        credentials_mock = MagicMock()
        credentials = {'data': {'owner': 'owner', 'tender_token': 'tender_token'}}
        credentials_mock.return_value = credentials
        handler.get_resource_credentials = credentials_mock

        contract = deepcopy(self.contract)
        handler.fill_base_contract_data(contract, self.tender)

        self.assertEquals(contract['tender_id'], self.tender['id'])
        self.assertEquals(contract['tender_token'], credentials['data']['tender_token'])
        self.assertEquals(contract['owner'], credentials['data']['owner'])
        self.assertEquals(contract['procuringEntity'], self.tender['procuringEntity'])

        info_calls = []

        self.tender['mode'] = 'test'
        contract = deepcopy(self.contract)
        handler.fill_base_contract_data(contract, self.tender)

        self.assertEquals(all(key in contract.keys() for key in
                              ['tender_id', 'procuringEntity']), True)

        #  testing deliveryDate mistmach
        contract = deepcopy(self.contract)
        contract['items'][0]['deliveryDate'] = dict()
        contract['items'][0]['deliveryDate']['startDate'] = \
            (datetime.now() + timedelta(days=1)).isoformat()
        contract['items'][0]['deliveryDate']['endDate'] = \
            datetime.now().isoformat()

        handler.fill_base_contract_data(contract, self.tender)
        info_calls.append(call('startDate value cleaned.',
                               extra={'JOURNAL_TENDER_ID': self.tender['id'],
                                      'MESSAGE_ID': DATABRIDGE_EXCEPTION,
                                      'JOURNAL_CONTRACT_ID': contract['id']}))
        mocked_logger.info.assert_has_calls(info_calls)

        #  testing with no items, so we need to copy them
        contract = deepcopy(self.contract)
        del contract['items']
        handler.fill_base_contract_data(contract, self.tender)
        info_calls.append(call('Copying contract {} items'.
                               format(contract['id']), extra={
            'JOURNAL_TENDER_ID': self.tender['id'],
            'MESSAGE_ID': DATABRIDGE_COPY_CONTRACT_ITEMS,
            'JOURNAL_CONTRACT_ID': contract['id']}))
        mocked_logger.info.assert_has_calls(info_calls)

        # testing with no items in tender, no items in contract
        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        handler.fill_base_contract_data(contract, tender)
        info_calls = info_calls + [
            call('Copying contract {} items'.
                 format(contract['id']), extra={
                'JOURNAL_TENDER_ID': self.tender['id'],
                'MESSAGE_ID': DATABRIDGE_COPY_CONTRACT_ITEMS,
                'JOURNAL_CONTRACT_ID': contract['id']}),
            call(
                "Clearing 'items' key for contract with empty 'items' list",
                extra={
                    'JOURNAL_CONTRACT_ID': contract['id'],
                    'JOURNAL_TENDER_ID': tender['id'],
                    'MESSAGE_ID': DATABRIDGE_COPY_CONTRACT_ITEMS})
        ]
        mocked_logger.info.assert_has_calls(info_calls)

        #  testing with lots
        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        lot = {'id': 'id'}
        tender['awards'][2]['lotID'] = lot['id']
        tender['lots'] = [lot]
        award = [aw for aw in tender['awards'] if
                 aw['id'] == contract['awardID']][0]
        handler.fill_base_contract_data(contract, tender)

        mocked_logger.debug.assert_has_calls(
            [call('Copying items matching related lot {}'
                  .format(award['lotID']))])

        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        lot = {'id': 'id'}
        tender['awards'][0]['lotID'] = lot['id']
        tender['lots'] = [lot]
        contract['awardID'] = 'fake_id'
        handler.fill_base_contract_data(contract, tender)
        mocked_logger.warn.assert_has_calls([call(
            'Not found related award for contact {} of tender {}'.format(
                contract['id'], tender['id']),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                  params={"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
                                  ))])

        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        lot = {'id': 'id'}
        tender['awards'][0]['lotID'] = lot['id']
        tender['lots'] = [lot]
        award = [award for award in tender['awards'] if award['id'] == contract['awardID']][0]
        item = {'id': 'id'}
        award['items'] = [item]
        handler.fill_base_contract_data(contract, tender)
        mocked_logger.debug.has_calls([call('Copying items from related award {}'
                                            .format(award['id']))])
        self.assertEquals(contract['items'], award['items'])  # copy check

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_fill_contract(self, mocked_logger, mocked_client):
        handler = CommonObjectMaker(self.config, 'cache_db')

        fill_base_contract_data = MagicMock()
        handler.fill_base_contract_data = fill_base_contract_data
        handler.fill_base_contract_data.return_value = fill_base_contract_data

        contract = deepcopy(self.contract)
        handler.fill_contract(contract, self.tender)

        self.assertEquals(contract['contractType'], 'common')
        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [call('Handle common tender {}'.format(self.tender['id']),
                  extra={'MESSAGE_ID': "handle_common_tenders"})]
        )

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_post_contract(self, mocked_logger, mocked_client):
        handler = CommonObjectMaker(self.config, 'cache_db')
        handler.output_client = MagicMock()

        contract = munchify({'tender_id': 'tender_id', 'id': 'id'})
        handler.post_contract(contract)
        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [call('Creating contract {} of tender {}'.format(contract['id'], contract['tender_id']),
                  extra={'JOURNAL_CONTRACT_ID': contract['id'], 'MESSAGE_ID': 'contract_creating',
                         'JOURNAL_TENDER_ID': 'tender_id'})]
        )
        handler.output_client.create_resource_item.assert_called_with({'data': contract.toDict()})

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_process_resource(self, mocked_logger, mocked_client):
        cache_db = AdaptiveCache({'0' * 32: datetime.now()})
        handler = CommonObjectMaker(self.config, cache_db)

        # test no contracts
        resource = {'id': '0' * 32}
        handler.process_resource(resource)
        self.assertEquals(
            mocked_logger.warn.call_args_list,
            [call('No contracts found in tender {}'.format(resource['id']),
                  extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': DATABRIDGE_EXCEPTION})]
        )

        # not active contract
        resource['id'] = '1' * 32
        resource['dateModified'] = datetime.now()
        resource['contracts'] = [{'status': 'cancelled', 'id': '2' * 32}]
        handler.process_resource(resource)
        self.assertTrue(cache_db.has(resource['id']))

        # contract exist in local db
        resource['id'] = '3' * 32
        resource['contracts'] = [{'status': 'active', 'id': '4' * 32}]
        cache_db.put('4' * 32, datetime.now())

        handler.process_resource(resource)

        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [call('Contract {} exists in local db'.format(resource['contracts'][0]['id']),
                  extra={'JOURNAL_CONTRACT_ID': resource['contracts'][0]['id'],
                         'MESSAGE_ID': DATABRIDGE_CACHED})]
        )
        self.assertTrue(cache_db.has(resource['id']))

        # not in local db
        resource['id'] = '5' * 32
        resource['contracts'] = [{'status': 'active', 'id': '6' * 32}]

        handler.process_resource(resource)
        handler.public_output_client.get_resource_item.assert_called_with(resource['contracts'][0]['id'])
        self.assertTrue(cache_db.has(resource['contracts'][0]['id']))
        self.assertEquals(cache_db[resource['contracts'][0]['id']], True)
        self.assertEquals(
            mocked_logger.info.call_args_list[2:],
            [call('Contract {} already exist'.format(resource['contracts'][0]['id']),
                  extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': DATABRIDGE_CONTRACT_EXISTS,
                         'JOURNAL_CONTRACT_ID': resource['contracts'][0]['id']})]
        )

        # Resource not found, agreement should be created
        resource['id'] = '7' * 32
        resource['contracts'] = [{'status': 'active', 'id': '8' * 32}]
        e = ResourceNotFound()
        handler.public_output_client.get_resource_item = MagicMock(side_effect=e)
        handler.fill_contract = MagicMock()
        handler.post_contract = MagicMock()

        handler.process_resource(resource)

        self.assertTrue(cache_db.has(resource['id']))
        self.assertEquals(cache_db[resource['id']], resource['dateModified'])
        handler.fill_contract.assert_called_with(munchify(resource['contracts'][0]), munchify(resource))
        handler.post_contract.assert_called_with(munchify(resource['contracts'][0]))
        self.assertTrue(cache_db.has(resource['contracts'][0]['id']))
        self.assertEquals(
            mocked_logger.info.call_args_list[3:],
            [call('Sync contract {} of tender {}'.format(resource['contracts'][0]['id'], resource['id']),
                  extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': DATABRIDGE_CONTRACT_TO_SYNC,
                         'JOURNAL_CONTRACT_ID': resource['contracts'][0]['id']})]
        )
        # Resource gone
        resource['id'] = '9' * 32
        resource['contracts'] = [{'status': 'active', 'id': '10' * 16}]
        e = ResourceGone()
        handler.public_output_client.get_resource_item = MagicMock(side_effect=e)

        handler.process_resource(resource)

        self.assertTrue(cache_db.has(resource['id']))
        self.assertEquals(cache_db[resource['id']], resource['dateModified'])
        self.assertEquals(
            mocked_logger.info.call_args_list[4:],
            [call('Sync contract {} of tender {} has been archived'.format(resource['contracts'][0]['id'],
                                                                           resource['id']),
                  extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': DATABRIDGE_CONTRACT_TO_SYNC,
                         'JOURNAL_CONTRACT_ID': resource['contracts'][0]['id']})]
        )


class TestEscoObjectMaker(unittest.TestCase):
    config = {'worker_config': {'handler_esco_contracting': {
        'input_resources_api_token': 'resources_api_token',
        'output_resources_api_token': 'resources_api_token',
        'resources_api_version': 'resources_api_version',
        'input_resources_api_server': 'resources_api_server',
        'input_public_resources_api_server': 'public_resources_api_server',
        'input_resource': 'resource',
        'output_resources_api_server': 'resources_api_server',
        'output_public_resources_api_server': 'public_resources_api_server',
        'output_resource': 'output_resource'
    }}}

    def setUp(self):
        with open(PWD + '/data/tender.json', 'r') as json_file:
            self.tender = json.load(json_file)['data']
        with open(PWD + '/data/not_accelerated_tender.json', 'r') as json_file:
            self.not_accelerated_tender = json.load(json_file)

        self.contract = self.tender['contracts'][0]

        self.assertEquals(self.contract['status'], 'active')

        self.keys = ['fundingKind', 'NBUdiscountRate',
                     'yearlyPaymentsPercentageRange', 'noticePublicationDate',
                     'minValue'] + ['milestones', 'contractType']

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_init(self, mocked_logger, mocked_client):
        handler = EscoObjectMaker(self.config, 'cache_db')
        self.assertEquals(handler.cache_db, 'cache_db')
        self.assertEquals(handler.handler_config, self.config['worker_config']['handler_esco_contracting'])
        self.assertEquals(handler.main_config, self.config)
        self.assertEquals(handler.config_keys,
                          ('input_resources_api_token', 'output_resources_api_token', 'resources_api_version',
                           'input_resources_api_server',
                           'input_public_resources_api_server', 'input_resource', 'output_resources_api_server',
                           'output_public_resources_api_server', 'output_resource')
                          )
        self.assertEquals(handler.keys_from_tender, ('procuringEntity',))
        mocked_logger.info.assert_called_once_with('Init Esco Contracting Handler.')

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_fill_base_contract_data(self, mocked_logger, mocked_client):
        handler = EscoObjectMaker(self.config, 'cache_db')

        credentials_mock = MagicMock()
        credentials = {'data': {'owner': 'owner', 'tender_token': 'tender_token'}}
        credentials_mock.return_value = credentials
        handler.get_resource_credentials = credentials_mock

        contract = deepcopy(self.contract)
        handler.fill_base_contract_data(contract, self.tender)

        self.assertEquals(contract['tender_id'], self.tender['id'])
        self.assertEquals(contract['tender_token'], credentials['data']['tender_token'])
        self.assertEquals(contract['owner'], credentials['data']['owner'])
        self.assertEquals(contract['procuringEntity'], self.tender['procuringEntity'])

        info_calls = []

        self.tender['mode'] = 'test'
        contract = deepcopy(self.contract)
        handler.fill_base_contract_data(contract, self.tender)

        self.assertEquals(all(key in contract.keys() for key in
                              ['tender_id', 'procuringEntity']), True)

        #  testing deliveryDate mistmach
        contract = deepcopy(self.contract)
        contract['items'][0]['deliveryDate'] = dict()
        contract['items'][0]['deliveryDate']['startDate'] = \
            (datetime.now() + timedelta(days=1)).isoformat()
        contract['items'][0]['deliveryDate']['endDate'] = \
            datetime.now().isoformat()

        handler.fill_base_contract_data(contract, self.tender)
        info_calls.append(call('startDate value cleaned.',
                               extra={'JOURNAL_TENDER_ID': self.tender['id'],
                                      'MESSAGE_ID': DATABRIDGE_EXCEPTION,
                                      'JOURNAL_CONTRACT_ID': contract['id']}))
        mocked_logger.info.assert_has_calls(info_calls)

        #  testing with no items, so we need to copy them
        contract = deepcopy(self.contract)
        del contract['items']
        handler.fill_base_contract_data(contract, self.tender)
        info_calls.append(call('Copying contract {} items'.
                               format(contract['id']), extra={
            'JOURNAL_TENDER_ID': self.tender['id'],
            'MESSAGE_ID': DATABRIDGE_COPY_CONTRACT_ITEMS,
            'JOURNAL_CONTRACT_ID': contract['id']}))
        mocked_logger.info.assert_has_calls(info_calls)

        # testing with no items in tender, no items in contract
        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        handler.fill_base_contract_data(contract, tender)
        info_calls = info_calls + [
            call('Copying contract {} items'.
                 format(contract['id']), extra={
                'JOURNAL_TENDER_ID': self.tender['id'],
                'MESSAGE_ID': DATABRIDGE_COPY_CONTRACT_ITEMS,
                'JOURNAL_CONTRACT_ID': contract['id']}),
            call(
                "Clearing 'items' key for contract with empty 'items' list",
                extra={
                    'JOURNAL_CONTRACT_ID': contract['id'],
                    'JOURNAL_TENDER_ID': tender['id'],
                    'MESSAGE_ID': DATABRIDGE_COPY_CONTRACT_ITEMS})
        ]
        mocked_logger.info.assert_has_calls(info_calls)

        #  testing with lots
        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        lot = {'id': 'id'}
        tender['awards'][2]['lotID'] = lot['id']
        tender['lots'] = [lot]
        award = [aw for aw in tender['awards'] if
                 aw['id'] == contract['awardID']][0]
        handler.fill_base_contract_data(contract, tender)

        mocked_logger.debug.assert_has_calls(
            [call('Copying items matching related lot {}'
                  .format(award['lotID']))])

        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        lot = {'id': 'id'}
        tender['awards'][0]['lotID'] = lot['id']
        tender['lots'] = [lot]
        contract['awardID'] = 'fake_id'
        handler.fill_base_contract_data(contract, tender)
        mocked_logger.warn.assert_has_calls([call(
            'Not found related award for contact {} of tender {}'.format(
                contract['id'], tender['id']),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                  params={"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
                                  ))])

        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        lot = {'id': 'id'}
        tender['awards'][0]['lotID'] = lot['id']
        tender['lots'] = [lot]
        award = [award for award in tender['awards'] if award['id'] == contract['awardID']][0]
        item = {'id': 'id'}
        award['items'] = [item]
        handler.fill_base_contract_data(contract, tender)
        mocked_logger.debug.has_calls([call('Copying items from related award {}'
                                            .format(award['id']))])
        self.assertEquals(contract['items'], award['items'])  # copy check

    @patch('openprocurement.bridge.contracting.utils.generate_milestones')
    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_fill_contract(self, mocked_logger, mocked_client, mocked_generete_milestones):
        credentials_mock = MagicMock()
        credentials = {'data': {'owner': 'owner', 'tender_token': 'tender_token'}}
        credentials_mock.return_value = credentials
        handler = EscoObjectMaker(self.config, 'cache_db')
        handler.get_resource_credentials = credentials_mock
        contract = deepcopy(self.contract)

        self.assertNotIn('contractType', contract)
        self.assertEquals(handler.fill_contract(contract, self.tender), None)
        self.assertIn('contractType', contract)
        self.assertEquals(contract['contractType'], 'esco')

        self.assertEquals(set(self.keys).issubset(set(contract.keys())), True)
        mocked_logger.info.assert_called_with('Handle esco tender {}'.format(
            self.tender['id']), extra={"MESSAGE_ID": "handle_esco_tenders"})

    @patch('openprocurement.bridge.contracting.utils.generate_milestones')
    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_handle_esco_tenders_multilot(self, mocked_logger, mocked_client, mocked_generate_m):
        tender = deepcopy(self.not_accelerated_tender)
        contract = deepcopy(self.not_accelerated_tender['contracts'][1])
        self.assertNotIn('contractType', contract)
        self.assertNotIn('yearlyPaymentsPercentageRange', contract)
        self.assertNotIn('fundingKind', contract)
        self.assertNotIn('minValue', contract)

        test_lot = {'id': uuid4().hex,
                    'yearlyPaymentsPercentageRange': 0.80000,
                    'fundingKind': 'other',
                    'minValue': {'amount': 0,
                                 'currency': 'UAH',
                                 'valueAddedTaxIncluded': True}}
        tender['lots'] = [test_lot]
        tender['awards'][2]['lotID'] = test_lot['id']

        credentials_mock = MagicMock()
        credentials = {'data': {'owner': 'owner', 'tender_token': 'tender_token'}}
        credentials_mock.return_value = credentials
        handler = EscoObjectMaker(self.config, 'cache_db')
        handler.get_resource_credentials = credentials_mock

        self.assertEquals(handler.fill_contract(contract, tender), None)
        self.assertEquals(set(self.keys).issubset(contract.keys()), True)
        mocked_logger.debug.assert_has_calls([call('Fill contract {} values from lot {}'.format(
            contract['id'], test_lot['id']))])

        #  no related awards
        tender = deepcopy(self.not_accelerated_tender)
        contract = deepcopy(self.not_accelerated_tender['contracts'][1])
        tender['lots'] = [test_lot]
        tender['awards'][2]['id'] = 'fake_id'
        self.assertEquals(handler.fill_contract(contract, tender), None)

        mocked_logger.warn.assert_has_calls([call(
            'Not found related award for contract {} of tender {}'.format(
                contract['id'], tender['id']))])

        # here no related lot found test
        tender = deepcopy(self.not_accelerated_tender)
        contract = deepcopy(self.not_accelerated_tender['contracts'][1])
        tender['lots'] = [test_lot]
        tender['awards'][2]['lotID'] = 'fake_id'
        self.assertEquals(handler.fill_contract(contract, tender), None)

        mocked_logger.critical.assert_has_calls([call(
            'Not found related lot for contract {} of tender {}'.format(
                contract['id'], tender['id']),
            extra={'MESSAGE_ID': 'not_found_related_lot'})])

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_post_contract(self, mocked_logger, mocked_client):
        handler = EscoObjectMaker(self.config, 'cache_db')
        handler.output_client = MagicMock()

        contract = munchify({'tender_id': 'tender_id', 'id': 'id'})
        handler.post_contract(contract)
        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [call('Creating contract {} of tender {}'.format(contract['id'], contract['tender_id']),
                  extra={'JOURNAL_CONTRACT_ID': contract['id'], 'MESSAGE_ID': 'contract_creating',
                         'JOURNAL_TENDER_ID': 'tender_id'})]
        )
        handler.output_client.create_resource_item.assert_called_with({'data': contract.toDict()})

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_process_resource(self, mocked_logger, mocked_client):
        cache_db = AdaptiveCache({'0' * 32: datetime.now()})
        handler = EscoObjectMaker(self.config, cache_db)

        # test no contracts
        resource = {'id': '0' * 32}
        handler.process_resource(resource)
        self.assertEquals(
            mocked_logger.warn.call_args_list,
            [call('No contracts found in tender {}'.format(resource['id']),
                  extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': DATABRIDGE_EXCEPTION})]
        )

        # not active contract
        resource['id'] = '1' * 32
        resource['dateModified'] = datetime.now()
        resource['contracts'] = [{'status': 'cancelled', 'id': '2' * 32}]
        handler.process_resource(resource)
        self.assertTrue(cache_db.has(resource['id']))

        # contract exist in local db
        resource['id'] = '3' * 32
        resource['contracts'] = [{'status': 'active', 'id': '4' * 32}]
        cache_db.put('4' * 32, datetime.now())

        handler.process_resource(resource)

        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [call('Contract {} exists in local db'.format(resource['contracts'][0]['id']),
                  extra={'JOURNAL_CONTRACT_ID': resource['contracts'][0]['id'],
                         'MESSAGE_ID': DATABRIDGE_CACHED})]
        )
        self.assertTrue(cache_db.has(resource['id']))

        # not in local db
        resource['id'] = '5' * 32
        resource['contracts'] = [{'status': 'active', 'id': '6' * 32}]

        handler.process_resource(resource)
        handler.public_output_client.get_resource_item.assert_called_with(resource['contracts'][0]['id'])
        self.assertTrue(cache_db.has(resource['contracts'][0]['id']))
        self.assertEquals(cache_db[resource['contracts'][0]['id']], True)
        self.assertEquals(
            mocked_logger.info.call_args_list[2:],
            [call('Contract {} already exist'.format(resource['contracts'][0]['id']),
                  extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': DATABRIDGE_CONTRACT_EXISTS,
                         'JOURNAL_CONTRACT_ID': resource['contracts'][0]['id']})]
        )

        # Resource not found, agreement should be created
        resource['id'] = '7' * 32
        resource['contracts'] = [{'status': 'active', 'id': '8' * 32}]
        e = ResourceNotFound()
        handler.public_output_client.get_resource_item = MagicMock(side_effect=e)
        handler.fill_contract = MagicMock()
        handler.post_contract = MagicMock()

        handler.process_resource(resource)

        self.assertTrue(cache_db.has(resource['id']))
        self.assertEquals(cache_db[resource['id']], resource['dateModified'])
        handler.fill_contract.assert_called_with(munchify(resource['contracts'][0]), munchify(resource))
        handler.post_contract.assert_called_with(munchify(resource['contracts'][0]))
        self.assertTrue(cache_db.has(resource['contracts'][0]['id']))
        self.assertEquals(
            mocked_logger.info.call_args_list[3:],
            [call('Sync contract {} of tender {}'.format(resource['contracts'][0]['id'], resource['id']),
                  extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': DATABRIDGE_CONTRACT_TO_SYNC,
                         'JOURNAL_CONTRACT_ID': resource['contracts'][0]['id']})]
        )
        # Resource gone
        resource['id'] = '9' * 32
        resource['contracts'] = [{'status': 'active', 'id': '10' * 16}]
        e = ResourceGone()
        handler.public_output_client.get_resource_item = MagicMock(side_effect=e)

        handler.process_resource(resource)

        self.assertTrue(cache_db.has(resource['id']))
        self.assertEquals(cache_db[resource['id']], resource['dateModified'])
        self.assertEquals(
            mocked_logger.info.call_args_list[4:],
            [call('Sync contract {} of tender {} has been archived'.format(resource['contracts'][0]['id'],
                                                                           resource['id']),
                  extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': DATABRIDGE_CONTRACT_TO_SYNC,
                         'JOURNAL_CONTRACT_ID': resource['contracts'][0]['id']})]
        )

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.contracting.handlers.logger')
    def test_generate_accelerated_milestones(self, mocked_logger, mocked_client):
        cache_db = AdaptiveCache({'0' * 32: datetime.now()})
        credentials_mock = MagicMock()
        credentials = {'data': {'owner': 'owner', 'tender_token': 'tender_token'}}
        credentials_mock.return_value = credentials
        handler = EscoObjectMaker(self.config, cache_db)
        handler.get_resource_credentials = credentials_mock

        contract = deepcopy(self.contract)
        tender = deepcopy(self.tender)
        handler.fill_contract(contract, tender)

        re_obj = ACCELERATOR_RE.search(contract['procurementMethodDetails'])
        accelerator = int(re_obj.groupdict()['accelerator'])

        contract_start_date = parse_date(contract['period']['startDate']) \
            if 'period' in contract and 'startDate' in contract['period'] \
            else parse_date(contract['dateSigned'])
        milestones = contract['milestones']
        last_scheduled_milestone = [m for m in milestones if m['status'] == 'scheduled'][-1]

        contract_start_date = parse_date(contract['period']['startDate'])
        delta = timedelta(days=DAYS_PER_YEAR * 15)
        max_end_date = contract_start_date + timedelta(seconds=delta.total_seconds() / accelerator)
        self.assertEquals(last_scheduled_milestone['period']['endDate'], contract['period']['endDate'])
        self.assertEqual(milestones[-1]['period']['endDate'], max_end_date.isoformat())
