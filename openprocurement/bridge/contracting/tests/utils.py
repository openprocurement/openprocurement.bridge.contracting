# -*- coding: utf-8 -*-
import json
import os
import unittest

from copy import deepcopy
from datetime import datetime, timedelta
from iso8601 import parse_date
from mock import MagicMock, patch, call
from pytz import timezone
from uuid import uuid4

from openprocurement.bridge.contracting.journal_msg_ids import (
    DATABRIDGE_COPY_CONTRACT_ITEMS,
    DATABRIDGE_EXCEPTION
)
from openprocurement.bridge.contracting.utils import (
    handle_common_tenders,
    handle_esco_tenders,
    fill_base_contract_data,
    journal_context,
    generate_milestones
)

PWD = os.path.dirname(os.path.realpath(__file__))
TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')


@patch('openprocurement.bridge.contracting.utils.logger')
class TestUtilsFucntions(unittest.TestCase):
    """Testing all functions inside utils.py.

    All mocks that are patched with class decorator are passed to each function
    as *mocks argument. So we can access them as  array index mocks[0].
    """

    def setUp(self):
        with open(PWD + '/data/tender.json', 'r') as json_file:
            self.tender = json.load(json_file)

        self.contract = self.tender['contracts'][1]
        self.assertEquals(self.contract['status'], 'active')

    def test_handle_common_tenders(self, *mocks):
        contract = deepcopy(self.contract)

        self.assertNotIn('contractType', contract)
        self.assertEquals(handle_common_tenders(contract, self.tender), None)
        self.assertIn('contractType', contract)
        self.assertEquals(contract['contractType'], 'common')
        mocks[0].info.assert_called_with('Handle common tender {}'.format(
            self.tender['id']), extra={"MESSAGE_ID": "handle_common_tenders"})

    @patch('openprocurement.bridge.contracting.utils.generate_milestones')
    def test_handle_esco_tenders(self, mocked_generete_milestones, *mocks):
        contract = deepcopy(self.contract)
        keys = ['fundingKind', 'NBUdiscountRate',
                'yearlyPaymentsPercentageRange', 'noticePublicationDate',
                'minValue'] + ['milestones', 'contractType']

        self.assertNotIn('contractType', contract)
        self.assertEquals(handle_esco_tenders(contract, self.tender), None)
        self.assertIn('contractType', contract)
        self.assertEquals(contract['contractType'], 'esco')

        self.assertEquals(set(keys).issubset(set(contract.keys())), True)
        mocks[0].info.assert_called_with('Handle esco tender {}'.format(
            self.tender['id']), extra={"MESSAGE_ID": "handle_esco_tenders"})

    def test_fill_base_contract_data(self, *mocks):

        info_calls = []

        self.tender['mode'] = 'test'
        contract = deepcopy(self.contract)
        fill_base_contract_data(contract, self.tender)

        self.assertEquals(
            all(key in contract.keys() for key in
                ['tender_id', 'procuringEntity'] ), True)

        #  testing deliveryDate mistmach
        contract = deepcopy(self.contract)
        contract['items'][0]['deliveryDate'] = dict()
        contract['items'][0]['deliveryDate']['startDate'] =\
            (datetime.now()+timedelta(days=1)).isoformat()
        contract['items'][0]['deliveryDate']['endDate'] = \
            datetime.now().isoformat()
        fill_base_contract_data(contract, self.tender)
        info_calls.append(call('startDate value cleaned.',
            extra={'JOURNAL_TENDER_ID': self.tender['id'],
                   'MESSAGE_ID': DATABRIDGE_EXCEPTION,
                   'JOURNAL_CONTRACT_ID': contract['id']}))
        mocks[0].info.assert_has_calls(info_calls)

        #  testing with no items, so we need to copy them
        contract = deepcopy(self.contract)
        del contract['items']
        fill_base_contract_data(contract, self.tender)
        info_calls.append(call('Copying contract {} items'.
                               format(contract['id']), extra={
            'JOURNAL_TENDER_ID': self.tender['id'],
            'MESSAGE_ID': DATABRIDGE_COPY_CONTRACT_ITEMS,
            'JOURNAL_CONTRACT_ID': contract['id']}))
        mocks[0].info.assert_has_calls(info_calls)

        # testing with no items in tender, no items in contract
        contract = deepcopy(self.contract)
        del contract['items']
        tender = deepcopy(self.tender)
        tender['items'] = list()
        fill_base_contract_data(contract, tender)
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
        mocks[0].info.assert_has_calls(info_calls)

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
        fill_base_contract_data(contract, tender)

        mocks[0].debug.assert_has_calls(
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
        fill_base_contract_data(contract, tender)
        mocks[0].warn.assert_has_calls([call(
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
        award = [award for award in tender['awards'] if award['id']==contract['awardID']][0]
        item = {'id': 'id'}
        award['items'] = [item]
        fill_base_contract_data(contract, tender)
        mocks[0].debug.has_calls([call('Copying items from related award {}'
                                       .format(award['id']))])
        self.assertEquals(contract['items'], award['items'])  # copy check

    def test_journal_context(self, *mocks):
        self.assertEquals(journal_context(record={}, params={'test': 'test'}),
                          {'JOURNAL_test': 'test'})

    def test_generate_milestones(self, *mocks):

        contract = deepcopy(self.contract)
        milestones = generate_milestones(contract, self.tender)

        mocks[0].info.assert_called_with(
            "Generate milestones for esco tender {}".format(self.tender['id']))
        self.assertEqual(len(milestones), 16)
        contract_end_date = parse_date(contract['period']['endDate'])
        for seq_number, milestone in enumerate(milestones):
            self.assertEquals(set(milestone.keys()),
                              {'status', 'description', 'sequenceNumber',
                               'title', 'period', 'value', 'dateModified',
                               'date', 'amountPaid', 'id'})
            seq_number += 1
            if seq_number == 1:
                self.assertEquals(milestone['status'], 'pending')
            elif contract_end_date.year >= \
                    parse_date(milestone['period']['startDate']).year \
                    and seq_number != 1:
                self.assertEquals(milestone['status'], 'scheduled')
            else:
                self.assertEquals(milestone['status'], 'spare')



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestUtilsFucntions))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
