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

from openprocurement.bridge.contracting.constants import DAYS_PER_YEAR, ACCELERATOR_RE
from openprocurement.bridge.contracting.journal_msg_ids import (
    DATABRIDGE_COPY_CONTRACT_ITEMS,
    DATABRIDGE_EXCEPTION
)
from openprocurement.bridge.contracting.utils import (
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
            self.tender = json.load(json_file)['data']
        with open(PWD + '/data/not_accelerated_tender.json', 'r') as json_file:
            self.not_accelerated_tender = json.load(json_file)

        self.contract = self.tender['contracts'][0]
        self.assertEquals(self.contract['status'], 'active')

        self.keys = ['fundingKind', 'NBUdiscountRate',
                'yearlyPaymentsPercentageRange', 'noticePublicationDate',
                'minValue'] + ['milestones', 'contractType']

    def test_journal_context(self, *mocks):
        self.assertEquals(journal_context(record={}, params={'test': 'test'}),
                          {'JOURNAL_test': 'test'})

    def test_generate_milestones(self, *mocks):
        contract = deepcopy(self.not_accelerated_tender['contracts'][1])
        contract_start_date = parse_date(contract['period']['startDate']) \
            if 'period' in contract and 'startDate' in contract['period'] \
            else parse_date(contract['dateSigned'])
        announcement_date = parse_date(self.not_accelerated_tender['noticePublicationDate'])
        target_milestones_count = 16 + (contract_start_date.year - announcement_date.year)
        milestones = generate_milestones(contract, self.not_accelerated_tender)

        mocks[0].info.assert_called_with(
            "Generate milestones for esco tender {}".format(self.not_accelerated_tender['id']))
        self.assertEqual(len(milestones), target_milestones_count)
        contract_end_date = parse_date(contract['period']['endDate'])
        for seq_number, milestone in enumerate(milestones):
            self.assertEquals(
                set(milestone.keys()),
                {'status', 'description', 'sequenceNumber', 'title', 'period', 'value', 'dateModified', 'date',
                 'amountPaid', 'id'}
            )
            seq_number += 1
            if seq_number == 1:
                self.assertEquals(milestone['status'], 'pending')
            elif contract_end_date.year >= \
                    parse_date(milestone['period']['startDate']).year \
                    and seq_number != 1:
                self.assertEquals(milestone['status'], 'scheduled')
            else:
                self.assertEquals(milestone['status'], 'spare')

        #  last scheduled milestone endDate = contract period endDate
        last_scheduled_miles = \
            [m for m in milestones if m['status'] == 'scheduled'][-1]
        self.assertEquals(last_scheduled_miles['period']['endDate'],
                          contract['period']['endDate'])
        #  last milestone endDate = contract start Date + 15 years
        contract_start_date = parse_date(contract['period']['startDate'])
        max_contract_end_date = contract_start_date + timedelta(days=DAYS_PER_YEAR * 15)
        self.assertEquals(
            max_contract_end_date.isoformat(),
            milestones[-1]['period']['endDate']
        )

        #  test if no period in contract
        contract = deepcopy(self.not_accelerated_tender['contracts'][1])
        del contract['period']
        milestones = generate_milestones(contract, self.not_accelerated_tender)
        last_scheduled_miles = [m for m in milestones if m['status'] == 'scheduled'][-1]
        self.assertEquals(last_scheduled_miles['period']['endDate'], contract['period']['endDate'])


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestUtilsFucntions))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
