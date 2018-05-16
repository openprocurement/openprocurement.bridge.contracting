# -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime, timedelta
from uuid import uuid4

from decimal import Decimal
from pytz import timezone
from iso8601 import parse_date


TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')
logger = logging.getLogger("openprocurement.bridge.contracting.databridge")


def to_decimal(fraction):
    # return Decimal(fraction.numerator) / Decimal(fraction.denominator)
    return float(fraction.numerator) / float(fraction.denominator)


def fill_base_contract_data(contract, tender):
    contract['tender_id'] = tender['id']
    contract['procuringEntity'] = tender['procuringEntity']

    # set contract mode
    if tender.get('mode'):
        contract['mode'] = tender['mode']

    # copy items from tender
    if not contract.get('items'):
        logger.info(
            'Copying contract {} items'.format(contract['id']),
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS},
                {"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
            )
        )
        if tender.get('lots'):
            related_awards = [aw for aw in tender['awards'] if aw['id'] == contract['awardID']]
            if related_awards:
                award = related_awards[0]
                if award.get("items"):
                    logger.debug('Copying items from related award {}'.format(award['id']))
                    contract['items'] = award['items']
                else:
                    logger.debug('Copying items matching related lot {}'.format(award['lotID']))
                    contract['items'] = [item for item in tender['items'] if item.get('relatedLot') == award['lotID']]
            else:
                logger.warn(
                    'Not found related award for contact {} of tender {}'.format(contract['id'], tender['id']),
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                        params={"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
                    )
                )
        else:
            logger.debug(
                'Copying all tender {} items into contract {}'.format(tender['id'], contract['id']),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS},
                    params={"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
                )
            )
            contract['items'] = tender['items']

    # delete `items` key if contract.items is empty list
    if isinstance(contract.get('items', None), list) and len(contract.get('items')) == 0:
        logger.info(
            "Clearing 'items' key for contract with empty 'items' list",
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS},
                {"CONTRACT_ID": contract['id'], "TENDER_ID": tender_to_sync['id']}
            )
        )
        del contract['items']

    if not contract.get('items'):
        logger.warn(
            'Contact {} of tender {} does not contain items info'.format(contract['id'], tender['id']),
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_MISSING_CONTRACT_ITEMS},
                {"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
            )
        )

    for item in contract.get('items', []):
        if 'deliveryDate' in item and item['deliveryDate'].get('startDate') and item['deliveryDate'].get('endDate'):
            if item['deliveryDate']['startDate'] > item['deliveryDate']['endDate']:
                logger.info(
                    "Found dates missmatch {} and {}".format(
                        item['deliveryDate']['startDate'], item['deliveryDate']['endDate']
                    ),
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                        params={"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
                    )
                )
                del item['deliveryDate']['startDate']
                logger.info(
                    "startDate value cleaned.",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                        params={"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
                    )
                )


def handle_common_tenders(contract, tender):
    contract['contractType'] = 'common'
    logger.info('Handle common tender {}'.format(tender['id']), extra={"MESSAGE_ID": "handle_common_tenders"})
