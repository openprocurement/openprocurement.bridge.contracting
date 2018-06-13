# -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime, timedelta
from uuid import uuid4

from decimal import Decimal
from pytz import timezone
from iso8601 import parse_date

from esculator.calculations import discount_rate_days, payments_days, calculate_payments
from openprocurement.bridge.contracting.constants import ACCELERATOR_RE, DAYS_PER_YEAR
from openprocurement.bridge.contracting.journal_msg_ids import (
    DATABRIDGE_EXCEPTION,
    DATABRIDGE_COPY_CONTRACT_ITEMS,
    DATABRIDGE_MISSING_CONTRACT_ITEMS
)

TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')
logger = logging.getLogger("openprocurement.bridge.contracting.databridge")


def journal_context(record={}, params={}):
    for k, v in params.items():
        record["JOURNAL_" + k] = v
    return record


def to_decimal(fraction):
    return str(Decimal(fraction.numerator) / Decimal(fraction.denominator))


def generate_milestones(contract, tender):
    accelerator = 0
    if 'procurementMethodDetails' in contract:
        re_obj = ACCELERATOR_RE.search(contract['procurementMethodDetails'])
        if re_obj and 'accelerator' in re_obj.groupdict():
            accelerator = int(re_obj.groupdict()['accelerator'])

    npv_calculation_duration = 20
    announcement_date = parse_date(tender['noticePublicationDate'])

    contract_days = timedelta(days=contract['value']['contractDuration']['days'])
    contract_years = timedelta(days=contract['value']['contractDuration']['years'] * DAYS_PER_YEAR)
    date_signed = parse_date(contract['dateSigned'])
    signed_delta = date_signed - announcement_date
    if 'period' not in contract or ('mode' in contract and contract['mode'] == 'test'):
        contract_end_date = announcement_date + contract_years + contract_days
        if accelerator:
            real_date_signed = announcement_date + timedelta(seconds=signed_delta.total_seconds() * accelerator)
            contract['dateSigned'] = real_date_signed.isoformat()

        contract['period'] = {
            'startDate': contract['dateSigned'],
            'endDate': contract_end_date.isoformat()
        }

    # set contract.period.startDate to contract.dateSigned if missed
    if 'startDate' not in contract['period']:
        contract['period']['startDate'] = contract['dateSigned']

    contract_start_date = parse_date(contract['period']['startDate'])
    contract_end_date = parse_date(contract['period']['endDate'])

    contract_duration_years = contract['value']['contractDuration']['years']
    contract_duration_days = contract['value']['contractDuration']['days']
    yearly_payments_percentage = contract['value']['yearlyPaymentsPercentage']
    annual_cost_reduction = contract['value']['annualCostsReduction']

    days_for_discount_rate = discount_rate_days(announcement_date, DAYS_PER_YEAR, npv_calculation_duration)
    days_with_payments = payments_days(
        contract_duration_years, contract_duration_days, days_for_discount_rate, DAYS_PER_YEAR,
        npv_calculation_duration
    )

    payments = calculate_payments(
        yearly_payments_percentage, annual_cost_reduction, days_with_payments, days_for_discount_rate
    )

    milestones = []
    years_before_contract_start = contract_start_date.year - announcement_date.year

    last_milestone_sequence_number = 16 + years_before_contract_start

    logger.info("Generate milestones for esco tender {}".format(tender['id']))
    for sequence_number in xrange(1, last_milestone_sequence_number + 1):
        date_modified = datetime.now(TZ)
        milestone = {
            'id': uuid4().hex,
            'sequenceNumber': sequence_number,
            'date': date_modified.isoformat(),
            'dateModified': date_modified.isoformat(),
            'amountPaid': {
                "amount": 0,
                "currency": contract['value']['currency'],
                "valueAddedTaxIncluded": contract['value']['valueAddedTaxIncluded']
            },
            'value': {
                "amount": to_decimal(payments[sequence_number - 1]) if sequence_number <= 21 else 0.00,
                "currency": contract['value']['currency'],
                "valueAddedTaxIncluded": contract['value']['valueAddedTaxIncluded']
            },
        }

        if sequence_number == 1:
            milestone_start_date = announcement_date
            milestone_end_date = TZ.localize(datetime(announcement_date.year + sequence_number, 1, 1))
            milestone['status'] = 'pending'
        elif sequence_number == last_milestone_sequence_number:
            milestone_start_date = TZ.localize(datetime(announcement_date.year + sequence_number - 1, 1, 1))
            milestone_end_date = contract_start_date + timedelta(days=DAYS_PER_YEAR * 15)
        else:
            milestone_start_date = TZ.localize(datetime(announcement_date.year + sequence_number - 1, 1, 1))
            milestone_end_date = TZ.localize(datetime(announcement_date.year + sequence_number, 1, 1))

        if contract_end_date.year >= milestone_start_date.year and sequence_number != 1:
            milestone['status'] = 'scheduled'
        elif contract_end_date.year < milestone_start_date.year:
            milestone['status'] = 'spare'

        if contract_end_date.year == announcement_date.year + sequence_number - 1:
            milestone_end_date = contract_end_date

        milestone['period'] = {
            'startDate': milestone_start_date.isoformat(),
            'endDate': milestone_end_date.isoformat()
        }
        title = "Milestone #{} of year {}".format(sequence_number, milestone_start_date.year)
        milestone['title'] = title
        milestone['description'] = title
        milestones.append(milestone)
    if accelerator:
        accelerate_milestones(milestones, DAYS_PER_YEAR, accelerator)
        # restore accelerated contract.dateSigned
        contract['dateSigned'] = date_signed.isoformat()
        # accelerate contract.period.endDate
        delta = contract_days + contract_years
        contract_end_date = announcement_date + timedelta(seconds=delta.total_seconds() / accelerator)
        contract['period'] = {
            'startDate': contract['dateSigned'],
            'endDate': contract_end_date.isoformat()
        }
    return milestones


def accelerate_milestones(milestones, days_per_year, accelerator):
    year = timedelta(seconds=timedelta(days=days_per_year).total_seconds() / accelerator)
    previous_end_date = None
    for index, milestone in enumerate(milestones):
        if index == 0:
            start_date = parse_date(milestone['period']['startDate'])
            end_date = parse_date(milestone['period']['endDate'])
            delta = end_date - start_date
            end_date = start_date + timedelta(seconds=delta.total_seconds() / accelerator)

            milestone['period']['endDate'] = end_date.isoformat()
        elif milestone['status'] == 'spare' and milestones[index - 1]['status'] in tuple(['scheduled', 'pending']):
            previous_start_date = parse_date(milestones[index - 1]['period']['startDate'])
            previous_end_date = previous_start_date + year
            real_start_date = parse_date(milestone['period']['startDate'])
            end_date = parse_date(milestone['period']['endDate'])
            delta = end_date - real_start_date
            end_date = previous_end_date + timedelta(seconds=delta.total_seconds() / accelerator)

            milestone['period'] = {
                'startDate': previous_end_date.isoformat(),
                'endDate': end_date.isoformat()
            }
        else:
            real_start_date = parse_date(milestone['period']['startDate'])
            end_date = parse_date(milestone['period']['endDate'])

            milestone['period']['startDate'] = milestones[index - 1]['period']['endDate']

            start_date = parse_date(milestones[index - 1]['period']['endDate'])
            delta = end_date - real_start_date
            end_date = start_date + timedelta(seconds=delta.total_seconds() / accelerator)

            milestone['period']['endDate'] = end_date.isoformat()


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
                {"CONTRACT_ID": contract['id'], "TENDER_ID": tender['id']}
            )
        )
        del contract['items']

    if not contract.get('items'):
        logger.warn(
            'Contract {} of tender {} does not contain items info'.format(contract['id'], tender['id']),
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
    fill_base_contract_data(contract, tender)
    contract['contractType'] = 'common'
    logger.info('Handle common tender {}'.format(tender['id']), extra={"MESSAGE_ID": "handle_common_tenders"})


def handle_esco_tenders(contract, tender):
    fill_base_contract_data(contract, tender)
    contract['contractType'] = 'esco'
    if 'procurementMethodDetails' in tender:
        contract['procurementMethodDetails'] = tender['procurementMethodDetails']
    logger.info('Handle esco tender {}'.format(tender['id']), extra={"MESSAGE_ID": "handle_esco_tenders"})

    keys = ['NBUdiscountRate', 'noticePublicationDate']
    keys_from_lot = ['fundingKind', 'yearlyPaymentsPercentageRange', 'minValue']

    # fill contract values from lot
    if tender.get('lots'):
        related_awards = [aw for aw in tender['awards'] if aw['id'] == contract['awardID']]
        if related_awards:
            lot_id = related_awards[0]['lotID']
            related_lots = [lot for lot in tender['lots'] if lot['id'] == lot_id]
            if related_lots:
                logger.debug('Fill contract {} values from lot {}'.format(contract['id'], related_lots[0]['id']))
                for key in keys_from_lot:
                    contract[key] = related_lots[0][key]
            else:
                logger.critical(
                    'Not found related lot for contract {} of tender {}'.format(contract['id'], tender['id']),
                    extra={'MESSAGE_ID': 'not_found_related_lot'}
                )
                keys += keys_from_lot
        else:
            logger.warn('Not found related award for contract {} of tender {}'.format(contract['id'], tender['id']))
            keys += keys_from_lot
    else:
        keys += keys_from_lot

    for key in keys:
        contract[key] = tender[key]
    contract['milestones'] = generate_milestones(contract, tender)
