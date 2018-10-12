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

    logger.info("Generate milestones for esco tender {}".format(tender['id']))
    max_contract_end_date = contract_start_date + timedelta(days=DAYS_PER_YEAR * 15)

    sequence_number = 1
    while True:
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
        else:
            milestone_start_date = TZ.localize(datetime(announcement_date.year + sequence_number - 1, 1, 1))
            milestone_end_date = TZ.localize(datetime(announcement_date.year + sequence_number, 1, 1))

        if contract_end_date.year == milestone_start_date.year:
            milestone_end_date = contract_end_date

        if milestone_start_date > max_contract_end_date:
            break

        milestone['period'] = {
            'startDate': milestone_start_date.isoformat(),
            'endDate': milestone_end_date.isoformat()
        }

        if contract_end_date.year >= milestone_start_date.year and sequence_number != 1:
            milestone['status'] = 'scheduled'
        elif contract_end_date.year < milestone_start_date.year:
            milestone['status'] = 'spare'

        title = "Milestone #{} of year {}".format(sequence_number, milestone_start_date.year)
        milestone['title'] = title
        milestone['description'] = title

        milestones.append(milestone)
        sequence_number += 1
    milestones[-1]['period']['endDate'] = max_contract_end_date.isoformat()

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
