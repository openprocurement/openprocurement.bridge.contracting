# -*- coding: utf-8 -*-
import logging
import os

from openprocurement.bridge.basic.handlers import HandlerTemplate
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
from openprocurement.bridge.contracting.utils import generate_milestones, journal_context

from openprocurement_client.exceptions import ResourceGone, ResourceNotFound


logger = logging.getLogger("openprocurement.bridge.contracting.databridge")


class BaseObjectMaker(HandlerTemplate):

    def __init__(self, config, cache_db):
        super(BaseObjectMaker, self).__init__(config, cache_db)
        self.basket = {}
        self.keys_from_tender = ('procuringEntity',)

    def fill_base_contract_data(self, contract, tender):
        credentials_data = self.get_resource_credentials(tender['id'])
        assert 'owner' in credentials_data.get('data', {})
        assert 'tender_token' in credentials_data.get('data', {})

        contract['owner'] = credentials_data['data']['owner']
        contract['tender_token'] = credentials_data['data']['tender_token']
        contract['tender_id'] = tender['id']
        for key in self.keys_from_tender:
            contract[key] = tender[key]

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
                        contract['items'] = [item for item in tender['items'] if
                                             item.get('relatedLot') == award['lotID']]
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
            if 'deliveryDate' in item and item['deliveryDate'].get('startDate') and item['deliveryDate'].get(
                    'endDate'):
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

    def post_contract(self, contract):
        data = {"data": contract.toDict()}
        logger.info(
            "Creating contract {} of tender {}".format(contract['id'], contract['tender_id']),
            extra=journal_context({"MESSAGE_ID": "contract_creating"},
                                  {"TENDER_ID": contract['tender_id'], "CONTRACT_ID": contract['id']})
        )
        self.output_client.create_resource_item(data)

    def process_resource(self, resource):
        if 'contracts' not in resource:
            logger.warn('No contracts found in tender {}'.format(resource['id']),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                              params={"TENDER_ID": resource['id']}))
            return
        for contract in resource['contracts']:
            if contract["status"] == "active":

                try:
                    if not self.cache_db.has(contract['id']):
                        self.public_output_client.get_resource_item(contract['id'])
                    else:
                        logger.info('Contract {} exists in local db'.format(contract['id']),
                                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_CACHED},
                                                          params={"CONTRACT_ID": contract['id']}))
                        self._put_resource_in_cache(resource)
                        continue
                except ResourceNotFound:
                    logger.info('Sync contract {} of tender {}'.format(contract['id'], resource['id']),
                                extra=journal_context(
                                    {"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
                                    {"CONTRACT_ID": contract['id'], "TENDER_ID": resource['id']}))
                except ResourceGone:
                    logger.info(
                        'Sync contract {} of tender {} has been archived'.format(contract['id'], resource['id']),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
                                              {"CONTRACT_ID": contract['id'],
                                               "TENDER_ID": resource['id']}))
                    self._put_resource_in_cache(resource)
                    continue
                else:
                    self.cache_db.put(contract['id'], True)
                    logger.info('Contract {} already exist'.format(contract['id']),
                                extra=journal_context({"MESSAGE_ID": DATABRIDGE_CONTRACT_EXISTS},
                                                      {"TENDER_ID": resource['id'], "CONTRACT_ID": contract['id']}))
                    self._put_resource_in_cache(resource)
                    continue

                self.fill_contract(contract, resource)
                self.post_contract(contract)
                self.cache_db.put(contract['id'], True)
            self._put_resource_in_cache(resource)


class CommonObjectMaker(BaseObjectMaker):
    def __init__(self, config, cache_db):
        logger.info("Init Common Contracting Handler.")
        self.handler_name = 'handler_common_contracting'
        super(CommonObjectMaker, self).__init__(config, cache_db)

    def fill_contract(self, contract, tender):
        self.fill_base_contract_data(contract, tender)
        contract['contractType'] = 'common'
        logger.info('Handle common tender {}'.format(tender['id']), extra={"MESSAGE_ID": "handle_common_tenders"})


class EscoObjectMaker(BaseObjectMaker):

    def __init__(self, config, cache_db):
        logger.info("Init Esco Contracting Handler.")
        self.handler_name = 'handler_esco_contracting'
        super(EscoObjectMaker, self).__init__(config, cache_db)

    def fill_contract(self, contract, tender):
        self.fill_base_contract_data(contract, tender)
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
                logger.warn(
                    'Not found related award for contract {} of tender {}'.format(contract['id'], tender['id']))
                keys += keys_from_lot
        else:
            keys += keys_from_lot

        for key in keys:
            contract[key] = tender[key]
        contract['milestones'] = generate_milestones(contract, tender)
