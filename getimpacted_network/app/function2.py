import logging
from app.variables import FUNCTION_NAME
from neo4j import GraphDatabase
import requests
import datetime
from datetime import datetime as dtt
import json
from app.Config import get_config

# Setup logging
log_format = '%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s()] [%(levelname)s] %(message)s'
logging.basicConfig(format=log_format)
logger = logging.getLogger(FUNCTION_NAME)
logger.setLevel(logging.INFO)


class Function:
    @staticmethod
    def check_severity(comparator):
        if comparator == '<=4':
            return (0, 5)
        if comparator == '4<=7':
            return (5, 8)
        if comparator == '>7':
            return (8, 20)
        if comparator == 'all':
            return (0, 20)


    @staticmethod
    def respond(status_code, res, req):
        return {
            'statusCode': status_code,
            'response': res,
            'context': req,
            'headers': {
                'Content-Type': 'application/json',
            }
        }

    @staticmethod
    def get_error_body(msg):
        return {
            'error': msg
        }

    @staticmethod
    def run(event, context):
        logger.info("In function.run")
        logger.info('Checking info if passed')

        query = '''
            match (event:Event)
            where event.event_epoch_time >  $fromtime and event.event_epoch_time < $totime and event.class1 in $categories
            and event.event_severity >= $lb and event.event_severity < $ub
            match (event)-[imp:IMPACTS]-(entity)

            with collect(entity.id) as imp_entities

            match (sup:Supplier)-[:SUPPLIES_TO]-(site:Site)
            return sup{
            .id, 
            .name,
            .location, 
            lat: sup.lat_long.y, 
            lng:sup.lat_long.x, 
            is_impacted: CASE WHEN sup.id in imp_entities THEN true ELSE false END,
            sites: collect(site{
            .id, 
            .description, 
            .location,
            is_impacted: CASE WHEN site.id in imp_entities THEN true ELSE false END,
            lat:site.lat_long.y, 
            lng:site.lat_long.x
            })}
                '''
        if event['event_type'] =='all':
            categories = json.loads((requests.get("https://w8zw2e7zk3.execute-api.us-east-1.amazonaws.com/dev/sense/filter")).text)['categories']
        else:
            categories = [event['event_type']]

        from_time = int((dtt.strptime(event['from_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        to_time = int((dtt.strptime(event['to_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
        lowbound_upbound = Function.check_severity(event['severity'])

        json_response = {}
        uri = get_config("graph_db_uri")
        pwd = get_config("graph_db_pwd")
        logger.info("Connecting to db at "+uri)
        with GraphDatabase.driver(uri, auth=("neo4j", pwd)) as driver:
            with driver.session() as session:
                logger.info('Starting session')
                results = list(session.run(query, parameters={'fromtime': from_time, 'totime': to_time,
                        'categories': categories, 'lb': lowbound_upbound[0], 'ub': lowbound_upbound[1] }))
                total_supplier_ids = []
                total_ou_ids = []
                impacted_supplier_ids = []
                impacted_ou_ids = []
                if results:
                    # print(results)
                    markers = []
                    network_lines = []
                    for record in results:
                        supplier = record[0]
                        # logger.info(supplier)
                        supplier_id_ = supplier['id']
                        supplier_type = 'supplier'
                        line_type = 'network'
                        is_sup_impacted = supplier['is_impacted']

                        total_supplier_ids.append(supplier_id_)
                        if is_sup_impacted:
                            supplier_type = 'impactedSupp'
                            line_type = 'impact'
                            impacted_supplier_ids.append(supplier_id_)
                            # logger.info('Impacted Supplier ' + supplier_id_)
                        supplier_lat_ = supplier['lat']
                        supplier_lng_ = supplier['lng']
                        supplier_name_ = supplier['name']
                        supplier_location_ = supplier['location']
                        supplier_marker = {
                            'id': supplier_id_,
                            'name': supplier_name_,
                            'lat': supplier_lat_,
                            'lng': supplier_lng_,
                            'location': supplier_location_, 
                            'type': supplier_type
                        }
                        if supplier_marker['type'] == 'impactedSupp':
                            markers.append(supplier_marker)
                        for site in supplier['sites']:
                            site_id_ = site['id']
                            site_description_ = site['description']
                            site_location_ = site['location']
                            is_site_impacted = site['is_impacted']
                            site_lat_ = site['lat']
                            site_lng_ = site['lng']
                            if site_id_ not in total_ou_ids:
                                total_ou_ids.append(site_id_)
                                if is_site_impacted:
                                    impacted_ou_ids.append(site_id_)
                                site_marker = {
                                    'id': site_id_,
                                    'name': site_description_,
                                    'location': site_location_,
                                    'lat': site_lat_,
                                    'lng': site_lng_,
                                    'type': 'operatingUnit'
                                }
                                #markers.append(site_marker)

                            if line_type == 'impact':
                                network_line = [
                                    {
                                        'lineType': line_type
                                    },
                                    {
                                        'lng': supplier_lng_,
                                        'name': supplier_name_,
                                        'id': supplier_id_,
                                        'lat': supplier_lat_
                                    },
                                    {
                                        'lng': site_lng_,
                                        'name': site_description_,
                                        'lat': site_lat_,
                                        'id': site_id_
                                    }
                                ]
                                network_lines.append(network_line)
                    json_response['markers'] = markers
                    json_response['networkLines'] = network_lines
                print('Impacted sup ids '+str(len(impacted_supplier_ids)))
                print(impacted_supplier_ids)
                print('Impacted ou ids '+str(len(impacted_ou_ids)))
                print(impacted_ou_ids)
                print('Total sup ids '+str(len(total_supplier_ids)))
                print(total_supplier_ids)
                print('Total OU ids '+str(len(total_ou_ids)))
                print(total_ou_ids)

                table_query = '''
                    MATCH (sup:Supplier)-[to:SUPPLIES_TO]->(site:Site)-[:HAS_ORDER]->(po:PO)-[has_item:HAS_ITEM]->(item:Item)
                    where sup.id in $impacted_sup_ids and sup.id = po.supplier_id
                    RETURN 
                    {
                    supplier_id: sup.id,
                    supplier_name: sup.name,
                    supplier_location: sup.location,
                    plant_id: site.id,
                    po_id: po.id,
                    po_create_date: po.create_date,
                    po_details: collect( {item_id: item.id, amount: has_item.amount, po_date: po.create_date, po_quantity: has_item.quantity})
                    }
                    '''
                table_result = list(session.run(table_query, parameters={'impacted_sup_ids': impacted_supplier_ids}))
                table_data = []
                if table_result:
                    # print('table data')
                    # print(table_result)
                    for record in table_result:
                        table_row = record[0]
                        table_data.append(table_row)
                json_response['tableData'] = table_data


                logger.info('Ending session')

        logger.debug('Driver closed?: ' + str(driver.closed()))

        # Return the response
        # response = {
        #     'response': json_response
        # }
        response = json_response
        logger.info("Response")
        logger.info(response)
        return Function.respond(status_code=200, res=response, req=event)
