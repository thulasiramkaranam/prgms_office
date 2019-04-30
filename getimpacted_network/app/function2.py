import logging
from app.variables import FUNCTION_NAME
from neo4j import GraphDatabase
from app.Config import get_config

# Setup logging
log_format = '%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s()] [%(levelname)s] %(message)s'
logging.basicConfig(format=log_format)
logger = logging.getLogger(FUNCTION_NAME)
logger.setLevel(logging.INFO)


class Function:
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
        if 'events' not in event:
            error_msg = 'events is a mandatory field for this operation. Format: ["RC_EC_1000397", "EC_100127", "201903181496"]'
            logger.error(error_msg)
            return Function.respond(status_code=400, res=Function.get_error_body(error_msg), req=event)
        else:
            events = event['events']
            # logger.info(events[0]['lat'])
            logger.info(events)

            # query = '''UNWIND $events as event
            #             RETURN event['lat'] as lat'''



            query = '''
match (event:Event)
where event.id in $events
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

            json_response = {}
            uri = get_config("graph_db_uri")
            pwd = get_config("graph_db_pwd")
            logger.info("Connecting to db at "+uri)
            with GraphDatabase.driver(uri, auth=("neo4j", pwd)) as driver:
                with driver.session() as session:
                    logger.info('Starting session')
                    results = list(session.run(query, parameters={'events': events}))
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
                                    markers.append(site_marker)


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
