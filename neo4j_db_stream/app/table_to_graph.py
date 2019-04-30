"""
Export DynamoDb table to csv file
"""

from boto3 import session
import csv
import logging
from boto3.dynamodb.conditions import Key, Attr
import json
from neo4j import GraphDatabase
from datetime import datetime
from app.Config import get_config



# Setup logging
log_format = '%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s()] [%(levelname)s] %(message)s'
logging.basicConfig(format=log_format)
logger = logging.getLogger("dynamo_to_csv")
logger.setLevel(logging.INFO)

boto_session = session.Session(profile_name="NEO-Apps")


def main(table, output=None):
    """Export DynamoDb Table."""
    print('export dynamodb: {}'.format(table))
    data = read_dynamodb_data(table)
    insert_in_graph_db(data['items'])


def insert_in_graph_db(items:list):
    def flatten_locations(locations:list):
        impacted_locations_list = []
        for loc in locations:
            print(loc)
            new_record = {'city': loc["city"],
                          'state': loc["state"],
                          'lat': loc["lat_long"]["lat"],
                          'long': loc["lat_long"]["long"],
                          'country': loc["Country"]}
            impacted_locations_list.append(new_record)
        return impacted_locations_list
    for item in items:
        if "medium_impacted_ou" in item:
            print("Found key")
        else:
            continue
        flat_mio_list = list(map(str, item['medium_impacted_ou']))
        item['mio_list'] = flat_mio_list
        flat_lio_list = list(map(str, item['low_impacted_ou']))
        item['lio_list'] = flat_lio_list
        flat_hio_list = list(map(str, item['high_impacted_ou']))
        item['hio_list'] = flat_hio_list
        flat_his_list = list(map(str, item['high_impacted_supplier']))
        item['his_list'] = flat_his_list
        flat_mis_list = list(map(str, item['medium_impacted_supplier']))
        item['mis_list'] = flat_mis_list
        flat_lis_list = list(map(str, item['low_impacted_supplier']))
        item['lis_list'] = flat_lis_list
        item['impacted_locations'] = flatten_locations(item['impacted_locations'])

        item['entities_impacted'] = str(item['entities_impacted'])
        item['epoch_time'] = str(item['epoch_time'])
        item['event_date'] = datetime.strptime(item['event_date'], '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")

        item.pop('medium_impacted_ou', None)
        item.pop('low_impacted_ou', None)
        item.pop('high_impacted_ou', None)
        item.pop('high_impacted_supplier', None)
        item.pop('medium_impacted_supplier', None)
        item.pop('low_impacted_supplier', None)
        item.pop('low_impacted_dc', None)
        item.pop('medium_impacted_dc', None)
        item.pop('high_impacted_dc', None)

        for key in item:
            print(key + ' :  ' + str(item[key])[:20])

        cypher_main = '''
                        MERGE (event:Event{id: $event_id})
                        ON CREATE SET
                        event.id = $event_id,
                        event.article_url = $article_url,
                        event.article_source = $article_source,
                        event.class1 = $class1,
                        event.class2 = $class2,
                        event.content = $content,
                        event.entities_impacted = $entities_impacted,
                        event.epoch_time = $epoch_time,
                        event.event_date = date($event_date),
                        event.feature = $feature,
                        event.headline = $headline,
                        event.probability1 = $probability1,
                        event.probability2 = $probability2,
                        event.publication_date = $publication_date,
                        event.severity = $severity,
                        event.summary = $summary,
                        event.tags = $tags

                        WITH event

                        UNWIND $impacted_locations as impact_loc
                        WITH event, impact_loc, point({ latitude: toFloat(impact_loc.lat), longitude: toFloat(impact_loc.long) }) as loc
                        MERGE (ev_loc:EventLocation{location: loc})
                        ON CREATE SET
                        ev_loc.city = impact_loc.city,
                        ev_loc.country = impact_loc.country,
                        ev_loc.state = impact_loc.state
                        MERGE (event) - [:IMPACTED_LOCATION] -> (ev_loc)

                        WITH event
                        Return event.id
                        '''

        cypher_mio = '''
                        MATCH (event:Event{id: $event_id}), (ou:Site)
                        WHERE ou.id IN $mio_list
                        MERGE (event)-[imp:IMPACTS]->(ou)
                        ON CREATE SET imp.impact = 'Medium'  
                        '''
        cypher_hio = '''
                        MATCH (event:Event{id: $event_id}), (ou:Site)
                        WHERE ou.id IN $hio_list
                        MERGE (event)-[imp:IMPACTS]->(ou)
                        ON CREATE SET imp.impact = 'High'    
                        '''
        cypher_lio = '''
                        MATCH (event:Event{id: $event_id}), (ou:Site)
                        WHERE ou.id IN $lio_list
                        MERGE (event)-[imp:IMPACTS]->(ou)
                        ON CREATE SET imp.impact = 'Low'   
                        '''
        cypher_mis = '''
                        MATCH (event:Event{id: $event_id}), (sup:Supplier)
                        WHERE sup.id IN $mis_list
                        MERGE (event)-[imp:IMPACTS]->(sup)
                        ON CREATE SET imp.impact = 'Medium'
                        '''
        cypher_his = '''
                        MATCH (event:Event{id: $event_id}), (sup:Supplier)
                        WHERE sup.id IN $his_list
                        MERGE (event)-[imp:IMPACTS]->(sup)
                        ON CREATE SET imp.impact = 'High'
                        '''
        cypher_lis = '''
                        MATCH (event:Event{id: $event_id}), (sup:Supplier)
                        WHERE sup.id IN $lis_list
                        MERGE (event)-[imp:IMPACTS]->(sup)
                        ON CREATE SET imp.impact = 'Low'    
                        '''

        uri = get_config("graph_db_uri")
        pwd = get_config("graph_db_pwd")
        with GraphDatabase.driver(uri, auth=("neo4j", pwd)) as driver:
            with driver.session() as session:
                logger.info('Starting session')

                def run_cypher(query_str: str):
                    logger.info("Going to run the query: " + query_str)
                    try:
                        run = session.run(query_str, parameters=item)
                        counters = run.summary().counters
                        logger.info('Counters set below')
                        logger.info(counters)
                        results = list(run)
                        if results:
                            print(json.dumps(results))
                    except Exception as e:
                        logger.error(e)
                        raise e

                        # import sys
                        # print("Oops!", sys.exc_info()[0], "occured.")

                run_cypher(cypher_main)
                if len(flat_lis_list) > 0:
                    run_cypher(cypher_lis)
                if len(flat_mis_list) > 0:
                    run_cypher(cypher_mis)
                if len(flat_his_list) > 0:
                    run_cypher(cypher_his)
                if len(flat_lio_list) > 0:
                    run_cypher(cypher_lio)
                if len(flat_mio_list) > 0:
                    run_cypher(cypher_mio)
                if len(flat_hio_list) > 0:
                    run_cypher(cypher_hio)

                logger.info('Ending session')

        logger.debug('Driver closed?: ' + str(driver.closed()))


def get_keys(data):
    keys = set([])
    for item in data:
        keys = keys.union(set(item.keys()))
    return keys


def read_dynamodb_data(table):
    """
    Scan all item from dynamodb.
    :param table: String
    :return: Data in Dictionary Format.
    """
    print('Connecting to AWS DynamoDb')
    dynamodb_resource = boto_session.resource('dynamodb')
    table = dynamodb_resource.Table(table)

    print('Downloading ', end='')
    keys = []
    for item in table.attribute_definitions:
        keys.append(item['AttributeName'])
    keys_set = set(keys)
    item_count = table.item_count
    print('Total item count: '+str(item_count))

    # Scanning begins here

    raw_data = table.scan()
    # raw_data = table.scan(FilterExpression=Key('event_id').eq("RC_EC_1000397"))
    if raw_data is None:
        return None

    items = raw_data['Items']
    field_names = set([]).union(get_keys(items))
    current_iteration = raw_data['Count']
    cur_total = (len(items) + current_iteration)
    print("Current iteration: {},  Total downloaded: {}".format(current_iteration, len(items)))

    while raw_data.get('LastEvaluatedKey'):
        raw_data = table.scan(ExclusiveStartKey=raw_data['LastEvaluatedKey'])
        items.extend(raw_data['Items'])
        field_names = field_names.union(get_keys(items))
        current_iteration = raw_data['Count']
        print("Current iteration: {},  Total downloaded: {}".format(current_iteration, len(items)))



    for field_name in field_names:
        if field_name not in keys_set:
            keys.append(field_name)
    return {'items': items, 'keys': keys}


def write_to_csv_file(data, filename):
    """
    Write to a csv file.
    :param data:
    :param filename:
    :return:
    """
    if data is None:
        return

    print("Writing to csv file.")
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=data['keys'],
                                quotechar='"')
        writer.writeheader()
        writer.writerows(data['items'])


if __name__ == '__main__':
    main(table='neo_app_sense_location_match_evnts')






