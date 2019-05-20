import logging
from app.variables import FUNCTION_NAME
from neo4j import GraphDatabase
from datetime import datetime
import itertools
from app.Config import  get_config

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
    def get_flat_list(list_of_map):
        return list(itertools.chain.from_iterable(list(map(lambda x: list(x.values()), list_of_map))))

    @staticmethod
    def run(event, context):
        logger.info("In function.run")
        logger.info('Checking info if passed')
        for record in event['Records']:
            event_type = record['eventName']
            if event_type == 'INSERT':
                logger.info('Dynamos stream id is '+record['eventID'])
                item = record['dynamodb']['NewImage']
                logger.info('event is as follows')
                logger.info(item)
                event_id = item['event_id']['S']
                article_url = item['article_url']['S']
                article_source = item['article_source']['S']
                class1 = item['class1']['S']
                class2 = item['class2']['S']
                content = item['content']['S']
                entities_impacted = item['entities_impacted']['N']
                epoch_time = item['epoch_time']['N']
                event_date = datetime.strptime(item['event_date']['S'], '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")
                feature = item['feature']['S']
                headline = item['headline']['S']
                high_impacted_dc = item['high_impacted_dc']['L']
                high_impacted_ou = item['high_impacted_ou']['L']
                high_impacted_supplier = item['high_impacted_supplier']['L']
                medium_impacted_dc = item['medium_impacted_dc']['L']
                medium_impacted_ou = item['medium_impacted_ou']['L']
                medium_impacted_supplier = item['medium_impacted_supplier']['L']
                low_impacted_dc = item['low_impacted_dc']['L']
                low_impacted_ou = item['low_impacted_ou']['L']
                low_impacted_supplier = item['low_impacted_supplier']['L']
                impacted_locations = item['impacted_locations']['L']
                probability1 = item['probability1']['S']
                probability2 = item['probability2']['S']
                publication_date = item['publication_date']['S']
                severity = item['severity']['S']
                event_severity = int(item['severity']['S'])
                summary = item['summary']['S']
                mtags = item['tags']['L']
                tags = []
                for tag in mtags:
                    tags.append(tag['S'])

                flat_hio_list = Function.get_flat_list(high_impacted_ou)
                flat_mio_list = Function.get_flat_list(medium_impacted_ou)
                flat_lio_list = Function.get_flat_list(low_impacted_ou)

                flat_hid_list = Function.get_flat_list(high_impacted_dc)
                flat_mid_list = Function.get_flat_list(medium_impacted_dc)
                flat_lid_list = Function.get_flat_list(low_impacted_dc)

                flat_his_list = Function.get_flat_list(high_impacted_supplier)
                flat_mis_list = Function.get_flat_list(medium_impacted_supplier)
                flat_lis_list = Function.get_flat_list(low_impacted_supplier)

                impacted_locations_list = []
                for entry in impacted_locations:
                    for key in entry:
                        map_record = entry[key]
                        new_record = {'city': map_record['city']['S'], 'state': map_record['state']['S'],
                                      'country': map_record['Country']['S']}
                        for loc_key in map_record['lat_long']:
                            location = map_record['lat_long'][loc_key]
                            lat = location['lat']['S']
                            long = location['long']['S']
                            new_record['lat'] = lat
                            new_record['long'] = long
                        impacted_locations_list.append(new_record)
                print()
                import json
                print(json.dumps(impacted_locations_list, indent=4, sort_keys=True))
                parameters = {
                    'event_id': event_id,
                    'article_url': article_url,
                    'article_source': article_source,
                    'class1': class1,
                    'class2': class2,
                    'content': content,
                    'entities_impacted': entities_impacted,
                    'epoch_time': epoch_time,
                    'event_date': event_date,
                    'feature': feature,
                    'headline': headline,
                    'probability1': probability1,
                    'probability2': probability2,
                    'publication_date': publication_date,
                    'severity': severity,
                    'summary': summary,
                    'event_severity': event_severity,
                    'tags': tags,
                    'mio_list': flat_mio_list,
                    'hio_list': flat_hio_list,
                    'lio_list': flat_lio_list,
                    # 'hid_list': flat_hid_list,
                    # 'mid_list': flat_mid_list,
                    # 'lid_list': flat_lid_list,
                    'his_list': flat_his_list,
                    'mis_list': flat_mis_list,
                    'lis_list': flat_lis_list,
                    'impacted_locations': impacted_locations_list
                }

                for key in parameters:
                    print(key + ' :  ' + str(parameters[key])[:20])

                # query = '''
                # UNWIND $impacted_locations as impact_loc
                # return impact_loc.city, impact_loc.lat, impact_loc.long
                # '''

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
                event.event_severity = $event_severity,
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
                                run = session.run(query_str, parameters=parameters)
                                counters = run.summary().counters
                                logger.info('Counters set below')
                                logger.info(counters)
                                results = list(run)
                                if results:
                                    print(json.dumps(results))
                            except Exception as e:
                                logger.error(e)
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

