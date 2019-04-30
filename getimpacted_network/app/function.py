import logging
from app.variables import FUNCTION_NAME
from neo4j import GraphDatabase

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
            error_msg = 'events is a mandatory field for this operation'
            logger.error(error_msg)
            return Function.respond(status_code=400, res=Function.get_error_body(error_msg), req=event)
        else:
            events = event['events']
            # logger.info(events[0]['lat'])
            logger.info(events)

            query = '''UNWIND $events as event
                        RETURN event['lat'] as lat'''

            query = '''
UNWIND $events as event
WITH collect(point({ latitude: toFloat(event['lat']),longitude: toFloat(event['lng']) })) as event_points, toFloat(1600000) as radius

MATCH (sup:Supplier)-[relation:SUPPLIES_TO]-(site:Site) WHERE exists(sup.lat_long)
//AND sup.id IN ['GLOBL-NFT01-0000007892','GLOBL-NFT01-0000140730']
SET sup.is_impacted = false
WITH sup, site, event_points, radius

UNWIND event_points as event_point
WITH sup {.*, is_impacted: CASE
WHEN not sup.is_impacted THEN distance(event_point, sup.lat_long) < radius
END} as sup, site
WITH 
collect({id:sup.id, name:sup.name, lat: sup.lat_long.y, lng:sup.lat_long.x, type: CASE sup.is_impacted WHEN true THEN "impactedSupp" ELSE "supplier" END}) as sup_markers, 
collect({name:site.description, lat:site.lat_long.y, lng:site.lat_long.x, type:"operatingUnit"}) as site_markers, 
collect([{lineType: CASE sup.is_impacted WHEN true THEN "impact" ELSE "network" END},{lat:sup.lat_long.y, lng:sup.lat_long.x, name: sup.name, id: sup.id},{lat:site.lat_long.y, lng:site.lat_long.x, name: site.description}]) as network_lines,
collect(distinct case when sup.is_impacted then sup.id end) as impacted_ids

WITH 
[marker in sup_markers WHERE not (marker.type = "supplier" AND marker.id in impacted_ids) | marker] as sup_markers,
[line in network_lines WHERE not(line[0].lineType = "network" and line[1].id in impacted_ids) | line] as network_lines
,site_markers
WITH sup_markers + site_markers as all_markers, network_lines
UNWIND all_markers as markers
UNWIND network_lines as lines
//Return {markers: collect(distinct markers)}
RETURN {markers:collect(distinct markers), networkLines: collect(distinct lines)}
                    '''

            uri = "bolt://172.16.36.117:7687"
            json_response = None
            with GraphDatabase.driver(uri, auth=("neo4j", "i-0a24fbbbfb5649282")) as driver:
                with driver.session() as session:
                    logger.info('Starting session')
                    results = list(session.run(query, parameters={'events': events}))
                    if results:
                        json_response = results[0][0]
                        # for record in results:
                        #     logger.info(record[0])
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
