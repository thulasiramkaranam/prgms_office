
from neo4j import GraphDatabase
import datetime
import requests
import json
from datetime import datetime as dtt
query_event = '''
    match (event:Event) return event{eventid: event.id, .event_date}
'''

def check_severity(comparator):
    if comparator == '<4':
        return ("0", "3")
    if comparator == '4=<7':
        return ("4", "6")
    if comparator == '>=7':
        return ("7", "20")

query = '''
match (event:Event)
where event.epoch_time >  $fromtime and event.epoch_time < $totime and event.class1 in $categories
and event.severity >= lb and event.severity < ub
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
uri = "bolt://172.16.36.117:7687"
pwd = "i-0a24fbbbfb5649282"
events_test = {"event_type":"all","from_time":"2019-02-01T06:18:25","to_time":"2019-05-13T06:18:25"}
from_time = int((dtt.strptime(events_test['from_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
to_time = int((dtt.strptime(events_test['to_time'], "%Y-%m-%dT%H:%M:%S")-datetime.datetime(1970,1,1)).total_seconds())
"""
events = {"events":["EC100223","GP100308","EC100232","GP100317","EC100224",
"EC100227","DR100206","ND100278","DR100222","ND100287","ND100287","ND100287",
"DR100168","DR100191","DR100195","ND100289","DR100197","DR100224","GP100309",
"DR100209","ND100276","EC100237","EC100237","ND100269","GP100325","GP100325",
"GP100316","EC100222","DR100226","EC100235","GP100302","GP100313","GP100315",
"GP100323","GP100323","EC100239","ND100268","EC100195","EC100236","EC100236",
"ND100281","ND100281","ND100281","ND100264","ND100264","ND100264","ND100264",
"ND100264","ND100273","DR100204","DR100211","ND100277","EC100229","ND100280",
"EC100233","EC100233","EC100233","ND100222","ND100286","DR100202","EC100228",
"GP100318","EC100226","ND100261","EC100221","EC100221","GP100311","GP100311",
"ND100284","EC100230","EC100230","GP100322","EC100238","DR100201","DR100201",
"ND100263","GP100305","GP100305","ND100283","GP100306","GP100314","GP100314",
"GP100314","GP100324","GP100307","EC100231","ND100272","ND100272","ND100272",
"DR100196","GP100321","GP100321","DR100194","DR100194","ND100288","DR100198","ND100274","EC100240","ND100271","ND100271","ND100271","EC100234","EC100234",
"DR100200","GP100312","EC100225","ND100270","ND100275","GP100310","DR100160","ND100279"]} """


 

with GraphDatabase.driver(uri, auth=("neo4j", pwd)) as driver:
    with driver.session() as session:
        
        results = list(session.run(query_event))
        print(len(results))
        
        for i in results:
            event_details = i[0]
            print(event_details)
            event_id = event_details['eventid']
            epoch_time = str(event_details['event_date'])
            event_epoch_time = int((dtt.strptime(epoch_time, "%Y-%m-%d")-datetime.datetime(1970,1,1)).total_seconds())
            print(event_epoch_time)
            
            update_query = '''
                    match (e:Event {id: $eventID}) set e.event_epoch_time = $epochTime return e
                '''
            results = list(session.run(update_query, parameters={'eventID': event_id, 'epochTime': event_epoch_time }))
            print(results)

        """


        for i in results:
            record = i[0]
            event_id = record['eventid']
            severity = record['severity']
            severity = int(severity)
            print (record)
            update_query = '''
                     match (e:Event {id: $eventID}) set e.event_severity = $severity return e
                 '''
            results = list(session.run(update_query, parameters={'eventID': event_id, 'severity': severity }))

        """
            


            

    




