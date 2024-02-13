import datetime
import logging
import os

import sys
sys.path.append("..")
from azure.cosmos import CosmosClient
from shared_utilities import helpers


import azure.functions as func



conn_string = os.getenv("CosmosDbConnectionString")
storageClient = CosmosClient.from_connection_string(conn_str=conn_string)



def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    
    next_saturday, next_sunday = helpers.find_next_weekend()
    # change second input '{next_saturday} and {next_sunday}' in the query below.


    # get Betfair 
    query = f"SELECT * FROM c where STARTSWITH(c.openDate, '{next_saturday}') or STARTSWITH(c.openDate, '{next_sunday}')" 
    betfair_list = []
    database = storageClient.get_database_client("fightstore")
    betfair_mma_events = database.get_container_client("betfair_mma_events")
    cosmos_bf = betfair_mma_events.query_items(query,enable_cross_partition_query= True)
    for item in cosmos_bf:
        betfair_list.append(item)
            
    # get Cards            
    query = f"SELECT * FROM c where STARTSWITH(c.cardDate, '{next_saturday}') or STARTSWITH(c.cardDate, '{next_sunday}')"
    cards=[]
    fc_mma_cards = database.get_container_client("fc_mma_cards")
    cosmos_fc = fc_mma_cards.query_items(query,enable_cross_partition_query = True)
    for idc, card in enumerate(cosmos_fc):
        for idf, fight in enumerate(card["fights"]):
            card["fights"][idf]["index"]=idf # add the index to the specific fight object for patch later
            
        cards.append(card)

    
    matched_cards, not_on_betfair = helpers.fuzzy_match_dict_list(cards, betfair_list, "fight_name" ,"name")
    

    logging.info("NOT ON BETFAIR: "+not_on_betfair, utc_timestamp)

    
    for card in matched_cards:
        for fight in card["fights"]:
            operations =[
                { "op": "add", "path": f"/fights/{str(fight["index"])}/betfair_event_id", "value": fight["betfair_event_id"] },
                { "op": "add", "path": f"/fights/{str(fight["index"])}/betfair_country_code", "value": fight["betfair_country_code"] },
                { "op": "add", "path": f"/fights/{str(fight["index"])}/betfair_open_date", "value": fight["betfair_open_date"] },
                { "op": "add", "path": f"/fights/{str(fight["index"])}/betfair_timezone", "value": fight["betfair_timezone"] }
            ]
            
            response = fc_mma_cards.patch_item(item=card["id"], partition_key=card["link"], patch_operations=operations)
            logging.info("PATCH RESPONSE: "+response, utc_timestamp)
            
    
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
