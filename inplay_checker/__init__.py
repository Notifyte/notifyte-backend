import datetime
import logging

import sys
sys.path.append("..")
from shared_utilities.Betfair import BetfairClient
from azure.cosmos import CosmosClient
import shared_utilities.helpers as helpers
import time
import os

import azure.functions as func

delay_api_key = os.getenv("betfair_delay_api_key") # app setting
username = os.getenv("betfair_username") # app setting
password = os.getenv("betfair_password") # app setting
conn_string = os.getenv("cosmosdb_deets")


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    bf_client = BetfairClient(api_key=delay_api_key, username=username, password=password)

    storageClient = CosmosClient.from_connection_string(conn_str=conn_string)
    database = storageClient.get_database_client("fightstore")
    fc_mma_cards = database.get_container_client("fc_mma_cards")

    # next_saturday, next_sunday, next_monday = helpers.find_next_weekend() # not this because it will be the weekend when this code runs
    this_saturday, this_sunday, this_monday = helpers.find_previous_weekend()

    # select next weekend card order by 
    # queryText = f"""
    # SELECT  c.title
    #     ,c.date
    #     ,c.id
    #     ,c.cardDate
    #     ,c.link
    #     ,cx.betfair_event_id
    #     ,cx.fight_name
    #     ,cx.betfair_open_date
    #     ,cx.start_time
    #     ,cx.exp_start
    # FROM   c
    # JOIN   cx IN c.fights
    # WHERE (cx.betfair_open_date BETWEEN '{this_saturday}' AND '{this_monday}')
    # AND IS_NULL(cx.start_time)
    # """

    queryText = f"""
    SELECT  c.title
        ,c.date
        ,c.id
        ,c.cardDate
        ,c.link
        ,c.fights
    FROM   c
    WHERE (c.cardDate BETWEEN '{this_saturday}' AND '{this_monday}')
    AND CONTAINS(c.title, 'UFC')
    """
    # Currently the idx is coming from the order of results from this query. Not the order they exist in the document....


    result = fc_mma_cards.query_items(query=queryText, enable_cross_partition_query=True)[0] # 0 = UFC events only / 1 event per weekend

    # items = [item for item in results]
    
    logging.info('Starting Incremental Checker next')
    
    # need to add in the expected start time updater 
    
    # check the api for 8 mins
    t_end = time.time() + (8*60)+50
    while time.time() < t_end:
        for idx, item in  enumerate(result["fights"]):
            time.sleep(0.4)
            print("CHECKING FIGHT: " + item["fight_name"])
            if bf_client.eventStarztedChecker([item["betfair_event_id"]]) == True:
                operations =[{ "op": "add", "path": "/fights/"+str(idx)+"/start_time", "value": time.time() }]
                response = fc_mma_cards.patch_item(item=result["id"], partition_key=result["link"], patch_operations=operations) #happens once.
                #TODO: log the response?
            
            # new_exp_start = bf_client.expStartChecker() # work out how to find this for individual event
            # old_exp_start = 1 # read current expStart time from the fightstore.
            # if old_exp_start != new_exp_start:
            #     operations =[{ "op": "add", "path": "/fights/"+str(idx)+"/exp_start", "value": new_exp_start }]
            #     response = fc_mma_cards.patch_item(item=item["id"], partition_key=item["link"], patch_operations=operations) #happens once.



            



    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
