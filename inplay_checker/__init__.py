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

    next_saturday, next_sunday, next_monday = helpers.find_next_weekend()

    # select next weekend card order by 

    queryText = f"""
    SELECT  c.title
        ,c.date
        ,c.id
        ,c.cardDate
        ,c.link
        ,cx.betfair_event_id
        ,cx.fight_name
        ,cx.betfair_open_date
        ,cx.start_time
    FROM   c
    JOIN   cx IN c.fights
    WHERE (cx.betfair_open_date BETWEEN '{next_saturday}' AND '{next_monday}')
    AND IS_NULL(cx.start_time)
    """

    results = fc_mma_cards.query_items(query=queryText, enable_cross_partition_query=True)

    items = [item for item in results]
    
    logging.info('Starting Incremental Checker next')

    # check the api for 8 mins
    t_end = time.time() + (8*60)+50
    while time.time() < t_end:
        for idx, item in  enumerate(items):
            time.sleep(0.4)
            print("CHECKING FIGHT: " + item["fight_name"])
            if bf_client.eventStartedChecker(item["betfair_event_id"]) == True:
                operations =[{ "op": "add", "path": "/fights/"+str(idx)+"/start_time", "value": int(time.time()) }]
                response = fc_mma_cards.patch_item(item=item["id"], partition_key=item["link"], patch_operations=operations)


    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
