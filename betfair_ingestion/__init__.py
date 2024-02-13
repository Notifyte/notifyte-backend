import sys
sys.path.append("..")
import datetime
import logging

import azure.functions as func

from shared_utilities.Betfair import BetfairClient
from azure.cosmos import CosmosClient
import os


delay_api_key = os.getenv("betfair_delay_api_key") # app setting
username = os.getenv("betfair_username") # app setting
password = os.getenv("betfair_password") # app setting

event_id = 6 # mma
conn_string = os.getenv("cosmosDbConnectionString")

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')


    bf_client = BetfairClient(delay_api_key, username, password)
        
    events = bf_client.listEvents(event_id)
    data = bf_client.processEvents(events)
    1
    storageClient = CosmosClient.from_connection_string(conn_str=conn_string)
    database = storageClient.get_database_client("fightstore")
    container = database.get_container_client("betfair_mma_events")

    existing_items = [i["id"] for i in container.read_all_items()]


    # update item or create if not exist.
    # TODO: is id always unique???
    
    for i in data:
        if i["id"] in existing_items:
            container.upsert_item(i)
            logging.info("UPSERT ITEM : " + str(i))
        else:
            container.create_item(i)
            logging.info("CREATE ITEM : " + str(i))
                        

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
