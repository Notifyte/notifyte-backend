import datetime
import logging
import os
import sys
sys.path.append("..")
from shared_utilities.FightCards import FightCardsClient
from azure.cosmos import CosmosClient
import azure.functions as func



def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
        
    conn_string = os.getenv("cosmosdb_deets") # app setting
    
    FC_client = FightCardsClient()
    processed = FC_client.process_cards()

    client = CosmosClient.from_connection_string(conn_str=conn_string)
    database = client.get_database_client("fightstore")
    container = database.get_container_client("fc_mma_cards")




    ## What is the right way of doing this?
    ## TODO: Use Bindings? PartitionId, DocumentId

    for i in processed:
        query=f"select * from c where c.link = '{i['link']}'"
        
        # check if query iterator is empty.
        if all(False for _ in container.query_items(query,enable_cross_partition_query= True)):
            container.create_item(i)
        else:
            print(f"item {i['link']} already exists.")





    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
