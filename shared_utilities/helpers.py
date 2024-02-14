from datetime import datetime, timedelta
import json
import os
from fuzzywuzzy import process
import pandas as pd

def find_next_weekend():
    # Get the current date
    current_date = datetime.now()
    # Calculate the number of days until the next Saturday
    days_until_saturday = (5 - current_date.weekday() + 7) % 7
    # Calculate the date of the next Saturday
    next_saturday = current_date + timedelta(days=days_until_saturday)
    # # Calculate the date of the next Sunday
    next_sunday = next_saturday + timedelta(days=1)

    return next_saturday.strftime('%Y-%m-%d'), next_sunday.strftime('%Y-%m-%d')


def fuzzy_match_dict_list(fight_cards, betfair_events, fc_key='name', bfe_key='name'):
    """_summary_

    Args:
        fight_cards (_type_): _description_
        dict_list2 (_type_): _description_
        key1 (str, optional): _description_. Defaults to 'name'.
        key2 (str, optional): _description_. Defaults to 'name'.

    
    - an card is a mma fight card (retrieved from fc_mma_cards)
    - a fight is a specific fight on that mma card
    - an event is an event with event id retrieved from betfair
    
    Returns:

        _type_: _description_
        matches: match both dataframes
        mismatches1: unmatched list 1
        mismatches2: unmatched list 2
    """
    matched_cards = []
    mismatches1 = []
    for card in fight_cards:
        specific_card = {
            'title': card["title"],
            'date': card["date"],
            'link': card["link"],
            'updatedAt': card["link"],
            'id': card["id"],
            'cardDate': card["cardDate"],
            'partition': card["partition"],
            'fights': []    
        }
        
        for fight in card["fights"]:
            match, score, *_ = process.extractOne(fight[fc_key], [event[bfe_key] for event in betfair_events])
            if score >= 80:
                merged_dict = {**fight, **next(merge_output(event) for event in betfair_events if event[bfe_key] == match)}
                specific_card["fights"].append(merged_dict)
            else:
                mismatches1.append(fight)
        matched_cards.append(specific_card)
        
    # mismatches2 = [event for event in betfair_events if all(fight[fc_key] != event[bfe_key] for fight in card["fights"])] # This gives wrong mismatches
    return matched_cards, mismatches1


def merge_output(event):
    output =   {
        'betfair_event_id': event["betfair_event_id"],
        'betfair_country_code': event["countryCode"],
        'betfair_open_date': event["openDate"],
        'betfair_timezone': event["timezone"] 
    }   
    return output
    
    
    
    {'id': '32926453',
  'name': 'JeongYeong Lee v Blake Bilder',
  'countryCode': 'GB',
  'timezone': 'GMT',
  'openDate': '2024-02-03T21:55:00.000Z',
  'betfair_event_id': '32926453',
  '_rid': 'osU7APNWX8cRAAAAAAAAAA==',
  '_self': 'dbs/osU7AA==/colls/osU7APNWX8c=/docs/osU7APNWX8cRAAAAAAAAAA==/',
  '_etag': '"0300d31e-0000-1100-0000-65b046280000"',
  '_attachments': 'attachments/',
  '_ts': 1706051112},     



def read_json(file_name):
        with open(file_name) as json_data:
            data = json.load(json_data)
            json_data.close()
        return data
    
def saveJson(json_var, file, path=""):
        # create a json with the full response. This file will be used for identifying contacts
    filename = os.path.join(path, file+".json")
    # print(filename)
    with open(filename, 'w') as fp:
        json.dump(json_var, fp, indent=4)
