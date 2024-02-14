import requests
import json
import uuid
import datetime

class FightCardsClient():
    def __init__(self) -> None:
        pass
        self.data=self.get_data()
    
    
    def get_data(self):
        url = "https://mmafightcardsapi.adaptable.app/"
        response = requests.get(url=url)
        return json.loads(response.text)
    
    
    
    # def getYear(month):
    #     if month < datetime.datetime.now().month - 7:
    #     # fix this ...            
    #         pass    


        
    def date_to_datetime(self,date):
        # date = "Saturday, January 20,  6:30 PM ET"
                
        month_dict = {
            'January':"01",
            'February':"02",
            'March':"03",
            'April':"04",
            'May':"05",
            'June':"06",
            'July':"07",
            'August':"08",
            'September':"09",
            'October':"10",
            'November':"11",
            'December':"12"
            }
            
        day = date.split(",")[1][-2:].replace(" ","0")
        year = datetime.datetime.now().year # may need to do year diff
        month = month_dict[date.split(",")[1].split(" ")[1]]
        return f"{year}-{month}-{day}"       
    
    
    def process_cards(self):
        """
        adds ingestion time and required fields for cosmos e.g. name, partition and id

        Returns:
            _type_: _description_
        """
        cards = []
        for card in self.data["data"]:
            card["updatedAt"] = self.data["updatedAt"]
            card["id"] = str(abs(hash(card["link"])))
            card["cardDate"] = self.date_to_datetime(card["date"])
            card["fights"] = self.process_fights(card["fights"])
            card["partition"] = str(abs(hash(card["title"])))
            cards.append(card)
        return cards
    
    
    
    def process_fights(self,fight_list):
        fights = []
        for fight in fight_list:
            fight["fight_name"] = fight['fighterA']['name'] + ' v ' + fight['fighterB']['name']
            
            fight["fight_id"] = str(abs(hash(fight["fight_name"]))) # TODO: Don't need this...

            # remove these so not to overwrite. 
             
            # fight["betfair_event_id"] = None
            # fight["betfair_country_code"] = None
            # fight["betfair_open_date"] = None
            # fight["betfair_timezone"] = None
            # fight["start_time"] = None
            # fight["end_time"] = None
            fights.append(fight)
        return fights
