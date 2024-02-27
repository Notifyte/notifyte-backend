import requests
import json
 
 

class BetfairClient():
        
    def __init__(self, api_key,username,password):
        self.session_token = self.getSessionToken(username, password, api_key)
        self.header = { 'X-Application' : api_key, 'X-Authentication' : self.session_token ,'content-type' : 'application/json' }
        self.checkConnection()
        
    def getSessionToken(self, username, password, api_key):
        url = f"https://identitysso.betfair.com/api/login?username={username}&password={password}"

        headers = {
        'X-Application': api_key,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Cookie': 'vid=f54cf082-ca87-11ee-bdf9-fa163e6dd2cb; wsid=f54cf081-ca87-11ee-bdf9-fa163e6dd2cb'
        }
        response = requests.request("POST", url, headers=headers)
        res = json.loads(response.text)
        return res.get('token')

        

    def checkConnection(self):
        # TODO: correct way of doing this?
        # this checks to see if connection is live or not by querying horse racing events. 
        test = self.listEvents(event_type_id=7)
        if isinstance(test, list):
            return "Connected to Betfair" # successful connection
        else: 
            # self.listEvents(event_type_id=7).get('faultcode')
            raise ConnectionError # connection to betfair has expired. Session Token has probably expired



    def listEvents(self, event_type_id):
        endpoint = "https://api.betfair.com/exchange/betting/rest/v1.0/"
        json_req='{"filter":{ "eventTypeIds": ['+str(event_type_id)+'] }}'
        url = endpoint + "listEvents/"
        response = requests.post(url, data=json_req, headers=self.header)
        return json.loads(response.text)
    
    def processEvents(self, event_list):
        # [{'name' if k == 'nsme' else k:v for k,v in elem.items()} for elem in my_list] # rename dict keys
        return [dict(item["event"], betfair_event_id=item['event']['id']) for item in event_list]


    def rpcQuery(self, body):
        # turnInPlayEnabled - what does this do can we use it?
        url = "https://api.betfair.com/betting/json-rpc/v1"
        response = requests.post(url=url,data=json.dumps(body), headers=self.header)
        return json.loads(response.text)
        
    
    
    def getMarketIdMarkets(self, market_id, inplay_markets):
        
        # this is the same as get Markets but allows you to pass in marketId for horse racing for example
        
        body = [
        {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": {
                    "marketIds": [market_id],
                    "turnInPlayEnabled": "True",
                    "inPlayOnly":inplay_markets
                },
                "maxResults": "200",
                "marketProjection": [
                    "COMPETITION",
                    "EVENT",
                    "EVENT_TYPE",
                    "RUNNER_DESCRIPTION",
                    "RUNNER_METADATA",
                    "MARKET_START_TIME"
                ]
            },
            "id": 1
        }
        ]
        return self.rpcQuery(body)
    
        
    def getMarkets(self, event_ids, inplay_markets):
        
        body = [
        {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": {
                    "eventIds": event_ids,
                    "turnInPlayEnabled": "True",
                    "inPlayOnly":inplay_markets
                },
                "maxResults": "200",
                "marketProjection": [
                    "COMPETITION",
                    "EVENT",
                    "EVENT_TYPE",
                    "RUNNER_DESCRIPTION",
                    "RUNNER_METADATA",
                    "MARKET_START_TIME"
                ]
            },
            "id": 1
        }
        ]
        return self.rpcQuery(body)
    
    
    def startedEvents(self,event_ids):        
        inplay_true_resp = self.getMarkets(event_ids,"True")[0]["result"]
        true_list = [i["event"]["id"] for i in inplay_true_resp[0]["result"]]
        
        inplay_false_resp = self.getMarkets(event_ids,"False")[0]["result"]
        false_list = [i["event"]["id"] for i in inplay_false_resp[0]["result"]]
        
        # ::: this logic needs to be tested :::
        # in false list only
            # event has not started yet.
        not_started = list(set(false_list).difference(true_list))
        
        # in true list only
            # event is live        
        started_events = list(set(true_list).difference(false_list))
        
        # not in both...
            # event has finished? - not surethis is accurate..?
            
        finished_events = list(set(event_ids).difference(true_list+false_list))
        
        
        return not_started,started_events,finished_events

        
    def eventStartedChecker(self,event_ids):
        # event_ids must be a list of 1 event currently
        inplay_true = self.getMarkets(event_ids,"True")[0]["result"]  
        inplay_false = self.getMarkets(event_ids,"False")[0]["result"] 
        # logic to check if an event has started or not? Tested with Horse racing / may need to test with different event types 
        if inplay_false != []:
            print("Event has not started yet")
            return False
        elif inplay_true != []:
            print("Event is Live")
            return True
        elif inplay_false == [] and inplay_true == []: 
            print("Event has not started or is finished")
            return False
        else:
            print("error, both inplay and not inplay have markets")
            return False
    
    
    # once per day, update events
    
    def getTodayEvents(self,event_type_id):
        
        response = self.listEvents(event_type_id)
        
        
        