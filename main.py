#!/usr/bin/python
import requests
import json
import time

from pymongo import MongoClient

class WoW:
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client.wow
        pass
    
    def load_config(self):
        with open('auction.conf', 'r') as f:
            self.config = json.load(f)
            print(self.config)
        


    def get_auction(self):
        r = requests.get('https://us.api.battle.net/wow/auction/data/dalaran?locale=en_US&apikey=pxxq5fb72acybqb976zpa87agtvqmcaj')
        response = r.json()
        for i in response['files']:
            print(i)
            if self.config['last_modified'] < i['lastModified']:
                self.config['last_modified'] = i['lastModified']
                print(self.config)
                with open('auction.conf', 'w') as f:
                    json.dump(self.config, f)
                auction = self.save_auction(i['url'])
                for v in auction['auctions']:
                    self.save_item(v['item'])
                    

    def save_item(self, item_id):
        save = False
        check = self.db.items.find({"id" : item_id})
        for i in check:
            save = True
        if not save:
            item = requests.get('https://us.api.battle.net/wow/item/{0}?locale=en_US&apikey=pxxq5fb72acybqb976zpa87agtvqmcaj'.format(item_id)).json()
            self.db.items.insert_one(item)
            return item
        else:
            return False
        
       
    def save_auction(self, url):
        auction = requests.get(url, stream=True).json()
        for i in auction['auctions']:
            i['time'] = self.config['last_modified']
            self.db.auctions.insert_one(i)
        #with open('historical/auction-{0}.json'.format(self.config['last_modified']), 'wb') as f:
        #    f.write(auction.text())
        return auction

    def aggregate(self):
        auctions_get = self.db.auctions.find()
        auctions = {}
        count = 0
        for i in auctions_get:
            if auctions.get(i['item'], '') == '':
                auctions[i['item']] = {"bids" : [], "item" : i['item'], "count" : 0}
                
            auctions[i['item']]['bids'].append(i['bid'])
            auctions[i['item']]['count'] += 1
            count += 1
            if count % 2000 == 0:
                print(auctions[i['item']])
            
        for k, v in auctions.iteritems():
            v['agg_type'] = 'count_aggregation'
            v['time'] = self.config['last_modified']
            self.db.aggregations.insert_one(v)

wow = WoW()
wow.load_config()
wow.get_auction()
wow.aggregate()
