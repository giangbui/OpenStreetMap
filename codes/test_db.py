"""
Complete the insert_data function to insert the data into MongoDB.
"""

import json

def insert_data(data, db):

    # Your code here. Insert the data into a collection 'arachnid'
    db.arachnid.insert(data)

    pass


if __name__ == "__main__":
    
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    db = client.kansas
 

    with open('kansas-city-lawrence-topeka_kansas.osm.json') as f:
        data = json.loads(f.read())
        insert_data(data, db)

    print(db.char.find().count())
        