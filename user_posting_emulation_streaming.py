from json import JSONEncoder
from datetime import datetime
from time import sleep
from sqlalchemy import text
import requests
import random
import json
import sqlalchemy


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def post_to_streaming_api(invoke_url, payload):
    headers = {'Content-Type': 'application/json'}

    # send request
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    print(response.status_code)
    print(response.text)

new_connector = AWSDBConnector()

engine = new_connector.create_db_connector()  

def run_infinite_post_data_stream():
    
    pin_url="https://ig1hl12pu9.execute-api.us-east-1.amazonaws.com/pinterest-data-pipeline692/streams/streaming-0a25072a5e0f-pin/record"
    geo_url="https://ig1hl12pu9.execute-api.us-east-1.amazonaws.com/pinterest-data-pipeline692/streams/streaming-0a25072a5e0f-geo/record"
    user_url="https://ig1hl12pu9.execute-api.us-east-1.amazonaws.com/pinterest-data-pipeline692/streams/streaming-0a25072a5e0f-user/record"

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_payload = json.dumps({
                "StreamName": "streaming-0a25072a5e0f-pin",
                 "Data":{   
                        "index": pin_result["index"], 
                        "unique_id": pin_result["unique_id"], 
                        "title": pin_result["title"], 
                        "description": pin_result["description"], 
                        "poster_name": pin_result["poster_name"], 
                        "follower_count": pin_result["follower_count"], 
                        "tag_list": pin_result["tag_list"], 
                        "is_image_or_video": pin_result["is_image_or_video"], 
                        "image_src": pin_result["image_src"], 
                        "downloaded": pin_result["downloaded"], 
                        "save_location": pin_result["save_location"], 
                        "category": pin_result["category"]
                        },
                        "PartitionKey" : "pin_partition1"             
                })
            print(pin_result)
            post_to_streaming_api(pin_url, pin_payload)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_payload = json.dumps({
                "StreamName": "streaming-0a25072a5e0f-geo",
                 "Data":{ 
                        "ind": geo_result["ind"], 
                        "timestamp": geo_result["timestamp"].isoformat(), 
                        "latitude": geo_result["latitude"], 
                        "longitude": geo_result["longitude"], 
                        "country": geo_result["country"]
                        },
                        "PartitionKey": "geo_partition1"  
                })
            print(geo_result)
            post_to_streaming_api(geo_url, geo_payload)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_payload = json.dumps({
                "StreamName": "streaming-0a25072a5e0f-geo",
                 "Data":{ 
                        "ind": user_result["ind"], 
                        "first_name": user_result["first_name"], 
                        "last_name": user_result["last_name"], 
                        "age": user_result["age"], 
                        "date_joined": user_result["date_joined"].isoformat()
                        },
                        "PartitionKey" : "user_partition1"    
                })
            print(user_result)
            post_to_streaming_api(user_url, user_payload)

if __name__ == "__main__":
     run_infinite_post_data_stream()
     print('Working')
