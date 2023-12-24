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

def post_to_api(invoke_url, payload_dict):
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    # convert dict to payload format
    payload = json.dumps({
        "records":[
            {"value": payload_dict}
            ]
        }, cls=DateTimeEncoder)

    # send request
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    print(response.status_code)
    print(response.text)

new_connector = AWSDBConnector()

engine = new_connector.create_db_connector()  

def run_infinite_post_data_loop():

    pin_url="https://ig1hl12pu9.execute-api.us-east-1.amazonaws.com/pinterest-data-pipeline692/topics/0a25072a5e0f.pin"
    geo_url="https://ig1hl12pu9.execute-api.us-east-1.amazonaws.com/pinterest-data-pipeline692/topics/0a25072a5e0f.geo"
    user_url="https://ig1hl12pu9.execute-api.us-east-1.amazonaws.com/pinterest-data-pipeline692/topics/0a25072a5e0f.user"

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
           
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            print(pin_result)
            post_to_api(pin_url, pin_result)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
           
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            print(geo_result)
            post_to_api(geo_url, geo_result)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
           
            for row in user_selected_row:
                user_result = dict(row._mapping)

            print(user_result)
            post_to_api(user_url, user_result)

if __name__ == "__main__":
     run_infinite_post_data_loop()
     print('Working')
