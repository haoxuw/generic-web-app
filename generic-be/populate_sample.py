import datetime

import pymongo
import yaml

# todo: use aws secret manager
client = pymongo.MongoClient(
    "mongodb+srv://admin:5Fr8dXesDpebjfiy@generic-be.gvene6b.mongodb.net/"
)

sample_data = [
    {
        "name": "John Doe",
        "id": "1",
        "message": "Hey!",
        "created_at": datetime.datetime.now().timestamp(),
    },
    {
        "name": "Jean Doe",
        "id": "2",
        "message": "Hello World!",
        "created_at": datetime.datetime.now().timestamp(),
    },
]

db = client["generic-be"]
db["monolith"].insert_many(sample_data)
