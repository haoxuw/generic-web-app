import os

import pymongo
from bson import json_util

import config_manager


class MongoConnection:
    __configs = config_manager.ConfigManager().configs
    __database_client = None

    @classmethod
    def get_instance(
        cls,
    ):
        if cls.__database_client is None:
            client = pymongo.MongoClient(cls.__configs["mongodb"]["connection_string"])
            cls.__database_client: pymongo.database.Database = client[
                cls.__configs["mongodb"]["database_name"]
            ]
        return cls.__database_client

    def get_collection(self, collection_name):
        return self.get_instance()[collection_name]

    @classmethod
    def json_str(cls, cursor):
        return json_util.dumps(cls.to_serializable(cursor=cursor), indent=4)

    @classmethod
    def to_serializable(cls, cursor):
        return [dict(row) for row in cursor]
