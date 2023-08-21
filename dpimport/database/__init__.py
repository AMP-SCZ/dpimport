import os
import logging
from pymongo import MongoClient
from bson.json_util import dumps

logger = logging.getLogger(__name__)


class Database(object):
    def __init__(self, dbname):
        self.dbname = dbname
        self.client = None
        self.db = None

    def connect(self):
        uri = os.environ["MONGODB_URI"]
        self.client = MongoClient(
            uri,
        )
        self.db = self.client[self.dbname]
        return self
