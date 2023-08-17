import ssl
import fnmatch
import logging
from pymongo import MongoClient
from bson.json_util import dumps

logger = logging.getLogger(__name__)


class Database(object):
    def __init__(self, config, dbname):
        self.config = config
        self.dbname = dbname
        self.client = None
        self.db = None

    def connect(self):
        uri = "mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{AUTH_SOURCE}"
        uri = uri.format(
            USERNAME=self.config["username"],
            PASSWORD=self.config["password"],
            HOST=self.config["hostname"],
            PORT=self.config["port"],
            AUTH_SOURCE=self.config["auth_source"],
        )
        self.client = MongoClient(
            uri,
        )
        self.db = self.client[self.dbname]
        return self
