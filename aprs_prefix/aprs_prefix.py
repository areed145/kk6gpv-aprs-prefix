import aprslib
from datetime import datetime, timezone
from pymongo import MongoClient
import os
import logging


class AprsPrefix:
    """
    Class for listening to prefix data from APRS
    """

    def __init__(self):
        self.setup_logging()
        self.mongo_connect()
        self.aprs_connect()

    def setup_logging(self):
        """initialize logging"""
        self.logger = logging.getLogger(name="aprs-prefix")
        self.logger.info("Logger started")

    def mongo_connect(self):
        """connect to mongo"""
        self.client = MongoClient(os.environ["MONGODB_CLIENT"])
        self.db = self.client.aprs
        self.raw = self.db.raw
        self.logger.info("Connected to Mongo")

    def aprs_connect(self):
        """connects to APRS"""
        self.ais = aprslib.IS("N0CALL", "13023", port=14580)
        self.ais.set_filter("p/KK6GPV")
        while True:
            try:
                self.ais.connect()
                self.logger.info("Connected to APRS")
                self.ais.consumer(self.unpack_dict, raw=False, immortal=True)
            except Exception as e:
                print(str(e))

    def unpack_dict(self, d):
        """function to unpack APRS message"""
        try:
            message = dict()
            message["timestamp_"] = datetime.now(timezone.utc)
            message["script"] = "prefix"
            for k, v in d.items():
                try:
                    for k1, v1 in v.items():
                        message[k + "_" + k1] = v1
                except Exception:
                    try:
                        message[k] = v
                    except Exception:
                        message[k] = str(v)
            self.raw.insert_one(message)
            self.logger.info(str(message))
        except Exception as e:
            self.logger.error("unpack failed")
            self.logger.error(str(e))


if __name__ == "__main__":
    aprsprefix = AprsPrefix()
