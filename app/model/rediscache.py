import redis
from flask_restful import Resource
from flask import current_app, Response, request

class RedisConnection(Resource):
    def __init__(self):
        self.app = None
        self.__driver = None
        self.__cachedriver = None

    def init_app(self, app, host, port, password, dbname):
        self.app = app
        self.__host = host
        self.__port = port
        self.__password = password
        self.__dbname = dbname
        self.connect()
    def connect(self):
        try:
            self.__cachedriver = redis.Redis(self.__host, self.__port, self.__password)
            print ("Successfully connected to Redis")
            return True
        except Exception as e:
            print("Error in connecting to Redis")
            return False

    def get_driver(self):
        return self.__cachedriver

    def set(self, key, value):
        return True

    def get(self, key):
        return True

    def __close(self):
        if self.__cachedriver is not None:
            self.__cachedriver = None
