import logging
import os
import sys
"""
sys.path.insert(0,'/app')
sys.path.insert(0,'/app/service')
sys.path.insert(0,'/app/model')
"""
from datetime import datetime as dt
#from werkzeug.local import LocalProxy
from flask_bcrypt import Bcrypt
from flask_jwt_extended import JWTManager
from flask_restful import Api
from flask_mongoengine import MongoEngine
from flask_pymongo import PyMongo
from flask import Flask, request, g
from app.config import config_by_name
from app.model.extensions import NeoDB, RedisCache
from app.service.extensions import logs
from celery import Celery
import pymongo

flask_bcrypt = Bcrypt()
jwt = JWTManager()
api = Api()
dbx = MongoEngine()
cloud_mongodb = PyMongo()

BROKER_URL = 'amqps://cjbcxstm:GLjRO0l4x0GGtZP3TRdtQ7pKARuFp0F4@albatross.rmq.cloudamqp.com/cjbcxstm'
BACKEND_URL = 'redis://krisraman:Gundan123@@redis-10913.c1.us-east1-2.gce.cloud.redislabs.com:10913/0'
def create_app(config_name: str):
    """Main app factory, runs all the other sections"""
    app = Flask(__name__.split(".")[0])
    app.config.from_object(config_by_name[config_name])
    print ("The env is", config_by_name[config_name])
    print ("The environment that google says", os.environ.get("BOILERPLATE_ENV"))

    dbconnectionstring = app.config['FTEYES_HOST'] + ":" + str(app.config['FTEYES_PORT']) + "/" + app.config['FTEYES_DB']
    app.config['MONGODB_SETTINGS'] = {
        'host': dbconnectionstring,
        'username': app.config['FTEYES_USERNAME'],
        'password': app.config['FTEYES_PASSWORD']
    }
    print ("The mongo db URI is", os.environ.get("MONGO_URI"))
    app.config['MONGO_URI'] = os.environ.get("MONGO_URI")
    app.config['SECURITY_PASSWORD_SALT'] = os.environ.get("SECURITY_PASSWORD_SALT")
    jwt.init_app(app)
    flask_bcrypt.init_app(app)
    dbx.init_app(app)

    #loud_mongodb.db.test_collection.insert_one({"From Flask": "Working through Flask23"})

    with app.app_context():
        from app.controller.routes import initialize_routes
        #from service.auth import LoginApi, SignupApi
        #api.add_resource(SignupApi, '/api/auth/signup')
        #api.add_resource(LoginApi, '/api/login')
        initialize_routes(api)
        api.init_app(app)
        register_extensions(app)
        cloud_mongodb.init_app(app)
        celery = Celery(broker=BROKER_URL,backend=BACKEND_URL)
    @app.before_request
    def before_request():
        g.db = cloud_mongodb.db
        g.celery = celery
        #g.celery.send_task("Multiply two numbers", (43,34))
    @app.after_request
    def after_request(response):
        """ Logging after every request. """
        logger = logging.getLogger("app.access")
        logger.info(
            "%s [%s] %s %s %s %s %s %s %s",
            request.remote_addr,
            dt.utcnow().strftime("%d/%b/%Y:%H:%M:%S.%f")[:-3],
            request.method,
            request.path,
            request.scheme,
            response.status,
            response.content_length,
            request.referrer,
            request.user_agent,
        )
        return response
    @app.route('/test', methods=['GET'])
    def home():
        return "<h1>Test</h1><p>Welccome to service. Hope this keeps working. Thank you,. Thank you</p>"
    return app

def register_extensions(app):
    logs.init_app(app)
    NeoDB.init_app(app, os.environ.get("GRAPH_DB_URI"), os.environ.get("GRAPH_DB_STR"), os.environ.get("GRAPH_DB_USER"),
                   os.environ.get("GRAPH_DB_PWD"))
    #RedisCache.init_app(app, app.config['REDIS_HOST'], app.config['REDIS_PORT'], app.config['REDIS_PASSWORD'], app.config['REDIS_DBNAME'])
    #from service.cachebuilder import load_category_cache, load_subcat_brand_cache
    #load_category_cache()
    #load_subcat_brand_cache()
    return None


def run():
    app= create_app(os.environ.get('BOILERPLATE_ENV') or 'dev')
    print("The environment variable is",  os.environ.get('BOILERPLATE_ENV'))
    #app.run(use_reloader=False,host="0.0.0.0",port=8081, debug=True)
    app.run( use_reloader=False, host="0.0.0.0", port=8081, debug=True)

if __name__ == '__main__':
    run()


