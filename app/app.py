import logging
import os
import sys
sys.path.insert(0,'/app')
sys.path.insert(0,'/app/service')
sys.path.insert(0,'/app/model')
from datetime import datetime as dt
from flask_bcrypt import Bcrypt
from flask_jwt_extended import JWTManager
from flask_restful import Api
from flask_mongoengine import MongoEngine
from flask import Flask, request
from config import config_by_name
from service.extensions import logs
from model.extensions import NeoDB

flask_bcrypt = Bcrypt()
jwt = JWTManager()
api = Api()
db = MongoEngine()

def create_app(config_name: str):
    """Main app factory, runs all the other sections"""
    app = Flask(__name__.split(".")[0])
    app.config.from_object(config_by_name[config_name])
    print ("The env is", config_by_name[config_name])

    dbconnectionstring = app.config['FTEYES_HOST'] + ":" + str(app.config['FTEYES_PORT']) + "/" + app.config['FTEYES_DB']
    app.config['MONGODB_SETTINGS'] = {
        'host': dbconnectionstring,
        'username': app.config['FTEYES_USERNAME'],
        'password': app.config['FTEYES_PASSWORD']
    }
    print ("The envvironment is ", os.getenv('PYTHONPATH'))
    jwt.init_app(app)
    flask_bcrypt.init_app(app)
    db.init_app(app)


    with app.app_context():
        from controller.routes import initialize_routes
        from service.auth import LoginApi, SignupApi
        #api.add_resource(SignupApi, '/api/auth/signup')
        #api.add_resource(LoginApi, '/api/login')
        initialize_routes(api)
        api.init_app(app)
        register_extensions(app)


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
        return "<h1>Test</h1><p>Welccome to service</p>"
    return app


def register_extensions(app):
    logs.init_app(app)
    NeoDB.init_app(app, app.config['FTEYES_GDB_URI'], app.config['FTEYES_GDB_DB'], app.config['FTEYES_GDB_USER'],
                   app.config['FTEYES_GDB_PWD'])
    return None


def run():
    app= create_app(os.getenv('BOILERPLATE_ENV') or 'dev')
    app.run(use_reloader=False)
    print ('After done running89')

if __name__ == '__main__':
    run()


