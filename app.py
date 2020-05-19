from flask import Blueprint
from engine import dataEngine
from flask_cors import CORS
main = Blueprint('main', __name__)

import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

def create_app(spark_context):
    global data_engine
    data_engine = dataEngine(spark_context)

    app = Flask(__name__)
    app.register_blueprint(main)
    CORS(app)
    return app

@main.route('/search/<cafename>/<local>', methods=["GET", "POST"])
def searchByCondition(cafename, local):
    return json.dumps(data_engine.search(cafename, local), ensure_ascii=False)

@main.route('/price/<managenum>', methods=["GET", "POST"])
def get_price(managenum):
    logger.debug("Get Price data about", managenum)
    return json.dumps(data_engine.get_price_data(managenum), ensure_ascii=False)

@main.route('/all_data/<float:lat>/<float:lng>', methods=["GET", "POST"])
def get_all(lat, lng):  
    logger.debug("All Cafe's location and name data return")
    result = data_engine.get_all_data(lat, lng)
    return json.dumps(result, ensure_ascii=False)

@main.route('/test', methods=["GET", "POST"])
def test():
    return "Hello world"

