from flask import Blueprint
from engine import dataEngine
main = Blueprint('main', __name__)

import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

@main.route('/all_data', methods=["GET", "POST"])
def get_all():
    logger.debug("All Cafe's location and name data return")
    result = data_engine.get_all_data()
    return json.dumps(result)


def create_app(spark_context):
    global data_engine
    data_engine = dataEngine(spark_context)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
