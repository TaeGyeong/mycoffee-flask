from flask import Blueprint
main = Blueprint('main', __name__)


import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

@main.route('/', methods=["GET"])
def main_test():
    return "Hello world"

# @main.route('/')

def create_app(spark_context, dataset_path):
    ##########
    # engine #
    ##########
    app = Flask(__name__)
    app.register_blueprint(main)
    return app