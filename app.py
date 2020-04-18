from flask import Blueprint
main = Blueprint('main', __name__)


import json

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

@main.route("", methods=["GET"])
def top_rankings