import os
from flask import Flask, request, jsonify
from pyspark import SparkContext, SparkConf
from xmlToCsv import xmlToCsv
from simpleWordcount import simpleWordCount
app = Flask(__name__)

@app.route('/')
def hello_world():
    return "Hello world!"

@app.route('/xmlcsv')
def xmlcsv():
    parser = xmlToCsv()
    return parser.getCsv()

@app.route('/wordcount')
def wordCount():
    w = simpleWordCount()
    w.word("text.txt")
    return "complete"

@app.route('/environments/<language>')
def environments(language):
    return jsonify({"language": language})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

