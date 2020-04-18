import os
from flask import Flask, request, jsonify
from pyspark import SparkContext, SparkConf
from xmlToCsv import xmlToCsv
from simpleWordcount import simpleWordCount
from data_text import read_test

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

@app.route('/read_test')
def test():
    a = read_test()
    if a.get_three_row():
        return "complete"
   

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
