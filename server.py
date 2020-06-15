import cherrypy, os, time, sys
from paste.translogger import TransLogger
from pyspark import SparkContext, SparkConf
from app import create_app
def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("mycoffee_reccomendation-server")
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py']).getOrCreate()
    
    return sc

def run_server(app):
    app_logged = TransLogger(app)
    cherrypy.tree.graft(app_logged , '/')
    
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })

    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == "__main__":
    sc = init_spark_context()
    app = create_app(sc)
    run_server(app)
