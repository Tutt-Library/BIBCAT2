__author__ = "Jeremy Nelson"

import os
import rdflib
from flask import Blueprint

catalog = Blueprint("catalog", 
    __name__,
    template_folder='templates')

BLUEPRINT_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_BASE = os.path.split(BLUEPRINT_DIR)[0] 

def setup():
    global KNOWLEDGE_GRAPH
    KNOWLEDGE_GRAPH = rdflib.Graph()
    # Load all turtle files in custom directory
    try:
        custom_directory = os.path.join(PROJECT_BASE, "custom")
        walker = next(os.walk(custom_directory))
        for name in walker[2]:
            if not name.endswith("ttl"):
                continue
            filepath = os.path.join(PROJECT_BASE, "custom/{}".format(name))
            KNOWLEDGE_GRAPH.parse(filepath, format='turtle')
    except StopIteration:
        pass


setup()
from .views import *
