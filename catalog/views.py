__author__ = "Jeremy Nelson"

import rdflib

from flask import render_template, jsonify, request, make_response
from flask import abort, url_for
from flask import current_app as app
from . import catalog, KNOWLEDGE_GRAPH

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")
SCHEMA = rdflib.Namespace("http://schema.org/")

def new_graph():
    graph = rdflib.Graph()
    graph.namespace_manager.bind("schema", SCHEMA)
    graph.namespace_manager.bind("bf", BF)
    return graph

def generate_response(graph, iri, ext=None):
    accept_header = request.headers.get("Accept")
    if "application/json" in accept_header or ext=="json":
        response = make_response(graph.serialize(format='json-ld'))
        response.headers["Content-Type"] = "application/json"
    elif "text/turtle" in accept_header or \
         "application/x-turtle" in accept_header or\
        ext=="ttl":
        response = make_response(graph.serialize(format='turtle'))
        response.headers["Content-Type"] =  "text/turtle"
    elif "application/rdf+xml" in accept_header or\
        ext == "xml" or ext == "rdf":
        response = make_response(graph.serialize())
        response.headers["Content-Type"] = "application/rdf+xml"
    elif "text/plain" in accept_header or ext == "nt":
        response = make_response(graph.serialize(format='nt'))
        response.headers["Content-Type"] = "text/plain"
    else:
        response = make_response(
            render_template("catalog/detail.html", 
                graph=graph, 
                iri=iri))
        response.headers["Content-Type"] = "text/html; charset=utf-8"
    return response


@catalog.route("/<uuid>")
@catalog.route("/<uuid>.<ext>")
def detail(uuid, ext=None):
    if "iri" in request.args:
        iri = rdflib.URIRef(request.args.get('iri'))
    else:
        iri = rdflib.URIRef("http://catalog.coloradocollege.edu/{0}".format(uuid))
    iri_types = [type_ for type_ in KNOWLEDGE_GRAPH.objects(subject=iri,
                                                            predicate=rdflib.RDF.type)]
    if len(iri_types) < 1:
        abort(404)
    output = new_graph()
    for p, o in KNOWLEDGE_GRAPH.predicate_objects(subject=iri):
        output.add((iri, p, o))
        if isinstance(o, rdflib.BNode):
            for p1, o1 in KNOWLEDGE_GRAPH.predicate_objects(subject=0):
                output.add((o, p1, o1))
    response = generate_response(output, iri, ext)
    return response
    

@catalog.route("/")
def home():
    if "INSTITUTION_IRI" in app.config:
        institution_iri = app.config["INSTITUTION_IRI"]
    else:
        institution_iri = KNOWLEDGE_GRAPH.value(
            predicate=rdflib.RDF.type,
            object=SCHEMA.CollegeOrUniversity)

    return render_template("catalog/index.html",
        graph=KNOWLEDGE_GRAPH,
        institution_iri=institution_iri)
