__author__ = "Jeremy Nelson"

import rdflib

from flask import render_template, jsonify, request, make_response
from flask import abort, url_for
from . import catalog, KNOWLEDGE_GRAPH

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")
SCHEMA = rdflib.Namespace("http://schema.org/")

def new_graph():
    graph = rdflib.Graph()
    graph.namespace_manager.bind("schema", SCHEMA)
    graph.namespace_manager.bind("bf", BF)
    return graph

@catalog.app_template_filter("get_label")
def get_label(graph, iri):
    label = graph.value(subject=iri, predicate=rdflib.RDFS.label)
    return label

@catalog.app_template_filter("display_triples")
def display_triple(graph, iri):
    output = "<dl>\n"
    for pred, obj in graph.predicate_objects(
        subject=iri):
        output += "<dt>{0}</dt>\n".format(pred)

        if isinstance(obj, rdflib.BNode):
            output += "<dd><ul>"
            print(obj)
            print(graph.value(subject=obj))
            for p1, o1 in graph.predicate_objects(
                subject=obj):
                print("IN predicate object")
                output += "<li><strong>{}</strong>={}</li>".format(
                    p1, o1)
            output += "</ul></dd>\n"
        else:
            output += "<dd>"
            if obj.startswith("http://catalog"):
                uuid = str(obj).split("/")[-1]
                label = get_label(KNOWLEDGE_GRAPH, obj)
                output += """<a href="{0}">{1}</a>""".format(
                    url_for('catalog.detail', uuid=uuid, ext='html'),
                    label)               
            else:
                output += str(obj)
            output += "</dd>"
    output += "</dl>"
    return output

@catalog.route("/<uuid>")
@catalog.route("/<uuid>.<ext>")
def detail(uuid, ext=None):
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
    accept_header = request.headers.get("Accept")
    if "application/json" in accept_header or ext=="json":
        response = make_response(output.serialize(format='json-ld'))
        response.headers["Content-Type"] = "application/json"
    elif "text/turtle" in accept_header or \
         "application/x-turtle" in accept_header or\
        ext=="ttl":
        response = make_response(output.serialize(format='turtle'))
        response.headers["Content-Type"] =  "text/turtle"
    elif "application/rdf+xml" in accept_header or\
        ext == "xml" or ext == "rdf":
        response = make_response(output.serialize())
        response.headers["Content-Type"] = "application/rdf+xml"
    elif "text/plain" in accept_header or ext == "nt":
        response = make_response(output.serialize(format='nt'))
        response.headers["Content-Type"] = "text/plain"
    else:
        response = make_response(
            render_template("catalog/detail.html", 
                graph=output, 
                iri=iri))
        response.headers["Content-Type"] = "text/html; charset=utf-8"
    return response
    

@catalog.route("/")
def home():
    return "Tiger Catalog, Number of triples in Knowledge Graph {}".format(len(KNOWLEDGE_GRAPH))
