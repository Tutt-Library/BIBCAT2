__author__ = "Jeremy Nelson"

import rdflib
import urllib.parse
from bs4 import BeautifulSoup
from flask import url_for
from flask import current_app as app
from types import SimpleNamespace
from .views import catalog, KNOWLEDGE_GRAPH, SCHEMA, BF


@catalog.app_template_filter("display_people")
def display_people(graph, iri):
    types_of = list(set([o for o in graph.objects(subject=iri,
                                                  predicate=rdflib.RDF.type)]))
    if BF.Person in types_of:
        return ''
    output = BeautifulSoup()
    for year_iri in KNOWLEDGE_GRAPH.subjects(predicate=SCHEMA.organizer,
        object=iri):
        year_label = KNOWLEDGE_GRAPH.value(subject=year_iri,
            predicate=rdflib.RDFS.label)
        div = output.new_tag("div")
        h2 = output.new_tag("h2")
        h2.string = year_label
        div.append(h2)
        people = dict()
        h3 = output.new_tag("h3")
        h3.string = "People"
        div.append(h3)
        ul = output.new_tag("ul")
        for pred, obj in KNOWLEDGE_GRAPH.predicate_objects(
            subject=year_iri):
            if isinstance(obj, rdflib.URIRef):
                pred_label = get_label(KNOWLEDGE_GRAPH, pred)
                obj_label = get_label(KNOWLEDGE_GRAPH, obj)
                for type_ in KNOWLEDGE_GRAPH.objects(subject=obj,
                    predicate=rdflib.RDF.type):
                    if type_ == BF.Person:
                        li = output.new_tag("li")
                        person_a = output.new_tag("a", href=str(obj))
                        if obj_label is None:
                            person_a.string = str(obj)
                        else:
                            person_a.string = obj_label
                        li.append(person_a)
                        if pred_label is not None:
                            title = output.new_tag('span')
                            title.string = ", {}".format(pred_label)
                            li.append(title)
                        ul.append(li)
                        if pred in people:
                            people[pred]["persons"].append({"iri": obj,
                                                            "name": obj_label})
                        else:
                            people[pred] = {"persons": [{"iri": obj,
                                                         "name": obj_label}],
                                            "label": pred_label}
        div.append(ul)
        output.append(div)
    return output.decode(pretty_print=True)

@catalog.app_template_filter("display_triples")
def display_triple(graph, iri):
    output = BeautifulSoup()
    dl = output.new_tag("dl")
    for pred, obj in graph.predicate_objects(
        subject=iri):
        dt, dd = output.new_tag("dt"), output.new_tag("dd")
        dt.string = pred.n3(KNOWLEDGE_GRAPH.namespace_manager)
        dl.append(dt)    
        if isinstance(obj, rdflib.BNode):
            ul = output.new_tag("ul")
            for p1, o1 in graph.predicate_objects(
                subject=obj):
                li = output.new_tag("li")
                strong = output.new_tag('strong')
                strong.string = str(p1)
                li.append(strong)
                li.string = str(o1)
                ul.append(li)
            dd.append(ul)
        else:
            if obj.startswith("http"):
                if obj.startswith("http://catalog"):
                    uuid = str(obj).split("/")[-1]
                    anchor = output.new_tag("a", 
                        href=url_for('catalog.detail', 
                                     uuid=uuid, 
                                     ext='html'))
                    anchor.string = get_label(KNOWLEDGE_GRAPH, obj)
                else:
                    anchor = output.new_tag('a', href=str(obj))
                    anchor.string = str(obj)
                dd.append(anchor)
            else:
                dd.string = str(obj)
        dl.append(dd)
    output.append(dl)
    return output.decode(pretty_print=True)

@catalog.app_template_filter("get_departments")
def get_college_depts(graph, iri):
    """Returns a list of college departments"""
    output = BeautifulSoup()
    ul = output.new_tag("ul")
    for dept_iri, label in KNOWLEDGE_GRAPH.query(COLLEGE_DEPTS,
        initBindings={"institution_iri": iri}):
        li = output.new_tag("li")
        anchor = output.new_tag('a', href=dept_iri)
        anchor.string = label
        li.append(anchor)
        text_spacer = output.new_tag("span")
        text_spacer.string = "-"
        li.append(text_spacer)
        about = output.new_tag('a', 
            href="{0}?iri={1}".format(
                url_for('catalog.detail', uuid='about'),
                dept_iri))
        about.string = "about"
        li.append(about)
        #li.append(BeautifulSoup(display_triple(graph, dept_iri)))
        ul.append(li)
    output.append(ul)
    return output.decode(pretty_print=True)

@catalog.app_template_filter("get_label")
def get_label(graph, iri):
    label = graph.value(subject=iri, predicate=rdflib.RDFS.label)
    return label

@catalog.app_template_filter("get_people")
def get_people(graph, iri):
    """Retrieves all people for an organization"""
                    
            
     

COLLEGE_DEPTS = """SELECT ?iri ?label
WHERE {
    ?iri a schema:CollegeDepartment ;
         rdfs:label ?label ;
         schema:parentOrganization ?institution_iri .
} ORDER BY ?label"""


