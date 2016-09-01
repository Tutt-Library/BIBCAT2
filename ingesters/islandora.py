"""Custom Islandora Repository Ingester for Colorado College, extends 
BIBCAT MODS Ingester."""
__author__ = "Jeremy Nelson"

import click
import datetime
import logging
import os
import rdflib
import requests
import sys
import xml.etree.ElementTree as etree

TIGER_BASE = os.path.abspath(
    os.path.split(
        os.path.dirname(__file__))[0])
sys.path.append(TIGER_BASE)
import bibcat.ingesters.mods as mods_ingester

NS_MGR = mods_ingester.NS_MGR
NS_MGR.bind("pcdm", rdflib.Namespace("http://pcdm.org/models#"))

class IslandoraIngester(mods_ingester.MODSIngester):

    def __init__(self, **kwargs):
        """Takes kwargs and initialize instance of IslandoraIngester"""
        if not 'rules' in kwargs:
            kwargs['rules'] = ['cc-mods-bf.ttl']
        kwargs["base_url"] = "https://catalog.coloradocollege.edu/"
        self.fedora_base = kwargs.pop("repository_url")
        self.ri_search = "{}/risearch".format(self.fedora_base)
        self.rest_url = "{}/objects/".format(self.fedora_base)
        self.auth = (kwargs.pop("user"), kwargs.pop("password"))
        super(IslandoraIngester, self).__init__(**kwargs)


    def __get_label__(self, pid):
        """Retrieves label for a PID using the REST API"""
        object_url = "{}{}?format=xml".format(self.rest_url, pid)
        result = requests.post(object_url,
            auth=self.auth)
        if result.status_code < 400:
            object_profile = etree.XML(result.text)
            label = object_profile.find("")
            if label:
                return rdflib.Literal(label.text)
        

    def ingest_collection(self, pid):
        """Takes a Fedora 3.8 PID, retrieves all children and ingests into
        triplestore 

        Args:
            pid: PID of Collection
        """
        sparql = MEMBER_OF_SPARQL.format(pid)
        children_response = requests.post(
            self.ri_search,
            data={"type": "tuples",
                  "lang": "sparql",
                  "format": "json",
                  "query": sparql})
        collection_graph = mods_ingester.new_graph()
        collection_iri = self.__generate_uri__()
        for type_of in [NS_MGR.pcdm.Collection, NS_MGR.bf.Work]:
            collection_graph.add((collection_iri,
				  NS_MGR.rdf.type,
				  type_of))
        local_bnode = rdflib.BNode()
        # Adds PID as a bf:Local identifier for the collection
        collection_graph.add((local_bnode, 
            NS_MGR.rdf.type, 
            NS_MGR.bf.Local))
        collection_graph.add((local_bnode,
            NS_MGR.rdf.value,
            rdflib.Literal(pid)))
        collection_graph.add((local_bnode,
            NS_MGR.rdfs.label,
            rdflib.Literal("Fedora 3.8 PID", lang="en")))
        collection_graph.add((collection_iri,
            NS_MGR.bf.identifiedBy,
            local_bnode))
        collection_label = self.__get_label__(pid)
        collection_graph.add(
            (collection_iri, NS_MGR.rdfs.label, collection_label))
        if children_response.status_code < 400:
            children = children_response.json().get('results')
            for row in children:
                uri = row.get('s')
                child_pid = uri.split("/")[-1]
                child_iri = self.process_pid(child_pid)
                # Adds Child Instance IRI as a part of the Collection
                # Parent
                collection_graph.add(
                    (collection_iri, NS_MGR.bf.hasPart, child_iri))
        collection_result = requests.post(
            self.triplestore_url,
            data=collection_graph.serialize(format='turtle'),
            headers={"Content-Type": "text/turtle"})

    def process_pid(self, pid):
        """Takes a PID, retrieves RELS-EXT and MODS and runs the ingester 
        MODS rules on MODS.

        Args:
            pid: PID of Fedora Object
        Returns:
            rdflib.URIRef of PID Instance
        """ 
               
        mods_url = "{0}{1}/datastreams/MODS/content".format(
            self.rest_url,
            pid)
        mods_result = requests.get(mods_url, auth=self.auth)
        mods_xml = etree.XML(mods_result.text)
        self.transform(mods_xml)
        instance_uri = self.graph.value(
            predicate=NS_MGR.rdf.type,
            object=NS_MGR.bf.Instance)
        # Retrieves RELS-EXT XML
        rels_ext_url = "{0}{1}/datastreams/RELS-EXT/content".format(
            self.rest_url,
            pid)
        rels_ext_result = requests.get(rels_ext_url, auth=self.auth)
        rels_ext = etree.XML(rels_ext_result.text)
        # Matches best BIBFRAME Work Class 
        addl_work_class = self.__guess_work_class__(rels_ext)
        work_bnode = self.graph.value(subject=instance_uri,
            predicate=NS_MGR.bf.instanceOf)
        self.graph.add(
            (work_bnode,
             NS_MGR.rdf.type,
             addl_work_class))
        # Matches best BIBFRAME Instance Class
        addl_instance_class = self.__guess_instance_class__(rels_ext)
        self.graph.add(
            (instance_uri, 
            NS_MGR.rdf.type, 
            addl_instance_class))
        return instance_uri

        


# SPARQL Templates        
MEMBER_OF_SPARQL = """SELECT DISTINCT ?s
WHERE {{
  ?s <fedora-rels-ext:isMemberOfCollection> <info:fedora/{}> .
}}"""

# Command-line handler
@click.command()
@click.option("--root", default="coccc:root", help="CC Root PID")
@click.option("--user", help="Fedora 3.8 User")
@click.option("--pwd", help="Fedora 3.8 Password")
@click.option("--fedora_base", help="Fedora 3.8 Base URL")
@click.option("--triplestore", 
    default="http://localhost:9999/blazegraph/sparql",
    help="Triplestore URL, uses Blazegraph default URL")
def run_ingester(root, user, pwd, fedora_base, triplestore):
    """Function runs ingester from the command line """
    logging.getLogger("requests").setLevel(logging.CRITICAL)
    start = datetime.datetime.utcnow()
    print("Start CC Islandora ingestion into BF 2.0 triplestore at {}".format(
        start.isoformat()))
    ingester = IslandoraIngester(
        repository_url=fedora_base,
        triplestore_url=triplestore,
        user=user,
        password=pwd)
    ingester.ingest_collection(root)
    end = datetime.datetime.utcnow()
    print("Finished CC Islandora ingestion at {}, total time={} minutes".format(
        end.isoformat(),
        (end-start).seconds / 60.0))
        
     
if __name__ == '__main__':
    run_ingester()
