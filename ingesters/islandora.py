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

FEDORA_NS = {
    "fedora_access": "http://www.fedora.info/definitions/1/0/access/",
    "fedora_model": "info:fedora/fedora-system:def/model#",
    "rdf": str(NS_MGR.rdf)
}
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

    def __add_pid_identifier__(self, pid, graph=None):
        """Adds PID as a Blank BF Identifier

        Args:
            pid(str): PID of Object
            graph(rdflib.Graph): Graph to add PID, default is 
                working ingester graph
        """
        if graph is None:
            graph = self.graph
        local_bnode = rdflib.BNode()
        graph.add((local_bnode, 
            NS_MGR.rdf.type, 
            NS_MGR.bf.Local))
        graph.add((local_bnode,
            NS_MGR.rdf.value,
            rdflib.Literal(pid)))
        graph.add((local_bnode,
            NS_MGR.rdfs.label,
            rdflib.Literal("Fedora 3.8 PID", lang="en")))
        return local_bnode

    def __get_content_model__(self, rels_ext):
        """Retrieves content models and filters out and
        returns.

        Args:
            rels_ext(etree.XML): XML doc of RELS-EXT
        """
        # Variable processing depending on content model
        has_models = rels_ext.findall(
            "rdf:Description/fedora_model:hasModel", 
            FEDORA_NS)
        if len(has_model) < 1:
            raise ValueError("{} missing content model in RELS-EXT".format(
                pid))
        has_models.pop("info:fedora/fedora-system:FedoraObject-3.0")
        if len(has_models) == 1:
            content_model = has_models[0].get(
                "{{{0}}}resource".format(NS_MGR.rdf))
            return content_model

    def __get_label__(self, pid):
        """Retrieves label for a PID using the REST API"""
        object_url = "{}{}?format=xml".format(self.rest_url, pid)
        result = requests.get(object_url,
            auth=self.auth)
        if result.status_code < 400:
            object_profile = etree.XML(result.text)
            label = object_profile.find(
                "fedora_access:objLabel", 
                FEDORA_NS)
            if label is not None:
                return rdflib.Literal(label.text)

    def __guess_instance_class__(self, work_classes):
        """Attempts to guess additional instanc classes for the Fedora Object

        Args:
            work_classes(list): List of BF Work Classes
        """
        instance_classes = []
        return instance_classes
       
    def __guess_work_class__(self, content_model, mods_xml):
        """Attempts to guess additional work classes for the Fedora Object
        
        Args:
            content_model(str): Islandora Content Model
            mods_xml(etree.XML): MODS XML
        Returns:
            List of BF Classes
        """
        bf_work_classes = []
        if content_model.startswith("info:fedora/islandora:sp_pdf"):
            bf_work_classes.append(NS_MGR.bf.Text)
        return bf_work_classes 

    def ingest_collection(self, pid):
        """Takes a Fedora 3.8 PID, retrieves all children and ingests into
        triplestore 

        Args:
            pid: PID of Collection
        """
        collection_graph = mods_ingester.new_graph()
        collection_iri = self.__generate_uri__()
        for type_of in [NS_MGR.pcdm.Collection, NS_MGR.bf.Work]:
            collection_graph.add((collection_iri,
				  NS_MGR.rdf.type,
				  type_of))
        # Adds PID as a bf:Local identifier for the collection
        local_bnode = self.__add_pid_identifier__(pid, collection_graph)
        collection_graph.add((collection_iri,
            NS_MGR.bf.identifiedBy,
            local_bnode))
        collection_label = self.__get_label__(pid)
        collection_graph.add(
            (collection_iri, NS_MGR.rdfs.label, collection_label))
        sparql = MEMBER_OF_SPARQL.format(pid)
        children_response = requests.post(
            self.ri_search,
            data={"type": "tuples",
                  "lang": "sparql",
                  "format": "json",
                  "query": sparql},
            auth=self.auth)
        if children_response.status_code < 400:
            children = children_response.json().get('results')
            for i,row in enumerate(children):
                uri = row.get('s')
                child_pid = uri.split("/")[-1]
                child_iri = self.process_pid(child_pid)
                # Adds Child Instance IRI as a part of the Collection
                # Parent
                collection_graph.add(
                    (collection_iri, NS_MGR.bf.hasPart, child_iri))
                if not i%10:
                    print(".", end="")
        collection_result = requests.post(
            self.triplestore_url,
            data=collection_graph.serialize(format='turtle'),
            headers={"Content-Type": "text/turtle"})
        return collection_iri

    def ingest_compound(self, pid, content_models):
        """Handles Complex Compound Objects

        Args:
            instance_uri(rdflib.URIRef): IRI of base instance
        """
      

    def process_pid(self, pid):
        """Takes a PID, retrieves RELS-EXT and MODS and runs the ingester 
        MODS rules on MODS.

        Args:
            pid: PID of Fedora Object
        Returns:
            rdflib.URIRef of PID Instance
        """ 
        # Retrieves RELS-EXT XML
        rels_ext_url = "{0}{1}/datastreams/RELS-EXT/content".format(
            self.rest_url,
            pid)
        rels_ext_result = requests.get(rels_ext_url, auth=self.auth)
        rels_ext = etree.XML(rels_ext_result.text)
        content_model = self.__get_content_model__(rels_ext)
        # If a collection model, returns result of calling ingest_collection
        if content_model.startswith('info:fedora/islandora:collectionCModel'):
            return self.ingest_collection(pid)
        mods_url = "{0}{1}/datastreams/MODS/content".format(
                    self.rest_url,
                    pid)
        mods_result = requests.get(mods_url, auth=self.auth)
        mods_xml = etree.XML(mods_result.text)
        self.transform(mods_xml)
        instance_uri = self.graph.value(
            predicate=NS_MGR.rdf.type,
            object=NS_MGR.bf.Instance)
        # Adds PID as Local Identifier
        local_bnode = self.__add_pid_identifier__(pid)
        self.graph.add((instance_uri, NS_MGR.bf.identifiedBy, local_bnode)) 
        # Matches best BIBFRAME Work Class 
        add_work_classes = self.__guess_work_class__(content_model, mods_xml)
        work_bnode = self.graph.value(subject=instance_uri,
            predicate=NS_MGR.bf.instanceOf)
        for work_class in addl_work_classes:
            self.graph.add(
                (work_bnode,
                 NS_MGR.rdf.type,
                 work_class))
        # Matches best BIBFRAME Instance Class
        addl_instance_classes = self.__guess_instance_class__(rels_ext)
        for instance_class in addl_instance_classes:
            self.graph.add(
                (instance_uri, 
                 NS_MGR.rdf.type, 
                 addl_instance_class))
        # Builds supporting Instances and Works if a compound object
        if content_model.startswith("info:fedora/islandora:compoundCModel"):
            return self.ingest_compound(instance_uri)
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
