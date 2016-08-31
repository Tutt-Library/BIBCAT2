"""Custom Islandora Repository Ingester for Colorado College, extends 
BIBCAT MODS Ingester."""
__author__ = "Jeremy Nelson"

import datetime
import rdflib
import xml.etree.ElementTree as etree
import bibcat.ingesters.mods as mods_ingester

NS_MGR = mods_ingester.NS_MGR

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
        collection_iri = self.__generate_iri__()
        for type_of in [NS_MGR.pcdm.Collection, NS_MGR.bf.Work]:
            collection_graph.add((collection_uri,
				  NS_MGR.rdf.type,
				  type_of))
        collection_label = self.__get_collection_label__(pid)
        if children_response.status_code < 400:
            children = children_response.json().get('results')
            for row in children:
                uri = row.get('s')
                child_pid = uri.split("/")[-1]
                child_iri = self.process_pid(child_pid 
        

# SPARQL Templates        
MEMBER_OF_SPARQL = """SELECT DISTINCT ?s
WHERE {{
  ?s <fedora-rels-ext:isMemberOfCollection> <info:fedora/{}> .
}}"""
