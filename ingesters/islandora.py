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

from fuzzywuzzy import fuzz

TIGER_BASE = os.path.abspath(
    os.path.split(
        os.path.dirname(__file__))[0])
sys.path.append(TIGER_BASE)
import bibcat.ingesters.mods as mods_ingester

NS_MGR = mods_ingester.NS_MGR
NS_MGR.bind("pcdm", rdflib.Namespace("http://pcdm.org/models#"))

FEDORA_NS = {
    "fedora": "info:fedora/fedora-system:def/relations-external#",
    "fedora_access": "http://www.fedora.info/definitions/1/0/access/",
    "fedora_model": "info:fedora/fedora-system:def/model#",
    "fedora_manage": "http://www.fedora.info/definitions/1/0/management/",
    "rdf": str(NS_MGR.rdf)
}

class CompoundObjToWork(object):

    def __init__(self, **kwargs):
        self.ingester = kwargs.get("ingester")
        self.instance = kwargs.get("instance")
        self.work_bnode = self.ingester.graph.value(
            subject=self.instance,
            predicate=NS_MGR.bf.instanceOf)
        self.work_iri = None


    def __move_triples__(self):
        for pred, obj in self.ingester.graph.predicate_objects(
            subject=self.work_bnode):
            self.ingester.graph.add((self.work_iri, pred, obj))
            self.ingester.graph.remove((self.instance, pred, obj))
            

    def __work_classes__(self):
        for obj in self.ingester.graph.objects(
            subject=self.work_bnode,
            predicate=NS_MGR.rdf.type):
            if isinstance(obj, rdflib.URIRef):
                self.ingester.graph.add((self.work_iri, NS_MGR.rdf.type, obj))
            self.ingester.graph.remove((self.work_bnode, NS_MGR.rdf.type, obj))
        
    def __work_title__(self):
        instance_title_bnode = self.ingester.graph.value(
            subject=self.instance,
            predicate=NS_MGR.bf.title)
        work_title_bnode = rdflib.BNode()
        self.ingester.graph.add((self.work_iri,
            NS_MGR.bf.title,
            work_title_bnode))
        self.ingester.graph.add((work_title_bnode,
            NS_MGR.rdf.type,
            NS_MGR.bf.WorkTitle))
        for pred, obj in self.ingester.graph.predicate_objects(
            subject=instance_title_bnode):
            if pred != NS_MGR.rdf.type:
                self.ingester.graph.add((work_title_bnode, pred, obj))
            self.ingester.graph.remove((instance_title_bnode, pred, obj))
            

    def run(self):
        self.work_iri = self.ingester.__generate_uri__()
        # Add Work RDF types
        self.__work_classes__()
        # Add Work Title
        self.__work_title__()
        # Move all triples from Instance Work bnode to IRI Work
        self.__move_triples__()
        return self.work_iri
        

        
    
        

class IslandoraIngester(mods_ingester.MODSIngester):

    def __init__(self, **kwargs):
        """Takes kwargs and initialize instance of IslandoraIngester"""
        if not 'rules' in kwargs:
            kwargs['rules'] = ['cc-mods-bf.ttl']
        kwargs["base_url"] = "https://catalog.coloradocollege.edu/"
        self.fedora_base = kwargs.pop("repository_url")
        self.ingested_pids = []
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

    def __add_dataset__(self, dataset_pid, datastream, instance_iri):
        """Method checks a potential datastream, if it size is greater
        than 0, then creates a new Instance with a related Work to the 
        original instance_iri.

        Args:
            dataset_pid(str): PID of potential dataset 
            datastream(etree.Element): Element of potential dataset
            instance_iri(rdflib.URIRef): IRI of BIBFRAME Instance
        """
        ds_profile = self.__get_datastream_profile__(dataset_pid)
        ds_size = ds_profile.find("fedora_manage:dsSize", FEDORA_NS)
        if int(ds_size.text) < 1:
            return
        dataset_uri = self.__generate_uri__()
        self.add_admin_metadata(dataset_uri)
        self.graph.add((dataset_uri, NS_MGR.rdf.type, NS_MGR.bf.Instance))
        self.graph.add((dataset_uri, NS_MGR.rdf.type, NS_MGR.bf.Electronic))
        # Adds filesize
        file_size = rdflib.BNode()
        self.graph.add((file_size, NS_MGR.rdf.type, NS_MGR.bf.FileSize))
        self.graph.add((file_size, NS_MGR.rdf.value, rdflib.Literal(ds_size.text)))
        self.graph.add((dataset_uri, NS_MGR.bf.digitalCharacteristic, file_size))
        # Adds title
        title_node = rdflib.BNode()
        self.graph.add((title_node, NS_MGR.rdf.type, NS_MGR.bf.InstanceTitle))
        self.graph.add((dataset_uri, NS_MGR.bf.title, title_node))
        label = ds_profile.find("fedora_manage:dsLabel", FEDORA_NS)
        if label and len(label.text) > 0:
            title = rdflib.Literal(label.text)
        else:
            title = rdflib.Literal("Dataset", lang="en")
        self.graph.add(
             (title_node,
              NS_MGR.bf.mainTitle,
              title)
        )
        # Adds mime-type 
        mime_type = ds_profile.find("fedora_manage:dsMIME", FEDORA_NS)
        if mime_type is not None:
            self.__add_encoding_format__(dataset_uri,
                mime_type.text)
        # Adds PID as local identifier to the item
        self.graph.add((dataset_uri,
            NS_MGR.bf.identifiedBy,
            self.__add_pid_identifier__(dataset_pid)))
        # Create a bf:Dataset as a Work bnode for dataset
        dataset_work = rdflib.BNode()
        self.graph.add((dataset_work, NS_MGR.rdf.type, NS_MGR.bf.Work))
        self.graph.add((dataset_work, NS_MGR.rdf.type, NS_MGR.bf.Dataset))
        self.graph.add((dataset_uri, NS_MGR.bf.instanceOf, dataset_work))
        # Create Item for this instance
        item_iri = self.__generate_uri__()
        self.add_admin_metadata(item_iri)
        self.graph.add((item_iri, NS_MGR.rdf.type, NS_MGR.bf.Item))
        self.graph.add((item_iri, NS_MGR.bf.itemOf, dataset_uri))
        institution = next(self.rules_graph.objects(predicate=NS_MGR.bf.heldBy))
        self.graph.add((item_iri, NS_MGR.bf.heldBy, institution))
        ds_location = rdflib.URIRef("{}{}/datastreams/FILE/content".format(
            self.rest_url, 
            dataset_pid))
        self.graph.add((item_iri, NS_MGR.bf.electronicLocator, ds_location))  
        # Dataset Accompanies Original Instance IRI
        self.graph.add((dataset_uri, NS_MGR.bf.accompanies, instance_iri))


    def __add_audio__(self, audio_pid):
        """Adds a new related Work, Instance, and Item for an audio datastream

        Args:
            audio_pid(str): PID of Audio Fedora Object
        """
        ds_profile = self.__get_datastream_profile__(audio_pid)
        audio_iri = self.__generate_uri__()
        # Adds instance RDF classes
        for rdf_type in [NS_MGR.bf.Instance, NS_MGR.bf.Electronic]:
            self.graph.add((audio_iri, NS_MGR.rdf.type, rdf_type))
        self.add_admin_metadata(audio_iri)
        label = ds_profile.find("fedora_manage:dsLabel", FEDORA_NS)
        if label and len(label.text) > 0:
            title = rdflib.Literal(label.text)
        else:
            title = rdflib.Literal("Audio File", lang="en")
        self.graph.add(
             (title_node,
              NS_MGR.bf.mainTitle,
              title)
        )
        # Adds mime-type 
        mime_type = ds_profile.find("fedora_manage:dsMIME", FEDORA_NS)
        if mime_type is not None:
            self.__add_encoding_format__(audio_iri,
                mime_type.text)
        # Adds PID as local identifier to the item
        self.graph.add((instance_iri,
            NS_MGR.bf.identifiedBy,
            self.__add_pid_identifier__(audio_pid)))
        item_iri = self.__generate_uri__()
        self.graph.add((item_iri, NS_MGR.rdf.type, NS_MGR.bf.Item))
        self.graph.add((item_iri, NS_MGR.bf.itemOf, audio_iri))
        institution = next(self.rules_graph.objects(predicate=NS_MGR.bf.heldBy))
        self.graph.add((item_iri, NS_MGR.bf.heldBy, institution))
        return audio_iri
        
 

    def __add_encoding_format__(self, instance_iri, mime_type):
        """Method adds mime type as encoding format to instance"""
        encoding_format = rdflib.BNode()
        self.graph.add(
            (encoding_format, NS_MGR.rdf.type, NS_MGR.bf.EncodingFormat))
        self.graph.add(
            (instance_iri, NS_MGR.bf.digitalCharacteristic, encoding_format))
        self.graph.add((
            encoding_format, 
            NS_MGR.rdf.value, 
            rdflib.Literal(mime_type)))     



    def __add_pdf_ds_to_item__(self, pdf_pid, pdf_datastream, instance_iri=None):
        """Method takes an existing PDF Datastream and either adds additional
        BF metadata to the Instance and Item if the Instance's Work is
        a Text or creates a new Instance and Item and creates a relationship
        between the original Instance. 
 
        Args:
            pdf_pid(str): PID of Datastream's Fedora Object 
            pdf_datastream(etree.Element): Element of PDF Datastream
            instance_iri(rdflib.URIRef): IRI of BIBFRAME Instance
        """
        ds_label = pdf_datastream.get("LABEL")
        if instance_iri is not None:
            primary_main_title = self.graph.value(subject=instance_iri,
                predicate=NS_MGR.bf.title)
            main_title = self.graph.value(subject=primary_main_title,
                predicate=NS_MGR.bf.mainTitle)
            # If the datastream title match 98% treat PDF datastream as 
            # primary instance
            if fuzz.ratio(str(main_title), ds_label) < 95:
                # Adds new instance
                original_instance = instance_iri
                instance_iri = self.__generate_uri__()
                self.add_admin_metadata(instance_iri)
                self.graph.add((instance_iri, NS_MGR.rdf.type, NS_MGR.bf.Instance))
                self.graph.add((instance_iri, NS_MGR.bf.supplementTo, original_instance))
                # Use the pdf_datastream label as a Instance Title
                instance_title = rdflib.BNode()
                self.graph.add((instance_iri, NS_MGR.bf.title, instance_title))
                self.graph.add((instance_title, NS_MGR.rdf.type, NS_MGR.bf.InstanceTitle))
                self.graph.add(
                   (instance_title, 
                    NS_MGR.bf.mainTitle, 
                    rdflib.Literal(ds_label)))
                item_iri = self.__generate_uri__()
                self.add_admin_metadata(item_iri)
                self.graph.add((item_iri, NS_MGR.rdf.type, NS_MGR.bf.Item))
                self.graph.add((item_iri, NS_MGR.bf.itemOf, instance_iri))
                institution = next(self.rules_graph.objects(predicate=NS_MGR.bf.heldBy))
                self.graph.add((item_iri, NS_MGR.bf.heldBy, institution))
        else: 
            item_iri = self.graph.value(predicate=NS_MGR.bf.itemOf,
                object=instance_iri)
        # Adds Encoding Format 
        self.__add_encoding_format__(instance_iri, pdf_datastream.get("mimeType"))
        
        # Adds PID as local identifier to the item
        self.graph.add((instance_iri,
            NS_MGR.bf.identifiedBy,
            self.__add_pid_identifier__(pdf_pid)))
            


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
        if len(has_models) < 1:
            raise ValueError("{} missing content model in RELS-EXT".format(
                pid))
        for i, row in enumerate(has_models):
            content_model = row.get(
                "{{{0}}}resource".format(NS_MGR.rdf))
            if content_model.startswith(
                "info:fedora/fedora-system:FedoraObject-3.0"):
                has_models.pop(i)
        if len(has_models) == 1:
            return has_models[0].get(
                "{{{0}}}resource".format(NS_MGR.rdf))

    def __get_datastreams__(self, pid):
        """Method returns XML doc of Datastreams

        Args:
            pid(str): PID of Fedora Object

        Returns:
            etree.Element: XML Document of result listing
        """
        ds_url = "{0}{1}/datastreams?format=xml".format(
            self.rest_url,
            pid)
        datastreams_result = requests.get(
            ds_url, 
            auth=self.auth)
        if datastreams_result.status_code > 399:
             raise ValueError(
                "Cannot retrieve datastream listing for {}".format(pid)) 
        ds_doc = etree.XML(datastreams_result.text)
        return ds_doc

    def __get_datastream_profile__(self, pid):
        """Method returns XML doc of Fedora Object Profile REST call

        Args:
            pid(str): PID of Fedora object
    
        Returns:
            etree.Element - Root element of Object Profile XML 
        """
        ds_profile_url = "{}{}/datastreams/FILE?format=xml".format(
            self.rest_url,
            pid)
        ds_profile_result = requests.get(ds_profile_url, auth=self.auth)
        if ds_profile_result.status_code > 399:
            raise ValueError("Failed to retrieve {} Datastream Profile".format(
                pid))
        return etree.XML(ds_profile_result.text)
   

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

    def __get_instance_iri__(self, pid):
        """Attempts to retrieve instance IRI based on the PID

        Args:
            pid: Fedora Object PID

        Returns:
            rdflib.URIRef: IRI of Instance or None if PID not found
        """
        sparql = NS_MGR.prefix() + """
        SELECT ?instance
        WHERE {{
            ?instance bf:identifiedBy ?pid .
            ?pid a bf:Local .
            ?pid rdf:value "{0}" .
        }}""".format(pid)
        instance_result = requests.post(
            self.triplestore_url,
            data={"query": sparql,
                  "format": "json"})
        if instance_result.status_code > 399:
            raise ValueError(
                "Could not retrieve instance IRI w/PID {}".format(pid))
        bindings = instance_result.json().get('results').get('bindings')
        if len(bindings) < 0:
            return
        elif len(bindings) == 1:
            return rdflib.URIRef(bindings[0]['instance']['value'])

    def __guess_instance_class__(self, work_classes):
        """Attempts to guess additional instanc classes for the Fedora Object

        Args:
            work_classes(list): List of BF Work Classes
        """
        instance_classes = []
        return instance_classes
       
    def __guess_work_class__(self, instance_iri, content_model):
        """Attempts to guess additional work classes for the Fedora Object
        
        Args:
            content_model(str): Islandora Content Model
        Returns:
            List of BF Classes
        """
        bf_work_classes = []
        if content_model.startswith("info:fedora/islandora:sp_pdf"):
            bf_work_classes.append(NS_MGR.bf.Text)
        genre_query = NS_MGR.prefix() +"""
        SELECT ?value
        WHERE {{
            <{0}> bf:genreForm ?genre .
            ?genre rdf:value ?value .
        }}""".format(instance_iri)
        for row in self.graph.query(genre_query):
            genre = str(row[0])
            #! Move to RDF rule?
            if genre.startswith("interview"):
                bf_work_classes.append(NS_MGR.bf.Audio)
        return bf_work_classes 

    def __mods_to_bibframe__(self, pid):
        """Downloads MODS based on the PID and runs the transformation 
        to BIBFRAME.

        Args:
            pid: PID of Fedora OBject

        Returns:
            rdflib.URIRef: URI of Instance
        """
        mods_url = "{0}{1}/datastreams/MODS/content".format(
                    self.rest_url,
                    pid)
        mods_result = requests.get(mods_url, auth=self.auth)
        if mods_result.status_code == 404:
            # Tries to retrieve first and use MODS Datastreams of 
            # any related Fedora Objects
            sparql = """SELECT DISTINCT ?s
WHERE {{
   ?s <fedora-rels-ext:isConstituentOf> <info:fedora/{0}> .
}}""".format(pid)
            constituents_response = requests.post(
                self.ri_search,
                data={"type": "tuples",
                      "lang": "sparql",
                      "format": "json",
                      "query": sparql},
                auth=self.auth)
            constituents = constituents_response.json().get('results')
            for row in constituents:
                child_pid = row.get('s').split("/")[-1]
                child_ds_doc = self.__get_datastreams__(child_pid)
                mods_ds = child_ds_doc.find(
                    """fedora_access:datastream[@disd="MODS"]""")
                if mods_ds is not None:
                    return self.__mods_to_bibframe__(child_pid)
            # Failed to find any mods
            return
        else:
            mods_result.encoding = 'utf-8'
            mods_xml = etree.XML(mods_result.text)
        self.transform(mods_xml)
        instance_iri = self.graph.value(
            predicate=NS_MGR.rdf.type,
            object=NS_MGR.bf.Instance)
        if instance_iri is None:
            raise ValueError("Unable to extract Instance from Graph")
        return instance_iri
            
                

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
                # Adds Child Instance IRI as a part of the Collection
                child_iri = self.process_pid(child_pid)
                if not child_iri:
                    continue
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

    def ingest_compound(self, pid):
        """Handles Complex Compound Objects by creating a BF Work for the
        compound object, iterates through datastreams and creates related
        Instances from the compound's constituents Fedora Objects.

        Args:
            pid(str): PID of Compound Object
        """
        sparql = """SELECT DISTINCT ?s
WHERE {{
   ?s <fedora-rels-ext:isConstituentOf> <info:fedora/{0}> .
}}""".format(pid)
        constituents_response = requests.post(
            self.ri_search,
            data={"type": "tuples",
                  "lang": "sparql",
                  "format": "json",
                  "query": sparql},
            auth=self.auth)
        if constituents_response.status_code > 399:
            raise ValueError("Ingesting Compound {} Failed".format(
                pid))
        primary_instance = self.__mods_to_bibframe__(pid)
        work_generator = CompoundObjToWork(ingester=self,
            instance=primary_instance)
        work_iri = work_generator.run()
        self.graph.add((work_iri,
            NS_MGR.bf.identifiedBy,
            self.__add_pid_identifier__(pid)))
        constituents = constituents_response.json().get('results')
        for row in constituents:
            child_pid = row.get('s').split("/")[-1]
            child_ds_doc = self.__get_datastreams__(child_pid)
            pdf_datastream = child_ds_doc.find(
                "fedora_access:datastream[@mimeType='application/pdf']",
                FEDORA_NS)
            if pdf_datastream is not None:
                self.__add_pdf_ds_to_item__(child_pid, 
                    pdf_datastream,
                    primary_instance)
            dataset_datastreams = child_ds_doc.findall(
                "fedora_access:datastream[@dsid='FILE']",
                FEDORA_NS)
            for row in dataset_datastreams:
                self.__add_dataset__(child_pid,
                    row,
                    primary_instance)
            wav_datastreams = child_ds_doc.findall(
                "fedora_access:datastream[@mimeType='audio/vnd.wave']",
                FEDORA_NS)
            for row in wav_datastreams:
                self.__add_audio__(child_pid)
            mp3_datastreams = child_ds_doc.findall(
                "fedora_access:datastream[@mimeType='audio/mpeg']",
                FEDORA_NS)
            for row in mp3_datastreams:
                self.__add_audio__(child_pid)

 


            self.ingested_pids.append(child_pid)
        self.add_to_triplestore()
        return primary_instance
      

    def process_pid(self, pid):
        """Takes a PID, retrieves RELS-EXT and MODS and runs the ingester 
        MODS rules on MODS.

        Args:
            pid: PID of Fedora Object
        Returns:
            rdflib.URIRef of PID Instance
        """ 
        pid_exists_sparql = PID_EXISTS.format(pid)
        pid_exists = requests.post(self.triplestore_url,
            data={"query": pid_exists_sparql,
                  "format": "json"})
        if pid_exists.status_code < 399:
            bindings = pid_exists.json().get("results").get("bindings")
            if len(bindings) > 0:
                return self.__get_instance_iri__(
                    bindings[0].get('subject').get('value'))
        # Retrieves RELS-EXT XML
        rels_ext_url = "{0}{1}/datastreams/RELS-EXT/content".format(
            self.rest_url,
            pid)
        rels_ext_result = requests.get(rels_ext_url, auth=self.auth)
        rels_ext = etree.XML(rels_ext_result.text)
        # Skips any PIDS that are constituents of another object
        if rels_ext.find("rdf:Description/fedora:isConstituentOf", 
            FEDORA_NS) is not None:
            return
        content_model = self.__get_content_model__(rels_ext)
        # Builds supporting Instances and Works if a compound object
        if content_model.startswith("info:fedora/islandora:compoundCModel"):
            return self.ingest_compound(pid)
        # If a collection model, returns result of calling ingest_collection
        if content_model.startswith('info:fedora/islandora:collectionCModel'):
            return self.ingest_collection(pid)
        # Retrieves MODS for Fedora Object and performs MODS to BIBFRAME
        # transformation
        instance_iri = self.__mods_to_bibframe__(pid)
        if not instance_iri:
            return
        # Adds PID as Local Identifier
        local_bnode = self.__add_pid_identifier__(pid)
        self.graph.add((instance_iri, NS_MGR.bf.identifiedBy, local_bnode)) 
        # Matches best BIBFRAME Work Class 
        addl_work_classes = self.__guess_work_class__(instance_iri, content_model)
        work_bnode = self.graph.value(subject=instance_iri,
            predicate=NS_MGR.bf.instanceOf)
        if work_bnode is None:
            work_bnode = rdflib.BNode()
            self.graph.add((instance_iri, NS_MGR.bf.instanceOf, work_bnode))
        for work_class in addl_work_classes:
            self.graph.add(
                (work_bnode,
                 NS_MGR.rdf.type,
                 work_class))
        # Matches best BIBFRAME Instance Class
        addl_instance_classes = self.__guess_instance_class__(rels_ext)
        for instance_class in addl_instance_classes:
            self.graph.add(
                (instance_iri, 
                 NS_MGR.rdf.type, 
                 addl_instance_class))
        self.add_to_triplestore()
        return instance_iri

        


# SPARQL Templates        
MEMBER_OF_SPARQL = """SELECT DISTINCT ?s
WHERE {{
  ?s <fedora-rels-ext:isMemberOfCollection> <info:fedora/{}> .
}}"""

PID_EXISTS = NS_MGR.prefix() + """
SELECT DISTINCT ?subject
WHERE {{
    ?subject bf:identifiedBy ?pid .
    ?pid rdf:value "{0}" .
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
