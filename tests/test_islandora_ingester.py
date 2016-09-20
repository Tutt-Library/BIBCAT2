"""Unit Tests for Islandora Ingester"""
__author__ = "Jeremy Nelson"

import logging
import os
import sys
import unittest
sys.path.append(os.path.abspath(os.path.curdir))
from ingesters.islandora import CompoundObjToWork, IslandoraIngester

logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("passlib").setLevel(logging.CRITICAL)

class TestCompoundObjToWork(unittest.TestCase):

    def setUp(self):
        self.ingester = IslandoraIngester(
            repository_url="http://localhost:8080/fedora",
            user="FedoraAdmin",
            password="FedoraAdmin")
        self.generator = CompoundObjToWork(ingester=self.ingester)

    def test_init(self):
        self.assertIsInstance(self.generator, CompoundObjToWork)
        self.assertEqual(self.ingester, self.generator.ingester)

    def test__move_triples__(self):
        pass

    def test__work_classes__(self):
        pass

    def test__work_title__(self):
        pass

    def test_run(self):
        pass

class TestIslandoraIngesterDefault(unittest.TestCase):

    def setUp(self):
        self.ingester = IslandoraIngester(
            repository_url="http://localhost:8080/fedora",
            user="FedoraAdmin",
            password="FedoraAdmin")

    def test_init(self):
        self.assertIsInstance(self.ingester, IslandoraIngester)

    def tearDown(self):
        pass


class TestIngestCompound(unittest.TestCase):

    def setUp(self):
        self.ingester = IslandoraIngester(
            repository_url="http://localhost:8080/fedora",
            user="FedoraAdmin",
            password="FedoraAdmin")

    def test_ingest_compound_exceptions(self):
        self.assertTrue(hasattr(self.ingester, "ingest_compound"))

class TestMODStoBIBFRAMEMethod(unittest.TestCase):

    def setUp(self):
        self.ingester = IslandoraIngester(
            repository_url="http://localhost:8080/fedora",
            user="FedoraAdmin",
            password="FedoraAdmin")

    def test_mods_to_bibframe_exists(self):
        self.assertTrue(hasattr(self.ingester, "__mods_to_bibframe__"))

    def test_mods_to_bibframe_missing_mods_ds(self):
        # Should replicate issue with coccc:26117
        pass

class Test__add_pdf_ds_to_item__(unittest.TestCase):

    def setUp(self):
        self.ingester = IslandoraIngester(
            repository_url="http://localhost:8080/fedora",
            user="FedoraAdmin",
            password="FedoraAdmin")

    def test__add_pdf_ds_to__item_exists(self):
         self.assertTrue(
            hasattr(self.ingester,
                    "__add_pdf_ds_to_item__"))

    

if __name__ == '__main__':
    unittest.main()
