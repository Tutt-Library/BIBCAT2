"""Unit Tests for Islandora Ingester"""
__author__ = "Jeremy Nelson"

import logging
import os
import sys
import unittest
sys.path.append(os.path.abspath(os.path.curdir))
from ingesters.islandora import IslandoraIngester

logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("passlib").setLevel(logging.CRITICAL)

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


if __name__ == '__main__':
    unittest.main()
