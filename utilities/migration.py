"""Migration Module for Transforming Colorado College's MARC21 Records to 
RDF Linked Data using BIBFRAME, Schema.org, Bibliotex, and other ontologies."""
__author__ = "Jeremy Nelson"

import datetime
import logging
import os
import pickle

from multiprocessing import Pool
import click
import bibcat
import pymarc
import rdflib

from zipfile import ZipFile, ZIP_DEFLATED
import lxml.etree

logging.getLogger('rdflib').setLevel(logging.CRITICAL)

PROJECT_BASE = os.path.abspath(
    os.path.dirname(
        os.path.dirname(__file__)))

print(PROJECT_BASE)

MARC2BF, ERRORS = None, []

def __rec2ld__(rec):
    marc_xml = pymarc.record_to_xml(rec, namespace=True)
    bf_rdf_xml = MARC2BF(lxml.etree.XML(marc_xml),
        baseuri='"http://catalog.coloradocollege.edu/"')
    bf_rdf = rdflib.Graph()
    bf_rdf.parse(data=lxml.etree.tostring(bf_rdf_xml),
        format='xml')
    bibcat.clean_uris(bf_rdf)
    try:
        raw_turtle = bf_rdf.serialize(format='turtle')
    except:
        raw_turtle = None
    return raw_turtle
        

@click.command()
@click.option("--marc_file", help="Full path to MARC21 file")
@click.option("--mrc2bf", help="Full path to LOC marc2bibframe2.xsl")
def generate_ld(marc_file, mrc2bf):
    """Function takes a MARC21 files, threading each transformation
    of MARC XML to RDF BIBFRAME XML.

    Args:
        marc_file(str): Full path to MARC 21 file"""
    global MARC2BF
    marc_reader = pymarc.MARCReader(open(marc_file, 'rb'),
        to_unicode=True, force_utf8=True, utf8_handling='ignore')
    count, recs = 0, []
    MARC2BF = lxml.etree.XSLT(
        lxml.etree.parse(mrc2bf))
    pool = Pool(processes=3)
    start = datetime.datetime.utcnow()
    click.echo("Started at {}".format(start))
    loc_zip = ZipFile(os.path.join(PROJECT_BASE,
                                   "KnowledgeGraph/loc-bf-{:03}.zip".format(
                                        start.toordinal())),
                      mode="w",
                      compression=ZIP_DEFLATED,
                      allowZip64=True)
    shard = 1
    while 1:
        try:
            rec = next(marc_reader) 
            #result = pool.apply_async(
            #    __rec2ld__,
            #    (rec,))
            raw_turtle = __rec2ld__(rec)
            if raw_turtle is None:
                ERRORS.append(count)
                continue
            loc_zip.writestr(
                "/resources/shard-{:004}/loc-bf2-{:08}.ttl".format(
                    shard,
                    count),
                raw_turtle)
            if not count%10000:
                click.echo("{:,}".format(count), nl=False)
                shard += 1
            if not count%1000:
                click.echo(".", nl=False)
            count += 1
        except StopIteration:
            break
        except:
            ERRORS.append(count)
            count += 1
            
    loc_zip.close()
    with open(os.path.join(
        PROJECT_BASE,
        "KnowledgeGraph/errors-{}.pickle".format(start.toordinal())),
        "wb+") as fo:
        pickle.dump(ERRORS, fo, pickle.HIGHEST_PROTOCOL)
    end = datetime.datetime.utcnow()
    click.echo("""Finished at {}, total time {:,} 
minutes for {:,} records, number of errors {:,}""".format(end, 
                                   (end-start).seconds / 60.0,
                                   count,
                                   len(ERRORS)))

if __name__ == "__main__":
    generate_ld()
