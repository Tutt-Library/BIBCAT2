__author__ = "Jeremy Nelson"

import datetime
import os
import sqlite3
import sys

import click
import lxml.etree
import pymarc
import requests

DB_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "data/usage.db"))

ITEM_TEMPLATE = "http://tiger.coloradocollege.edu/xrecord={}"

MARC5_WRITER = pymarc.MARCWriter(open(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "data/marc_5_usage.mrc")),
    "wb+"))

MARC10_WRITER = pymarc.MARCWriter(open(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "data/marc_10_usage.mrc")), 
    "wb+"))

def __get_item_id__(item_url, connection):
    cursor = connection.cursor() 
    cursor.execute("SELECT id FROM iiiItems WHERE item_url=?",
        (item_url,))
    item_result = cursor.fetchone()
    if item_result:
        return item_result[0]
    cursor.execute("INSERT INTO iiiItems (item_url) VALUES (?)",
        (item_url,))
    connection.commit()
    cursor.execute("SELECT max(id) FROM iiiItems")
    item_result = cursor.fetchone()
    cursor.close()
    return item_result[0]

def __get_usage__(rec, connection):
    if not '945' in rec:
        return
    field945_y = rec['945']['y']
    if field945_y is None:
        return
    item_url = ITEM_TEMPLATE.format(field945_y[1:-1])
    item_result = requests.get(item_url)
    if item_result.status_code == 404:
        return
    item_xml = lxml.etree.XML(item_result.text.encode())
    sql = """INSERT INTO log (item, last_check_out, total_checkouts) 
VALUES (?,?,?)"""
    item_id = __get_item_id__(item_url, connection)
    total_checkouts_list = item_xml.xpath(
        "TYPEINFO/ITEM/FIXFLD[FIXLABEL/text()='TOT CHKOUT']/FIXVALUE")
    total_checkouts = 0
    if len(total_checkouts_list) > 0:
        total_checkouts = int(total_checkouts_list[0].text)
        rec.force_utf8 = True
        if total_checkouts > 10:
            MARC10_WRITER.write(rec)
        elif total_checkouts > 5:
            MARC5_WRITER.write(rec)
    last_checkin_list = item_xml.xpath(
        "TYPEINFO/ITEM/FIXFLD[FIXLABEL/text()='LCHKIN']/FIXVALUE")
    last_checkin = None
    if len(last_checkin_list) > 0:
        for date_pattern in ["%d-%m-%y", "%d-%m-%Y"]:
            try:
                last_checkin = datetime.datetime.strptime(
                    last_checkin_list[0].text,
                    date_pattern)
            except ValueError:
                continue
    cursor = connection.cursor()
    cursor.execute(sql, (item_id, last_checkin, total_checkouts))
    connection.commit()
    cursor.close()
    
    
    

@click.command()
@click.option("--marc", help='MARC21 full file path')
def harvest(marc):
    marc_reader = pymarc.MARCReader(open(marc, 'rb'),
        to_unicode=True,
        utf8_handling='ignore')
    start = datetime.datetime.utcnow()
    click.echo("Started usage harvesting for all III Item records in {} at {}".format(
        start,
        marc))
    connection = sqlite3.connect(DB_PATH)
    for i,rec in enumerate(marc_reader):
        __get_usage__(rec, connection)
        if not i%100 and i > 0:
            click.echo(".", nl=False)
        if not i%1000:
            click.echo("{:,}".format(i), nl=False)
    end = datetime.datetime.utcnow()
    click.echo("Finished harvesting usage for {:,} items at {}, total time {:,} minutes".format(i, end, (end-start).seconds / 60.0)) 

if __name__ == '__main__':
    harvest()
