#!/usr/bin/python

from pymongo import *
import pprint
from datetime import *
from math import fabs
from sys import argv
import pickle
import csv

pp = pprint.PrettyPrinter(indent=4)
cl = MongoClient('uccgen03.den02', 27017)

def run_search(month):
    #import pdb; pdb.set_trace()

    #
    # Set the output file name, and write the column headers.
    #
    file = open('julius.csv', 'wb+')
    file.write('udr.pkg' + ',')
    file.write('billrec.cust' + ',')
    file.write('udr.gw' + ',')
    file.write('udr.tg.out' + ',')
    file.write('udr.file' + ',')
    file.write('udr.vend.name' + ',')
    file.write('udr.cust.name' + ',')
    file.write('udr.dial.nat' + '\n')
    file.close()

    for day in month:
        get_day(day)


def get_day(day):
    colname = 'merged_cdrs_' + day.strftime('%Y%m%d')
    #colname = 'merged_cdrs_20140806'
    c = cl.merged_cdrs[colname]


    #
    # This is the data structure that maps to the result of your query.
    #
    dd = { 'udr.pkg':1,
        'billrec.cust':1,
        'udr.gw':1,
        'udr.tg.out':1,
        'udr.file':1,
        'udr.vend.name':1,
        'udr.cust.name':1,
        'udr.dial.nat':1 }

    #
    # This is the Mongo query.
    #
    curs = c.find({'udr.prod.code':"DD", 'udr.pkg':""}, dd.keys())

    #
    # You get a cursor back, and can just iterate through,
    # writing to the csv file.
    #
    file = open('julius.csv', 'ab+')
    for item in curs:
        file.write(str(item['udr']['pkg'])+ ',')
        file.write(str(item['billrec']['cust']) + ',')
        file.write(str(item['udr']['gw']) + ',')
        file.write(str(item['udr']['tg']['out']) + ',')
        file.write(str(item['udr']['file']) + ',')
        file.write(str(item['udr']['vend']['name']) + ',')
        file.write(str(item['udr']['cust']['name']) + ',')
        file.write(str(item['udr']['dial']['nat']) + '\n')

    file.close()


if __name__ == '__main__':
    if len(argv) <= 2:
        print 'Usage: udd.py <start date yyyy-mm-dd> <end date yyyy-mm-dd>'
        exit(0)

    startDate = datetime.strptime(argv[1], '%Y-%m-%d')
    endDate = datetime.strptime(argv[2], '%Y-%m-%d')
    nDays = (endDate - startDate).days + 1
    month = [startDate + timedelta(days=x) for x in xrange(nDays)]

    run_search(month)

