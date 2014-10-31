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

def cdrs(day, invoice, criteria):
    colname = 'merged_cdrs_' + day.strftime('%Y%m%d')
    c = cl.merged_cdrs[colname]

    pipe = [\
        {'$match' : criteria},
        {'$group' : { '_id': 1, 'total' : {'$sum': '$rated.bill.amt'}}},
    ]

    res = c.aggregate(pipe)

    return res['result'][0]['total'] if len(res['result'])>0 else 0

def writeheader(writer):
    writer.writerow(dict((fn, fn) for fn in writer.fieldnames))


def run_search(month, subscriber):
    #import pdb; pdb.set_trace()

    file = open('results.csv', 'wb+')
    file.write('billrec.num.called' + ',')
    file.write('billrec.num.calling' + ',')
    file.write('udr.start.date' + ',')
    file.write('udr.start.time' + ',')
    file.write('udr.disc.date' + ',')
    file.write('udr.disc.time' + ',')
    file.write('udr.ani.nat' + ',')
    file.write('udr.lrn.nat' + ',')
    file.write('udr.ip.orig' + ',')
    file.write('udr.ip.term' + '\n')
    file.close()

    for day in month:
        get_day(day, subscriber)


def get_day(day, subscriber):
    colname = 'merged_cdrs_' + day.strftime('%Y%m%d')
    #colname = 'merged_cdrs_20140806'
    c = cl.merged_cdrs[colname]

    pipe = [\
        {'$match':{ '$or' : [ {'billrec.num.called':subscriber}, {'billrec.num.calling':subscriber}]}},
        {'$group':{ '_id':
                       {'billrec_num_called':'$billrec.num.called',
                       'billrec_num_calling':'$billrec.num.calling',
                       'udr_start_date':'$udr.start.date',
                       'udr_start_time':'$udr.start.time',
                       'udr_disc_date':'$udr.disc.date',
                       'udr_disc_time':'$udr.disc.time',
                       'udr_ani_nat':'$udr.ani.nat',
                       'udr_lrn_nat':'$udr.lrn.nat',
                       'udr_ip_orig':'$udr.ip.orig',
                       'udr_ip_term':'$udr.ip.term'
                       }}},
           ]

    docs = c.aggregate(pipe)

    file = open('results.csv', 'ab+')
    for item in docs['result']:
        file.write(str(item['_id']['billrec_num_called'])+ ',')
        file.write(str(item['_id']['billrec_num_calling']) + ',')
        file.write(str(item['_id']['udr_start_date']) + ',')
        file.write(str(item['_id']['udr_start_time']) + ',')
        file.write(str(item['_id']['udr_disc_date']) + ',')
        file.write(str(item['_id']['udr_disc_time']) + ',')
        file.write(str(item['_id']['udr_ani_nat']) + ',')
        file.write(str(item['_id']['udr_lrn_nat']) + ',')
        file.write(str(item['_id']['udr_ip_orig']) + ',')
        file.write(str(item['_id']['udr_ip_term']) + '\n')

    file.close()


if __name__ == '__main__':
    if len(argv) <= 3:
        print 'Usage: subpoena.py <start date yyyy-mm-dd> <end date yyyy-mm-dd> <subscriber#>'
        exit(0)

    startDate = datetime.strptime(argv[1], '%Y-%m-%d')
    endDate = datetime.strptime(argv[2], '%Y-%m-%d')
    nDays = (endDate - startDate).days + 1
    month = [startDate + timedelta(days=x) for x in xrange(nDays)]
    subscriber = argv[3]

    run_search(month, subscriber)

