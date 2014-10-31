#!/usr/bin/python

from pymongo import *
import pprint
from datetime import *
from math import fabs
from sys import argv

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


def hasUdr(day, invoice):
    criteria = {'rated.bill.inv': invoice, 'has.u' : {'$gt': 0}}
    return cdrs(day,invoice,criteria)

def hasBillrec(day, invoice):
    criteria = {'rated.bill.inv': invoice, 'has.b' : {'$gt': 0}}
    return cdrs(day,invoice,criteria)

def full_cdrs(day, invoice):
    criteria = {'has.b':{'$gt':0},
                'has.u':{'$gt':0},
                'has.r':{'$gt':0},
                'rated.bill.inv': invoice}
    return cdrs(day,invoice,criteria)


def summary(day, invoice):
    c = cl.summaries.ezcom_invoices 
    criteria = {'invoice': invoice}
    res = c.find_one(criteria)
    dayStr = day.strftime('%Y-%m-%d')
    return res['dates'][dayStr]['revenue'] if res is not None and dayStr in res['dates'] else 0

def run_invoice(month, invoice):
    columns = [summary, hasUdr, hasBillrec, full_cdrs]
    def valueStr(values):
        def eq(a,b):
            try:
                return fabs(a-b) < 0.1
            except:
                return a!=b

        res = ''
        prev = None
        for i in columns:
            val = values[i]
            res += ' %s %10s' % ('<>' if prev and not eq(val,prev) else '  ', val)
            prev = val
        return res

    columnNames = dict((x,x.__name__) for x in columns)
    totals = dict((x,0) for x in columns)

    print 'From %s to %s:' % (month[0], month[-1])
    print '%10s' % 'Day', valueStr(columnNames) 

    for day in month:
        amounts = dict((x,x(day,invoice)) for x in columns)
        for i in totals:
            totals[i] += amounts[i]
        print day.strftime('%Y-%m-%d'), valueStr(amounts)

    print 'Totals:', valueStr(totals)




if __name__ == '__main__':
    if len(argv) <= 3:
        print 'Usage: summary-discrepancy.py <start date yyyy-mm-dd> <end date yyyy-mm-dd> <invoice#>'
        exit(0)

    startDate = datetime.strptime(argv[1], '%Y-%m-%d')
    endDate = datetime.strptime(argv[2], '%Y-%m-%d')
    nDays = (endDate - startDate).days + 1
    month = [startDate + timedelta(days=x) for x in xrange(nDays)]
    invoice = int(argv[3])

    run_invoice(month, invoice)

