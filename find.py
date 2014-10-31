    # This is the bit you want for doing a find, Julius
    dd =  { 'billrec.num.called': 1,
        'billrec.num.calling': 1,
        'udr.start.date': 1,
        'udr.start.time': 1,
        'udr.disc.date': 1,
        'udr.disc.time': 1,
        'udr.ani.nat': 1,
        'udr.lrn.nat': 1,
        'udr.ip.orig': 1,
        'udr.ip.term': 1,
        '_id': 0 }
    #curs = cl.merged_cdrs.merged_cdrs_20140806.find({'udr.ani.nat':"6172792393"},dd.keys())
    curs = c.find( { '$or' : [ {'billrec.num.called':'subscriber'}, {'billrec.num.calling':'subscriber'}]},dd.keys())

    with open('mycsvfile.csv','wb') as f:
        for item in curs:
            w = csv.writer(f)
            w.writerow(item.keys())
            w.writerow(item.values())
            w.writerow(str(item['udr']['ani']['nat']))
