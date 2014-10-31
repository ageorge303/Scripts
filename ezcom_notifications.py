
# EZ-COM notifications are CSV files delivered via FTP
# (currently to ossbilling1.dal01:/home/ezcom/notifications)

import os, sys, re, csv

from voex.native import ServiceError

from cdrPublishingService.errors import *


class EzcomNotification(object):
    def __init__(self):
        self.path = None
        self._rows = []


    def add_row(self,row):
        self._rows.append(row)

        
    def rows(self):
        #for i in self._rows:
        #    yield i
        return self._rows
            

    def count(self):
        return len(self._rows)
    
    
        
class InvoiceDetailReport(EzcomNotification):
    """ Class for parsing IDR files from EZ-COM """

    FIELDNAMES_V1 = ['Aggregator ID',
                     'Customer Number',
                     'Customer Name',
                     'SAP ID',
                     'Invoice #',
                     'Total Call Amount',
                     'Invoice Amount',
                     'Bill Frequency',
                     'Last Invoice Date',
                     'R8 Call Amount',
                     'IP Call Amount',
                     'IL Call Amount',
                     '2P Call Amount',
                     'MRC',
                     'Taxes',
                     'Total Invoice Minutes']

    # Newer format (which should include headers by default) introduced
    # additional call types:
    FIELDNAMES = ['Aggregator',
                  'Account#',
                  'Name',
                  'SAP ID',
                  'Invoice#',
                  'Call Amount',
                  'Invoice$',
                  'Bill Freq',
                  'Inv Date',
                  'R8 Call Amt',
                  'IP Call Amt',
                  'IL Call Amt',
                  '2P Call Amt',
                  'R9 Call Amt',
                  '3P Call Amt',
                  'FF Call Amt',
                  'Charges',
                  'Taxes',
                  'Minutes']
    
    def __init__(self):
        super(InvoiceDetailReport,self).__init__()
        self.batch_name = None
        self.is_v1 = False

        self.index_account_nr = {}
        self.index_sap_id = {}
        self.index_name = {}
        self.index_invoice_nr = {}
        
    def parse_file(self,path):
        self.path = path
        fh = open(path,'rU')
        self.parse(fh)

        self.set_batch_name_from_path(path)

    
    def parse(self,fh):
        # Determine the CSV dialect, and whether headers are included
        # The CSV from EzCom doesn't currently contain field names, but it may
        # in the future
        sample = fh.read(5000)
        fh.seek(0)
        
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)

        fieldnames = None
        #if not sniffer.has_header(sample): # sniffer header detection not accurate...
        if not 'Aggregator' in sample:
            # If the IDR doesn't have a header row, it is most likely a V1 IDR:
            fieldnames = self.FIELDNAMES_V1
            self.is_v1 = True

        reader = csv.DictReader(fh, dialect=dialect, fieldnames=fieldnames)
        for row in reader:
            for k in row.keys():
                row[k] = row[k].strip()
            self.add_row(row)

        if self.is_v1:
            self.fix_v1_to_current()

        self.index()
        

    def fix_v1_to_current(self):
        """ Translate column names from V1 format to latest format, and add missing columns """
        for row in self.rows():
            row['Aggregator'] = row['Aggregator ID']
            row['Account#'] = row['Customer Number']
            row['Name'] = row['Customer Name']
            #row['SAP ID'] = row['SAP ID']
            row['Invoice#'] = row['Invoice #']
            row['Call Amount'] = row['Total Call Amount']
            row['Invoice$'] = row['Invoice Amount']
            row['Bill Freq'] = row['Bill Frequency']
            row['Inv Date'] = row['Last Invoice Date']
            row['R8 Call Amt'] = row['R8 Call Amount']
            row['IP Call Amt'] = row['IP Call Amount']
            row['IL Call Amt'] = row['IL Call Amount']
            row['2P Call Amt'] = row['2P Call Amount']
            row['R9 Call Amt'] = '0.0'
            row['3P Call Amt'] = '0.0'
            row['FF Call Amt'] = '0.0'
            row['Charges'] = row['MRC']
            #row['Taxes'] = row['Taxes']
            row['Minutes'] = row['Total Invoice Minutes']
            
            

    def set_batch_name(self,batch_name):
        self.batch_name = batch_name

        # Also add to any of the individual rows that have been loaded:
        for inv in self.rows():
            inv['Batch Name'] = batch_name


    def set_batch_name_from_path(self,path):
        batch_name, ext = os.path.splitext(os.path.basename(path))
        self.set_batch_name(batch_name)


    def index(self):
        """ Populate the indices """
        for inv in self.rows():
            self.index_account_nr[inv['Account#']] = inv
            self.index_sap_id[inv['SAP ID']] = inv
            self.index_name[inv['Name']] = inv
            self.index_invoice_nr[inv['Invoice#']] = inv


    def find_by_name(self,name):
        try: return self.index_name[name]
        except KeyError: return None

    def find_by_account_nr(self,account):
        try: return self.index_account_nr[account]
        except KeyError: return None

        
    class Supplemental(EzcomNotification):
        """ Supplemental invoice detail report (SDR) that goes along with an IDR.
            Each row of the SDR gives total calls, mou, and charges per call-type / sub-call-type.
            The IDR gives just the total amount per Call-type, so this is the detail underneath
            that """
        FIELDNAMES = ['Aggregator',
                      'Account#',
                      'Name',
                      'SAP ID',
                      'Invoice#',
                      'Call Type',
                      'Sub-Call Type',
                      'Total Calls',
                      'Total Minutes',
                      'Total Amount']

        def __init__(self,idr=None):
            super(InvoiceDetailReport.Supplemental,self).__init__()
            self.idr = idr

        def parse_file(self,path=None):
            if not path:
                path = self.determine_path_from_idr()
            self.path = path
            if os.path.exists(self.path):
                fh = open(path,'rU')
                self.parse(fh)
            elif self.idr.is_v1:
                # No SDR for old IDR formats
                pass
            else:
                raise ServiceError(Errors.MissingSDR,self.path)


        def determine_path_from_idr(self):
            """ Determine the filename to load given the IDR this SDR is tied to.
                For an IDR name 'IDR_xxx', the SDR will be 'SDR_xxx' """
            if self.idr and self.idr.path:
                path = self.idr.path
                dir = os.path.dirname(path)
                base = os.path.basename(path)
                if base.startswith('IDR_'):
                    return os.path.join(dir,'SDR_' + base[4:])
            raise ServiceError(Errors.MissingSDR,self.idr.path if self.idr else None)

    
        def parse(self,fh):
            # Determine the CSV dialect, and whether headers are included
            sample = fh.read(5000)
            fh.seek(0)
        
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)

            fieldnames = None
            #if not sniffer.has_header(sample): # sniffer header detection not accurate...
            if not 'Aggregator' in sample:
                fieldnames = self.FIELDNAMES
            
            reader = csv.DictReader(fh, dialect=dialect, fieldnames=fieldnames)
            for row in reader:
                for k in row.keys():
                    row[k] = row[k].strip()
                self.add_row(row)

            if self.idr:
                self.associate_idr()
                self.summarize_call_types()
        

        def associate_idr(self):
            """ Match up the detail rows from this file with the higher-level summary from the IDR """
            for row in self.rows():
                self.associate_idr_row(row)


        def associate_idr_row(self,row):

            #import pdb; pdb.set_trace()

            inv = self.idr.find_by_name(row['Name'])
            if not inv:
                return

            row['Invoice'] = inv
            
            call_type = row['Call Type']
            sub_type = row['Sub-Call Type']

            print 'Invoice'


            if '%s Detail' % call_type in inv and sub_type in inv['%s Detail' % call_type]:
                #print "Pair Exists"
                totalAmount = row['Invoice']['%s Detail' % (call_type)][sub_type]['Total Amount']
                totalCalls = row['Invoice']['%s Detail' % (call_type)][sub_type]['Total Calls']
                totalMinutes = row['Invoice']['%s Detail' % (call_type)][sub_type]['Total Minutes']
            else:
                #print "Pair Does Not exist"
                totalAmount = 0
                totalCalls = 0
                totalMinutes = 0


            if '%s Detail' % (call_type) in inv:
                print "Detail exists"
                detail = inv['%s Detail' % (call_type)]
            else:
                print "Detail doesn't exist"
                detail = inv['%s Detail' % (call_type)] = {}

            detail[row['Sub-Call Type']] = row

            row['Invoice']['%s Detail' % (call_type)][sub_type]['Total Amount'] = totalAmount + float(row['Total Amount'])
            row['Invoice']['%s Detail' % (call_type)][sub_type]['Total Calls'] = totalCalls + float(row['Total Calls'])
            row['Invoice']['%s Detail' % (call_type)][sub_type]['Total Minutes'] = totalMinutes + float(row['Total Minutes'])

            #import pprint
            #pp = pprint.PrettyPrinter(indent=4)
            #pp.pprint(row)


        def summarize_call_types(self):
            """ The IDR gives total amount per call type. This method also adds
                total call count, and total mou, by summing up the sub-call types
                per call type """
            for row in self.rows():
                self.summarize_call_types_row(row)


        def summarize_call_types_row(self,row):
            try: inv = row['Invoice']
            except KeyError: return

            call_type = row['Call Type']
            sub_type = row['Sub-Call Type']

            row_calls = int(row['Total Calls'])
            try: inv['%s Call Count' % (call_type)] += row_calls
            except KeyError: inv['%s Call Count' % (call_type)] = row_calls
            
            row_mou = float(row['Total Minutes'])
            try: inv['%s Call Minutes' % (call_type)] += row_mou
            except KeyError: inv['%s Call Minutes' % (call_type)] = row_mou



class AccountList(EzcomNotification):
    """ Class for parsing AL files from EZ-COM """

    FIELDNAMES = ['Aggregator ID',
                  'Customer Number',
                  'Customer Name',
                  'SAP ID',
                  'Bill Frequency',
                  'Email']
    
    def parse_file(self,path):
        fh = open(path,'rU')
        self.parse(fh)

    def parse(self,fh):
        # Determine the CSV dialect, and whether headers are included
        # The CSV from EzCom doesn't currently contain field names, but it may
        # in the future
        sample = fh.read(5000)
        fh.seek(0)
        
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)

        fieldnames = None
        #if not sniffer.has_header(sample): # sniffer header detection not accurate...
        if not 'Customer Name' in sample:
            fieldnames = self.FIELDNAMES

        reader = csv.DictReader(fh, dialect=dialect, fieldnames=fieldnames)
        for row in reader:
            for k in row.keys():
                row[k] = row[k].strip()
            self.add_row(row)
            
