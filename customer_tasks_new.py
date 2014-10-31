
from dw.tasks import BaseTask

from dw.model.customer import *
from dw.utils.ssh import SCP
from /home/ageorge import customer_new

import logging, csv, datetime, os
import pdb

class LoadCustomerList(BaseTask):
    ONE_DAY = datetime.timedelta(days=1)
    FORMAT  = "AL_%m%d%Y.CSV"
    
    def run_task(self,*args,**kwargs):
        remote = kwargs.pop('remote')
        if remote:
            remote_host, remote_dir = remote.split(":",1)
            path = self.get_latest_file(remote_host,remote_dir,kwargs.get('local_dir','/tmp'))
        else:
            path = kwargs.pop('path')

        if not path:
            return

        self.log.info("Loading customer account list: %s" % (path))

        loader = CustomerListLoader(logger=self.log)
        count = loader.load(path)

        self.log.info("Loaded %d customer accounts" % (count))

    def get_latest_file(self,remote_host,remote_dir,local_dir):
        #check for latest file
        dt = datetime.datetime.now()
        scp = SCP(remote_host,'apps','/home/apps/.ssh/id_rsa')
        for i in range(30):
            fname = dt.strftime(self.FORMAT)
            #check for local file first
            local_file = os.path.join(local_dir,fname)
            if os.path.exists(local_file):
                self.log.info("Latest customer account list already loaded: %s" % (fname))
                return None
            try:
                scp.copy_from(os.path.join(remote_dir,fname),local_dir)
                return local_file
            except Exception:
                #file probably wasn't there, just continue
                pass
            
            dt -= self.ONE_DAY

        self.log.info("No customer account list found within last 30 days")
        return None

class CustomerListLoader(object):
    """
    Parse an ezcom AL file, create/update customer accounts
    """

    def __init__(self,logger=None):
        self.log = logger or logging.getLogger()


    def load(self,path):
        parser = EZComAccountListParser()
        source = os.path.basename(path)

        count = 0
        for row in parser.parse_file(path):
            ezcom_id = int(row['Customer Number'])
            cust = Customer.find_by_ezcom_id(ezcom_id)
            if cust:
                self.log.debug("Update existing customer: %s" % (cust))
                pass
            if not cust:
                cust = Customer(ezcom_id=ezcom_id)
                self.log.debug("Add new customer: %s" % (cust))

            pdb.set_trace()

            cust.name = row['Customer Name']
            cust.sap_id = row['SAP ID']
            cust.bill_freq = row['Bill Frequency']
            cust.email = row['Email']
            cust.aggregator = row['Aggregator ID']
            cust.source = source

            if cust.aggregator == "PEER":
                continue

            if not 'is_ucc' in cust:
                cust.is_ucc = cust.determine_is_ucc()

            cust.save()
            count += 1

        return count


class EZComAccountListParser(object):
    """
    Parse an ezcom customer account list file (AL_*.CSV)
    """

    def __init__(self):
        pass

    FIELDNAMES = ['Aggregator ID',
                  'Customer Number',
                  'Customer Name',
                  'SAP ID',
                  'Bill Frequency',
                  'Email']

    def parse_file(self,path):
        with open(path,'rU') as fh:
            for row in self.parse(fh):
                yield(row)

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
            yield(row)
        

class LoadCustomerServiceIDs(BaseTask):
    """
    Loads the CustomerServiceID mappings used by UDRs, from a CSV file.
    The CSV file is a dump from the TGDB, taken using:
    mysql> select * into outfile '/tmp/customer_service.csv' 
           fields terminated by ',' optionally enclosed by '"'
           lines terminated by '\n'
           from CustomerService;
    """

    def run_task(self,*args,**kwargs):
        import pdb; pdb.set_trace()
        path = kwargs.pop('path')

        self.log.info("Loading customer service IDs: %s" % (path))
        self.load_from_file(path)

    FIELD_NAMES = ['CustomerServiceID',
                   'CustomerID',
                   'ServiceName',
                   'Description',
                   'IsEhanced',
                   'BillingID',
                   'selectionMethod']

    def load_from_file(self,path):
        reader = csv.DictReader(open(path,'rU'),fieldnames=self.FIELD_NAMES)
        for row in reader:
            cust = Customer.find_by_sap_id(row['BillingID'])
            if not cust:
                cust = Customer(sap_id=row['BillingID'])
            cust['customer_service'] = {
                'service_id': row['CustomerServiceID'],
                'id': row['CustomerID'],
                'service_name': row['ServiceName'],
                'billing_id': row['BillingID'],
                }
            cust.save()


class LoadVendorServiceIDs(BaseTask):
    """
    Loads the VendorServiceID mappings used by UDRs, from a CSV file.
    The CSV file is a dump from the TGDB, taken using:
    mysql> select * into outfile '/tmp/vendor_service.csv' 
           fields terminated by ',' optionally enclosed by '"'
           lines terminated by '\n'
           from VendorService;
    """

    def run_task(self,*args,**kwargs):
        path = kwargs.pop('path')

        self.log.info("Loading vendor service IDs: %s" % (path))
        self.load_from_file(path)

    FIELD_NAMES = ['VendorServiceID',
                   'VendorID',
                   'ServiceName',
                   'BillingIp',
                   'IsBillingIpFake',
                   'Description',
                   'IsEnhanced',
                   'EffBegin',
                   'EffEnd',
                   'selectionMethod']

    def load_from_file(self,path):
        reader = csv.DictReader(open(path,'rU'),fieldnames=self.FIELD_NAMES)
        for row in reader:
            # ...
            pass

