#!/opt/anaconda/bin/python
import os
import sys
import optparse
import shutil
import csv
from collections import namedtuple

AttemptCsvComponents = [
            "derivedStartDate",
            "derivedStartTime",
            "derivedDisconnectTime",
            "mCustomerServiceName",
            "mAniNational",
            "mDestNational",
            "mDialedNational",
            "mDisconnectReason",
            "mDisconnectInitiator",
            "derivedDisconnectDate" ]
AttemptCsvLineComponents = namedtuple('AttemptCsvLineComponents', ' '.join(AttemptCsvComponents))


class StaticFieldTranslator:
    
    DisconnectReasonDict = {
                            '400': '41',
                            '401': '21',
                            '402': '21',
                            '403': '21',
                            '404': '1',
                            '405': '63',
                            '406': '79',
                            '407': '21',
                            '408': '102',
                            '410': '22',
                            '413': '127',
                            '414': '127',
                            '415': '79',
                            '416': '127',
                            '420': '127',
                            '421': '127',
                            '423': '127',
                            '480': '18',
                            '481': '41',
                            '482': '25',
                            '483': '25',
                            '484': '28',
                            '485': '1',
                            '486': '17',
                            '487': '16',
                            '500': '41',
                            '501': '79',
                            '502': '38',
                            '503': '41',
                            '504': '102',
                            '505': '127',
                            '513': '127',
                            '600': '17',
                            '603': '21',
                            '604': '1',
                            '1000': '16' ,
                            '1001': '41',
                            '1002': '41',
                            '1003': '41',
                            '1004': '41',
                            '1005': '102',
                            '1006': '63',
                            '1007': '47',
                            '1008': '34',
                            '1009': '41',
                            '1010': '102',
                            '1011': '102',
                            '1012': '16',
                            '1013': '41',
                            '1014': '41',
                            '1015': '44',
                            '1016': '42',
                            '1017': '42',
                            '1018': '34',
                            '1019': '34',
                            '1020': '34',
                            '1021': '63',
                            '1022': '41',
                            '1023': '16',
                            '1024': '102',
                            '1025': '102',
                            '1026': '102',
                            '1027': '41',
                            '1028': '3',
                            '1029': '41',
                            '1030': '96',
                            '1031': '42',
                            '1032': '42',
                            '1033': '21',
                            '1034': '41',
                            '1035': '41',
                            '1036': '41',
                            '1037': '57',
                            '1038': '34',
                            '1039': '41',
                            '1040': '42',
                            '1041': '42',
                            '1042': '42',
                            '1043': '34',
                            '1044': '102',
                            '1045': '63',
                            '1046': '63',
                            '1047': '16',
                            '1048': '102',
                            '1049': '96',
                            '1050': '47',
                            '1051': '3',
                            '1052': '102',
                            '1053': '44',
                            '1054': '41',
                            '1055': '41',
                            '1056': '101',
                            '1057': '101',
                            '1058': '110',
                            '1059': '34',
                            '1060': '34',
                            '1061': '34',
                            '1062': '34',
                            '1063': '34',
                            '1064': '41'
                            } 
    
    @staticmethod
    def translate_all(components):
        ''' call all translators
            each translator returns a new AttemptCsvLineComponents instance -- this is inefficient
            at the cost of the namedtuple benefits
        '''
        result = StaticFieldTranslator.translate_mDisconnectReason(components)
        return result
    
    @staticmethod
    def translate_mDisconnectReason(components):
        '''translate the mDisconnectReason field, if applicable'''
        new_val = StaticFieldTranslator.DisconnectReasonDict.get(components.mDisconnectReason, components.mDisconnectReason)
        if new_val == components.mDisconnectReason:
            return components
        else:
            return components._replace(mDisconnectReason=new_val)
        
        
            
def ParseOpts():
    '''Parse the command line options'''
    parser = optparse.OptionParser()
    
    parser.add_option("--inputfile", dest="inputfile", help="csv file to translate")
    parser.add_option("--outputfile", dest="outputfile", help="destination for resulting translated file")
        
    opts, args = parser.parse_args()
    return opts, args
    
    
def main():
    opts, args = ParseOpts()
    
    if opts.inputfile is None or opts.outputfile is None:
        print 'ERROR: empty arguments found[%s]' % (opts,)
    else:
        input_file_copy_filename = opts.inputfile + '.TMP'
        try:
            #operate on a copy of the file since the existing file might still be being written to
            shutil.copy(opts.inputfile, input_file_copy_filename)
            
            with open(input_file_copy_filename, 'rb') as inputcsvfile:
                csvreader = csv.reader(inputcsvfile)
                
                with open(opts.outputfile, 'w') as outputcsvfile:
                    outputcsvwriter = csv.writer(outputcsvfile, quoting=csv.QUOTE_ALL)
                    
                    for row in csvreader:
                        try:
                            attemptComponents = AttemptCsvLineComponents(*row)
                            attemptComponents = StaticFieldTranslator.translate_all(attemptComponents)
                            outputcsvwriter.writerow(attemptComponents)
                        except TypeError:
                            #print any parse errors, but continue regardless
                            print ('WARNING: invalid attempt line read into AttemptCsvLineComponents namedtuple:' +
                                '\n\t%s' +
                                '\n\tEither the file was not fully written, or an invalid file format is being read') % (row, )    
        except:
            print "Unexpected error [%s]" % (sys.exc_info()[0],)
            raise                            
        finally:
            if os.path.isfile(input_file_copy_filename):
                os.remove(input_file_copy_filename)
        
              

if __name__ == '__main__':
    main()
