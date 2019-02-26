'''
Main routine for Class RDFUpdater
Created on 25 Feb. 2019

@author: Alex Ip
'''
import logging
import sys
from rdf_updater import RDFUpdater

logger = logging.getLogger('rdf_updater')

DEBUG = False

def main():
    rdf_updater = RDFUpdater(debug=DEBUG) 
    #logger.debug(pformat(rdf_updater.__dict__))
    
    print()
     
    rdf_updater.get_rdfs() # Read RDFs from sources into files
     
    print()
     
    rdf_updater.put_rdfs() # Write RDFs to triple-store from files


    
    
    
    
if __name__ == '__main__':
    # Setup logging handler if required
    if not logger.handlers:
        # Set handler for root root_logger to standard output
        console_handler = logging.StreamHandler(sys.stdout)
        #console_handler.setLevel(logging.INFO)
        console_handler.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    main()