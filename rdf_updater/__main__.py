'''
Main routine for Class RDFUpdater
Created on 25 Feb. 2019

@author: Alex Ip
'''
import logging
import sys
import argparse
from rdf_updater import RDFUpdater

logger = logging.getLogger('rdf_updater')

DEBUG = False

def main():
    # Define command line arguments
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-s", "--settings", help="Settings file", type=str)
    parser.add_argument('-n', '--no_skosify', action='store_const', const=True, default=False,
                        help='Do not perform SKOS validation and inferencing. Default is SKOS enabled')
    parser.add_argument('-g', '--update_github', action='store_const', const=True, default=False,
                        help='Update vocab settings from GitHub. Default is no update')
    parser.add_argument('--debug', action='store_const', const=True, default=False,
                        help='output debug information. Default is no debug info')
    
    args = parser.parse_args()


    rdf_updater = RDFUpdater(settings_path=args.settings, 
                             update_github=args.update_github, 
                             debug=args.debug
                             ) 
    #logger.debug(pformat(rdf_updater.__dict__))
    
    print()
     
    rdf_updater.get_rdfs() # Read RDFs from sources into files
     
    print()
    
    if not args.no_skosify:
        rdf_updater.skosify_rdfs()
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