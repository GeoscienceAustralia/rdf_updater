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
    parser.add_argument('-u', '--update_config', help='Update RDF config from "directories", "github" or "csiro"', type=str)
    parser.add_argument('--debug', action='store_const', const=True, default=False,
                        help='output debug information. Default is no debug info')
    
    args = parser.parse_args()
    
    skosify = not args.no_skosify

    rdf_updater = RDFUpdater(settings_path=args.settings, 
                             update_config=[update_source.strip() for update_source in (args.update_config or '').split(',')], 
                             debug=args.debug
                             ) 

    rdf_updater.get_rdfs() # Read RDFs from sources into files
     
    print()
       
    if skosify:
        rdf_updater.skosify_rdfs()
        print()
     
    rdf_updater.write_rdfs_to_triple_stores(skosified=skosify) # Write RDFs to triple-stores from files
      
    print()
    
    rdf_updater.resolve_ConceptScheme_indirection()
    
    rdf_updater.flatten_linksets()

    #rdf_updater.output_summary_text() # Output text summary to file
    
    print('Finished!')

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