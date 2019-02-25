'''
Class definition for RDFUpdater
Created on 25 Feb. 2019

@author: Alex Ip
'''
import logging
import os
import sys
import yaml
import requests
import json
import re
from pprint import pformat

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Initial logging level for this module
logger.debug('__name__ = {}'.format(__name__))

class RDFUpdater(object):
    settings = None
    
    def __init__(self, settings_path=None, debug=True):
        
        # Initialise and set debug property
        self._debug = None
        self.debug = debug

        package_dir = os.path.dirname(os.path.abspath(__file__))
        settings_path = settings_path or os.path.join(package_dir, 'rdf_updater_settings.yml')
        self.settings = yaml.safe_load(open(settings_path))
        
        logger.debug('Settings: {}'.format(pformat(self.settings)))
        
        
    def get_rdfs(self):
        def get_rdf(rdf_config):
            if rdf_config['source_type'] == 'sparql':
                url = rdf_config['sparql_endpoint']
                http_method = requests.post
                headers = {'Accept': 'application/rdf+xml',
                           'Content-Type': 'application/sparql-query'
                           }
                params = None
                data = '''CONSTRUCT {?s ?p ?o}
WHERE {?s ?p ?o .}'''
            elif rdf_config['source_type'] == 'http_get':
                url = rdf_config['uri']
                http_method = requests.get
                headers = {'Accept': 'application/rdf+xml'}
                params = {'format': 'skos'}
                data = None
            else:
                raise Exception('Bad source type for RDF')
            #logger.debug('http_method = {}, url = {}, headers = {}, params = {}, data = {}'.format(http_method, url, headers, params, data))
            response = http_method(url, headers=headers, params=params, data=data, timeout=self.settings['timeout'])
            #logger.debug('Response content: {}'.format(str(response.content)))
            assert response.status_code == 200, 'Response status code != 200'
            return(response.content)
                
                
        for rdf_name, rdf_config in self.settings['rdf_configs'].items():
            logger.debug('Processing {}'.format(rdf_name))
            try:
                rdf = get_rdf(rdf_config)
                #logger.debug('rdf = {}'.format(rdf))
                logger.debug('Writing RDF to file {}'.format(rdf_config['rdf_file_path']))
                with open(rdf_config['rdf_file_path'], 'wb') as rdf_file:
                    rdf_file.write(rdf)
            except Exception as e:
                logger.warning('RDF get from {} to file failed: {}'.format(rdf_config['source_type'], e))
                
        logger.debug('Finished')
        
        
    def put_rdfs(self):
        def put_rdf(rdf_config, rdf):
            url = self.settings['triple_store_url'] + '/data'
            headers = {'Content-Type': 'application/rdf+xml'}
            params = {'graph': rdf_config['uri']}
            
            logger.debug('Writing RDF to {}'.format(url))
            response = requests.put(url, headers=headers, params=params, data=rdf, timeout=self.settings['timeout'])
            #logger.debug('Response content: {}'.format(response.content))
            assert response.status_code == 200 or response.status_code == 201, 'Response status code {}  != 200 or 201: {}'.format(response.status_code, response.content)
            return(response.content)
                
                
        for rdf_name, rdf_config in self.settings['rdf_configs'].items():
            logger.debug('Processing {}'.format(rdf_name))
            try:
                logger.debug('Reading RDF from {}'.format(rdf_config['rdf_file_path']))
                with open(rdf_config['rdf_file_path'], 'rb') as rdf_file:
                    rdf = rdf_file.read()
                #logger.debug('rdf = {}'.format(rdf))
                result = json.loads(put_rdf(rdf_config, rdf))
                logger.debug('result = {}'.format(result))
            except Exception as e:
                logger.warning('RDF put from file to triple-store failed: {}'.format(e))
                
        logger.debug('Finished')
        
        
    @property
    def debug(self):
        return self._debug
    
    @debug.setter
    def debug(self, debug_value):
        if self._debug != debug_value or self._debug is None:
            self._debug = debug_value
            
            if self._debug:
                logger.setLevel(logging.DEBUG)
            else:
                logger.setLevel(logging.INFO)
                
        logger.debug('Logger {} set to level {}'.format(logger.name, logger.level))


def main():
    rdf_updater = RDFUpdater() 
    #logger.debug(pformat(rdf_updater.__dict__))
    rdf_updater.get_rdfs()
    
    rdf_updater.put_rdfs()
    
    
    
    
if __name__ == '__main__':
    # Setup logging handlers if required
    if not logger.handlers:
        # Set handler for root root_logger to standard output
        console_handler = logging.StreamHandler(sys.stdout)
        #console_handler.setLevel(logging.INFO)
        console_handler.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    main()
