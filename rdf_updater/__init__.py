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
import base64
from pprint import pprint, pformat
from lxml import etree
import skosify  # contains skosify, config, and infer
import rdflib
from rdflib import Graph
from _collections import OrderedDict
from glob import glob
from time import sleep

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Initial logging level for this module
logger.debug('__name__ = {}'.format(__name__))

SPARQL_QUERY_LIMIT = 2000 # Maximum number of results to return per SPARQL query

MAX_RETRIES = 2

RETRY_SLEEP_SECONDS = 10

class RDFUpdater(object):
    settings = None
    
    def __init__(self, 
                 settings_path=None, 
                 update_config=[],
                 debug=False):
        
        # Initialise and set debug property
        self._debug = None
        self.debug = debug

        package_dir = os.path.dirname(os.path.abspath(__file__))
        settings_path = settings_path or os.path.join(package_dir, 'rdf_updater_settings.yml')
        self.settings = yaml.safe_load(open(settings_path))
        
        new_rdf_configs = {}
        if 'github' in update_config:
            logger.info('Reading vocab configs from GitHub')
            new_rdf_configs.update(self.update_github_settings())
            
        if 'directories' in update_config:
            logger.info('Reading vocab configs from directories')
            new_rdf_configs.update(self.update_directory_settings())
            
        if 'csiro' in update_config:
            logger.info('Reading vocab configs from CSIRO')
            new_rdf_configs.update(self.update_csiro_settings())
            
        if update_config and new_rdf_configs:
            logger.info('Writing updated vocab configs to settings file {}'.format(settings_path))
            self.settings['rdf_configs'].update(new_rdf_configs)
            with open(settings_path, 'w') as settings_file:
                yaml.safe_dump(self.settings, settings_file)
        
        #logger.debug('Settings: {}'.format(pformat(self.settings)))
        
        
    def get_rdfs(self):
        def get_rdf(rdf_config):
            #print(rdf_config)
            if rdf_config['source_type'] == 'sparql':
                url = rdf_config['sparql_endpoint']
                http_method = requests.post
                headers = {'Accept': 'application/rdf+xml',
                           'Content-Type': 'application/sparql-query',
                           'Accept-Encoding': 'UTF-8'
                           }
                params = None
                data = '''CONSTRUCT {?s ?p ?o}
WHERE {?s ?p ?o .}'''
            elif rdf_config['source_type'] == 'http_get':
                rdf_url = rdf_config.get('rdf_url')
                url = rdf_config['graph_name']
                http_method = requests.get
                headers = None
                params = None
                if rdf_config.get('format') == 'ttl':
                    if rdf_url and 'github' in rdf_url: # File is to be retrieved from GitHub
                        url = rdf_url
                        headers = {'Accept': '*/*',
                                   'Accept-Encoding': 'UTF-8'
                                   }
                    else: # Assume SISSVoc source
                        url += '/' # SISSVoc needs to have a trailing slash
                        params = {'_format': 'text/turtle'}
                        headers = {'Accept': 'application/text',
                                   'Accept-Encoding': 'UTF-8'
                                   }
                elif rdf_config.get('format') == 'nq':
                    if rdf_url:
                        url = rdf_url
                else:
                    headers = {'Accept': 'application/text',
                               'Accept-Encoding': 'UTF-8'
                               }

                    if rdf_config.get('rdf_url'):
                        url = rdf_config.get('rdf_url')
                    else: # Special case for ODM2
                        params = {'format': 'skos'}
                data = None
            elif rdf_config['source_type'] == 'file':
                rdf_path = rdf_config['rdf_url'].replace('file://', '')
                logger.info('Reading RDF from file {} '.format(rdf_path))
                with open(rdf_path, 'r') as rdf_file:
                    rdf = rdf_file.read()
                return rdf    
            else:
                raise Exception('Bad source type for RDF')
            #logger.debug('http_method = {}, url = {}, headers = {}, params = {}, data = {}'.format(http_method, url, headers, params, data))
            logger.info('Reading RDF from {} via {}'.format(url, rdf_config['source_type']))
            response = http_method(url, headers=headers, params=params, data=data, timeout=self.settings['timeout'])
            #logger.debug('Response content: {}'.format(str(response.content)))
            assert response.status_code == 200, 'Response status code != 200'
            return response.text # Convert binary to UTF-8 string
        
                
        logger.info('Reading RDFs from sources to files')    
        
        for _rdf_name, rdf_config in self.settings['rdf_configs'].items():
            logger.info('Obtaining data for {}'.format(rdf_config['graph_label'] ))
            try:
                rdf = get_rdf(rdf_config)
                
                # Perform global and specific regular expression string replacements
                for regex_replacement in (self.settings.get('regex_replacements') or []) + (rdf_config.get('regex_replacements') or []):
                    #logger.debug('Performing re.sub({}, {}, <rdf_text>)'.format(*[repr(regex) for regex in regex_replacement]))
                    rdf = re.sub(regex_replacement[0], regex_replacement[1], rdf) # Add encoding if missing
                
                #logger.debug('rdf = {}'.format(rdf))
                logger.info('Writing RDF to file {}'.format(rdf_config['rdf_file_path']))
                rdf_directory = os.path.dirname(os.path.abspath(rdf_config['rdf_file_path']))
                if not os.path.exists(rdf_directory):
                    logger.debug('Creating directory {}'.format(rdf_directory))
                    os.makedirs(rdf_directory)
                with open(rdf_config['rdf_file_path'], 'w', encoding='utf-8') as rdf_file:
                    rdf_file.write(rdf)
            except Exception as e:
                logger.error('ERROR: RDF get from {} to file failed: {}'.format(rdf_config['source_type'], e))
                
        logger.info('Finished reading to files')
        
        
    def write_rdfs_to_triple_stores(self, skosified=True):
        '''
        Function to write all RDFs to all triple-stores
        '''
        def put_rdf(rdf_config, rdf, triple_store_settings):
            url = triple_store_settings['url'] + '/data'
            if not skosified and rdf_config.get('format') == 'ttl': 
                headers = {'Content-Type': 'text/turtle'}
            else:
                headers = {'Content-Type': 'application/rdf+xml'}
                
            username = triple_store_settings.get('username')
            password = triple_store_settings.get('password')
            
            if (username and password):
                #logger.debug('Authenticating with username {} and password {}'.format(username, password))
                headers['Authorization'] = 'Basic ' + base64.encodebytes('{}:{}'.format(username, password).encode('utf-8')).strip().decode('utf-8')
            
            params = {'graph': rdf_config['graph_name']}
            
            #logger.debug('url = {}, headers = {}, params = {}'.format(url, headers, params))
            response = requests.put(url, headers=headers, params=params, data=rdf.encode('utf-8'), timeout=self.settings['timeout'])
            #logger.debug('Response content: {}'.format(response.content))
            assert response.status_code == 200 or response.status_code == 201, 'Response status code {}  != 200 or 201: {}'.format(response.status_code, response.content)
            return(response.content)
                
        for triple_store_name, triple_store_settings in self.settings['triple_stores'].items():
            logger.info('Writing RDFs to triple-store {} from files'.format(triple_store_name))           
            for _rdf_name, rdf_config in self.settings['rdf_configs'].items():
                if skosified:
                    rdf_file_path = os.path.splitext(rdf_config['rdf_file_path'])[0] + '_skos.rdf' # SKOSified RDF
                else:
                    rdf_file_path = rdf_config['rdf_file_path'] # Original RDF
                    
                logger.info('Writing RDF from file {} to triple-store {}'.format(rdf_file_path, triple_store_settings['url']))
                try:
                    with open(rdf_file_path, 'r', encoding='utf-8') as rdf_file:
                        rdf = rdf_file.read()
                    #logger.debug('rdf = {}'.format(rdf))
                    result = json.loads(put_rdf(rdf_config, rdf, triple_store_settings))
                    #logger.debug('result = {}'.format(result))
                    logger.info('{} triples (re)written to graph {}'.format(result['tripleCount'],
                                                                            rdf_config['graph_name']))
                except Exception as e:
                    logger.error('ERROR: RDF put from file to triple-store failed: {}'.format(e))
                    
        logger.info('Finished writing to triple-stores')
        
     
    def get_graph_values_from_rdf(self, rdf_xml):
        '''
        Function to derive and return graph_name & graph_label from rdf_xml text
        '''
        #TODO: Re-implement this with rdflib if possible
        vocab_tree = etree.fromstring(rdf_xml)
        
        # Find all vocab elements
        vocab_elements = vocab_tree.findall(path='skos:ConceptScheme', namespaces=vocab_tree.nsmap)
        if not vocab_elements: #No skos:ConceptSchemes defined - look for resource element parents instead                      
            logger.error('ERROR: RDF has no explicit skos:ConceptScheme elements')
            return
        
        #logger.debug('vocab_elements = {}'.format(pformat(vocab_elements)))
        
        if len(vocab_elements) == 1:
            vocab_element = vocab_elements[0]
            graph_name = vocab_element.attrib.get('{' + vocab_tree.nsmap['rdf'] + '}about')
        else:
            logger.warning('WARNING: RDF has multiple vocab elements')
            #TODO: Make this work better when there are multiple vocabs in one RDF
            # Find shortest URI for vocab and use that for named graphs
            # This is a bit nasty, but it works for poorly-defined subcollection schemes
            vocab_element = None
            graph_name = None
            for search_vocab_element in vocab_elements:
                search_vocab_uri = search_vocab_element.attrib.get('{' + vocab_tree.nsmap['rdf'] + '}about')
                if (not graph_name) or len(search_vocab_uri) < len(graph_name):
                    graph_name = search_vocab_uri
                    vocab_element = search_vocab_element
            
        label_element = vocab_element.find(path = 'rdfs:label', namespaces=vocab_tree.nsmap)
        if label_element is None:
            label_element = vocab_element.find(path = 'skos:prefLabel[@{http://www.w3.org/XML/1998/namespace}lang="en"]', namespaces=vocab_tree.nsmap)
        if label_element is None:
            label_element = vocab_element.find(path = 'dcterms:title[@{http://www.w3.org/XML/1998/namespace}lang="en"]', namespaces=vocab_tree.nsmap)
        graph_label = label_element.text
                    
        return graph_name, graph_label
    
    
                        
    def update_directory_settings(self):   
        '''
        Function to update rdf_configs section of settings file from specified directories using directory_configs settings
        '''
        result_dict = {}
        for dir_name, dir_config in self.settings['directory_configs'].items():
            logger.debug('Reading configurations for {}'.format(dir_name))
            for rdf_path in glob(os.path.join(dir_config['source_dir'], '*.rdf'), recursive=False):
                try:
                    with open(rdf_path, 'r') as rdf_file:
                        rdf_xml = rdf_file.read()
                        
                    # Perform global and specific regular expression string replacements
                    for regex_replacement in (self.settings.get('regex_replacements') or []) + (dir_config.get('regex_replacements') or []):
                        rdf_xml = re.sub(regex_replacement[0], regex_replacement[1], rdf_xml) # Add encoding if missing
                    
                    graph_name, graph_label = self.get_graph_values_from_rdf(rdf_xml.encode('utf-8'))                        

                except Exception as e:       
                    logger.warning('Unable to find vocab information in file {}: {}'.format(rdf_path, e))
                    continue
                
                vocab_dict = {'graph_label': graph_label,
                               'graph_name': graph_name,
                               'source_type': 'file',
                               'rdf_file_path': dir_config['rdf_dir'] + '/' + os.path.basename(rdf_path),
                               'rdf_url': 'file://' + rdf_path
                               }
                
                if dir_config.get('regex_replacements'):
                    vocab_dict['regex_replacements'] = dir_config['regex_replacements']
                    
                #logger.debug('vocab_dict = {}'.format(pformat(vocab_dict)))
                result_dict[os.path.splitext(os.path.basename(rdf_path))[0]] = vocab_dict      
                          
        return result_dict        
                    
     
    def update_csiro_settings(self):   
        '''
        Function to update rdf_configs section of settings file from specified CSIRO vocabs using csiro_configs settings
        '''
        result_dict = {}
        for source_name, source_config in self.settings['csiro_configs'].items():
            logger.debug('Reading configurations for {}'.format(source_name))
            source_url  = source_config['source_url']
            source_format  = source_config['source_format']
            try:
                response = requests.get(source_url) #, timeout=self.settings['timeout'])
                assert response.status_code == 200, 'Response status code != 200'
                
                rdf = Graph()
                rdf.parse(data=response.text, format=source_format)
                
                sparql_query = '''
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                PREFIX dct: <http://purl.org/dc/terms/>
                
                SELECT DISTINCT ?vocab
                WHERE
                    {?vocab a skos:ConceptScheme .}
                ORDER BY ?vocab
                '''
                
                result = rdf.query(sparql_query)
                
                vocabs = [str(binding[rdflib.term.Variable('vocab')]) for binding in result.bindings]
                assert vocabs, 'No skos:ConceptScheme URIs found in {}'.format(source_url)
                
            except Exception as e:       
                logger.warning('Unable to find vocab information from URL {}: {}'.format(source_url, e))
                continue
            
            for vocab in vocabs:
                vocab_dict = {'graph_label': 'CSIRO ' + os.path.basename(vocab),
                               'graph_name': vocab,
                               'source_type': 'http_get',
                               'rdf_file_path': source_config['rdf_dir'] + '/' + os.path.basename(vocab) + '.rdf',
                               'rdf_url': vocab + '?_format=rdf&_view=with_metadata'
                               }
                
                if source_config.get('regex_replacements'):
                    vocab_dict['regex_replacements'] = source_config['regex_replacements']
                    
                #logger.debug('vocab_dict = {}'.format(pformat(vocab_dict)))
                result_dict[os.path.basename(vocab)] = vocab_dict      
                          
        #logger.debug('result_dict = {}'.format(pformat(result_dict)))
        return result_dict       
                    
     
    def update_github_settings(self):
        '''
        Function to update rdf_configs section of settings file from GitHub using git_configs settings
        '''
        result_dict = {}
        for github_name, github_config in self.settings['git_configs'].items():
            logger.debug('Reading configurations for {}'.format(github_name))
            url = github_config['github_url'].replace('/github.com/', '/api.github.com/repos/') + '/contents/' + github_config['source_tree']
            #logger.debug(url)
            response = requests.get(url, timeout=self.settings['timeout'])
            assert response.status_code == 200, 'Response status code != 200' 
            #logger.debug('response content = {}'.format(pformat(json.loads(response.content.decode('utf-8')))))
            rdfs = {tree_dict['name']: tree_dict['download_url']
                    for tree_dict in json.loads(response.content.decode('utf-8'))
                    if tree_dict.get('name') and tree_dict.get('download_url') 
                    }
            #logger.debug('url_list = {}'.format(pformat(url_list)))
            for rdf_name, rdf_url in rdfs.items():
                try:
                    # Skip non-RDF files
                    if os.path.splitext(os.path.basename(rdf_url))[1] != '.rdf':
                        logger.debug('Skipping {}'.format(rdf_url))
                        continue
                    
                    logger.debug('Reading config from {}'.format(rdf_name))
                    response = requests.get(rdf_url, timeout=self.settings['timeout'])
                    #logger.debug('Response content: {}'.format(str(response.content)))
                    assert response.status_code == 200, 'Response status code != 200'
    
                    graph_name, graph_label = self.get_graph_values_from_rdf(response.content)                        
                except Exception as e:
                    logger.warning('Unable to find vocab information in {}: {}'.format(rdf_url, e))
                    continue
                
                vocab_dict = {'graph_label': graph_label,
                               'graph_name': graph_name,
                               'source_type': 'http_get',
                               'rdf_file_path': github_config['rdf_dir'] + '/' + rdf_name,
                               'rdf_url': rdf_url
                               }
                
                if github_config.get('regex_replacements'):
                    vocab_dict['regex_replacements'] = github_config['regex_replacements']
                    
                #logger.debug('vocab_dict = {}'.format(pformat(vocab_dict)))
                result_dict[os.path.splitext(rdf_name)[0]] = vocab_dict
        return result_dict  
    
    def skosify_rdfs(self):
        '''
        Function to SKOSify original RDF files and write them to disk with the suffix "_skos.rdf"
        Also writes a hashable sorted n-triple file with the suffix "_skos.nt"
        '''
        def skosify_rdf(rdf_config, root_logger):
            rdf_file_path = rdf_config['rdf_file_path']
            skos_rdf_file_path = os.path.splitext(rdf_file_path)[0] + '_skos.rdf'
            skos_nt_file_path = os.path.splitext(rdf_file_path)[0] + '_skos.nt'
            log_file_name = os.path.splitext(rdf_file_path)[0] + '.log'
            
            logger.info('SKOSifying RDF from {}'.format(rdf_file_path))

            #===================================================================
            # # The following is a work-around for a unicode issue in rdflib
            # rdf_file = open(rdf_file_path, 'rb') # Note binary reading
            # rdf = Graph()
            # rdf.parse(rdf_file, format='xml')
            # rdf_file.close()
            #===================================================================
            
            if os.path.splitext(rdf_file_path)[-1] == '.nq':
                rdf_format='nquads'
            elif os.path.splitext(rdf_file_path)[-1] == '.ttl':
                rdf_format='ttl'
            else:
                rdf_format='xml'
            
            with open(rdf_file_path, 'r', encoding='utf-8') as rdf_file:
                rdf_text = rdf_file.read()
                rdf = Graph()
                rdf.parse(data=rdf_text, format=rdf_format)
            
            # Capture SKOSify WARNING level output to log file    
            try:
                os.remove(log_file_name)
            except:
                pass
            log_file_handler = logging.FileHandler(log_file_name, encoding='utf-8')
            log_file_handler.setLevel(logging.WARNING)
            log_file_formatter = logging.Formatter(u'%(message)s')
            log_file_handler.setFormatter(log_file_formatter)
            root_logger.addHandler(log_file_handler)
            
            skos_rdf = skosify.skosify(rdf, 
                                  label=rdf_config['graph_label'] ,
                                  break_cycles=True,
                                  eliminate_redundancy=True,
                                  )
            
            logger.debug('Adding SKOS inferences to {}'.format(skos_rdf_file_path)) 
            skosify.infer.skos_related(skos_rdf)
            skosify.infer.skos_topConcept(skos_rdf)
            skosify.infer.skos_hierarchical(skos_rdf, narrower=True)
            skosify.infer.skos_transitive(skos_rdf, narrower=True)
              
            skosify.infer.rdfs_classes(skos_rdf)
            skosify.infer.rdfs_properties(skos_rdf)
            
            logger.debug('Writing RDF-XML SKOS file {}'.format(skos_rdf_file_path))
            with open(skos_rdf_file_path, 'wb') as skos_rdf_file: # Note binary writing
                skos_rdf.serialize(destination=skos_rdf_file, format='xml')
            
            logger.debug('Writing hashable n-triple SKOS file {}'.format(skos_nt_file_path))
            with open(skos_nt_file_path, 'w') as skos_nt_file: # Note string writing
                for line in [line.decode('utf-8') 
                             for line in sorted(skos_rdf.serialize(format='nt').splitlines())
                             if line
                             ]:
                    skos_nt_file.write(line + '\n')
                    
            root_logger.removeHandler(log_file_handler) # Stop logging to file
            del log_file_handler # Force closing of log file
            if os.stat(log_file_name).st_size:
                logger.debug('SKOSify messages written to {}'.format(log_file_name))
            else:
                os.remove(log_file_name) # No messages

        
        logger.info('SKOSifying RDFs from files') 
        root_logger = logging.getLogger() # Capture output from Skosify to log file   
             
        for _rdf_name, rdf_config in self.settings['rdf_configs'].items():
            #logger.info('Validating data for {}'.format(rdf_config['graph_label'] ))
            
            try:
                skosify_rdf(rdf_config, root_logger)
            except Exception as e:
                logger.warning('RDF SKOSification from file {} failed: {}'.format(rdf_config['rdf_file_path'], e))
                continue
                        
        logger.info('SKOSification of RDF files completed')
        
    
    def submit_sparql_query(self, sparql_query, triple_store_name=None, accept_format='application/json'):
        '''
        Function to submit a sparql query and return the textual response
        '''
        # Default to any triple store if none specified
        triple_store_settings = self.settings['triple_stores'].get(triple_store_name) or list(self.settings['triple_stores'].values())[0]
                
        url = triple_store_settings['url']
        headers = {'Accept': accept_format,
                   'Content-Type': 'application/sparql-query',
                   'Accept-Encoding': 'UTF-8'
                   }

        is_update = 'insert {' in sparql_query.lower() or 'update {' in sparql_query.lower() or 'delete {' in sparql_query.lower()
        if is_update:
            url += '/update'
            headers['Content-Type'] = 'application/sparql-update'
            logger.debug('Updating triple-store at {}'.format(url))
        else:
            logger.debug('Querying triple-store at {}'.format(url))
        
        #logger.debug('sparql_query = {}'.format(sparql_query))
        accept_format = {'json': 'application/json',
                         'xml': 'application/xml'}.get(accept_format) or 'application/json'
        username = triple_store_settings.get('username')
        password = triple_store_settings.get('password')
            
        if (username and password):
            #logger.debug('Authenticating with username {} and password {}'.format(username, password))
            headers['Authorization'] = 'Basic ' + base64.encodebytes('{}:{}'.format(username, password).encode('utf-8')).strip().decode('utf-8')
            
        params = None
                
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                response = requests.post(url, 
                                       headers=headers, 
                                       params=params, 
                                       data=sparql_query, 
                                       timeout=self.settings['timeout'])
                #logger.debug('Response content: {}'.format(str(response.content)))
                assert response.status_code in [200, 204], 'Response status code {} != 200 or 204'.format(response.status_code)
                return response.text
            except Exception as e:
                logger.warning('SPARQL query failed: {}'.format(e))
                retries += 1
                sleep(RETRY_SLEEP_SECONDS)
    
    
    def get_graph_names(self, triple_store_name=None):
        '''
        Function to generate a list of all graph names
        '''
        sparql_query = '''SELECT DISTINCT ?graph
WHERE {
    GRAPH ?graph {
        ?s ?p ?o .
    }
}
'''
        response_text = self.submit_sparql_query(sparql_query, triple_store_name)
        #print(response_text)
        return [bindings_dict['graph']['value']
                for bindings_dict in json.loads(response_text
                                                )["results"]["bindings"]
                ]
            
        
    def get_vocabs(self, triple_store_name=None):
        '''
        Function to generate a list of dicts for all concepts
        '''
        sparql_query = '''PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT DISTINCT ?graph ?vocab ?vocab_label
WHERE {
    GRAPH ?graph {
        {?vocab a skos:ConceptScheme .}
    OPTIONAL {
        {?vocab dct:title ?vocab_label .} 
        UNION {?vocab rdfs:label ?vocab_label .}
        }
    }
}
ORDER BY ?graph ?vocab
'''
        return [{'graph': bindings_dict['graph']['value'],
                 'vocab': bindings_dict['vocab']['value'],
                 'vocab_label': bindings_dict['vocab_label']['value'] 
                    if bindings_dict.get('vocab_label') 
                    else os.path.basename(bindings_dict['vocab']['value'])
                 }
                for bindings_dict in json.loads(self.submit_sparql_query(sparql_query,
                                                                         triple_store_name
                                                                         )
                                                )["results"]["bindings"]
                ]
            
        
        
    def get_concepts(self, filter_graph=None, filter_vocab=None, triple_store_name=None):
        '''
        Function to generate a list of bindings for all concepts optionally filtered by graph and/or vocab
        '''
        returned_item_count = -1 # Anything but zero
        query_offset = 0
        concept_list = []
    
        while returned_item_count != 0:
        
            sparql_query = '''PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT distinct ?graph ?vocab ?vocab_label ?concept ?concept_preflabel ?concept_description ?broader_concept
WHERE {
    GRAPH ?graph {
            {?vocab a skos:ConceptScheme .}
        OPTIONAL {
            {?vocab dct:title ?vocab_label .} 
            UNION {?vocab rdfs:label ?vocab_label .}
            }
            {?concept skos:inScheme ?vocab .}
        ?concept skos:prefLabel ?concept_preflabel .
        OPTIONAL {?concept skos:definition ?concept_description .}
        OPTIONAL {?concept skos:broader ?broader_concept .}
        FILTER(lang(?concept_preflabel) = "en" || lang(?concept_preflabel) = "")
'''        
            if filter_vocab:
                sparql_query += '''
        FILTER(?vocab = <{}>)'''.format(filter_vocab)
            
            sparql_query += '''
        }
'''
            if filter_graph:
                sparql_query += '''
    FILTER(?graph = <{}>)'''.format(filter_graph)
    
            sparql_query += '''
    }
'''
            sparql_query += '''ORDER BY ?graph ?vocab ?concept
LIMIT {}
OFFSET {}'''.format(SPARQL_QUERY_LIMIT, query_offset)
        
            #logger.debug('sparql_query = {}'.format(sparql_query))
            response_dict = json.loads(self.submit_sparql_query(sparql_query, triple_store_name)
                                     )  
            returned_item_count = len(response_dict["results"]["bindings"])
            if returned_item_count:
                if query_offset or (returned_item_count == SPARQL_QUERY_LIMIT):
                    logger.debug('{} items returned in paginated query'.format(returned_item_count))
   
                concept_list += [{'graph': bindings_dict['graph']['value'],
                                   'vocab': bindings_dict['vocab']['value'],
                                   'vocab_label': bindings_dict['vocab_label']['value']
                                        if bindings_dict.get('vocab_label') 
                                        else os.path.basename(bindings_dict['vocab']['value']),
                                   'concept': bindings_dict['concept']['value'],
                                   'concept_preflabel': bindings_dict['concept_preflabel']['value'],
                                   'concept_description': bindings_dict['concept_description']['value'] 
                                        if bindings_dict.get('concept_description') 
                                        else 'None',
                                   'broader_concept': bindings_dict['broader_concept']['value'] 
                                        if bindings_dict.get('broader_concept') 
                                        else None
                                   } 
                                  for bindings_dict in response_dict["results"]["bindings"]
                                  ]
                query_offset += returned_item_count   
                
            # Don't go around again if we have fewer items than the limit
            if returned_item_count < SPARQL_QUERY_LIMIT:
                break
        
        return concept_list
    
        
    def get_vocab_tree(self, triple_store_name, filter_graph=None, filter_vocab=None):
        '''
        Function to generate a dict containing a tree of vocabs and concepts
        Returned dict is keyed by graph, vocab, broader_concept, narrower_concept...
        '''
        
        def get_concept_tree(concept_list, vocab, broader_concept=None):
            '''
            Recursive helper function to generate tree of broader/narrower concepts in graph
            '''
            def get_narrower_concepts(concept_list, vocab, broader_concept):
                '''
                Helper function to generate sublist of narrower concepts for a given broader concept
                N.B: when broader_concept is None, the list will contain top concepts and also 
                concepts with broader concepts in other vocabs
                '''
                concept_sublist = [concept_dict 
                                    for concept_dict in concept_list
                                    if (
                                        # Narrower concepts must be in same vocab
                                        (concept_dict['vocab'] == vocab)
                                        and (
                                                (
                                                (broader_concept is None) # Get top concepts
                                                and (
                                                    (concept_dict.get('broader_concept') is None) # Top concept?
                                                    or (concept_dict['broader_concept'] not in set([concept_dict['concept'] 
                                                                                                              for concept_dict in concept_list
                                                                                                              if (concept_dict['vocab'] == vocab)
                                                                                                              ])) # Broader concept not in same vocab
                                                    )
                                                )
                                                or (
                                                    (broader_concept is not None) 
                                                    and (concept_dict.get('broader_concept') is not None) 
                                                    and (concept_dict['broader_concept'] == broader_concept)
                                                )
                                            )
                                        )
                                    ]
                return concept_sublist
            
            concept_tree_dict = OrderedDict()
            
            for narrower_concept_dict in get_narrower_concepts(concept_list, vocab, broader_concept):
                concept = narrower_concept_dict["concept"]
                
                concept_dict = {'preflabel': narrower_concept_dict["concept_preflabel"]}
                
                concept_dict['description'] = narrower_concept_dict["concept_description"]
                    
                narrower_concept_tree_dict = get_concept_tree(concept_list, vocab, broader_concept=concept)
                if narrower_concept_tree_dict:
                    concept_dict['narrower_concepts'] = narrower_concept_tree_dict
                
                concept_tree_dict[concept] = concept_dict
                
            return concept_tree_dict
        
        vocab_list = self.get_vocabs(triple_store_name)
        
        logger.debug('Checking for multiple vocab definitions in different graphs')
        for vocab, graphs in [(vocab, sorted(list(set([vocab_dict['graph'] for vocab_dict in vocab_list if vocab_dict['vocab'] == vocab]))))
                              for vocab in sorted(list(set([vocab_dict['vocab'] for vocab_dict in vocab_list])))
                              ]:
            if len(graphs) != 1:
                logger.warning('WARNING: Vocabulary {} is defined in {} graphs: {}'.format(vocab, len(graphs), graphs))
        
        if filter_graph:
            vocab_list = [vocab_dict for vocab_dict in vocab_list if vocab_dict['graph'] == filter_graph]
        
        if filter_vocab:
            vocab_list = [vocab_dict for vocab_dict in vocab_list if vocab_dict['vocab'] == filter_vocab]
            
        graph_list = sorted(list(set([vocab_dict['graph'] for vocab_dict in vocab_list])))
        graph_count = len(graph_list)
        
        result_dict = OrderedDict()

        vocab_count = 0
        concept_count = 0
        item_count = 0
        for graph in graph_list:
            logger.debug('Querying graph {}'.format(graph))
            concept_list = self.get_concepts(filter_graph=graph, filter_vocab=filter_vocab)

            logger.debug('{} items found in graph {}'.format(len(concept_list), graph))
            item_count += len(concept_list)
            concept_count += len(set([concept_dict['concept'] 
                                      for concept_dict in concept_list]))
                  
            graph_dict = OrderedDict()
            result_dict[graph] = graph_dict
            
            # Create list of unique (vocab, vocab_label) tuples for graph
            graph_vocab_list = sorted(list(set([(concept_dict['vocab'], concept_dict['vocab_label'])
                                          for concept_dict in concept_list])))
            vocab_count += len(graph_vocab_list)
            
            for vocab, vocab_label in graph_vocab_list:           
                vocab_dict = {'label': vocab_label}        
                graph_dict[vocab] = vocab_dict
                
                vocab_dict['concepts'] = get_concept_tree(concept_list, vocab, broader_concept=None) 
            
        logger.info('{} concepts found in {} vocabs in {} graphs (total of {} items returned)'.format(concept_count, 
                                                                                                           vocab_count, 
                                                                                                           graph_count, 
                                                                                                           item_count))
        return result_dict
    
    
    def output_vocab_data(self, concept_tree_dict, output_stream=sys.stdout, level=0, indent='\t'):
        '''
        Recursive function to output concept_tree_dict to specified stream
        '''
        if level == 0: # Graph
            for graph, graph_dict in concept_tree_dict.items():
                output_stream.write('Graph "{}"\n'.format(graph))
                self.output_vocab_data(graph_dict, output_stream, level=level+1)
                output_stream.write('\n')
        elif level == 1: # vocab
            for vocab, vocab_dict in concept_tree_dict.items():
                output_stream.write('{}Vocab "{}": {}\n'.format(indent, vocab_dict['label'], vocab))
                self.output_vocab_data(vocab_dict['concepts'], output_stream, level=level+1)
        else: # Concept
            for concept, concept_dict in concept_tree_dict.items():
                output_stream.write('{}Concept "{}": {} ({})\n'.format((indent * level),
                    concept_dict['preflabel'], 
                    concept,
                    re.sub('[\s\r\n]+', ' ', concept_dict.get('description') or ''))
                    ) 
                narrower_concepts_dict = concept_dict.get('narrower_concepts')
                if narrower_concepts_dict:
                    self.output_vocab_data(narrower_concepts_dict, output_stream, level=level+1)


    def output_summary_text(self, triple_store_name=None, graph=None, vocab=None):               
        '''
        Function to output summary text file containing indented tree of all broader/narrower concepts in all vocabs in all graphs
        '''
        summary_output_path = self.settings.get('summary_output_path')
        if summary_output_path:
            logger.debug('Outputting summary text to {}'.format(summary_output_path))
            output_stream = open(summary_output_path, 'w', encoding='utf-8')
        else:
            output_stream = sys.stdout
                    
        vocab_tree = self.get_vocab_tree(triple_store_name, graph, vocab)
        
        self.output_vocab_data(vocab_tree, output_stream)
        
        
    def resolve_ConceptScheme_indirection(self, triple_store_name=None):
        '''
        Function to resolve ConceptScheme indirection by copying predicates and objects from ldv:currentVersion and/or owl:sameAs ConceptSchemes
        Fix will only be performed on URIs matching self.settings['fix_indirection_regex']
        '''
        # Skip this operation if no URI regex defined
        if not self.settings.get('fix_indirection_uri_regex'):
            return 
    
        for graph_name in self.get_graph_names(triple_store_name=triple_store_name):
            # Only perform this operation on specified vocabs
            if not re.search(self.settings['fix_indirection_uri_regex'], graph_name):
                continue
            
            logger.debug('Resolving ConceptScheme indirection in vocab {}'.format(graph_name))
            
            sparql_query = '''PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ldv: <http://purl.org/linked-data/version#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
INSERT {{ 
    GRAPH <{vocab_uri}> 
    {{ ?subject ?predicate ?object }}
}}
WHERE {{
    {{ GRAPH ?graph {{
        {{
            ?subject a skos:ConceptScheme .
            ?subject (ldv:currentVersion | owl:sameAs)+ ?equivalentConceptScheme .
            ?equivalentConceptScheme a skos:ConceptScheme .
            ?equivalentConceptScheme ?predicate ?object .
            FILTER(?subject = <{vocab_uri}>)
            FILTER (?object != <{vocab_uri}>)
        }}
        UNION
        {{
            ?object a skos:ConceptScheme .
            ?object (ldv:currentVersion | owl:sameAs)+ ?equivalentConceptScheme .
            ?equivalentConceptScheme a skos:ConceptScheme .
            ?subject ?predicate ?equivalentConceptScheme .
            FILTER(?object = <{vocab_uri}>)
            FILTER (?subject != <{vocab_uri}>)
        }}
        FILTER NOT EXISTS {{ ?subject ?predicate ?object }}
        #FILTER STRSTARTS(STR(?predicate), STR(skos:))
    }} }}
    UNION
    {{
        {{
            ?subject a skos:ConceptScheme .
            ?subject (ldv:currentVersion | owl:sameAs)+ ?equivalentConceptScheme .
            ?equivalentConceptScheme a skos:ConceptScheme .
            ?equivalentConceptScheme ?predicate ?object .
            FILTER(?subject = <{vocab_uri}>)
            FILTER (?object != <{vocab_uri}>)
        }}
        UNION
        {{
            ?object a skos:ConceptScheme .
            ?object (ldv:currentVersion | owl:sameAs)+ ?equivalentConceptScheme .
            ?equivalentConceptScheme a skos:ConceptScheme .
            ?subject ?predicate ?equivalentConceptScheme .
            FILTER(?object = <{vocab_uri}>)
            FILTER (?subject != <{vocab_uri}>)
        }}
        FILTER NOT EXISTS {{ ?subject ?predicate ?object }}
        #FILTER STRSTARTS(STR(?predicate), STR(skos:))
    }}
}}
'''.format(vocab_uri=graph_name)

            #print(sparql_query)
            self.submit_sparql_query(sparql_query, triple_store_name)
            
        return
        
                
    def flatten_linksets(self, triple_store_name=None):
        '''
        Function to flatten linksets
        Fix will only be performed on URIs matching self.settings['flatten_linkset_uri_regex']
        '''
        # Skip this operation if no URI regex defined
        if not self.settings.get('flatten_linkset_uri_regex'):
            return 
    
        for graph_name in self.get_graph_names(triple_store_name=triple_store_name):
            # Only perform this operation on specified vocabs
            if not re.search(self.settings['flatten_linkset_uri_regex'], graph_name):
                continue
            
            logger.debug('Flattening linkset {}'.format(graph_name))
            
            sparql_query = '''PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ldv: <http://purl.org/linked-data/version#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfsn: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
INSERT {{ GRAPH <{linkset_graph}>
    {{ ?subject ?predicate ?object }}
    }}
WHERE {{
    {{ GRAPH <{linkset_graph}> {{
        {{ 
        ?statement a rdfsn:Statement .
        ?statement rdfsn:subject ?subject .
        ?statement rdfsn:predicate ?predicate .
        ?statement rdfsn:object ?object .
        }}
        UNION # Commutative predicates skos:exactMatch or skos:closeMatch
        {{ ?statement a rdfsn:Statement .
        ?statement rdfsn:subject ?object .
        ?statement rdfsn:predicate ?predicate .
        ?statement rdfsn:object ?subject .
        FILTER((?predicate = skos:exactMatch) || (?predicate = skos:closeMatch))
        }}
        UNION
        {{ ?statement a rdfsn:Statement .
        ?statement rdfsn:subject ?object .
        ?statement rdfsn:predicate ?broadMatch .
        BIND(skos:narrowMatch AS ?predicate)
        ?statement rdfsn:object ?subject .
        FILTER(?broadMatch = skos:broadMatch)
        }}
        UNION
        {{
        ?statement a rdfsn:Statement .
        ?statement rdfsn:subject ?object .
        ?statement rdfsn:predicate ?narrowMatch .
        BIND(skos:broadMatch AS ?predicate)
        ?statement rdfsn:object ?subject .
        FILTER(?narrowMatch = skos:narrowMatch)
        }} 
    }} 
    FILTER NOT EXISTS {{ ?subject ?predicate ?object }}
    }}
}}
'''.format(linkset_graph=graph_name)

            print(sparql_query)
            self.submit_sparql_query(sparql_query, triple_store_name)
            
            logger.debug('Purging bad SKOS matches from linkset {}'.format(graph_name))
            
            sparql_query = '''PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
DELETE {{ GRAPH <{linkset_graph}> 
    {{ ?s ?p ?o }}
}} 
WHERE {{GRAPH <{linkset_graph}>
    {{?s ?p ?o .}}
    FILTER (STRSTARTS(STR(?p), STR(skos:)))
    FILTER (STRSTARTS(STR(?s), '{linkset_graph}/statement/') ||
        STRSTARTS(STR(?o), '{linkset_graph}/statement/')
        )
}}
'''.format(linkset_graph=graph_name)

            print(sparql_query)
            self.submit_sparql_query(sparql_query, triple_store_name)
            
        return
        
                
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

