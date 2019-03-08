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
from rdflib import Graph
from unidecode import unidecode

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Initial logging level for this module
logger.debug('__name__ = {}'.format(__name__))

class RDFUpdater(object):
    settings = None
    
    def __init__(self, 
                 settings_path=None, 
                 update_github=False,
                 debug=False):
        
        # Initialise and set debug property
        self._debug = None
        self.debug = debug

        package_dir = os.path.dirname(os.path.abspath(__file__))
        settings_path = settings_path or os.path.join(package_dir, 'rdf_updater_settings.yml')
        self.settings = yaml.safe_load(open(settings_path))
        
        if update_github:
            logger.info('Reading vocab configs from GitHub')
            self.settings['rdf_configs'].update(self.get_github_settings())
            
            logger.info('Writing updated vocab configs to settings file {}'.format(settings_path))
            with open(settings_path, 'w') as settings_file:
                yaml.safe_dump(self.settings, settings_file)
        
        #logger.debug('Settings: {}'.format(pformat(self.settings)))
        
        
    def get_rdfs(self):
        def get_rdf(rdf_config):
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
                url = rdf_config['uri']
                http_method = requests.get
                if rdf_config.get('format') == 'ttl':
                    url += '/' # SISSVoc needs to have a trailing slash
                    headers = None
                    params = {'_format': 'text/turtle'}
                else:
                    headers = {'Accept': 'application/rdf+xml',
                               'Accept-Encoding': 'UTF-8'
                               }
                    params = None

                    if rdf_config.get('rdf_url'):
                        url = rdf_config.get('rdf_url')
                    else: # Special case for ODM2
                        params = {'format': 'skos'}
                data = None
            else:
                raise Exception('Bad source type for RDF')
            #logger.debug('http_method = {}, url = {}, headers = {}, params = {}, data = {}'.format(http_method, url, headers, params, data))
            logger.info('Reading RDF from {} via {}'.format(url, rdf_config['source_type']))
            response = http_method(url, headers=headers, params=params, data=data, timeout=self.settings['timeout'])
            #logger.debug('Response content: {}'.format(str(response.content)))
            assert response.status_code == 200, 'Response status code != 200'
            return(response.content).decode('utf-8') # Convert binary to UTF-8 string
                
        logger.info('Reading RDFs from sources to files')    
        
        for _rdf_name, rdf_config in self.settings['rdf_configs'].items():
            logger.info('Obtaining data for {}'.format(rdf_config['name']))
            try:
                rdf = get_rdf(rdf_config)
                rdf = re.sub('^(\<\?xml version="1.0")\s*(\?\>.*)', '\\1 encoding="UTF-8"\\2', rdf) # Add encoding if missing
                rdf = re.sub('\r\n', '\n', rdf) # Fix bad EOLs
                
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
        
        
    def put_rdfs(self, skosified=True):
        def put_rdf(rdf_config, rdf):
            url = self.settings['triple_store']['url'] + '/data'
            if rdf_config.get('format') == 'ttl': 
                headers = {'Content-Type': 'text/turtle'}
            else:
                headers = {'Content-Type': 'application/rdf+xml'}
                
            username = self.settings['triple_store'].get('username')
            password = self.settings['triple_store'].get('password')
            
            if (username and password):
                logger.debug('Authenticating with username {} and password {}'.format(username, password))
                headers['Authorization'] = 'Basic ' + base64.encodebytes('{}:{}'.format(username, password).encode('utf-8')).strip().decode('utf-8')
            
            params = {'graph': rdf_config['uri']}
            
            logger.info('Writing RDF to {}'.format(url))
            #logger.debug('url = {}, headers = {}, params = {}'.format(url, headers, params))
            response = requests.put(url, headers=headers, params=params, data=rdf.encode('utf-8'), timeout=self.settings['timeout'])
            #logger.debug('Response content: {}'.format(response.content))
            assert response.status_code == 200 or response.status_code == 201, 'Response status code {}  != 200 or 201: {}'.format(response.status_code, response.content)
            return(response.content)
                
        logger.info('Writing RDFs to triple-store {} from files'.format(self.settings['triple_store']['url']))           
        for _rdf_name, rdf_config in self.settings['rdf_configs'].items():
            logger.info('Writing data for {} to triple-store'.format(rdf_config['name']))
            if skosified:
                rdf_file_path = os.path.splitext(rdf_config['rdf_file_path'])[0] + '_skos.rdf'
            else:
                rdf_file_path = rdf_config['rdf_file_path'] # Original RDF
                
            try:
                logger.info('Reading RDF from {}'.format(rdf_file_path))
                with open(rdf_file_path, 'r', encoding='utf-8') as rdf_file:
                    rdf = rdf_file.read()
                #logger.debug('rdf = {}'.format(rdf))
                result = json.loads(put_rdf(rdf_config, rdf))
                #logger.debug('result = {}'.format(result))
                logger.info('{} triples (re)written'.format(result['tripleCount']))
            except Exception as e:
                logger.error('ERROR: RDF put from file to triple-store failed: {}'.format(e))
                
        logger.info('Finished writing to triple-store')
        
     
    def get_github_settings(self):   
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
    
                    vocab_tree = etree.fromstring(response.content)
                    
                    # Find all collection elements
                    collection_elements = vocab_tree.findall(path='skos:Collection', namespaces=vocab_tree.nsmap)
                    if not collection_elements: #No skos:collections defined - look for resource element parents instead                      
                        logger.warning('WARNING: {} has no explicit skos:Collection elements'.format(rdf_name))
                        resource_elements = vocab_tree.findall(path='.//rdf:Description/rdf:type[@rdf:resource="http://www.w3.org/2004/02/skos/core#Collection"]', namespaces=vocab_tree.nsmap)
                        collection_elements = [resource_element.getparent() for resource_element in resource_elements]
                    
                    #logger.debug('collection_elements = {}'.format(pformat(collection_elements)))
                    
                    if len(collection_elements) == 1:
                        collection_element = collection_elements[0]
                        collection_uri = collection_element.attrib.get('{' + vocab_tree.nsmap['rdf'] + '}about')
                    else:
                        logger.warning('WARNING: {} has multiple Collection elements'.format(rdf_name))
                        #TODO: Make this work better when there are multiple collections in one RDF
                        # Find shortest URI for collection and use that for named graphs
                        # This is a bit nasty, but it works for poorly-defined subcollection schemes
                        collection_element = None
                        collection_uri = None
                        for search_collection_element in collection_elements:
                            search_collection_uri = search_collection_element.attrib.get('{' + vocab_tree.nsmap['rdf'] + '}about')
                            if (not collection_uri) or len(search_collection_uri) < len(collection_uri):
                                collection_uri = search_collection_uri
                                collection_element = search_collection_element
                        
                    label_element = collection_element.find(path = 'rdfs:label', namespaces=vocab_tree.nsmap)
                    if label_element is None:
                        label_element = collection_element.find(path = 'dcterms:title[@{http://www.w3.org/XML/1998/namespace}lang="en"]', namespaces=vocab_tree.nsmap)
                    collection_label = label_element.text
                                            
                except Exception as e:
                    logger.warning('Unable to find collection information in {}: {}'.format(rdf_url, e))
                    continue
                
                collection_dict = {'name': collection_label,
                               'uri': collection_uri,
                               'source_type': 'http_get',
                               'rdf_file_path': github_config['rdf_dir'] + '/' + rdf_name,
                               'rdf_url': rdf_url
                               }
                logger.debug('collection_dict = {}'.format(pformat(collection_dict)))
                result_dict[os.path.splitext(rdf_name)[0]] = collection_dict
        return result_dict  
    
    def skosify_rdfs(self):
        def skosify_rdf(rdf_config, root_logger):
            rdf_file_path = rdf_config['rdf_file_path']
            skos_rdf_file_path = os.path.splitext(rdf_file_path)[0] + '_skos.rdf'
            skos_nt_file_path = os.path.splitext(rdf_file_path)[0] + '_skos.nt'
            log_file_name = os.path.splitext(rdf_file_path)[0] + '.log'
            
            logger.info('SKOSifying RDF from {}'.format(rdf_file_path))

            # The following is a work-around for a unicode issue in rdflib
            rdf_file = open(rdf_file_path, 'rb') # Note binary reading
            rdf = Graph()
            rdf.parse(rdf_file, format='xml')
            rdf_file.close()
            
            # Capture SKOSify WARNING level output to log file    
            try:
                os.remove(log_file_name)
            except:
                pass
            log_file_handler = logging.FileHandler(log_file_name)
            log_file_handler.setLevel(logging.WARNING)
            log_file_formatter = logging.Formatter('%(message)s')
            log_file_handler.setFormatter(log_file_formatter)
            root_logger.addHandler(log_file_handler)
            
            skos_rdf = skosify.skosify(rdf, 
                                  label=rdf_config['name'],
                                  eliminate_redundancy=True,
                                  preflabel_policy='all' #TODO: This is necessary to avoid a unicode bug in skosify - fix it
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
            #logger.info('Validating data for {}'.format(rdf_config['name']))
            
            try:
                skosify_rdf(rdf_config, root_logger)
            except Exception as e:
                logger.warning('RDF SKOSification from file {} failed: {}'.format(rdf_config['rdf_file_path'], e))
                continue
                        
        logger.info('SKOSification of RDF files completed')
    
    def submit_sparql_query(self, sparql_query, accept_format='json'):
        '''
        Function to submit a sparql query and return the textual response
        '''
        accept_format = {'json': 'application/json',
                         'xml': 'application/xml'}.get(accept_format) or 'application/json'
        headers = {'Accept': accept_format,
                   'Content-Type': 'application/sparql-query',
                   'Accept-Encoding': 'UTF-8'
                   }
        username = self.settings['triple_store'].get('username')
        password = self.settings['triple_store'].get('password')
            
        if (username and password):
            logger.debug('Authenticating with username {} and password {}'.format(username, password))
            headers['Authorization'] = 'Basic ' + base64.encodebytes('{}:{}'.format(username, password).encode('utf-8')).strip().decode('utf-8')
            
        params = None
        response = requests.post(self.settings['triple_store']['url'], 
                               headers=headers, 
                               params=params, 
                               data=sparql_query, 
                               timeout=self.settings['timeout'])
        #logger.debug('Response content: {}'.format(str(response.content)))
        assert response.status_code == 200, 'Response status code != 200'
        return(response.content).decode('utf-8') # Convert binary to UTF-8 string
    
    def get_graph_names(self):
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
        return [bindings_dict['graph']['value']
                for bindings_dict in json.loads(self.submit_sparql_query(sparql_query))["results"]["bindings"]
                ]
            
        
        
    def get_collection_data(self, graph_name):
        '''
        Function to generate a tree of all collections and concepts in a given graph
        '''
        
        def get_concept_tree(bindings_list, collection, broader_concept=None):
            '''
            Recursive helper function to generate tree of broader/narrower concepts in graph
            '''
            def get_narrower_concepts(bindings_list, collection, broader_concept):
                '''
                Helper function to generate sublist of narrower concepts for a given broader concept
                N.B: when broader_concept is None, the list will contain top concepts and also 
                concepts with broader concepts in other collections
                '''
                collection_concepts = set([bindings_dict['concept']['value'] 
                                       for bindings_dict in bindings_list
                                       if (bindings_dict['collection']['value'] == collection)
                                       ])
                
                bindings_sublist = [bindings_dict for bindings_dict in bindings_list
                                    if (bindings_dict['collection']['value'] == collection
                                        and (
                                             (broader_concept is None and 
                                                ((bindings_dict.get('broader_concept') is None) # Top concept?
                                                 or (bindings_dict['broader_concept']['value'] not in collection_concepts))) # Broader concept not in same collection
                                             or ((broader_concept is not None) and (bindings_dict.get('broader_concept') is not None) 
                                                 and (bindings_dict['broader_concept']['value'] == broader_concept))
                                             )
                                        )
                                    ]
                #print(collection, broader_concept, bindings_sublist)
                
                return bindings_sublist
            
            concept_tree_dict = {}
            
            for bindings_dict in get_narrower_concepts(bindings_list, collection, broader_concept):
                concept = bindings_dict["concept"]["value"]
                
                concept_dict = {'preflabel': bindings_dict["concept_preflabel"]["value"]}
                
                if bindings_dict.get('concept_description'):
                    concept_dict['description'] = bindings_dict["concept_description"]["value"]
                    
                narrower_concept_tree_dict = get_concept_tree(bindings_list, collection, broader_concept=concept)
                if narrower_concept_tree_dict:
                    concept_dict['narrower_concepts'] = narrower_concept_tree_dict
                
                concept_tree_dict[concept] = concept_dict
                
            return concept_tree_dict
                
        
        sparql_query = '''PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?collection ?collection_label ?concept ?concept_preflabel ?concept_description ?broader_concept
FROM <{graph_name}>
WHERE {
    OPTIONAL {?collection a skos:Collection .}
    OPTIONAL {?collection a skos:ConceptScheme .}
    OPTIONAL {?collection skos:member ?concept .}
    OPTIONAL {?concept skos:inScheme ?collection .}
    ?concept skos:prefLabel ?concept_preflabel .
    OPTIONAL {?collection rdfs:label ?collection_label .}
    OPTIONAL {?concept skos:definition ?concept_description .}
    OPTIONAL {?concept skos:broader ?broader_concept .}
    FILTER(lang(?concept_preflabel) = "en" || lang(?concept_preflabel) = "")
}
'''.replace('{graph_name}', graph_name)
#(lang(?collection_label) = "en" || lang(?collection_label) = "")
#(lang(?concept_description) = "en" || lang(?concept_description) = "")

        response_dict = json.loads(self.submit_sparql_query(sparql_query)
                                 )  
        bindings_list = response_dict["results"]["bindings"]
        #pprint(bindings_list)
              
        result_dict = {bindings_dict['collection']['value']: 
            {'label': (bindings_dict['collection_label']['value'] 
                      if bindings_dict.get('collection_label')
                      else os.path.basename(bindings_dict['collection']['value']))}
            for bindings_dict in bindings_list}
        
        for collection, collection_dict in result_dict.items():
            collection_dict['concepts'] = get_concept_tree(bindings_list, collection, broader_concept=None) 
            
        return result_dict
    
    
    def output_collection_data(self, concept_tree_dict, output_stream=sys.stdout, level=0):
        '''
        Recursive function to output concept_tree_dict to specified stream
        '''
        if not level:
            for collection, collection_dict in concept_tree_dict.items():
                output_stream.write(unidecode('Collection "{}": {}\n'.format(collection_dict['label'], collection))) 
                #print(collection_dict)
                self.output_collection_data(collection_dict['concepts'], output_stream, level=level+1)
        else:
            for concept, concept_dict in concept_tree_dict.items():
                #print(concept_dict)
                output_stream.write(unidecode('{}Concept "{}": {} ({})\n'.format(('  ' * level),
                    concept_dict['preflabel'], 
                    concept,
                    concept_dict.get('description') or ''))
                    ) 
                narrower_concepts_dict = concept_dict.get('narrower_concepts')
                if narrower_concepts_dict:
                    self.output_collection_data(narrower_concepts_dict, output_stream, level=level+1)


    def output_summary_text(self):               
        '''
        Function to output summary text file
        '''
        summary_output_path = self.settings.get('summary_output_path')
        if summary_output_path:
            logger.debug('Outputting summary text to {}'.format(summary_output_path))
            output_stream = open(summary_output_path, 'w')
        else:
            output_stream = sys.stdout
                    
        for graph_name in self.get_graph_names():
            output_stream.write('Graph: {}'.format(graph_name) + '\n')
        
            collection_data = self.get_collection_data(graph_name)
            #pprint(collection_data)
            
            self.output_collection_data(collection_data, output_stream)
            output_stream.write('\n')
                
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

