# -*- coding: utf-8 -*-
"""Module for similarity decalculation
This module is responsible for the following tasks:
    - Create projection for similarity calculations 
    - Create node embeddings for all nodes in the projection
    - Execute KNN to compute the data similarity between the Asegurado nodes
    - Calculate the name similarity using Sorensen similarity algorithm between Asegurado nodes
    - Compute the confidence score based on the data similarity and name similarity
    - Execute Louvain algorithm to create clusters of Asegurados based on the confidence score
    - Create golder record based on the clusters obtained from Louvain algorithm
"""
import logging
import os
from typing import Dict, Any
from pyneoinstance import Neo4jInstance
from .context import GDSConnection
from utils.functions import get_logger

class SimilarityPipeline:
  def __init__(
        self,
        connection: GDSConnection,
        config: Dict[str, Any]
      ):
    
    self.connection = connection
    self.gds = self.connection.get_gds()
    self.config = config
    self.queries = config['cypher']
    self.graph = Neo4jInstance(
        os.getenv('NEO4J_URI'),
        os.getenv('NEO4J_USER'),
        os.getenv('NEO4J_PASSWORD')
    )
    self.logger = get_logger('SimilarityPipeline', level=logging.INFO)
    self.asegurado_label = [l for l in self.config['degreeProjectionNodeLabels'] if 'Asegurado' in l]
    
    self.create_projection('similarity')
    self.generate_embeddings()
    self.compute_data_similarity()
    self.projection.drop()
    self.compute_confidence()
    self.create_is_same_entity_rela()
    self.create_projection('clustering')
    self.resolve_entity()
    self.projection.drop()
    self.logger.info('Concluded the entity resolution pipeline')
    # self.remove_nuevo_label()

  def remove_nuevo_label(self):
    msg = f'Removing node label AseguradoNuevo'
    self.logger.info(msg)
    query = self.config['cypher']['removeLabel'].format(
      nodeLabel = 'AseguradoNuevo'
    )
    self.graph.execute_write_query(
        query, 
        database=os.getenv('NEO4J_DATABASE')
    )

  def create_projection(self, type: str):
    if type == 'similarity':
        projection_name = self.config['similarityProjectionName']
        query = self.queries['similarityProjection'].format(
            projName = projection_name,
            ponderacionTelefono = self.config['ponderacion']['Telefono'],
            ponderacionDomicilio = self.config['ponderacion']['Domicilio'],
            ponderacionCalendario = self.config['ponderacion']['Calendario'],
            ponderacionEmail = self.config['ponderacion']['Email'],
            ponderacionRFC = self.config['ponderacion']['RFC'],
            concurrency = self.config['concurrency']
        )
       
    elif type == 'clustering':
      projection_name = self.config['louvainProjectionName']
      query = self.queries['sameEntityProjection'].format(
            projName = projection_name,
            confidenceThreshold = self.config['confidenceThreshold'],
            concurrency = self.config['concurrency']
        )
    self.projection = self.connection.create_cypher_projection(
        projection_name,
        query
    )
  
  def generate_embeddings(self):
    self.logger.info("Generating node embeddings using FastRP algorithm")
    result = self.gds.fastRP.mutate(
      self.projection,
      embeddingDimension=self.config['embeddingDimension'],
      iterationWeights=self.config['embeddingIterationWeight'],
      relationshipWeightProperty='weight',
      concurrency=self.config['concurrency'],
      mutateProperty=self.config['embeddingPropertyName']
    )
    node_properties_written = result['nodePropertiesWritten']
    pre_processing_millis = result['preProcessingMillis']
    compute_millis = result['computeMillis']
    mutate_millis = result['mutateMillis']
    total_time = pre_processing_millis + compute_millis + mutate_millis
    msg = f'Generated {node_properties_written:,.0f} embeddigns in {total_time:,.0f} ms'
    self.logger.info(msg)
    filter_graph_name = self.config['similarityProjectionName']+"_filtered" 
    if self.gds.graph.exists(filter_graph_name)['exists']:
        self.gds.graph.drop(filter_graph_name)
    proj, _ =self.gds.graph.filter(
      self.config['similarityProjectionName']+"_filtered",
      self.projection,
      f'n:{self.asegurado_label[0]}',
      '*',
      concurrency=self.config['concurrency']
    )
    self.projection.drop()
    self.projection = proj
    self.logger.info('Writing embeddings to graph')
    self.gds.graph.nodeProperties.write(
      self.projection,
      self.config['embeddingPropertyName'],
      self.asegurado_label,
      writeConcurrency=self.config['concurrency']
    )

  def compute_data_similarity(self):
    self.logger.info(f'Deleting existing {self.config['similarityRelaType']}')
    query = self.config['cypher']['deleteRela'].format(
      nodeLabel='AseguradoNuevo',
      relaType=self.config['similarityRelaType']
    )
    self.graph.execute_write_query(
        query, 
        database=os.getenv('NEO4J_DATABASE')
    )
    self.logger.info("Computing data similarity using KNN algorithm")
    result = self.gds.knn.mutate(
      self.projection,
      topK = self.config['knnTopK'],
      similarityCutoff = self.config['similarityThreshold'],
      nodeProperties=self.config['embeddingPropertyName'],
      mutateRelationshipType=self.config['similarityRelaType'],
      mutateProperty=self.config['dataSimilarityPropertyName'],
      concurrency=self.config['concurrency'],
      nodeLabels=self.asegurado_label,
      initialSampler='randomWalk',
      sampleRate=self.config['knnSampleRate']
    )
    pre_processing_millis = result['preProcessingMillis']
    post_processing_millis = result['postProcessingMillis']
    compute_millis = result['computeMillis']
    mutate_millis = result['mutateMillis']
    total_time = pre_processing_millis + post_processing_millis + compute_millis + mutate_millis
    node_pairs = result['nodePairsConsidered']
    similarity_dist = result['similarityDistribution']
    msg = f'Computed similarity between {node_pairs:,.0f} nodes in {total_time:,.0f} ms with distribution {similarity_dist}'
    self.logger.info(msg)
    self.logger.info(f'Writing new relationships {self.config['similarityRelaType']}')
    result = self.gds.graph.relationship.write(
        self.projection,
        self.config['similarityRelaType'],
        self.config['dataSimilarityPropertyName'],
        writeConcurrency = self.config['concurrency']
    )
    write_millis = result['writeMillis']
    rela_type = result['relationshipType']
    rela_prop = result['relationshipProperty']
    rela_written = result['relationshipsWritten']
    msg = f'Wrote {rela_written:,.0f} {rela_type} relationships with property {rela_prop} in {write_millis:,.0f} ms'
    self.logger.info(msg)

  def compute_confidence(self):
    msg = 'Computing confidence score and adding explainabilty'
    self.logger.info(msg)
    query = self.config['cypher']['explainabilty'].format(
      nodeLabel = 'AseguradoNuevo',
      relaType = self.config['similarityRelaType'],
      dataSimilarityWeight = self.config['dataSimilarityWeight'],
      nameSimilarityWeight = self.config['nameSimilarityWeight']
    )
    self.graph.execute_write_query(
      query,
      database=os.getenv('NEO4J_DATABASE')
    )

  def create_is_same_entity_rela(self):
    msg='Deleting previous IS_SAME_ENTITY_WITH relationships'
    self.logger.info(msg)
    query = self.config['cypher']['deleteRela'].format(
      nodeLabel = 'AseguradoNuevo',
      relaType = 'IS_SAME_ENTITY_WITH'
    )
    self.graph.execute_write_query(
      query,
      database=os.getenv('NEO4J_DATABASE')
    )
    msg = 'Creating IS_SAME_ENTITY_WITH relationship'
    self.logger.info(msg)
    query = self.config['cypher']['isSameEntityWithRela'].format(
      nodeLabel = 'AseguradoNuevo',
      relaType = self.config['similarityRelaType'],
      confidenceThreshold = self.config['confidenceThreshold']
    )
    self.graph.execute_write_query(
      query,
      database=os.getenv('NEO4J_DATABASE')
    )
  
  def resolve_entity(self):
    msg = 'Deleting existing EntidadAsegurado nodes'
    self.logger.info(msg)
    query = self.config['cypher']['deleteNode'].format(
      nodeLabel = self.config['entidadAseguradoLabel']
    )
    self.graph.execute_write_query(
      query,
      database=os.getenv('NEO4J_DATABASE')
    )
    msg = 'Executing the Louvain clustering algorithm in write mode'
    self.logger.info(msg)
    result = self.gds.louvain.write(
      self.projection,
       writeProperty = 'louvainId',
       relationshipWeightProperty = 'confidence',
       maxLevels = 1,
       concurrency = self.config['concurrency']
    )
    time = result['preProcessingMillis'] + \
           result['computeMillis'] + \
           result['postProcessingMillis'] + \
           result['writeMillis']
    props_written = result['nodePropertiesWritten']
    num_clusters = result['communityCount']
    distribution = result['communityDistribution']
    msg = f'Louvain found {num_clusters:,.0f} in {time:,.0f} ms with distribution {distribution} and wrote {props_written:,.0f} properties'
    self.logger.info(msg)
    self.logger.info(f'Creating nodes {self.config['entidadAseguradoLabel']}')
    query = self.config['cypher']['createConstraint'].format(
      nodeLabel = self.config['entidadAseguradoLabel']
    )
    self.graph.execute_write_query(
      query,
      database=os.getenv('NEO4J_DATABASE')
    )
    query = self.config['cypher']['entityAseguradoNode'].format(
      nodeLabel = self.config['entidadAseguradoLabel']
    )
    self.graph.execute_write_query(
      query,
      database=os.getenv('NEO4J_DATABASE')
    )
    self.graph.execute_write_query(
      self.config['cypher']['entidadRela'],
      database=os.getenv('NEO4J_DATABASE')
    )