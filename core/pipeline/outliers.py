# -*- coding: utf-8 -*-
"""Module for outliers detection
This module is responsible for the following tasks:
    - Execute Weakly Connected Components (WCC) algorithm on the given projection
    - Calculate the component size distribution of the WCC results
    - Execute data degree centrality algorithms on the given projection
    - Based on the distribution of the degree centrality, identify outliers in the projection
    - Add node labels on the nodes that are not identified as outliers
    - Exucute WCC again on the projection without including the outlier nodes
    - Recalculate the component size distribution of the WCC results after adding the outlier labels
    - Write to file the component size distribution before and after the outlier analysis
    - Write the algorithm results to the Neo4j database
"""
import logging
import os
import time
from typing import Dict, Any, List
from pyneoinstance import Neo4jInstance
from .context import GDSConnection
from utils.functions import get_logger
from datetime import date

class OutliersPipeline:
  def __init__(
        self,
        connection: GDSConnection,
        config: Dict[str, Any]
      ):
    
    self.connection = connection
    self.gds = self.connection.get_gds()
    self.config = config
    self.queries = config['cypher']
    self.logger = get_logger('OutliersPipeline', level=logging.INFO)
    self.labels = self.config['degreeProjectionNodeLabels']
    self.concurrency = self.config['concurrency']
    self.features_labels = [l for l in self.labels if 'Asegurado' not in l]
    self.asegurado_label = [l for l in self.labels if l not in self.features_labels]
    self.graph = Neo4jInstance(
        os.getenv('NEO4J_URI'),
        os.getenv('NEO4J_USER'),
        os.getenv('NEO4J_PASSWORD')
      )
    self.logger.info("Starting Outliers Pipeline")
    self.remove_prior_labels()
    self.create_projection()
    self.wcc("WccId","pre")
    self.data_degree_property = self.config['degreeAttributePropertyName']
    self.asegurado_degree_property = self.config['degreeAseguradoPropertyName']
    self.degree(self.data_degree_property, "REVERSE")
    self.degree(self.asegurado_degree_property, "NATURAL")
    self.write_wcc_distribution("pre")
    self.write_results(['preWccId',self.asegurado_degree_property])
    time.sleep(10)
    self.transform_degree()
    self.outlier_detection()
    self.wcc("WccId","post")
    self.write_wcc_distribution("post")
    self.write_results(['postWccId'])
    self.logger.info("Completed Outliers Pipeline")
  
  def create_projection(self):
    self.projection = self.connection.create_projection(
        self.config['degreeProjectionName'],
        self.labels,
        self.config['degreeProjectionRelationshipTypes'],
        concurrency=self.concurrency
      )

  def remove_prior_labels(self):
    msg = 'Removing prior outliers analysis results'
    self.logger.info(msg)
    for label in self.features_labels:
      query = self.config['cypher']['removeLabel'].format(
        nodeLabel = f'Valid{label}'
      )
    self.graph.execute_write_query(
      query,
      database=os.getenv('NEO4J_DATABASE')
    )

  def filter_projection(self):
    filter_name = "filtered_projection"
    self.degree("degree","UNDIRECTED",self.concurrency)
    self.logger.info("Removing nodes with out relationships")
    if self.gds.graph.exists(filter_name)['exists']:
      self.gds.graph.drop(filter_name)

      proj, results = self.gds.graph.filter(
        filter_name,
        self.projection,
        "n.degree > 0.0",
        "*",
        concurrency=self.concurrency
      )
      nodes_kept = results['nodeCount']
      relationships_kept = results['relationshipCount']
      time = results['projectMillis']
      message = f"Filtered projection created with {nodes_kept:,.0f} node and {relationships_kept:,.0f} relationships in {time} ms"
      self.logger.info(message)
      self.projection.drop()
      self.projection = proj
    
  def write_results(self, properties: List[str]):
    self.logger.info(f"Writing properties {properties} for label {self.asegurado_label[0]} to database")
    self.gds.graph.nodeProperties.write(
      self.projection,
      properties,
      self.asegurado_label,
      writeConcurrency=self.concurrency
    )
    if 'postWccId' not in properties:
      self.logger.info("Writing features nodes properties to database")
      self.gds.graph.nodeProperties.write(
        self.projection,
        [self.data_degree_property],
        self.features_labels,
        writeConcurrency=self.concurrency
      )
    self.projection.drop()

  def transform_degree(self):  
    for label in self.features_labels:
      message = f"Transforming property {self.data_degree_property} for label {label}"
      self.logger.info(message)
      query = self.transform_query.format(
        nodeLabel=label,
        propertyName=self.data_degree_property
        )
      self.graph.execute_write_query(
        query,
        database=os.getenv('NEO4J_DATABASE')
      )

  def wcc(self, property_name: str,type:str):
    property=f"{type}{property_name}"
    if type=='post':
      self.labels = self.asegurado_label + [f'Valid{l}'for l in self.features_labels]
      self.create_projection()
    results = self.gds.wcc.mutate(
      self.projection,
      mutateProperty=property,
      concurrency=self.concurrency
    )
    components_found = results['componentCount']
    properties_added = results['nodePropertiesWritten']
    component_size_dist = {k:round(v,1) for k,v in results['componentDistribution'].items()}
    time = results['preProcessingMillis'] + \
           results['computeMillis'] + \
           results['postProcessingMillis'] + \
           results['mutateMillis']
    message = f"Completed WCC {type}-outliers in {time:,.0f} ms"
    self.logger.info(message)
    message = f"Found {components_found:,.0f} components and added {properties_added:,.0f} node properties with the following distribution {component_size_dist}."
    self.logger.info(message)
    if type=='post':
      self.logger.info("Writing WCC post outliers results to database")
      self.gds.graph.nodeProperties.write(
        self.projection,
        [property],
        self.asegurado_label,
        writeConcurrency=self.concurrency
      )

  def degree(self, property_name: str, orientataion: str):
    results = self.gds.degree.mutate(
      self.projection,
      mutateProperty=property_name,
      concurrency=self.concurrency,
      orientation=orientataion)
    degree_type = ""
    if orientataion == "NATURAL":
      degree_type = "asegurado"
    elif orientataion == "REVERSE":
      degree_type = "data"
    properties_added = results['nodePropertiesWritten']
    degree_dist = {k:round(v,1) for k,v in results['centralityDistribution'].items()}
    time = results['preProcessingMillis'] + \
           results['computeMillis'] + \
           results['postProcessingMillis'] + \
           results['mutateMillis']
    message = f"Completed {degree_type} degree centrality in {time:,.0f} ms."
    self.logger.info(message)
    message = f"Added {properties_added:,.0f} node properties with the following distribution {degree_dist}."
    self.logger.info(message)

  def transform_degree(self):
    for label in self.features_labels:
      message = f"Transforming {self.data_degree_property} for label {label}"
      self.logger.info(message)
      query = self.queries['degreeTransform'].format(
        nodeLabel=label,
        propertyName=self.data_degree_property
      )
      self.graph.execute_write_query(
        query, 
        database=os.getenv('NEO4J_DATABASE')
      )

  def outlier_detection(self):
    stds = self.config['stds'] 
    outlier_query = self.queries['validCharacteristics']
    for label in self.features_labels:
      message = f"Detecting outliers for label {label} with stds {stds[label]}"
      self.logger.info(message)
      query = outlier_query.format(
        nodeLabel=label,
        std=stds[label]
      )
      self.graph.execute_write_query(query, database=os.getenv('NEO4J_DATABASE'))
      self.logger.info(f"Writing the outlier results to the results directory")
      results = self.graph.execute_read_query(
        self.queries['outlierStats'],
        database=os.getenv('NEO4J_DATABASE')
      )
      today = date.today().strftime("%Y%m%d")
      results.to_csv(f"results/outliers_{today}.csv", index=False)
  
  def write_wcc_distribution(self, type: str):
    query = self.queries['wccDistribution'].format(
      wccType=type
    )
    self.logger.info(f"Writing WCC {type}-outliers distribution to results directory")
    results = self.graph.execute_read_query(
      query,
      database=os.getenv('NEO4J_DATABASE')
    )
    today = date.today().strftime("%Y%m%d")
    results.to_csv(f"results/wcc_{type}outlier_distribution_{today}.csv", index=False)