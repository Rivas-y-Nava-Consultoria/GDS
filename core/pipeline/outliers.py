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
from graphdatascience import Graph
from pyneoinstance import Neo4jInstance
from .context import GDSConnection
from utils.functions import get_logger
import logging
import os

class OutliersPipeline:
  def __init__(
      self, gds: GDSConnection, 
      projection: Graph,
      data_degree_property: str,
      asegurado_degree_property: str,
      concurrency: int,
      transform_query: str
      ):
    
    self.gds = gds
    self.projection = projection
    self.logger = get_logger('OutliersPipeline', level=logging.INFO)
    self.transform_query = transform_query

    filter_name = "filtered_projection"
    labels = self.projection.node_labels()
    features_labels = [l for l in labels if 'Asegurado' not in l]
    asegurado_label = [l for l in labels if l not in features_labels]
    self.logger.info("Starting Outliers Pipeline")
    self.degree("degree","UNDIRECTED",concurrency)
    self.logger.info("Removing nodes with out relationships")
    
    if self.gds.graph.exists(filter_name)['exists']:
      self.gds.graph.drop(filter_name)

    proj, results = self.gds.graph.filter(
      filter_name,
      self.projection,
      "n.degree > 0.0",
      "*",
      concurrency=concurrency
    )
    nodes_kept = results['nodeCount']
    relationships_kept = results['relationshipCount']
    time = results['projectMillis']
    message = f"Filtered projection created with {nodes_kept:,.0f} node and {relationships_kept:,.0f} relationships in {time} ms"
    self.logger.info(message)
    self.projection.drop()
    self.projection = proj
    self.wcc("WccId","pre",concurrency)
    self.degree(data_degree_property, "REVERSE", concurrency)
    self.degree(asegurado_degree_property, "NATURAL",concurrency)
    # self.logger.info("Writing degree property to database")
    # self.gds.graph.nodeProperties.write(
    #   self.projection,
    #   ['degree'],
    #   writeConcurrency=concurrency
    # )
    self.logger.info(f"Writing {asegurado_label[0]} properties to database")
    self.gds.graph.nodeProperties.write(
      self.projection,
      ['preWccId',asegurado_degree_property],
      asegurado_label,
      writeConcurrency=concurrency
    )
    self.logger.info("Writing features nodes properties to database")
    self.gds.graph.nodeProperties.write(
      self.projection,
      [data_degree_property],
      features_labels,
      writeConcurrency=concurrency
    )
    self.projection.drop()
    graph = Neo4jInstance(
      os.getenv('NEO4J_URI'),
      os.getenv('NEO4J_USER'),
      os.getenv('NEO4J_PASSWORD')
    )
    for label in features_labels:
      message = f"Transforming property {data_degree_property} for label {label}"
      self.logger.info(message)
      query = self.transform_query.format(
         nodeLabel=label,
         propertyName=data_degree_property
         )
    graph.execute_write_query(query, database=os.getenv('NEO4J_DATABASE'))
    self.logger.info("Completed Outliers Pipeline")

  def wcc(self, property_name: str,type:str,concurrency: int):
    property=f"{type}{property_name}"
    results = self.gds.wcc.mutate(
      self.projection,
      mutateProperty=property,
      concurrency=concurrency)
    components_found = results['componentCount']
    properties_added = results['nodePropertiesWritten']
    component_size_dist = {k:round(v,1) for k,v in results['componentDistribution'].items()}
    time = results['preProcessingMillis'] + \
           results['computeMillis'] + \
           results['postProcessingMillis'] + \
           results['mutateMillis']
    message = f"Completed WCC in {time:,.0f} ms"
    self.logger.info(message)
    message = f"Found {components_found:,.0f} components and added {properties_added:,.0f} node properties with the following distribution {component_size_dist}."
    self.logger.info(message)

  def degree(self, property_name: str, orientataion: str,concurrency: int):
    results = self.gds.degree.mutate(
      self.projection,
      mutateProperty=property_name,
      concurrency=concurrency,
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