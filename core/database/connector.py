from __future__ import annotations
from graphdatascience import GraphDataScience
from typing import Tuple, Optional, Dict, List, Any
from .context import get_logger
import logging

class GDSConnection:

  def __init__(self, uri: str, user: str, password: str, database: str) -> None:
    self.logger = get_logger('GDSConnection', level=logging.INFO)
    self.gds = GraphDataScience(uri, auth=(user, password), database=database)
  
  def get_gds(self) -> GraphDataScience.Graph:
    return self.gds
  
  def close(self):
    self.gds.close()

  def create_projection(self, name: str, node_labels: List[str], relationship_types: List[str], concurrency = 48) -> GraphDataScience.Graph:    
    if self.gds.graph.exists(name)['exists']:
      self.gds.graph.drop(name)
    proj, proj_stats  = self.gds.graph.project(
        name,
        node_labels,
        relationship_types,
        readConcurrency=concurrency
    )
    num_nodes = proj_stats['nodeCount']
    num_relas = proj_stats['relationshipCount']
    time = proj_stats['projectMillis']
    message = f"Creating projection {name} with node {num_nodes:,.0f} and relationship types {num_relas:,.0f} in {time} ms"
    self.logger.info(message)
    return proj
  
  def create_cypher_projection(self, name: str, query: str) -> GraphDataScience.Graph:
    if self.gds.graph.exists(name)['exists']:
      self.gds.graph.drop(name)
    proj_stats = self.gds.run_cypher(query)
    num_nodes = proj_stats['nodeCount'].iloc[0]
    num_relas = proj_stats['relationshipCount'].iloc[0]
    time = proj_stats['projectMillis'].iloc[0]
    message = f"Creating cypher projection {name} with node {num_nodes:,.0f} and relationship types {num_relas:,.0f} in {time:,.0f} ms"
    proj = self.gds.graph.get(name)
    self.logger.info(message)
    return proj