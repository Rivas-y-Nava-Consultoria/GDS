from __future__ import annotations
from graphdatascience import GraphDataScience
from typing import Tuple, Optional, Dict, List, Any
from .context import get_logger
import logging

class GDSConnection:

  def __init__(self, uri: str, user: str, password: str, database: str) -> None:
    self.logger = get_logger('GDSConnection', level=logging.INFO)
    self.gds = GraphDataScience(uri, auth=(user, password), database=database)
  
  def close(self):
    self.gds.close()

  def create_projection(self, name: str, node_labels: List[str], relationship_types: List[str], concurrency = 48) -> Any:    
    proj_stats, proj = self.gds.graph.project(
        name,
        node_labels,
        relationship_types,
        readConcurrency=concurrency
    )

    self.logger.info(proj_stats)
    return proj