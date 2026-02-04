from graphdatascience import GraphDataScience
from core import GDSConnection
from dotenv import load_dotenv
import os
import pandas as pd

def main():    
  uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
  user = os.getenv('NEO4J_USER', 'neo4j')
  password = os.getenv('NEO4J_PASSWORD', 'password')
  database = os.getenv('NEO4J_DATABASE', 'neo4j')

  if not uri or not user or not password:
      raise ValueError("NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD must be set in environment variables.")

  conn = GDSConnection(uri, user, password, database)
  # gds = GraphDataScience(uri, auth=(user, password), database=database)

  print('GDS version: ', conn.gds.version())

  df = pd.DataFrame(conn.gds.graph.list())
  print(df)

  # proj = conn.gds.graph.project(
  #       "graph_projection_test",
  #       ['Asegurado', 'Telefono'],
  #       ['TIENE_TELEFONO'],
  #       readConcurrency=48
  #     )
  
if __name__ == "__main__":
    load_dotenv()
    main()