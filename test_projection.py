from core import GDSConnection
from core import OutliersPipeline
from dotenv import load_dotenv
import os
import pandas as pd

def main():    
  uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
  user = os.getenv('NEO4J_USER', 'neo4j')
  password = os.getenv('NEO4J_PASSWORD')
  database = os.getenv('NEO4J_DATABASE', 'neo4j')

  if not password:
      raise ValueError("NEO4J_PASSWORD must be set in environment variables.")

  conn = GDSConnection(uri, user, password, database)
  gds = conn.get_gds()
  projection = conn.create_projection(
        "test",
        ['Asegurado', 'Telefono'],
        ['TIENE_TELEFONO'],
        concurrency=48
      )
  outlier_pipeline = OutliersPipeline(
     gds,
     projection,
     data_degree_property="numeroAsegurados",
     asegurado_degree_property="numeroData",
     concurrency=48)
  conn.close()
if __name__ == "__main__":
    load_dotenv()
    main()