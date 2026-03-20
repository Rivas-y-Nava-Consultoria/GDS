from core import GDSConnection
from core import OutliersPipeline
from core import SimilarityPipeline
from dotenv import load_dotenv
import yaml
import os
import pandas as pd

def main():    
  gds_uri = os.getenv('GDS_URI', 'bolt://localhost:7687')
  user = os.getenv('NEO4J_USER', 'neo4j')
  password = os.getenv('NEO4J_PASSWORD')
  database = os.getenv('NEO4J_DATABASE', 'neo4j')

  if not password:
      raise ValueError("NEO4J_PASSWORD must be set in environment variables.")
  
  with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)
 
  conn = GDSConnection(gds_uri, user, password, database)
  
  OutliersPipeline(
     conn,
     config
  )
  SimilarityPipeline(
      conn,
      config
  )
  conn.close()

if __name__ == "__main__":
    load_dotenv()
    main()