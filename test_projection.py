from core import GDSConnection
from core import OutliersPipeline
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
  gds = conn.get_gds()
  projection = conn.create_projection(
        config['degreeProjectionName'],
        config['degreeProjectionNodeLabels'],
        config['degreeProjectionRelationshipTypes'],
        concurrency=config['concurrency']
      )
  outlier_pipeline = OutliersPipeline(
     gds,
     projection,
     data_degree_property=config['degreeAttributePropertyName'],
     asegurado_degree_property=config['degreeAseguradoPropertyName'],
     concurrency=config['concurrency'],
     transform_query=config['cypher']['degreeTransform'])
  conn.close()

if __name__ == "__main__":
    load_dotenv()
    main()