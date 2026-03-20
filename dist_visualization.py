from core import GDSConnection
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt

def main():
  gds_uri = os.getenv('GDS_URI', 'bolt://localhost:7687')
  user = os.getenv('NEO4J_USER', 'neo4j')
  password = os.getenv('NEO4J_PASSWORD')
  database = os.getenv('NEO4J_DATABASE', 'neo4j')
  conn = GDSConnection(gds_uri, user, password, database)
  gds = conn.get_gds()
  node_label = "Telefono"
  query = f"""
  MATCH(n:{node_label})
  RETURN 
    log(n.numeroAsegurados) AS numeroAseguradosScaled
  """
  print(query)
  data = gds.run_cypher(query)
  data.hist(bins=10)
  plt.title("Distribution of numeroAseguradosScaled")
  plt.xlabel("numeroAseguradosScaled")
  plt.ylabel("Frequency")
  plt.savefig(f"{node_label}_numeroAseguradosScaled_distribution.png")

if __name__ == "__main__":
    load_dotenv()
    main()