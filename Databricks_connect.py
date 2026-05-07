import os
os.environ["DATABRICKS_CONFIG_PROFILE"] = "vscode_nageswar_mac"

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless(True).getOrCreate()

df = spark.sql("SELECT 'Hello from Serverless!' as message")
df.show()
