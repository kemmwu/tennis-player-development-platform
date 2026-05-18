import os
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

client = WorkspaceClient(
    host  = os.getenv("DATABRICKS_HOST"),
    token = os.getenv("DATABRICKS_TOKEN")
)

catalogs = client.catalogs.list()
print("Catalogs in your workspace:")
for c in catalogs:
    print(f"  - {c.name}")