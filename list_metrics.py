import os
from dotenv import load_dotenv
load_dotenv()
from src.steep_client import SteepClient

client = SteepClient(os.environ["STEEP_API_TOKEN"])
metrics = client.list_metrics(expand=True)
if metrics:
    print("Keys in first metric:", list(metrics[0].keys()))
    print("First metric:", metrics[0])
for m in sorted(metrics, key=lambda x: x.get("name", x.get("label", x.get("title", "")))):
    name = m.get("name") or m.get("label") or m.get("title") or m.get("displayName") or str(m)
    print(f"{m['id']}\t{name}")
