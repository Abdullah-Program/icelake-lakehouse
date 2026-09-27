import requests
import os
from dotenv import load_dotenv

load_dotenv()

def resolve_polaris_url():
    url = os.getenv("POLARIS_URL", "http://localhost:8181")
    import socket
    try:
        socket.gethostbyname("polaris")
        return url.replace("localhost:8181", "polaris:8181")
    except socket.gaierror:
        return url.replace("polaris:8181", "localhost:8181")

POLARIS_URL = resolve_polaris_url()
CLIENT_ID = os.getenv("POLARIS_CLIENT_ID")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET")

resp = requests.post(
    f"{POLARIS_URL}/api/catalog/v1/oauth/tokens",
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "PRINCIPAL_ROLE:ALL"
    }
)
token = resp.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# Pehle table drop karo (purge=true taaki underlying files bhi clean ho)
r = requests.delete(
    f"{POLARIS_URL}/api/catalog/v1/icelake/namespaces/wikipedia/tables/articles",
    headers=headers
)
print("Table delete:", r.status_code, r.text[:200])

# Namespace pehle delete karo (catalog delete se pehle zaroori hai kai baar)
r = requests.delete(f"{POLARIS_URL}/api/catalog/v1/icelake/namespaces/wikipedia", headers=headers)

# Ab catalog delete karo
r = requests.delete(f"{POLARIS_URL}/api/management/v1/catalogs/icelake", headers=headers)
print("Catalog delete:", r.status_code, r.text[:200])