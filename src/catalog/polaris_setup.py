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

def get_token():
    resp = requests.post(
        f"{POLARIS_URL}/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL"
        }
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def setup_catalog():
    token = get_token()
    print("[OK] Token obtained")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # 1. Create catalog (warehouse)
    catalog_payload = {
        "catalog": {
            "name": "icelake",
            "type": "INTERNAL",
            "properties": {
                "default-base-location": "s3://icelake/",
                "s3.endpoint": "http://garage:3900",
                "s3.path-style-access": "true"
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": ["s3://icelake/", "s3://icelake"],
                "stsUnavailable": True,
                "endpoint": "http://garage:3900",
                "pathStyleAccess": True
            }
        }
    }
    resp = requests.post(
        f"{POLARIS_URL}/api/management/v1/catalogs",
        headers=headers,
        json=catalog_payload["catalog"]
    )
    print("Catalog:", resp.status_code, resp.text[:200])
    
    # 2. Create namespace
    ns_payload = {"namespace": ["wikipedia"]}
    resp = requests.post(
        f"{POLARIS_URL}/api/catalog/v1/icelake/namespaces",
        headers=headers,
        json=ns_payload
    )
    print("Namespace:", resp.status_code, resp.text[:200])
    
    return token

if __name__ == "__main__":
    setup_catalog()