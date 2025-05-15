import os
from dotenv import load_dotenv
import requests
import geopandas as gpd
import pandas as pd

load_dotenv()
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

def download_municipalities_benelux(output_path: str) -> None:
    urls = {
        "BEL": "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_BEL_2.json",
        "NLD": "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_NLD_2.json",
        "LUX": "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_LUX_2.json"
    }
    gdfs = []
    for iso, url in urls.items():
        print(f"Downloading {iso} municipalities…")
        gdf = gpd.read_file(url)
        gdf["country_iso"] = iso
        gdfs.append(gdf)
    benelux = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    benelux.to_file(output_path, driver="GeoJSON")
    print(f"Saved combined GeoJSON to {output_path}")


def fetch_openaq_stations(output_path: str):
    base_url = "https://api.openaq.org/v3/locations"
    headers = {"X-API-Key": OPENAQ_API_KEY}
    iso_codes = ["BE", "NL", "LU"]
    all_locations = []

    for iso in iso_codes:
        page = 1
        while True:
            params = {
                "iso": iso,       
                "limit": 1000,    
                "page": page
            }
            resp = requests.get(base_url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            all_locations.extend(data["results"])

            if page * params["limit"] >= data["meta"]["found"]:
                break
            page += 1

    stations = []
    for loc in all_locations:
        stations.append({
            "id": loc["id"],
            "name": loc.get("name"),
            "latitude": loc["coordinates"]["latitude"],
            "longitude": loc["coordinates"]["longitude"],
            "parameters": [p["parameter"] if "parameter" in p else p["name"]
                           for p in loc.get("parameters", loc.get("sensors", []))]
        })

    pd.DataFrame(stations).to_json(output_path, orient="records", force_ascii=False)
    print(f"Saved {len(stations)} stations to {output_path}")