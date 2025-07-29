import os
import json
import shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import gcsfs
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union
from shapely.geometry import Point

# --- Config ---
GCS_BUCKET = "cis-ice-charts-public"
OUTPUT_DIR = Path("geojsons")
PARQUET_PATH = "geojson_assets.parquet"
GROUPED_PARQUET_PATH = "daily_items.parquet"

# Custom filters
SKIP_SUFFIXES = [".tar"]
SKIP_PREFIXES = ["BAD_", "TMP_", "TEST_"]

START_DATE = os.getenv("START_DATE", "2025-01-01")
END_DATE = os.getenv("END_DATE", "2025-01-05")
START_DATE = datetime.strptime(START_DATE, "%Y-%m-%d").date()
END_DATE = datetime.strptime(END_DATE, "%Y-%m-%d").date()

ASSET_BASE_URL_GEOJSON = os.getenv("ASSET_BASE_URL_GEOJSON", "http://127.0.0.1:9091/geojsons")
STYLE_URL = os.getenv("STYLE_URL", "https://raw.githubusercontent.com/gtif-cerulean/assets/refs/heads/main/styles/dmi-ice-charts.json")

# Make sure directories exist
OUTPUT_DIR.mkdir(exist_ok=True)

# GCS access
fs = gcsfs.GCSFileSystem(token='anon')

def extract_date(folder_name):
    import re
    match = re.search(r"(\d{8})", folder_name)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d").date()
        except ValueError:
            return None
    return None

def should_skip(folder_name):
    if any(folder_name.endswith(suffix) for suffix in SKIP_SUFFIXES):
        return True
    if any(folder_name.startswith(prefix) for prefix in SKIP_PREFIXES):
        return True
    return False

def load_existing_parquet(path):
    if os.path.exists(path):
        return gpd.read_parquet(path)
    return gpd.GeoDataFrame(columns=["id", "datetime", "geometry", "assets", "links"], crs="EPSG:4326")

def add_style_link(row):
    # Skip if base URL isn't set
    if not STYLE_URL:
        return row.get("links", [])

    assets = row.get("assets", {})
    asset_keys = list(assets.keys())
    if not asset_keys:
        return row.get("links", [])

    # Remove old style links
    links = [link for link in row.get("links", []) if link.get("rel") != "style"]

    # Append new style link
    links.append({
        "rel": "style",
        "href": f"{STYLE_URL}",
        "type": "text/vector-styles",
        "asset:keys": asset_keys
    })

    return links

def create_stac_item(date, id, assets, asset_type):
    if not assets:
        return {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": id,
            "datetime": pd.to_datetime(date),
            "geometry": None,
            "bbox": None,
            "assets": {},
            "links": [],
            "properties": {
                "description": "Error downloading or processing assets",
                "invalid": True
            }
        }

    geoms = [a["geometry"] for a in assets]
    merged_geom = unary_union(geoms).envelope
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": id,
        "datetime": pd.to_datetime(date),
        "geometry": merged_geom,
        "bbox": list(merged_geom.bounds),
        "assets": {
            f"asset_{i}": {
                "href": a["url"],
                "type": asset_type,
                "roles": ["data"]
            }
            for i, a in enumerate(assets)
        },
        "links": []
    }

def main():
    print(f"Listing folders in GCS bucket: {GCS_BUCKET}")
    folders = fs.ls(GCS_BUCKET)
    folders = [f for f in folders if f != GCS_BUCKET]  # remove root bucket
    print(f"Found {len(folders)} folders.")

    new_records = []
    grouped_items = defaultdict(list)
    existing_ids = set(load_existing_parquet(PARQUET_PATH)["id"])

    for path in folders:
        folder_name = path.split("/")[-1]

        if should_skip(folder_name) or folder_name in existing_ids:
            continue

        date = extract_date(folder_name)
        if not date or not (START_DATE <= date <= END_DATE):
            continue

        geojson_path = f"{GCS_BUCKET}/{folder_name}/{folder_name}.geojson"
        if not fs.exists(geojson_path):
            print(f"❌ No .geojson found in {folder_name}")
            # Mark as empty to avoid retry
            new_records.append(
                create_stac_item(date, folder_name, [], "application/geo+json")
            )
            continue

        # Download and parse
        local_path = OUTPUT_DIR / f"{folder_name}.geojson"
        try:
            with fs.open(geojson_path, "rb") as remote, open(local_path, "wb") as local:
                local.write(remote.read())
        except Exception as e:
            print(f"⚠️ Error downloading {geojson_path}: {e}")
            new_records.append(
                create_stac_item(date, folder_name, [], "application/geo+json")
            )
            continue

        try:
            gdf = gpd.read_file(local_path)
            geom = unary_union(gdf.geometry).envelope
        except Exception as e:
            print(f"❌ Error reading {local_path}: {e}")
            new_records.append(
                create_stac_item(date, folder_name, [], "application/geo+json")
            )
            continue

        asset_url = f"{ASSET_BASE_URL_GEOJSON}/{folder_name}.geojson"
        new_records.append(
            create_stac_item(date, folder_name, [{"url": asset_url, "geometry": geom}], "application/geo+json")
        )
        grouped_items[date].append({"url": asset_url, "geometry": geom})

    # Save individual items
    existing = load_existing_parquet(PARQUET_PATH)
    if new_records:
        df = gpd.GeoDataFrame(new_records, crs="EPSG:4326")
        updated = pd.concat([existing, df], ignore_index=True)
        updated.to_parquet(PARQUET_PATH)
        print(f"✅ Saved {len(new_records)} new records to {PARQUET_PATH}")
    else:
        print("No new individual geojson assets to save.")

    # Grouped daily items
    grouped_existing = load_existing_parquet(GROUPED_PARQUET_PATH)
    grouped_records = []
    for date, assets in grouped_items.items():
        grouped_records.append(
            create_stac_item(date, date.strftime("%Y-%m-%d"), assets, "application/geo+json")
        )

    if grouped_records:
        grouped_df = gpd.GeoDataFrame(grouped_records, crs="EPSG:4326")
        merged = pd.concat([grouped_existing, grouped_df], ignore_index=True)
    else:
        merged = grouped_existing

    # Merge per day
    final = merge_items_per_day(merged)
     # Add style links to grouped items
    final["links"] = final.apply(add_style_link, axis=1)
    final.to_parquet(GROUPED_PARQUET_PATH)
    print(f"✅ Saved grouped items to {GROUPED_PARQUET_PATH} ({len(final)} total items)")

def merge_items_per_day(df):
    merged = []

    # Exclude items marked as invalid
    if "properties" in df.columns:
        df = df[~df["properties"].apply(lambda props: props.get("invalid", False) if isinstance(props, dict) else False)]

    for id_, group in df.groupby("id"):
        geoms = group["geometry"].tolist()
        geom = unary_union(geoms).envelope

        assets = []
        for asset_dict in group["assets"]:
            if isinstance(asset_dict, dict):
                assets.extend(asset_dict.values())

        rekeyed_assets = {f"asset_{i}": a for i, a in enumerate(assets)}

        links = []
        for link_set in group.get("links", []):
            if isinstance(link_set, list):
                links.extend(link_set)

        date = pd.to_datetime(group["datetime"].iloc[0])
        merged.append({
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": id_,
            "geometry": geom,
            "bbox": list(geom.bounds),
            "datetime": date,
            "assets": rekeyed_assets,
            "links": links
        })

    return gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")

if __name__ == "__main__":
    main()
