import os
import time
import json
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from pymongo.errors import OperationFailure

STATION_INFO_URL = os.getenv(
    "VELIB_STATION_INFO_URL",
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json",
)
STATION_STATUS_URL = os.getenv(
    "VELIB_STATION_STATUS_URL",
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json",
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/velib")
MONGO_DB = os.getenv("MONGO_DB", "velib")
STATIONS_COLLECTION = os.getenv("STATIONS_COLLECTION", "stations")
AVAIL_COLLECTION = os.getenv("AVAIL_COLLECTION", "availability_raw")

INGEST_INTERVAL_SECONDS = int(os.getenv("INGEST_INTERVAL_SECONDS", "180"))
TTL_DAYS = int(os.getenv("TTL_DAYS", "90"))
SAMPLE_N = int(os.getenv("SAMPLE_N", "0"))  # 0 = all

UA = "Mozilla/5.0 (compatible; velib-tp-ingest/1.0)"


def _fetch_json(url: str, timeout: int = 30) -> dict:
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _ensure_ttl_index(col, field: str, ttl_seconds: int):
    """
    Ensure a TTL index exists on `field` with `expireAfterSeconds=ttl_seconds`.
    If an equivalent index exists with different options (common IndexOptionsConflict),
    we drop and recreate it safely.
    """
    target_key = [(field, 1)]

    # Find any existing index on the same key
    existing = None
    for idx in col.list_indexes():
        key_list = list(idx.get("key", {}).items())  # e.g. [('ts', 1)]
        if key_list == target_key:
            existing = idx
            break

    if existing:
        current_ttl = existing.get("expireAfterSeconds", None)
        idx_name = existing.get("name")

        # If TTL already correct -> nothing to do
        if current_ttl == ttl_seconds:
            return

        # Otherwise drop & recreate with correct TTL
        if idx_name:
            col.drop_index(idx_name)

    # Create (or recreate) with TTL
    col.create_index(field, expireAfterSeconds=ttl_seconds)


def _ensure_indexes(db):

    col = db[AVAIL_COLLECTION]
    ttl_seconds = TTL_DAYS * 24 * 3600

    try:
        _ensure_ttl_index(col, "ts", ttl_seconds)
    except OperationFailure as e:
        raise


def _normalize_stations(payload: dict):
    stations = payload.get("data", {}).get("stations", [])
    if SAMPLE_N > 0:
        stations = stations[:SAMPLE_N]
    out = []
    for s in stations:
        out.append(
            {
                "station_id": s.get("station_id"),
                "name": s.get("name"),
                "lat": s.get("lat"),
                "lon": s.get("lon"),
                "capacity": s.get("capacity"),
                "rental_methods": s.get("rental_methods"),
                "stationCode": s.get("stationCode"),
            }
        )
    return out


def _normalize_status(payload: dict):
    stations = payload.get("data", {}).get("stations", [])
    if SAMPLE_N > 0:
        stations = stations[:SAMPLE_N]
    now = datetime.now(timezone.utc)
    out = []
    for s in stations:
        out.append(
            {
                "ts": now,  # TTL uses this
                "timestamp": now.isoformat(),
                "station_id": s.get("station_id"),
                "num_bikes_available": s.get("num_bikes_available", s.get("numBikesAvailable")),
                "num_docks_available": s.get("num_docks_available", s.get("numDocksAvailable")),
                "is_installed": s.get("is_installed"),
                "is_renting": s.get("is_renting"),
                "is_returning": s.get("is_returning"),
                "last_reported": s.get("last_reported"),
            }
        )
    return out


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    _ensure_indexes(db)

    print(f"[ingest] mongo={MONGO_URI} db={MONGO_DB} every={INGEST_INTERVAL_SECONDS}s ttl={TTL_DAYS}d sample={SAMPLE_N}")

    while True:
        try:
            info = _fetch_json(STATION_INFO_URL)
            status = _fetch_json(STATION_STATUS_URL)

            stations_docs = _normalize_stations(info)
            status_docs = _normalize_status(status)

            for doc in stations_docs:
                sid = doc.get("station_id")
                if not sid:
                    continue
                db[STATIONS_COLLECTION].update_one({"station_id": sid}, {"$set": doc}, upsert=True)

            if status_docs:
                db[AVAIL_COLLECTION].insert_many(status_docs, ordered=False)

            print(f"[ingest] ok stations={len(stations_docs)} avail_added={len(status_docs)}")
        except HTTPError as e:
            print(f"[ingest] HTTPError {e.code}: {e.reason}")
        except PyMongoError as e:
            print(f"[ingest] Mongo error: {e}")
        except Exception as e:
            print(f"[ingest] Unexpected error: {e}")

        time.sleep(INGEST_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
