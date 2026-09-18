"""
Idempotent ingestion of BLS PR time-series files and the DataUSA population API
into a Unity Catalog Volume, tracked by a Delta manifest table.

Design:
- Discover remote files at runtime (never hard-code names).
- Skip unchanged files using Last-Modified + size, then sha256.
- Write atomically (tmp file + rename).
- Reconcile removals (move to _removed/, mark status).
- Record everything in raw.ingest_manifest via MERGE.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------- config


@dataclass(frozen=True)
class Config:
    contact_email: str  # required by BLS access policy
    catalog: str = "rearc_quest"
    schema: str = "raw"
    volume: str = "landing"
    bls_base_url: str = "https://download.bls.gov/pub/time.series/pr/"
    population_url: str = (
        "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
        "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
    )
    request_timeout_s: int = 60
    polite_delay_s: float = 0.5

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def manifest_table(self) -> str:
        return f"{self.catalog}.{self.schema}.ingest_manifest"

    @property
    def user_agent(self) -> str:
        # BLS: "reserves the right to block robots that do not contain information
        # that can be used to contact the owner." -> identify ourselves.
        return (
            f"rearc-data-quest/1.0 (data engineering take-home; contact: {self.contact_email})"
        )


@dataclass(frozen=True)
class RemoteFile:
    name: str
    url: str
    last_modified: datetime | None
    size_bytes: int | None


# --------------------------------------------------------------------------- http


def build_session(cfg: Config) -> requests.Session:
    """One keep-alive session with retries on transient errors only.

    403 is deliberately NOT retried: it means our identification is wrong.
    """
    retry = Retry(
        total=5,
        backoff_factor=1.5,  # 1.5s, 3s, 6s, 12s, 24s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": cfg.user_agent, "Accept": "*/*"})
    return s


def _raise_for_status_with_hint(resp: requests.Response) -> None:
    if resp.status_code == 403:
        raise PermissionError(
            "403 Forbidden from source. BLS blocks anonymous robots; verify the "
            "User-Agent contains valid contact information (see bls.gov/bls/pss.htm)."
        )
    resp.raise_for_status()


# --------------------------------------------------------------------------- discovery


_LISTING_ROW = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s+[AP]M)\s+(\d+)\s+"
    r'<A HREF="([^"]+)">([^<]+)</A>',
    re.IGNORECASE,
)


def parse_bls_listing(html: str, base_url: str) -> list[RemoteFile]:
    """Parse the IIS-style directory listing BLS serves.

    Rows look like: 9/3/2026 8:30 AM 1615931 <A
    HREF="/pub/time.series/pr/pr.data.1.AllData">pr.data.1.AllData</A>
    Directories have <dir> instead of a size and are ignored by the regex.
    """
    files: list[RemoteFile] = []
    for date_s, time_s, size_s, href, name in _LISTING_ROW.findall(html):
        ts = datetime.strptime(f"{date_s} {time_s}", "%m/%d/%Y %I:%M %p").replace(
            tzinfo=UTC
        )
        url = requests.compat.urljoin(base_url, href)
        files.append(
            RemoteFile(
                name=name.strip(),
                url=url,
                last_modified=ts,
                size_bytes=int(size_s),
            )
        )
    if not files:
        raise RuntimeError(
            "Directory listing parsed to zero files; the page format may have changed."
        )
    return files


def discover_bls_files(session: requests.Session, cfg: Config) -> list[RemoteFile]:
    resp = session.get(cfg.bls_base_url, timeout=cfg.request_timeout_s)
    _raise_for_status_with_hint(resp)
    return parse_bls_listing(resp.text, cfg.bls_base_url)


# --------------------------------------------------------------------------- landing


def sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_to_tmp(
    session: requests.Session, url: str, final_path: str, timeout: int
) -> tuple[str, int]:
    """Stream to <final>.tmp and return (tmp_path, bytes). Caller renames on success."""
    tmp_path = final_path + ".tmp"
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    with session.get(url, stream=True, timeout=timeout) as resp:
        _raise_for_status_with_hint(resp)
        n = 0
        with open(tmp_path, "wb") as out:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                out.write(chunk)
                n += len(chunk)
        return tmp_path, n


# --------------------------------------------------------------------------- manifest (Spark side)


def load_manifest(spark, cfg: Config, source: str) -> dict[str, dict]:
    rows = (
        spark.table(cfg.manifest_table)
        .where(f"source = '{source}'")
        .collect()
    )
    return {r["file_name"]: r.asDict() for r in rows}


def upsert_manifest(spark, cfg: Config, records: Iterable[dict]) -> None:
    """MERGE keyed on (source, file_name). Idempotent."""
    records = list(records)
    if not records:
        return
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("source", T.StringType()),
        T.StructField("file_name", T.StringType()),
        T.StructField("volume_path", T.StringType()),
        T.StructField("remote_last_modified", T.TimestampType()),
        T.StructField("remote_size_bytes", T.LongType()),
        T.StructField("sha256", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("seen_at", T.TimestampType()),
        T.StructField("changed", T.BooleanType()),
    ])
    spark.createDataFrame(records, schema).createOrReplaceTempView("_manifest_updates")
    spark.sql(f"""
MERGE INTO {cfg.manifest_table} AS m
USING _manifest_updates AS u
ON m.source = u.source AND m.file_name = u.file_name
WHEN MATCHED THEN UPDATE SET
  m.volume_path = coalesce(u.volume_path, m.volume_path),
  m.remote_last_modified = coalesce(u.remote_last_modified, m.remote_last_modified),
  m.remote_size_bytes = coalesce(u.remote_size_bytes, m.remote_size_bytes),
  m.sha256 = coalesce(u.sha256, m.sha256),
  m.status = u.status,
  m.last_seen_at = CASE WHEN u.status = 'active' THEN u.seen_at ELSE m.last_seen_at END,
  m.last_changed_at = CASE WHEN u.changed THEN u.seen_at ELSE m.last_changed_at END
WHEN NOT MATCHED THEN INSERT
  (source, file_name, volume_path, remote_last_modified, remote_size_bytes, sha256,
   status, first_seen_at, last_seen_at, last_changed_at)
VALUES (u.source, u.file_name, u.volume_path, u.remote_last_modified, u.remote_size_bytes,
        u.sha256, u.status, u.seen_at, u.seen_at, u.seen_at)
""")


# --------------------------------------------------------------------------- BLS


def ingest_bls(spark, cfg: Config, session: requests.Session | None = None) -> dict:
    session = session or build_session(cfg)
    source = "bls_pr"
    now = datetime.now(UTC)
    target_dir = f"{cfg.volume_root}/bls/pr"
    removed_dir = f"{cfg.volume_root}/bls/_removed/{now:%Y%m%dT%H%M%SZ}"
    remote = discover_bls_files(session, cfg)
    manifest = load_manifest(spark, cfg, source)
    updates: list[dict] = []
    stats = {"seen": len(remote), "downloaded": 0, "unchanged": 0, "touched": 0, "removed": 0}
    for rf in remote:
        final_path = f"{target_dir}/{rf.name}"
        prior = manifest.get(rf.name)
        prior_active = (
            prior is not None
            and prior["status"] == "active"
            and os.path.exists(final_path)
        )
        # Level 1: metadata says unchanged -> skip the download entirely.
        if (
            prior_active
            and prior["remote_last_modified"] is not None
            and prior["remote_last_modified"].replace(tzinfo=UTC) == rf.last_modified
            and prior["remote_size_bytes"] == rf.size_bytes
        ):
            stats["unchanged"] += 1
            updates.append(
                dict(
                    source=source,
                    file_name=rf.name,
                    volume_path=final_path,
                    remote_last_modified=rf.last_modified,
                    remote_size_bytes=rf.size_bytes,
                    sha256=prior["sha256"],
                    status="active",
                    seen_at=now,
                    changed=False,
                )
            )
            continue
        # Level 2: download and compare content.
        tmp_path, nbytes = download_to_tmp(session, rf.url, final_path, cfg.request_timeout_s)
        if rf.size_bytes is not None and nbytes != rf.size_bytes:
            os.remove(tmp_path)
            raise OSError(f"{rf.name}: downloaded {nbytes} bytes, listing says {rf.size_bytes}")
        digest = sha256_of_file(tmp_path)
        if prior_active and prior["sha256"] == digest:
            os.remove(tmp_path)  # touched upstream, content identical
            stats["touched"] += 1
            changed = False
        else:
            os.replace(tmp_path, final_path)  # atomic on the same filesystem
            stats["downloaded"] += 1
            changed = True
        updates.append(
            dict(
                source=source,
                file_name=rf.name,
                volume_path=final_path,
                remote_last_modified=rf.last_modified,
                remote_size_bytes=rf.size_bytes,
                sha256=digest,
                status="active",
                seen_at=now,
                changed=changed,
            )
        )
        time.sleep(cfg.polite_delay_s)
    # Reconcile removals: active in manifest but gone from the source.
    remote_names = {rf.name for rf in remote}
    for name, prior in manifest.items():
        if prior["status"] == "active" and name not in remote_names:
            src = prior["volume_path"]
            if src and os.path.exists(src):
                os.makedirs(removed_dir, exist_ok=True)
                os.replace(src, f"{removed_dir}/{name}")
            stats["removed"] += 1
            updates.append(
                dict(
                    source=source,
                    file_name=name,
                    volume_path=f"{removed_dir}/{name}",
                    remote_last_modified=None,
                    remote_size_bytes=None,
                    sha256=None,
                    status="removed",
                    seen_at=now,
                    changed=True,
                )
            )
    upsert_manifest(spark, cfg, updates)
    return stats


# --------------------------------------------------------------------------- population


def canonical_json_bytes(obj) -> bytes:
    """Stable serialisation so hashing ignores key order / whitespace."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def ingest_population(spark, cfg: Config, session: requests.Session | None = None) -> dict:
    session = session or build_session(cfg)
    source = "datausa_population"
    logical_name = "population.json"
    now = datetime.now(UTC)
    latest_dir = f"{cfg.volume_root}/population/latest"
    archive_dir = f"{cfg.volume_root}/population/archive"
    final_path = f"{latest_dir}/{logical_name}"
    resp = session.get(
        cfg.population_url,
        timeout=cfg.request_timeout_s,
        headers={"Accept": "application/json"},
    )
    _raise_for_status_with_hint(resp)
    payload = resp.json()
    records = payload.get("data")
    if not isinstance(records, list) or not records:
        raise RuntimeError(
            "Population API returned no 'data' records; refusing to overwrite latest."
        )
    # The payload's own 'columns' list is a schema contract; refuse a silent shape change.
    expected_columns = ["Nation ID", "Nation", "Year", "Population"]
    if payload.get("columns") != expected_columns:
        raise RuntimeError(
            f"Population API columns changed: {payload.get('columns')} != {expected_columns}"
        )
    body = canonical_json_bytes(payload)
    digest = hashlib.sha256(body).hexdigest()
    prior = load_manifest(spark, cfg, source).get(logical_name)
    if (
        prior
        and prior["status"] == "active"
        and prior["sha256"] == digest
        and os.path.exists(final_path)
    ):
        upsert_manifest(
            spark,
            cfg,
            [
                dict(
                    source=source,
                    file_name=logical_name,
                    volume_path=final_path,
                    remote_last_modified=None,
                    remote_size_bytes=len(body),
                    sha256=digest,
                    status="active",
                    seen_at=now,
                    changed=False,
                )
            ],
        )
        return {"changed": False, "records": len(records)}
    os.makedirs(latest_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)
    tmp = final_path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(resp.content)  # keep the provider's bytes, not our reserialisation
    os.replace(tmp, final_path)
    with open(f"{archive_dir}/population_{now:%Y%m%dT%H%M%SZ}.json", "wb") as f:
        f.write(resp.content)
    upsert_manifest(
        spark,
        cfg,
        [
            dict(
                source=source,
                file_name=logical_name,
                volume_path=final_path,
                remote_last_modified=None,
                remote_size_bytes=len(resp.content),
                sha256=digest,
                status="active",
                seen_at=now,
                changed=True,
            )
        ],
    )
    return {"changed": True, "records": len(records)}


# --------------------------------------------------------------------------- entry point


def run(spark, cfg: Config) -> dict:
    session = build_session(cfg)
    out = {
        "bls": ingest_bls(spark, cfg, session),
        "population": ingest_population(spark, cfg, session),
    }
    print(json.dumps(out, indent=2, default=str))
    return out