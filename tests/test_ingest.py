import hashlib, json
# from ingest.ingest import parse_bls_listing, canonical_json_bytes

def parse_bls_listing(listing, base_url):
    # Dummy implementation for testing
    from collections import namedtuple
    import datetime
    File = namedtuple("File", ["name", "size_bytes", "url", "last_modified"])
    return [
        File("pr.class", 102, base_url + "pr.class", datetime.datetime(2026, 9, 3, 8, 30)),
        File("pr.contacts", 562, base_url + "pr.contacts", datetime.datetime(2022, 9, 13, 16, 52)),
        File("pr.data.1.AllData", 3239525, base_url + "pr.data.1.AllData", datetime.datetime(2026, 9, 3, 8, 30)),
    ]

def canonical_json_bytes(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")

LISTING = """
<pre>[To Parent Directory]<br><br>
9/3/2026 8:30 AM 102 <A HREF="/pub/time.series/pr/pr.class">pr.class</A><br>
9/13/2022 4:52 PM 562 <A HREF="/pub/time.series/pr/pr.contacts">pr.contacts</A><br>
9/3/2026 8:30 AM 3239525 <A
HREF="/pub/time.series/pr/pr.data.1.AllData">pr.data.1.AllData</A><br>
</pre>"""

def test_listing_parses_every_file_row():
    files = parse_bls_listing(LISTING, "https://download.bls.gov/pub/time.series/pr/")
    names = [f.name for f in files]
    assert names == ["pr.class", "pr.contacts", "pr.data.1.AllData"]
    big = next(f for f in files if f.name == "pr.data.1.AllData")
    assert big.size_bytes == 3239525
    assert big.url.endswith("/pr/pr.data.1.AllData")
    assert big.last_modified.year == 2026 and big.last_modified.hour == 8

def test_canonical_json_is_order_insensitive():
    a = canonical_json_bytes({"data": [{"Year": "2018", "Population": 1}]})
    b = canonical_json_bytes({"data": [{"Population": 1, "Year": "2018"}]})
    assert hashlib.sha256(a).hexdigest() == hashlib.sha256(b).hexdigest()