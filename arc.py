#!/usr/bin/env python3
"""Draws archive satellite pictures for the Guta Forecasting Center radar site and uploads them, signed.

    python3 arc.py --events --shard 0 --of 6     # every named weather event of the Time Machine, built ahead of time
    python3 arc.py --serve --loop 320            # draw what visitors ask for, the moment they ask

--events: for each event in events.json, the scans leading up to its moment in the infrared, water vapour and
visible bands, drawn and uploaded with a mark beside each so the server never clears them out.

--serve: the server keeps a list of archive pictures somebody is waiting for (GET /archive/queue). This reads it
every second, draws each on four processors and uploads it at once; the server sends it to the visitor the
moment it lands, and draws it itself only if this has not answered within a few seconds.

The drawing code is pipeline.bin, encrypted. The server's address and the signing key come from the environment
(repository secrets) and are never printed.
"""
import argparse
import base64
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
EVENT_BANDS = (13, 9, 2)        # clean infrared, water vapour, visible
EVENT_FRAMES = int(os.environ.get("ARC_EVENT_FRAMES", "24"))
KEEP_MARK = ".keep.json"
KINDS = ("sat", "mrms", "hrrr", "view", "prod", "cth", "recon", "render", "ahi", "ahiview", "eum")
os.environ.setdefault("GWCFC_NO_UPLOADS", "1")


def log(msg):
    print(time.strftime("%H:%M:%S", time.gmtime()), msg, flush=True)


def unlock_pipeline():
    """The drawing code, unpacked to a temporary folder and put on the import path. The key is derived from the
    signing key, so only a job here (or the owner) can unlock it."""
    plain = os.path.join(HERE, "pipeline")
    if os.path.isdir(plain):
        sys.path.insert(0, plain)
        return True
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    body = "".join(ln.strip() for ln in key.splitlines() if ln.strip() and not ln.startswith("-----"))
    if not body:
        return False
    env = dict(os.environ, GWCFC_BUNDLE_PASS=hashlib.sha256(body.encode()).hexdigest())
    out = tempfile.mkdtemp(prefix="pl-")
    dec = subprocess.run(["openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2", "-iter", "200000", "-in",
                          os.path.join(HERE, "pipeline.bin"), "-pass", "env:GWCFC_BUNDLE_PASS"],
                         capture_output=True, env=env)
    if dec.returncode != 0:
        return False
    with tarfile.open(fileobj=io.BytesIO(dec.stdout), mode="r:gz") as tf:
        tf.extractall(out, filter="data")
    sys.path.insert(0, out)
    return True


def sign(msg, key_path):
    fd, mp = tempfile.mkstemp(prefix="m-")
    with os.fdopen(fd, "wb") as fh:
        fh.write(msg)
    try:
        return subprocess.run(["openssl", "pkeyutl", "-sign", "-inkey", key_path, "-rawin", "-in", mp],
                              capture_output=True, check=True).stdout
    finally:
        os.unlink(mp)


def post(base, key_path, run, kind, part, body, tries=5, region="satellite"):
    import requests
    last = ""
    for n in range(tries):
        sha = hashlib.sha256(body).hexdigest()
        ts = str(int(time.time()))
        msg = "\n".join(["gwcfc-upload-v1", "tree", region, run, kind, str(part), sha, ts]).encode()
        headers = {"X-GWCFC-Model": "tree", "X-GWCFC-Region": region, "X-GWCFC-Run": run, "X-GWCFC-Kind": kind,
                   "X-GWCFC-Part": str(part), "X-GWCFC-Sha256": sha, "X-GWCFC-Ts": ts,
                   "X-GWCFC-Sig": base64.b64encode(sign(msg, key_path)).decode(),
                   "Content-Type": "application/octet-stream", "User-Agent": "gwcfc-archive-builder"}
        try:
            r = requests.post(base + "/models/upload", data=body, headers=headers, timeout=300)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code} {r.text[:200]}"
            if r.status_code in (400, 403, 409, 413):
                break
        except Exception as e:                          # noqa: BLE001
            last = e.__class__.__name__                 # the address is in the message, so only the kind is kept
        time.sleep(min(40, 4 * 2 ** n))
    raise RuntimeError(f"upload of {kind} {part} failed: {last}")


def send(base, key_path, files, job="archive", region="satellite"):
    """These (path on disk, path under the folder) files in parts of a few megabytes, then the commit."""
    run = time.strftime("%Y%m%d_%H", time.gmtime())
    batch, size, part = [], 0, 0

    def flush():
        nonlocal batch, size, part
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tf:
            for path, rel in batch:
                tf.add(path, arcname=rel, recursive=False)
        post(base, key_path, run, "part", part, buf.getvalue(), region=region)
        part += 1
        batch, size = [], 0
    for item in files:
        sz = os.path.getsize(item[0])
        if batch and size + sz > 2.5e6:                 # small parts are taken at once by the server, not queued
            flush()
        batch.append(item)
        size += sz
    if batch:
        flush()
    post(base, key_path, run, "commit", part, json.dumps({"job": job}).encode(), region=region)


_sent = {}
_sent_lock = __import__("threading").Lock()
WX = os.path.expanduser("~/wxdata")


def new_under(root, subs):
    """Files under these folders of root that have not been sent yet: [(path, path under root)]."""
    out = []
    with _sent_lock:
        for sub in subs:
            for dp, dn, fn in os.walk(os.path.join(root, sub)):
                dn[:] = [d for d in dn if d not in ("raw", "asks") and (not d.startswith("_") or d == "_closeups")]
                for n in fn:
                    p = os.path.join(dp, n)
                    if n.endswith((".png", ".json", ".webp")) and not n.startswith(".") and _sent.get(p) != os.path.getsize(p):
                        out.append((p, os.path.relpath(p, root).replace(os.sep, "/")))
        for p, _r in out:
            _sent[p] = os.path.getsize(p)
    return out


def draw_ask(base, key_path, spec):
    """A close-up, a colour composite, a zone's cloud-top heights or a day of recon: drawn where the server would
    have written it, uploaded, and then the small marker the server is waiting for."""
    kind, tree = spec["kind"], spec.get("tree") or "satellite"
    if kind == "view":
        import sat_archive as SA
        SA.render_view(spec["bucket"], spec["key"], int(spec["band"]), spec["sector"], spec["box"],
                       edge=spec.get("edge"), nearest=bool(spec.get("nearest")))
        root, subs = os.path.join(WX, "satellite"), ["archive"]
    elif kind == "prod":
        import sat_products as SP
        png, _b = SP.render(spec["bucket"], spec["key"], spec["prod"], spec["sector"],
                            os.path.join(WX, "satellite", "products"), box=spec.get("box"),
                            nearest_pixels=bool(spec.get("nearest")))
        try:
            SP.to_webp(png)
        except Exception:                               # noqa: BLE001
            pass
        root, subs = os.path.join(WX, "satellite"), ["products"]
    elif kind == "cth":
        import sat_cth as CTH
        CTH.build_frame(spec["bucket"], spec["key"], spec["sector"], tuple(spec["bbox"]),
                        os.path.join(WX, "satellite", "cth"))
        root, subs = os.path.join(WX, "satellite"), ["cth"]
    elif kind == "render":
        # The older records and a boxed GOES-R scan, by the same choice the server's own door makes.
        import sat_archive as SA
        if spec.get("view") and spec.get("rec") == "b1":
            SA.render_view_b1(spec["key"], spec["box"], SA.CACHE_DIR, edge=spec.get("edge"))
        elif spec.get("view") and spec.get("rec") == "gsg":
            SA.render_view_gsg(spec["key"], int(spec["band"]), spec["box"], SA.CACHE_DIR, edge=spec.get("edge"))
        else:
            SA.render(spec["bucket"], spec["key"], int(spec["band"]), spec["sector"], post=spec.get("post") or "east",
                      box=spec.get("box"), nearest=bool(spec.get("nearest")))
        root, subs = os.path.join(WX, "satellite"), ["archive"]
    elif kind in ("ahi", "ahiview"):
        import sat_archive as SA
        import sat_ahi as AHI
        key, prod, box = spec["key"], spec.get("prod"), spec.get("box")
        if kind == "ahi":
            (AHI.render_prod(key, prod, SA.CACHE_DIR, box=box) if prod
             else AHI.render(key, int(spec["band"]), SA.CACHE_DIR, box=box))
        else:
            (AHI.render_view_fldk_prod(key, prod, box, SA.CACHE_DIR, edge=spec.get("edge")) if prod
             else AHI.render_view_fldk(key, int(spec["band"]), box, SA.CACHE_DIR, edge=spec.get("edge"),
                                       nearest=bool(spec.get("nearest"))))
        root, subs = os.path.join(WX, "satellite"), ["archive", "products"]
    elif kind == "eum":
        import eumetsat_pipeline as EUM
        EUM.closeup(spec["sat"], spec["prod"], spec["t"], tuple(spec["box"]), edge=spec.get("edge"))
        root, subs = os.path.join(WX, "satellite"), ["eumetsat/_closeups"]
    elif kind == "recon":
        import recon_pipeline as RP
        RP.build_day(spec["day"], out_dir=os.path.join(WX, "recon"))
        root, subs = os.path.join(WX, "recon"), ["day"]
    else:
        return 0
    files = new_under(root, subs)
    mark = os.path.join(root, "asks", str(spec["ask"]) + ".json")
    os.makedirs(os.path.dirname(mark), exist_ok=True)
    with open(mark, "w") as fh:
        json.dump({"at": time.time(), "kind": kind}, fh)
    send(base, key_path, files + [(mark, "asks/" + os.path.basename(mark))], region=tree)
    return len(files)


_radar_sent = {}
_radar_lock = __import__("threading").Lock()


def draw_radar(base, key_path, spec):
    """One archived radar mosaic or HRRR hour, with the small file that tells the server what was made.

    An HRRR hour paints every field it has, the one asked for first: that one is sent, with its result, the moment it
    is on disk, and the rest follow when they are painted."""
    import radar_archive as RA
    kind, what, at = spec["kind"], str(spec["what"]), int(spec["at"])
    root = os.path.join(RA.DATA, "radar-archive")
    res = os.path.join(root, "results", f"{kind}_{what}_{at}.json")
    sent_n = [0]

    def publish(meta):
        os.makedirs(os.path.dirname(res), exist_ok=True)
        with open(res, "w") as fh:
            json.dump(meta, fh)
        files = []
        with _radar_lock:
            for dp, dn, fn in os.walk(root):
                dn[:] = [d for d in dn if d not in ("results", "l3")]
                for n in fn:
                    p = os.path.join(dp, n)
                    if n.endswith((".png", ".json", ".webp")) and _radar_sent.get(p) != os.path.getsize(p):
                        files.append((p, os.path.relpath(p, root).replace(os.sep, "/")))
            for p, _r in files:
                _radar_sent[p] = os.path.getsize(p)
        # The pictures first, the result last: the server answers the moment the result is there.
        send(base, key_path, files + [(res, "results/" + os.path.basename(res))], region="radar-archive")
        sent_n[0] += len(files)

    early = [False]

    def first(meta):
        publish(meta)
        early[0] = True

    try:
        meta = RA.mrms_frame(what, at) if kind == "mrms" else RA.hrrr_frame(what, at, first=first)
    except (ValueError, LookupError) as e:
        meta = {"error": str(e)[:300], "type": e.__class__.__name__}
    # Once more at the end: the rest of the hour's pictures (or everything, when nothing was sent early).
    if not early[0] or kind == "hrrr":
        publish(meta)
    return sent_n[0]


def rel_of(SA, png):
    """archive/<bucket>/<sector>/C13/<stamp>.png for a picture in the local cache."""
    return "archive/" + os.path.relpath(png, SA.CACHE_DIR).replace(os.sep, "/")


def draw(SA, spec):
    png, _b = SA.render(spec["bucket"], spec["key"], int(spec["band"]), spec["sector"], post=spec.get("post") or "east")
    side = png[:-4] + ".json"
    return [(p, rel_of(SA, p)) for p in (png, side) if os.path.exists(p)]


def post_for(lon, when):
    """GOES-West for the Pacific side once it existed, else GOES-East."""
    return "west" if lon < -112 and when.year >= 2019 else "east"


def run_events(base, key_path, shard, of, dry):
    import sat_archive as SA
    from datetime import datetime, timezone
    with open(os.path.join(HERE, "events.json")) as fh:
        events = json.load(fh)
    done = failed = 0
    for n, ev in enumerate(events):
        if n % of != shard:
            continue
        when = datetime.strptime(ev["t"], "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
        post_ = post_for(ev["lon"], when)
        sector = "conus" if (24 <= ev["lat"] <= 50 and -125 <= ev["lon"] <= -66) else "fulldisk"
        for band in EVENT_BANDS:
            try:
                got = SA.frames_around(post_, band, sector, int(when.timestamp() * 1000), EVENT_FRAMES)
            except Exception as e:                      # noqa: BLE001
                log(f"{ev['name']} band {band}: could not list ({e.__class__.__name__})")
                continue
            frames = (got or {}).get("frames") or []
            bucket = (got or {}).get("bucket")
            if not frames or bucket not in SA.BUCKETS:
                # Before the GOES-R satellites (2017) the server answers from the older records itself.
                log(f"{ev['name']} band {band}: no GOES-R scans at that date; left to the server's older records")
                continue
            batch = []

            def one(fr):
                try:
                    files = draw(SA, {"bucket": bucket, "key": fr["key"], "band": band, "sector": sector, "post": post_})
                    mark = files[0][0][:-4] + KEEP_MARK
                    with open(mark, "w") as fh:
                        json.dump({"event": ev["name"]}, fh)
                    return files + [(mark, rel_of(SA, mark))]
                except Exception as e:                  # noqa: BLE001
                    return e
            with ThreadPoolExecutor(max_workers=3) as ex:
                for res in ex.map(one, frames):
                    if isinstance(res, Exception):
                        failed += 1
                    else:
                        batch += res
                        done += 1
            size = sum(os.path.getsize(p) for p, _r in batch)
            if batch and not dry:
                # In parts of a few megabytes, so each is quick for the server to take.
                part, psize = [], 0
                for item in batch:
                    s = os.path.getsize(item[0])
                    if part and psize + s > 20e6:
                        send(base, key_path, part, job="archive-events")
                        part, psize = [], 0
                    part.append(item)
                    psize += s
                if part:
                    send(base, key_path, part, job="archive-events")
            log(f"{ev['name']} band {band} {sector}: {len(frames)} scans, {size / 1e6:.1f} MB")
            for p, _r in batch:
                try:
                    os.unlink(p)
                except OSError:
                    pass
    log(f"events: {done} pictures drawn, {failed} failed")
    return 0


def run_serve(base, key_path, minutes):
    import requests
    import sat_archive as SA
    end = time.time() + minutes * 60
    busy, last_in = set(), 0.0
    pool = ThreadPoolExecutor(max_workers=4)

    def work(spec, k):
        t0 = time.time()
        try:
            if spec.get("kind") in ("view", "prod", "cth", "recon", "render", "ahi", "ahiview", "eum") and re.fullmatch(r"[0-9a-f]{24}", str(spec.get("ask", ""))) \
                    and spec.get("tree") in ("satellite", "recon"):
                n = draw_ask(base, key_path, spec)
                log(f"drew and sent a {spec['kind']}: {n} files in {time.time() - t0:.1f}s")
                return
            if spec.get("kind") in ("mrms", "hrrr"):
                n = draw_radar(base, key_path, spec)
                log(f"drew and sent {spec['kind']} {spec['what']}: {n} files in {time.time() - t0:.1f}s")
                return
            files = draw(SA, spec)
            send(base, key_path, files)
            log(f"drew and sent {spec['sector']} band {spec['band']} in {time.time() - t0:.1f}s")
            for p, _r in files:
                try:
                    os.unlink(p)
                except OSError:
                    pass
        except Exception as e:                          # noqa: BLE001
            log(f"an ask failed: {e.__class__.__name__}")
        finally:
            busy.discard(k)
    while time.time() < end:
        t0 = time.time()
        try:
            # The check-in that tells the server this is here to be asked (every minute).
            if t0 - last_in > 60:
                post(base, key_path, time.strftime("%Y%m%d_%H", time.gmtime()), "commit", 0,
                     json.dumps({"job": "archive"}).encode())
                # And for the radar archive, which the server asks about on its own: a builder that could only draw
                # satellite pictures must not be waited on for a radar mosaic.
                post(base, key_path, time.strftime("%Y%m%d_%H", time.gmtime()), "commit", 0,
                     json.dumps({"job": "archive"}).encode(), region="radar-archive")
                # And that this builder knows close-ups, composites, cloud heights and recon days.
                post(base, key_path, time.strftime("%Y%m%d_%H", time.gmtime()), "commit", 0,
                     json.dumps({"job": "archive-v2"}).encode())
                # And the older satellite records, the Himawari archive and Meteosat close-ups.
                post(base, key_path, time.strftime("%Y%m%d_%H", time.gmtime()), "commit", 0,
                     json.dumps({"job": "archive-v3"}).encode())
                last_in = t0
            jobs = requests.get(base + "/archive/queue", timeout=10,
                                headers={"User-Agent": "gwcfc-archive-builder"}).json().get("jobs") or []
            for spec in jobs:
                k = json.dumps(spec, sort_keys=True)
                if spec.get("kind") in KINDS and k not in busy and len(busy) < 12:
                    busy.add(k)
                    pool.submit(work, spec, k)
        except Exception as e:                          # noqa: BLE001
            log(f"tick failed: {e.__class__.__name__}")
        time.sleep(max(0.2, 1.0 - (time.time() - t0)))      # the list is read every second
    pool.shutdown(wait=True)
    return 0


def run_tcradar(base, key_path, dry):
    """The Hurricane Hunters' airborne radar archive, drawn whole: the storm list, then every swath (the source file
    is mirrored once, about 1.5 GB), uploaded as it goes so a job cut short keeps what it sent."""
    code = sys.path[0]
    env = dict(os.environ, PYTHONPATH=code)
    root = os.path.join(WX, "tcradar")

    def push():
        files = new_under(root, ["png"]) + [(os.path.join(root, n), n) for n in ("index.json", "places.json")
                                             if os.path.exists(os.path.join(root, n))]
        if files and not dry:
            # In parts of 20 MB: these are many small pictures, and there is no visitor waiting on any one of them.
            batch, size = [], 0
            for item in files:
                sz = os.path.getsize(item[0])
                if batch and size + sz > 20e6:
                    send_big(base, key_path, batch)
                    batch, size = [], 0
                batch.append(item)
                size += sz
            if batch:
                send_big(base, key_path, batch)
        log(f"tcradar: {len(files)} files {'(not sent)' if dry else 'sent'}")
    subprocess.run([sys.executable, os.path.join(code, "tcradar_pipeline.py"), "--index"], env=env, cwd=code, timeout=1800)
    end = time.time() + 5 * 3600
    while time.time() < end:
        r = subprocess.run([sys.executable, os.path.join(code, "tcradar_pipeline.py"), "--bake", "--resume",
                            "--budget", "1200"], env=env, cwd=code, capture_output=True, text=True, timeout=3600)
        tail = (r.stdout or "").strip().splitlines()[-1:]
        log("tcradar: " + (tail[0][:160] if tail else f"exit {r.returncode}"))
        before = len(_sent)
        push()
        if len(_sent) == before:
            break                                       # a whole turn drew nothing new: it is finished
    return 0


def send_big(base, key_path, files):
    run = time.strftime("%Y%m%d_%H", time.gmtime())
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for path, rel in files:
            tf.add(path, arcname=rel, recursive=False)
    post(base, key_path, run, "part", 0, buf.getvalue(), region="tcradar")
    post(base, key_path, run, "commit", 1, json.dumps({"job": "tcradar"}).encode(), region="tcradar")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", action="store_true")
    ap.add_argument("--serve", action="store_true")
    ap.add_argument("--tcradar", action="store_true")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--of", type=int, default=1)
    ap.add_argument("--loop", type=int, default=320)
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args(argv)
    base = (os.environ.get("GWCFC_BASE") or "").strip().rstrip("/")
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    if (not a.dry and (not base or "PRIVATE KEY" not in key)) or not unlock_pipeline():
        print("::warning::Nothing drawn: the repository secrets GWCFC_BASE and GWCFC_UPLOAD_KEY are needed "
              "(Settings > Secrets and variables > Actions).", flush=True)
        return 0
    key_path = None
    if not a.dry:
        fd, key_path = tempfile.mkstemp(prefix="k-")
        with os.fdopen(fd, "w") as fh:
            fh.write(key.strip() + "\n")
        os.chmod(key_path, 0o600)
    if a.events:
        return run_events(base, key_path, a.shard, a.of, a.dry)
    if a.serve:
        return run_serve(base, key_path, a.loop)
    if a.tcradar:
        return run_tcradar(base, key_path, a.dry)
    return 2


if __name__ == "__main__":
    sys.exit(main())
