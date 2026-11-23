#!/usr/bin/env python3
"""Save and restore a quiescent NSVU index using named RADOS pool snapshots."""

import argparse
from contextlib import ExitStack
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import sys
from tempfile import NamedTemporaryFile
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "results/.high-recall-experiment.lock"


def read(path):
    return json.loads(path.read_text())


def save(path, document):
    with NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as output:
        json.dump(document, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
        temporary = Path(output.name)
    temporary.replace(path)
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def load_digest():
    spec = importlib.util.spec_from_file_location("index_content_digest",
                                                  Path(__file__).with_name("index-content-digest.py"))
    digest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(digest)
    return digest


def inventory(ioctx):
    ioctx.set_namespace("\x01")  # LIBRADOS_ALL_NSPACES
    records = {}
    try:
        for obj in ioctx.list_objects():
            if obj.nspace or obj.locator_key or obj.key in records:
                raise RuntimeError("Unexpected namespace, locator, or duplicate NSVU object")
            records[obj.key] = None
    finally:
        ioctx.set_namespace("")
    for oid in records:
        size, _ = ioctx.stat(oid)
        version = ioctx.get_last_version()
        if version <= 0:
            raise RuntimeError(f"Missing RADOS object version for {oid}")
        records[oid] = {"bytes": size, "version": version}
    return records


def changed_objects(baseline, current):
    return sorted(oid for oid in baseline.keys() | current.keys()
                  if baseline.get(oid) != current.get(oid))


def pg_layout(keyring, placement):
    def ceph(*args):
        return json.loads(subprocess.check_output(
            ["ceph", "--keyring", keyring, *args, "-f", "json"], timeout=30))

    pools = {row["pool_name"]: row for row in ceph("osd", "pool", "ls", "detail")}
    pgs = {row["pgid"]: row for row in ceph("pg", "dump", "pgs")["pg_stats"]}
    for item in placement:
        pool = pools.get(item["pool"], {})
        pool_id, target = item["pool_id"], item["physical_osd"]
        pg = pgs.get(f"{pool_id}.0", {})
        if (pool.get("pool_id") != pool_id or pool.get("pg_num") != 1 or
                pool.get("pg_placement_num") != 1 or pool.get("pg_autoscale_mode") != "off" or
                pool.get("size") != 1 or pool.get("min_size") != 1 or
                pg.get("state") != "active+clean" or pg.get("up") != [target] or
                pg.get("acting") != [target]):
            raise RuntimeError(f"Pool ID, geometry, or acting OSD changed for {item['pool']}")


def validate_placement(placement):
    names = [p["pool"] for p in placement]
    expected = {"nsvu_meta"} | {f"nsvu_owner_{i}" for i in range(len(names) - 1)}
    if not 2 <= len(names) <= 6 or len(set(names)) != len(names) or set(names) != expected:
        raise RuntimeError("Placement must name only nsvu_meta and the contiguous NSVU owner pools")


def validate_objects(name, records):
    if name == "nsvu_meta":
        if set(records) != {"hnsw.global.meta"}:
            raise RuntimeError("Unexpected objects in nsvu_meta")
    elif ("hnsw.owner.meta" not in records or any(
            oid != "hnsw.owner.meta" and not re.fullmatch(r"hnsw\.owner\.data\.\d{6,}", oid)
            for oid in records)):
        raise RuntimeError(f"Unexpected objects in {name}")


def require_fresh_import(ioctx, rados):
    keys = ("meta/cur_element_count", "meta/next_global_id", "meta/version")
    with rados.ReadOpCtx() as op:
        entries, _ = ioctx.get_omap_vals_by_keys(op, keys)
        ioctx.operate_read_op(op, "hnsw.global.meta")
        rows = {(key.decode() if isinstance(key, bytes) else key): value for key, value in entries}
    if set(rows) != set(keys):
        raise RuntimeError("Importer has not published the complete global metadata")
    count, next_id, version = (struct.unpack("<Q", rows[key])[0] for key in keys)
    if count == 0 or count != next_id or version != 1:
        raise RuntimeError("Capture requires the freshly imported index before any updates")
    return count


def capture(path, document, cluster, contexts, keyring):
    for name, ioctx in contexts.items():
        if inventory(ioctx) != document["pools"][name]["objects"]:
            raise RuntimeError(f"HEAD changed since snapshot capture started in {name}")
    for name, ioctx in contexts.items():
        pool = document["pools"][name]
        snaps = {snap.name: snap.snap_id for snap in ioctx.list_snaps()}
        snap_id = snaps.get(document["snapshot"])
        if "snap_id" in pool and snap_id != pool["snap_id"]:
            raise RuntimeError(f"Previously created snapshot changed in {name}")
        if snap_id is None:
            ioctx.create_snap(document["snapshot"])
            snap_id = ioctx.lookup_snap(document["snapshot"]).snap_id
        pool["snap_id"] = snap_id
        save(path, document)
    for name, ioctx in contexts.items():
        pool = document["pools"][name]
        if inventory(ioctx) != pool["objects"]:
            raise RuntimeError(f"HEAD changed during snapshot capture in {name}")
        with cluster.open_ioctx(name) as snapshot:
            snapshot.set_read(pool["snap_id"])
            for oid, record in pool["objects"].items():
                size, _ = snapshot.stat(oid)
                if size != record["bytes"] or snapshot.get_last_version() != record["version"]:
                    raise RuntimeError(f"Snapshot does not match the captured object in {name}/{oid}")
    pg_layout(keyring, document["placement"])
    document["state"] = "ready"
    save(path, document)


def restore_pool(head, snapshot, pool, name, digest, object_not_found):
    baseline = pool["objects"]
    current = inventory(head)
    candidates = set(changed_objects(baseline, current)) | set(pool.get("restore_objects", []))
    rolled_back = 0
    for oid in sorted(candidates):
        if oid not in baseline:
            if oid not in current:
                continue
            try:
                snapshot.stat(oid)
            except object_not_found:
                head.snap_rollback(oid, name)
                rolled_back += 1
            else:
                raise RuntimeError(f"Unexpected object in snapshot: {oid}")
            continue
        size = baseline[oid]["bytes"]
        if snapshot.stat(oid)[0] != size:
            raise RuntimeError(f"Snapshot size differs from the original inventory: {oid}")
        head.snap_rollback(oid, name)
        rolled_back += 1
        if (head.stat(oid)[0] != size or
                digest.omap_digest(head, oid) != digest.omap_digest(snapshot, oid) or
                digest.payload_digest(head, oid, size) != digest.payload_digest(snapshot, oid, size)):
            raise RuntimeError(f"Restored data or OMAP differs from the snapshot: {oid}")
    restored = inventory(head)
    if set(restored) != set(baseline) or any(restored[oid]["bytes"] != baseline[oid]["bytes"] for oid in baseline):
        raise RuntimeError("Restored object inventory differs from the snapshot inventory")
    return rolled_back


def restore(path, document, cluster, contexts, rados, keyring):
    start = time.monotonic()
    digest = load_digest()
    counts, restored = {}, {}
    for name, head in contexts.items():
        pool = document["pools"][name]
        pool["restore_objects"] = sorted(set(pool.get("restore_objects", [])) |
                                         set(changed_objects(pool["objects"], inventory(head))))
    document["state"] = "restoring"
    save(path, document)
    for name, head in contexts.items():
        pool = document["pools"][name]
        with cluster.open_ioctx(name) as snapshot:
            snapshot.set_read(pool["snap_id"])
            counts[name] = restore_pool(head, snapshot, pool, document["snapshot"], digest, rados.ObjectNotFound)
        restored[name] = inventory(head)
    pg_layout(keyring, document["placement"])
    for name, objects in restored.items():
        pool = document["pools"][name]
        pool["objects"] = objects
        del pool["restore_objects"]
    document["state"] = "ready"
    save(path, document)
    return {"state": "restored", "objects_rolled_back": counts, "seconds": round(time.monotonic() - start, 3)}


def run(args, rados):
    path = args.checkpoint.resolve()
    document = read(path) if path.exists() else None
    if document is None and (args.action != "capture" or args.placement is None):
        raise RuntimeError("A new capture requires --placement and a new checkpoint file")
    if document is not None and (document.get("schema_version") != 1 or
            not re.fullmatch(r"nsvu-index-[a-f0-9]{32}", document.get("snapshot", ""))):
        raise RuntimeError("Unrecognized NSVU snapshot checkpoint")
    placement = document["placement"] if document else read(args.placement)["pools"]
    validate_placement(placement)
    with ExitStack() as stack:
        lock = stack.enter_context(LOCK.open("a"))
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (read(path) if path.exists() else None) != document:
            raise RuntimeError("Checkpoint changed while acquiring the experiment lock")
        cluster = stack.enter_context(rados.Rados(conffile="/etc/ceph/ceph.conf", name="client.admin",
                                                  conf={"keyring": args.keyring, "rados_osd_op_timeout": "45"}))
        if cluster.get_fsid() != args.expected_fsid or (document and document["fsid"] != args.expected_fsid):
            raise RuntimeError("Ceph FSID mismatch")
        pg_layout(args.keyring, placement)
        contexts = {item["pool"]: stack.enter_context(cluster.open_ioctx(item["pool"])) for item in placement}
        for item in placement:
            if contexts[item["pool"]].get_pool_id() != item["pool_id"]:
                raise RuntimeError(f"Pool ID changed for {item['pool']}")
        if document is None:
            count = require_fresh_import(contexts["nsvu_meta"], rados)
            document = {"schema_version": 1, "fsid": args.expected_fsid, "placement": placement, "state": "creating",
                        "snapshot": "nsvu-index-" + uuid.uuid4().hex, "base_nodes": count,
                        "pools": {name: {"objects": inventory(ioctx)} for name, ioctx in contexts.items()}}
            for name, pool in document["pools"].items():
                validate_objects(name, pool["objects"])
            save(path, document)
        if args.action == "capture":
            if document["state"] != "creating":
                raise RuntimeError("Capture needs a new or incomplete checkpoint file")
            capture(path, document, cluster, contexts, args.keyring)
            return {"state": "ready", "snapshot": document["snapshot"], "base_nodes": document["base_nodes"]}
        if args.action == "drop":
            if document["state"] == "restoring":
                raise RuntimeError("Complete the interrupted restore before deleting its snapshot")
            for name, ioctx in contexts.items():
                snapshots = {snap.name: snap.snap_id for snap in ioctx.list_snaps()}
                snap_id = snapshots.get(document["snapshot"])
                if snap_id is not None and snap_id != document["pools"][name].get("snap_id"):
                    raise RuntimeError(f"Cannot verify snapshot identity in {name}")
            document["state"] = "dropping"
            save(path, document)
            for ioctx in contexts.values():
                if document["snapshot"] in {snap.name for snap in ioctx.list_snaps()}:
                    ioctx.remove_snap(document["snapshot"])
            document["state"] = "dropped"
            save(path, document)
            return {"state": "dropped", "snapshot": document["snapshot"]}
        if document["state"] not in {"ready", "restoring"}:
            raise RuntimeError("Restore needs a complete snapshot checkpoint")
        changes = {}
        for name, ioctx in contexts.items():
            pool = document["pools"][name]
            if ioctx.lookup_snap(document["snapshot"]).snap_id != pool["snap_id"]:
                raise RuntimeError(f"Snapshot ID changed in {name}")
            current = inventory(ioctx)
            validate_objects(name, current.keys() | pool["objects"].keys())
            changes[name] = sorted(set(changed_objects(pool["objects"], current)) |
                                   (set(pool.get("restore_objects", [])) &
                                    (current.keys() | pool["objects"].keys())))
        if args.action == "plan":
            return {"state": document["state"], "objects_to_rollback": changes}
        return restore(path, document, cluster, contexts, rados, args.keyring)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("capture", "plan", "restore", "drop"))
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--placement", type=Path, help="Runner pool-placement-latest.json for a new capture")
    parser.add_argument("--keyring", required=True)
    parser.add_argument("--expected-fsid", required=True)
    args = parser.parse_args()
    import rados
    try:
        print(json.dumps(run(args, rados)))
    except BlockingIOError:
        sys.exit("The index is in use by another high-recall experiment or snapshot operation")
    except (OSError, KeyError, RuntimeError, ValueError, subprocess.SubprocessError, rados.Error) as error:
        sys.exit(str(error))


if __name__ == "__main__":
    main()
