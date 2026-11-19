#!/usr/bin/env python3
"""Read-only logical digest of the selected NSVU experiment pools (not an export)."""

import argparse
import hashlib
import json
import struct
from pathlib import Path

DEFAULT_OWNERS = 5


def positive_integer(value):
    try:
        result = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer")
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def framed(digest, value):
    digest.update(struct.pack("<Q", len(value)))
    digest.update(value)


def payload_digest(ioctx, oid, size, block=4 * 1024 * 1024):
    digest = hashlib.sha256()
    offset = 0
    while offset < size:
        expected = min(block, size - offset)
        data = ioctx.read(oid, expected, offset)
        if len(data) != expected:
            raise RuntimeError("short object read: %s at %d" % (oid, offset))
        digest.update(data)
        offset += len(data)
    return digest.hexdigest()


def omap_digest(ioctx, oid):
    import rados
    digest = hashlib.sha256()
    cursor, count = "", 0
    while True:
        with rados.ReadOpCtx() as op:
            iterator, _ = ioctx.get_omap_vals(op, cursor, "", 8192)
            ioctx.operate_read_op(op, oid)
            rows = list(iterator)
        if not rows:
            break
        for key, value in rows:
            text_key = key.decode("utf-8") if isinstance(key, bytes) else key
            if text_key <= cursor:
                raise RuntimeError("OMAP cursor did not advance")
            framed(digest, text_key.encode("utf-8"))
            framed(digest, bytes(value))
            cursor = text_key
            count += 1
    return digest.hexdigest(), count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keyring", required=True)
    parser.add_argument("--expected-fsid", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--owners", type=positive_integer, default=DEFAULT_OWNERS,
                        help="Number of logical owner pools (default: 5)")
    args = parser.parse_args()
    import rados
    records = []
    with rados.Rados(conffile="/etc/ceph/ceph.conf", name="client.admin",
                     conf={"keyring": args.keyring}) as cluster:
        if cluster.get_fsid() != args.expected_fsid:
            raise RuntimeError("FSID mismatch before read-only digest")
        pools = ["nsvu_meta"] + ["nsvu_owner_%d" % i for i in range(args.owners)]
        for pool in pools:
            with cluster.open_ioctx(pool) as ioctx:
                names = sorted(obj.key for obj in ioctx.list_objects())
                for oid in names:
                    size, _ = ioctx.stat(oid)
                    omap, count = omap_digest(ioctx, oid)
                    records.append({"pool": pool, "object": oid, "bytes": size,
                                    "payload_sha256": payload_digest(ioctx, oid, size),
                                    "omap_sha256": omap, "omap_entries": count})
    aggregate = hashlib.sha256(json.dumps(records, sort_keys=True,
                                         separators=(",", ":")).encode()).hexdigest()
    document = {"schema_version": 1, "fsid": args.expected_fsid, "owners": args.owners,
                "logical_sha256": aggregate, "objects": records}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n")
    temporary.replace(args.output)
    print(json.dumps({"objects": len(records), "logical_sha256": aggregate}))


if __name__ == "__main__":
    main()
