#!/usr/bin/env python3
"""Reproducible interleaving lab for the LaminaDB transaction guide.

Runs five deterministic scenarios against throwaway laminad instances using
only the public HTTP API. Every step prints one JSON observation so the guide
can cite real request/commit facts. Usage:

    python3 -I examples/txn_strategy_lab.py --binary bin/laminad-linux
"""
import argparse, json, pathlib, socket, subprocess, tempfile, time, urllib.request, urllib.error


class Session:
    def __init__(self, binary, root):
        self.binary = str(binary)
        self.root = pathlib.Path(root)
        self.process = None
        self.log = None

    def start(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/v1/lamina"
        self.log = tempfile.TemporaryFile()
        self.process = subprocess.Popen(
            [self.binary, "-data", str(self.root), "-listen", f"127.0.0.1:{port}"],
            stdout=self.log, stderr=self.log,
        )
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log.seek(0)
                raise RuntimeError(self.log.read().decode())
            try:
                self.call("GET", "/health")
                return self
            except (OSError, urllib.error.URLError):
                time.sleep(0.03)
        raise RuntimeError("service did not become ready")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()
            self.log = None

    def call(self, method, path, data=None):
        encoded = None if data is None else json.dumps(data, ensure_ascii=False).encode()
        req = urllib.request.Request(
            self.url + path, data=encoded, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=12) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def begin(self, readonly=False):
        status, body = self.call("POST", "/transactions", {"read_only": readonly})
        assert status == 201, body
        return body

    def commit(self, tx):
        return self.call("POST", f"/transactions/{tx}/commit")

    def rollback(self, tx):
        return self.call("POST", f"/transactions/{tx}/rollback")

    def put(self, tx, row, insert_only=False):
        return self.call("POST", f"/transactions/{tx}/put",
                         {"relation": "accounts", "row": row, "insert_only": insert_only})

    def delete(self, tx, key):
        return self.call("POST", f"/transactions/{tx}/delete",
                         {"relation": "accounts", "key": key})

    def query(self, **kwargs):
        return self.call("POST", "/query", {"relation": "accounts", **kwargs})

    def vacuum(self, horizon):
        return self.call("POST", "/vacuum", {"horizon": horizon})

    def stats(self):
        return self.call("GET", "/stats")


def emit(lab, step, status, body):
    print(json.dumps({"lab": lab, "step": step, "http": status, "obs": body},
                     ensure_ascii=False), flush=True)


def fresh_instance(binary, temp, name):
    root = pathlib.Path(temp) / name
    s = Session(binary, root).start()
    schema = {
        "name": "accounts", "primary": "id",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "balance", "type": "int"},
            {"name": "note", "type": "text", "nullable": True},
        ],
    }
    emit(name, "create-relation", *s.call("POST", "/relations", schema))
    tx = s.begin()["id"]
    s.put(tx, {"id": 1, "balance": 100, "note": None})
    s.put(tx, {"id": 2, "balance": 100, "note": None})
    status, seeded = s.commit(tx)
    emit(name, "seed-two-rows", status, seeded)
    return s


def balance_of(body):
    return {row["id"]: row["balance"] for row in body["rows"]}


def lab_long_txn(binary, temp):
    """Strategy A: one long-lived interactive transaction vs background vacuum."""
    name = "lab1-long-lived-txn"
    s = fresh_instance(binary, temp, name)
    head = s.stats()[1]["sequence"]

    editor = s.begin()  # snapshot pinned at head
    emit(name, "editor-begin", 201, editor)

    writer = s.begin()["id"]
    s.put(writer, {"id": 1, "balance": 150, "note": None})
    emit(name, "writer-commit-150", *s.commit(writer))

    requested = s.stats()[1]["sequence"]
    emit(name, "vacuum-while-editor-open", *s.vacuum(requested))  # expect pinned
    emit(name, "editor-repeatable-read", *s.query(transaction=editor["id"]))
    emit(name, "current-read", *s.query())
    emit(name, "stats-while-pinned", *s.stats())

    emit(name, "editor-rollback", *s.rollback(editor["id"]))
    requested = s.stats()[1]["sequence"]
    emit(name, "vacuum-after-editor-closed", *s.vacuum(requested))  # expect removed>=1
    emit(name, "time-travel-to-editor-snapshot", *s.query(at=head))  # expect 410
    emit(name, "stats-after-reclaim", *s.stats())

    emit(name, "checkpoint", *s.call("POST", "/checkpoint"))
    s.stop()
    s.start()
    emit(name, "stats-after-restart", *s.stats())  # horizon durable
    emit(name, "time-travel-still-gone-after-restart", *s.query(at=head))
    s.stop()


def lab_short_txn(binary, temp):
    """Strategy B: close and reopen transactions per interaction; at-queries do not pin."""
    name = "lab2-reopen-basis"
    s = fresh_instance(binary, temp, name)
    head = s.stats()[1]["sequence"]

    b1 = s.begin()
    emit(name, "b1-read-basis", *s.query(transaction=b1["id"]))
    writer = s.begin()["id"]
    s.put(writer, {"id": 1, "balance": 150, "note": None})
    emit(name, "writer-commit", *s.commit(writer))
    emit(name, "b1-still-old-basis", *s.query(transaction=b1["id"]))
    s.rollback(b1["id"])

    b2 = s.begin()
    emit(name, "b2-read-basis-after-reopen", *s.query(transaction=b2["id"]))
    s.rollback(b2["id"])

    emit(name, "at-query-before-vacuum", *s.query(at=head))
    emit(name, "vacuum-to-head", *s.vacuum(s.stats()[1]["sequence"]))
    emit(name, "at-query-after-vacuum", *s.query(at=head))  # expect 410: at does not pin

    late_tx = s.begin()  # snapshot 4, before the late relation exists
    emit(name, "create-late-relation", *s.call("POST", "/relations", {
        "name": "late", "primary": "k",
        "columns": [{"name": "k", "type": "int"}]}))  # created at 5
    emit(name, "old-txn-query-new-relation", *s.call("POST", "/query", {
        "relation": "late", "transaction": late_tx["id"]}))  # expect 410
    emit(name, "old-txn-put-new-relation", *s.call("POST", f"/transactions/{late_tx['id']}/put", {
        "relation": "late", "row": {"k": 1}}))  # expect 409
    s.rollback(late_tx["id"])
    s.stop()


def lab_write_skew(binary, temp):
    """Disjoint-key writers both commit: SI is not serialization."""
    name = "lab3-write-skew"
    s = fresh_instance(binary, temp, name)
    t1, t2 = s.begin()["id"], s.begin()["id"]

    status, view = s.query(transaction=t1)
    total = sum(r["balance"] for r in view["rows"])
    emit(name, "t1-decision-read", status, {"rows": view["rows"], "total": total})
    status, view = s.query(transaction=t2)
    emit(name, "t2-decision-read", status, {"rows": view["rows"],
                                            "total": sum(r["balance"] for r in view["rows"])})

    # User-level invariant: total balance must never fall below 100. Each
    # snapshot sees total=200, so each computes total-100=100 >= 100 and proceeds.
    s.put(t1, {"id": 1, "balance": 0, "note": None})
    s.put(t2, {"id": 2, "balance": 0, "note": None})
    emit(name, "t1-commit", *s.commit(t1))
    emit(name, "t2-commit-disjoint-key", *s.commit(t2))  # expect 200, no conflict
    status, final = s.query()
    emit(name, "final-state", status, {"rows": final["rows"],
                                       "total": sum(r["balance"] for r in final["rows"])})
    s.stop()


def lab_conflicts(binary, temp):
    """Same-key first-committer-wins and unique final-state validation."""
    name = "lab4-fcw-and-unique"
    s = fresh_instance(binary, temp, name)
    emit(name, "create-unique-index", *s.call("POST", "/indexes", {
        "name": "by_note", "relation": "accounts", "column": "note", "unique": True}))

    t1, t2 = s.begin()["id"], s.begin()["id"]
    s.put(t1, {"id": 1, "balance": 110, "note": None})
    s.put(t2, {"id": 1, "balance": 220, "note": None})
    s.put(t2, {"id": 9, "balance": 1, "note": None})  # extra write must be discarded too
    head_before_t1 = s.stats()[1]["sequence"]
    emit(name, "t1-commit", *s.commit(t1))
    head_after_t1 = s.stats()[1]["sequence"]
    emit(name, "t2-same-key-commit", *s.commit(t2))  # expect 409
    emit(name, "t2-recommit-after-failure", *s.commit(t2))  # expect 404, tx closed
    head_after_failure = s.stats()[1]["sequence"]
    emit(name, "head-sequence-around-failure", 200, {
        "before_t1": head_before_t1, "after_t1": head_after_t1,
        "after_failed_t2": head_after_failure,
        "failure_consumed_sequence": head_after_failure != head_after_t1})
    emit(name, "t2-extra-write-not-published", *s.query(where=[
        {"column": "id", "op": "=", "value": 9}]))

    u1, u2 = s.begin()["id"], s.begin()["id"]
    s.put(u1, {"id": 3, "balance": 10, "note": "shared"})
    s.put(u2, {"id": 4, "balance": 20, "note": "shared"})  # different primary key
    emit(name, "u1-commit-unique-value", *s.commit(u1))
    emit(name, "u2-commit-different-key-same-unique", *s.commit(u2))  # expect 409

    swap = s.begin()["id"]
    s.put(swap, {"id": 1, "balance": 110, "note": "n2"})
    s.put(swap, {"id": 2, "balance": 100, "note": "n1"})
    emit(name, "in-txn-unique-swap", *s.commit(swap))  # expect 200
    s.stop()


def lab_retention_shape(binary, temp):
    """Version-chain retention rule, tombstones and monotonic horizon."""
    name = "lab5-retention-shape"
    s = fresh_instance(binary, temp, name)
    for balance in (110, 120, 130):
        tx = s.begin()["id"]
        s.put(tx, {"id": 1, "balance": balance, "note": None})
        emit(name, f"update-id1-to-{balance}", *s.commit(tx))
    tx = s.begin()["id"]
    s.delete(tx, 2)
    emit(name, "delete-id2", *s.commit(tx))
    head = s.stats()[1]["sequence"]
    emit(name, "stats-before-vacuum", *s.stats())

    # Horizon in the middle of the id1 chain and before the id2 tombstone.
    mid = head - 2
    emit(name, f"vacuum-horizon-{mid}", *s.vacuum(mid))
    emit(name, f"read-at-horizon-{mid}", *s.query(at=mid))
    older = mid - 1
    emit(name, f"read-below-horizon-{older}", *s.query(at=older))  # expect 410
    emit(name, "deleted-key-still-absent-current", *s.query(where=[
        {"column": "id", "op": "=", "value": 2}]))
    emit(name, "stats-after-partial-vacuum", *s.stats())

    emit(name, "vacuum-cannot-regress", *s.vacuum(older))  # expect 400
    emit(name, "vacuum-past-tombstone", *s.vacuum(head))
    emit(name, "stats-after-tombstone-removal", *s.stats())

    ro = s.begin(readonly=True)["id"]  # snapshot 8
    tx = s.begin()["id"]
    s.put(tx, {"id": 1, "balance": 999, "note": None})
    emit(name, "commit-after-ro-began", *s.commit(tx))  # seq 9
    emit(name, "readonly-txn-pins-vacuum", *s.vacuum(s.stats()[1]["sequence"]))  # pinned to 8
    emit(name, "readonly-write-rejected", *s.call("POST", f"/transactions/{ro}/put", {
        "relation": "accounts", "row": {"id": 1, "balance": 1, "note": None}}))  # 409
    s.rollback(ro)
    emit(name, "vacuum-after-ro-closed", *s.vacuum(s.stats()[1]["sequence"]))  # unpinned
    s.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--lab", default="all",
                        choices=["all", "1", "2", "3", "4", "5"])
    args = parser.parse_args()
    labs = {
        "1": lab_long_txn, "2": lab_short_txn, "3": lab_write_skew,
        "4": lab_conflicts, "5": lab_retention_shape,
    }
    with tempfile.TemporaryDirectory(prefix="lamina-txn-lab-") as temp:
        chosen = labs.keys() if args.lab == "all" else [args.lab]
        for key in chosen:
            labs[key](args.binary, temp)


if __name__ == "__main__":
    main()
