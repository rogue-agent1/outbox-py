#!/usr/bin/env python3
"""Transactional outbox pattern for reliable messaging."""
import time, threading, queue, sys

class OutboxEntry:
    def __init__(self, event_type, payload):
        self.id = id(self); self.event_type = event_type; self.payload = payload
        self.created = time.time(); self.published = False

class TransactionalOutbox:
    def __init__(self):
        self.db = {}; self.outbox = []; self.lock = threading.Lock()
    def execute_with_outbox(self, key, value, event_type):
        with self.lock:
            self.db[key] = value
            entry = OutboxEntry(event_type, {"key": key, "value": value})
            self.outbox.append(entry)
            return entry
    def poll_unpublished(self):
        with self.lock:
            return [e for e in self.outbox if not e.published]
    def mark_published(self, entry_id):
        with self.lock:
            for e in self.outbox:
                if e.id == entry_id: e.published = True; return True
        return False

if __name__ == "__main__":
    ob = TransactionalOutbox()
    ob.execute_with_outbox("user:1", {"name": "Alice"}, "UserCreated")
    ob.execute_with_outbox("order:1", {"item": "book"}, "OrderPlaced")
    pending = ob.poll_unpublished()
    print(f"Pending: {len(pending)}")
    for e in pending:
        print(f"  Publishing: {e.event_type} {e.payload}")
        ob.mark_published(e.id)
    print(f"After publish: {len(ob.poll_unpublished())} pending")
