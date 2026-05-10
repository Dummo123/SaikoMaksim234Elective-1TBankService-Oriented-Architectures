import logging
import datetime
from cassandra.cluster import Cluster, ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from cassandra.query import BatchStatement, BatchType, SimpleStatement

log = logging.getLogger("cassandra_client")


class CassandraClient:
    """
    Thin wrapper around the Python Cassandra driver.

    Write consistency = QUORUM so that a write is acknowledged by the majority
    of replicas (2 out of 3). This means a single node failure does not cause
    data loss and the cluster keeps accepting writes.

    Read consistency = ONE for most lookups — the consumer is the only writer,
    and we accept the small risk of reading slightly stale data from the local
    replica in exchange for lower latency. QUORUM reads are used where strong
    consistency is explicitly needed (e.g., idempotency check).
    """

    def __init__(self, hosts, keyspace):
        self.hosts = hosts
        self.keyspace = keyspace
        self.cluster = None
        self.session = None

        # Prepared statements populated after connect()
        self._stmts = {}

    def connect(self):
        profile_default = ExecutionProfile(
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.ONE,
            request_timeout=15.0,
        )
        profile_quorum = ExecutionProfile(
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.QUORUM,
            request_timeout=15.0,
        )

        self.cluster = Cluster(
            self.hosts,
            execution_profiles={
                EXEC_PROFILE_DEFAULT: profile_default,
                "quorum": profile_quorum,
            },
            protocol_version=4,
            connect_timeout=20,
        )
        self.session = self.cluster.connect(self.keyspace)
        self._prepare_all()
        log.info("Cassandra connected: hosts=%s keyspace=%s", self.hosts, self.keyspace)

    def close(self):
        if self.cluster:
            self.cluster.shutdown()

    # ─── Prepared statements ───────────────────────────────────────────────

    def _prepare_all(self):
        s = self.session

        self._stmts["check_processed"] = s.prepare(
            "SELECT event_id FROM processed_events WHERE event_id = ?"
        )
        self._stmts["get_pz_inv"] = s.prepare(
            "SELECT available_quantity, reserved_quantity, last_event_timestamp "
            "FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?"
        )
        self._stmts["get_prod_inv"] = s.prepare(
            "SELECT total_available, total_reserved "
            "FROM inventory_by_product WHERE product_id = ?"
        )
        self._stmts["get_zone_inv"] = s.prepare(
            "SELECT available_quantity, reserved_quantity "
            "FROM inventory_by_zone WHERE zone_id = ? AND product_id = ?"
        )
        self._stmts["get_order"] = s.prepare(
            "SELECT order_id, status, items FROM orders WHERE order_id = ?"
        )

        # Upsert statements for each table — used inside BATCH
        self._stmts["upsert_pz"] = s.prepare(
            "INSERT INTO inventory_by_product_zone "
            "(product_id, zone_id, available_quantity, reserved_quantity, "
            " last_event_id, last_event_timestamp, updated_at, supplier_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        self._stmts["upsert_prod"] = s.prepare(
            "INSERT INTO inventory_by_product "
            "(product_id, total_available, total_reserved, last_updated) "
            "VALUES (?, ?, ?, ?)"
        )
        self._stmts["upsert_zone"] = s.prepare(
            "INSERT INTO inventory_by_zone "
            "(zone_id, product_id, available_quantity, reserved_quantity, last_updated) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        self._stmts["insert_processed"] = s.prepare(
            "INSERT INTO processed_events (event_id, event_type, processed_at) "
            "VALUES (?, ?, ?) USING TTL 604800"
        )
        self._stmts["insert_history"] = s.prepare(
            "INSERT INTO event_history "
            "(product_id, event_timestamp, event_id, event_type, zone_id, quantity) "
            "VALUES (?, ?, ?, ?, ?, ?) USING TTL 2592000"
        )
        self._stmts["upsert_order"] = s.prepare(
            "INSERT INTO orders (order_id, status, items, created_at) VALUES (?, ?, ?, ?)"
        )
        self._stmts["update_order_status"] = s.prepare(
            "UPDATE orders SET status = ?, completed_at = ? WHERE order_id = ?"
        )

    # ─── Read helpers ──────────────────────────────────────────────────────

    def is_processed(self, event_id: str) -> bool:
        """QUORUM read so we don't miss a recent write on a replica."""
        row = self.session.execute(
            self._stmts["check_processed"],
            [event_id],
            execution_profile="quorum",
        ).one()
        return row is not None

    def get_zone_inventory(self, product_id: str, zone_id: str) -> dict:
        row = self.session.execute(
            self._stmts["get_pz_inv"], [product_id, zone_id]
        ).one()
        if row:
            return {
                "available": row.available_quantity or 0,
                "reserved": row.reserved_quantity or 0,
                "last_ts": row.last_event_timestamp or 0,
            }
        return {"available": 0, "reserved": 0, "last_ts": 0}

    def get_product_inventory(self, product_id: str) -> dict:
        row = self.session.execute(
            self._stmts["get_prod_inv"], [product_id]
        ).one()
        if row:
            return {
                "total_available": row.total_available or 0,
                "total_reserved": row.total_reserved or 0,
            }
        return {"total_available": 0, "total_reserved": 0}

    def get_zone_product_inventory(self, zone_id: str, product_id: str) -> dict:
        row = self.session.execute(
            self._stmts["get_zone_inv"], [zone_id, product_id]
        ).one()
        if row:
            return {
                "available": row.available_quantity or 0,
                "reserved": row.reserved_quantity or 0,
            }
        return {"available": 0, "reserved": 0}

    def get_order(self, order_id: str):
        return self.session.execute(self._stmts["get_order"], [order_id]).one()

    # ─── Batch write helpers ───────────────────────────────────────────────

    def _new_batch(self) -> BatchStatement:
        """
        Logged batch — Cassandra guarantees either all statements succeed or
        none do. This prevents partial updates across the three inventory tables.
        Consistency = QUORUM for all writes.
        """
        return BatchStatement(
            batch_type=BatchType.LOGGED,
            consistency_level=ConsistencyLevel.QUORUM,
        )

    def _now(self):
        return datetime.datetime.now(datetime.timezone.utc)

    def execute_inventory_update(
        self,
        event_id: str,
        event_type: str,
        product_id: str,
        zone_id: str,
        new_avail: int,
        new_reserved: int,
        event_ts: int,
        prod_total_avail: int,
        prod_total_reserved: int,
        zone_avail: int,
        zone_reserved: int,
        history_qty: int,
        supplier_id=None,
    ):
        """
        Single LOGGED BATCH that updates all three inventory tables,
        marks the event as processed, and appends to the audit log.
        Either all writes land or none do.
        """
        batch = self._new_batch()
        now = self._now()

        batch.add(
            self._stmts["upsert_pz"],
            [product_id, zone_id, new_avail, new_reserved, event_id, event_ts, now, supplier_id],
        )
        batch.add(
            self._stmts["upsert_prod"],
            [product_id, prod_total_avail, prod_total_reserved, now],
        )
        batch.add(
            self._stmts["upsert_zone"],
            [zone_id, product_id, zone_avail, zone_reserved, now],
        )
        batch.add(
            self._stmts["insert_processed"],
            [event_id, event_type, now],
        )
        batch.add(
            self._stmts["insert_history"],
            [product_id, event_ts, event_id, event_type, zone_id, history_qty],
        )

        self.session.execute(batch)

    def mark_processed_only(self, event_id: str, event_type: str):
        """For events that don't touch inventory (e.g. skipped out-of-order)."""
        self.session.execute(
            self._stmts["insert_processed"],
            [event_id, event_type, self._now()],
            execution_profile="quorum",
        )

    def create_order(self, order_id: str, items_json: str, event_id: str, event_type: str):
        batch = self._new_batch()
        now = self._now()
        batch.add(self._stmts["upsert_order"], [order_id, "CREATED", items_json, now])
        batch.add(self._stmts["insert_processed"], [event_id, event_type, now])
        self.session.execute(batch)

    def complete_order(self, order_id: str, event_id: str, event_type: str):
        batch = self._new_batch()
        now = self._now()
        batch.add(self._stmts["update_order_status"], ["COMPLETED", now, order_id])
        batch.add(self._stmts["insert_processed"], [event_id, event_type, now])
        self.session.execute(batch)

    def execute_move_update(
        self,
        event_id: str,
        event_type: str,
        product_id: str,
        from_zone_id: str,
        to_zone_id: str,
        from_avail: int,
        from_reserved: int,
        to_avail: int,
        to_reserved: int,
        event_ts: int,
        zone_from_avail: int,
        zone_from_reserved: int,
        zone_to_avail: int,
        zone_to_reserved: int,
        quantity: int,
    ):
        """PRODUCT_MOVED — updates two zones in a single batch."""
        batch = self._new_batch()
        now = self._now()
        prod_inv = self.get_product_inventory(product_id)

        batch.add(
            self._stmts["upsert_pz"],
            [product_id, from_zone_id, from_avail, from_reserved, event_id, event_ts, now, None],
        )
        batch.add(
            self._stmts["upsert_pz"],
            [product_id, to_zone_id, to_avail, to_reserved, event_id, event_ts, now, None],
        )
        batch.add(
            self._stmts["upsert_prod"],
            [product_id, prod_inv["total_available"], prod_inv["total_reserved"], now],
        )
        batch.add(
            self._stmts["upsert_zone"],
            [from_zone_id, product_id, zone_from_avail, zone_from_reserved, now],
        )
        batch.add(
            self._stmts["upsert_zone"],
            [to_zone_id, product_id, zone_to_avail, zone_to_reserved, now],
        )
        batch.add(self._stmts["insert_processed"], [event_id, event_type, now])
        batch.add(
            self._stmts["insert_history"],
            [product_id, event_ts, event_id, event_type, f"{from_zone_id}→{to_zone_id}", quantity],
        )
        self.session.execute(batch)
