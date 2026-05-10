import json
import logging

log = logging.getLogger("handlers")


class EventHandler:
    """
    Routes each incoming event to the correct handler method.
    Every method follows the same pattern:
      1. Read current state from Cassandra
      2. Check out-of-order timestamp where relevant
      3. Compute new state
      4. Write all affected tables in a single logged BATCH
    """

    def __init__(self, cassandra_client):
        self.db = cassandra_client

    def handle(self, event: dict):
        event_type = event.get("event_type")
        dispatch = {
            "PRODUCT_RECEIVED":   self._product_received,
            "PRODUCT_SHIPPED":    self._product_shipped,
            "PRODUCT_MOVED":      self._product_moved,
            "PRODUCT_RESERVED":   self._product_reserved,
            "PRODUCT_RELEASED":   self._product_released,
            "INVENTORY_COUNTED":  self._inventory_counted,
            "ORDER_CREATED":      self._order_created,
            "ORDER_COMPLETED":    self._order_completed,
        }
        handler = dispatch.get(event_type)
        if handler is None:
            raise ValueError(f"Unknown event_type: {event_type}")
        handler(event)

    # ─── Out-of-order guard ────────────────────────────────────────────────

    def _is_stale(self, product_id: str, zone_id: str, event_ts: int) -> bool:
        """
        Returns True if the event timestamp is older than or equal to the last
        processed event for this product-zone pair. Stale events are silently
        dropped — they would overwrite newer state with older values.
        """
        current = self.db.get_zone_inventory(product_id, zone_id)
        if current["last_ts"] and event_ts <= current["last_ts"]:
            log.warning(
                "Out-of-order event dropped: product=%s zone=%s event_ts=%d last_ts=%d",
                product_id, zone_id, event_ts, current["last_ts"],
            )
            return True
        return False

    # ─── Individual handlers ───────────────────────────────────────────────

    def _product_received(self, event: dict):
        pid = event["product_id"]
        zid = event["zone_id"]
        qty = event["quantity"]
        ts  = event["event_timestamp"]
        eid = event["event_id"]
        etype = event["event_type"]
        supplier_id = event.get("supplier_id")  # V2 field, None for V1 events

        if self._is_stale(pid, zid, ts):
            self.db.mark_processed_only(eid, etype)
            return

        pz   = self.db.get_zone_inventory(pid, zid)
        prod = self.db.get_product_inventory(pid)
        zn   = self.db.get_zone_product_inventory(zid, pid)

        new_avail    = pz["available"] + qty
        new_reserved = pz["reserved"]

        self.db.execute_inventory_update(
            event_id=eid, event_type=etype,
            product_id=pid, zone_id=zid,
            new_avail=new_avail, new_reserved=new_reserved,
            event_ts=ts,
            prod_total_avail=prod["total_available"] + qty,
            prod_total_reserved=prod["total_reserved"],
            zone_avail=zn["available"] + qty,
            zone_reserved=zn["reserved"],
            history_qty=qty,
            supplier_id=supplier_id,
        )
        log.info("RECEIVED product=%s zone=%s qty=%d → available=%d supplier=%s",
                 pid, zid, qty, new_avail, supplier_id)

    def _product_shipped(self, event: dict):
        pid = event["product_id"]
        zid = event["zone_id"]
        qty = event["quantity"]
        ts  = event["event_timestamp"]
        eid = event["event_id"]
        etype = event["event_type"]

        if self._is_stale(pid, zid, ts):
            self.db.mark_processed_only(eid, etype)
            return

        pz   = self.db.get_zone_inventory(pid, zid)
        prod = self.db.get_product_inventory(pid)
        zn   = self.db.get_zone_product_inventory(zid, pid)

        if pz["available"] < qty:
            raise ValueError(
                f"Insufficient stock: product={pid} zone={zid} "
                f"available={pz['available']} requested={qty}"
            )

        new_avail = pz["available"] - qty

        self.db.execute_inventory_update(
            event_id=eid, event_type=etype,
            product_id=pid, zone_id=zid,
            new_avail=new_avail, new_reserved=pz["reserved"],
            event_ts=ts,
            prod_total_avail=prod["total_available"] - qty,
            prod_total_reserved=prod["total_reserved"],
            zone_avail=zn["available"] - qty,
            zone_reserved=zn["reserved"],
            history_qty=qty,
        )
        log.info("SHIPPED product=%s zone=%s qty=%d → available=%d", pid, zid, qty, new_avail)

    def _product_moved(self, event: dict):
        pid       = event["product_id"]
        from_zone = event["from_zone_id"]
        to_zone   = event["to_zone_id"]
        qty       = event["quantity"]
        ts        = event["event_timestamp"]
        eid       = event["event_id"]
        etype     = event["event_type"]

        # Check staleness for both zones
        from_inv = self.db.get_zone_inventory(pid, from_zone)
        to_inv   = self.db.get_zone_inventory(pid, to_zone)

        if from_inv["last_ts"] and ts <= from_inv["last_ts"]:
            log.warning("Out-of-order MOVE dropped: product=%s from=%s ts=%d last=%d",
                        pid, from_zone, ts, from_inv["last_ts"])
            self.db.mark_processed_only(eid, etype)
            return

        if from_inv["available"] < qty:
            raise ValueError(
                f"Cannot move: product={pid} from={from_zone} "
                f"available={from_inv['available']} requested={qty}"
            )

        zn_from = self.db.get_zone_product_inventory(from_zone, pid)
        zn_to   = self.db.get_zone_product_inventory(to_zone, pid)

        self.db.execute_move_update(
            event_id=eid, event_type=etype,
            product_id=pid,
            from_zone_id=from_zone, to_zone_id=to_zone,
            from_avail=from_inv["available"] - qty,
            from_reserved=from_inv["reserved"],
            to_avail=to_inv["available"] + qty,
            to_reserved=to_inv["reserved"],
            event_ts=ts,
            zone_from_avail=zn_from["available"] - qty,
            zone_from_reserved=zn_from["reserved"],
            zone_to_avail=zn_to["available"] + qty,
            zone_to_reserved=zn_to["reserved"],
            quantity=qty,
        )
        log.info("MOVED product=%s from=%s to=%s qty=%d", pid, from_zone, to_zone, qty)

    def _product_reserved(self, event: dict):
        pid = event["product_id"]
        zid = event["zone_id"]
        qty = event["quantity"]
        ts  = event["event_timestamp"]
        eid = event["event_id"]
        etype = event["event_type"]

        if self._is_stale(pid, zid, ts):
            self.db.mark_processed_only(eid, etype)
            return

        pz   = self.db.get_zone_inventory(pid, zid)
        prod = self.db.get_product_inventory(pid)
        zn   = self.db.get_zone_product_inventory(zid, pid)

        if pz["available"] < qty:
            raise ValueError(
                f"Cannot reserve: product={pid} zone={zid} "
                f"available={pz['available']} requested={qty}"
            )

        self.db.execute_inventory_update(
            event_id=eid, event_type=etype,
            product_id=pid, zone_id=zid,
            new_avail=pz["available"] - qty,
            new_reserved=pz["reserved"] + qty,
            event_ts=ts,
            prod_total_avail=prod["total_available"] - qty,
            prod_total_reserved=prod["total_reserved"] + qty,
            zone_avail=zn["available"] - qty,
            zone_reserved=zn["reserved"] + qty,
            history_qty=qty,
        )
        log.info("RESERVED product=%s zone=%s qty=%d", pid, zid, qty)

    def _product_released(self, event: dict):
        pid = event["product_id"]
        zid = event["zone_id"]
        qty = event["quantity"]
        ts  = event["event_timestamp"]
        eid = event["event_id"]
        etype = event["event_type"]

        if self._is_stale(pid, zid, ts):
            self.db.mark_processed_only(eid, etype)
            return

        pz   = self.db.get_zone_inventory(pid, zid)
        prod = self.db.get_product_inventory(pid)
        zn   = self.db.get_zone_product_inventory(zid, pid)

        if pz["reserved"] < qty:
            raise ValueError(
                f"Cannot release: product={pid} zone={zid} "
                f"reserved={pz['reserved']} requested={qty}"
            )

        self.db.execute_inventory_update(
            event_id=eid, event_type=etype,
            product_id=pid, zone_id=zid,
            new_avail=pz["available"] + qty,
            new_reserved=pz["reserved"] - qty,
            event_ts=ts,
            prod_total_avail=prod["total_available"] + qty,
            prod_total_reserved=prod["total_reserved"] - qty,
            zone_avail=zn["available"] + qty,
            zone_reserved=zn["reserved"] - qty,
            history_qty=qty,
        )
        log.info("RELEASED product=%s zone=%s qty=%d", pid, zid, qty)

    def _inventory_counted(self, event: dict):
        """
        Physical count overrides whatever is in the system — this is intentional.
        Timestamp check still applies: an old count should not overwrite a newer one.
        """
        pid = event["product_id"]
        zid = event["zone_id"]
        qty = event["quantity"]
        ts  = event["event_timestamp"]
        eid = event["event_id"]
        etype = event["event_type"]

        if self._is_stale(pid, zid, ts):
            self.db.mark_processed_only(eid, etype)
            return

        pz      = self.db.get_zone_inventory(pid, zid)
        prod    = self.db.get_product_inventory(pid)
        zn      = self.db.get_zone_product_inventory(zid, pid)
        delta   = qty - pz["available"]

        self.db.execute_inventory_update(
            event_id=eid, event_type=etype,
            product_id=pid, zone_id=zid,
            new_avail=qty,
            new_reserved=pz["reserved"],
            event_ts=ts,
            prod_total_avail=prod["total_available"] + delta,
            prod_total_reserved=prod["total_reserved"],
            zone_avail=qty,
            zone_reserved=zn["reserved"],
            history_qty=qty,
        )
        log.info("COUNTED product=%s zone=%s → available set to %d (delta=%+d)",
                 pid, zid, qty, delta)

    def _order_created(self, event: dict):
        order_id   = event["order_id"]
        items_json = event.get("order_items") or "[]"
        eid        = event["event_id"]
        etype      = event["event_type"]

        # Parse order items and reserve stock for each line
        try:
            items = json.loads(items_json)
        except (json.JSONDecodeError, TypeError):
            items = []

        self.db.create_order(order_id, items_json, eid, etype)

        for item in items:
            item_pid = item.get("product_id")
            item_zid = item.get("zone_id")
            item_qty = item.get("quantity", 0)
            if item_pid and item_zid and item_qty > 0:
                reserve_event = {
                    "event_id":        f"{eid}_reserve_{item_pid}",
                    "event_type":      "PRODUCT_RESERVED",
                    "product_id":      item_pid,
                    "zone_id":         item_zid,
                    "quantity":        item_qty,
                    "event_timestamp": event["event_timestamp"] + 1,
                    "order_id":        order_id,
                }
                self._product_reserved(reserve_event)

        log.info("ORDER_CREATED order=%s items=%d", order_id, len(items))

    def _order_completed(self, event: dict):
        order_id = event["order_id"]
        eid      = event["event_id"]
        etype    = event["event_type"]

        order = self.db.get_order(order_id)
        if order is None:
            raise ValueError(f"Order not found: {order_id}")
        if order.status == "COMPLETED":
            log.info("ORDER already completed, skipping: %s", order_id)
            self.db.mark_processed_only(eid, etype)
            return

        # Ship all reserved items (reserved_quantity decreases, available unchanged)
        try:
            items = json.loads(order.items or "[]")
        except (json.JSONDecodeError, TypeError):
            items = []

        for item in items:
            item_pid = item.get("product_id")
            item_zid = item.get("zone_id")
            item_qty = item.get("quantity", 0)
            if item_pid and item_zid and item_qty > 0:
                pz   = self.db.get_zone_inventory(item_pid, item_zid)
                prod = self.db.get_product_inventory(item_pid)
                zn   = self.db.get_zone_product_inventory(item_zid, item_pid)

                self.db.execute_inventory_update(
                    event_id=f"{eid}_ship_{item_pid}",
                    event_type="ORDER_COMPLETED",
                    product_id=item_pid,
                    zone_id=item_zid,
                    new_avail=pz["available"],
                    new_reserved=max(0, pz["reserved"] - item_qty),
                    event_ts=event["event_timestamp"] + 1,
                    prod_total_avail=prod["total_available"],
                    prod_total_reserved=max(0, prod["total_reserved"] - item_qty),
                    zone_avail=zn["available"],
                    zone_reserved=max(0, zn["reserved"] - item_qty),
                    history_qty=item_qty,
                )

        self.db.complete_order(order_id, eid, etype)
        log.info("ORDER_COMPLETED order=%s items=%d", order_id, len(items))
