import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends, Path, status
from fastapi.responses import JSONResponse
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_async_session
from src.models.db import OrderDB, OrderItemDB, ProductDB, PromoCodeDB, UserOperationDB
from src.models.generated import OrderCreate, OrderResponse, OrderStatus, OrderUpdate, ProductStatus
from src.routers.auth import RoleChecker, _auth_guard

router = APIRouter()

# N minutes rate-limit window (configurable)
RATE_LIMIT_MINUTES = 1

allow_users        = RoleChecker(["USER"])
allow_users_admins = RoleChecker(["USER", "ADMIN"])

_OPEN_STATUSES  = [OrderStatus.CREATED.value, OrderStatus.PAYMENT_PENDING.value]
_CANCELLABLE    = [OrderStatus.CREATED.value, OrderStatus.PAYMENT_PENDING.value]


def error_resp(code: int, err_code: str, msg: str, details=None):
    return JSONResponse(
        status_code=code,
        content={"error_code": err_code, "message": msg, "details": details},
    )


async def _check_rate_limit(user_id: uuid.UUID, op_type: str, session: AsyncSession):
    """Check the single most recent operation of op_type. If < RATE_LIMIT_MINUTES ago, reject."""
    window_start = datetime.now(timezone.utc) - timedelta(minutes=RATE_LIMIT_MINUTES)
    recent = (
        await session.execute(
            select(UserOperationDB)
            .where(
                and_(
                    UserOperationDB.user_id        == user_id,
                    UserOperationDB.operation_type == op_type,
                    UserOperationDB.created_at     >= window_start,
                )
            )
            .order_by(UserOperationDB.created_at.desc())
            .limit(1)
        )
    ).scalars().first()

    return recent is not None


async def _build_order_response(order: OrderDB, session: AsyncSession) -> dict:
    items = (await session.execute(select(OrderItemDB).where(OrderItemDB.order_id == order.id))).scalars().all()
    return {
        "id":              order.id,
        "user_id":         order.user_id,
        "status":          order.status,
        "promo_code_id":   order.promo_code_id,
        "total_amount":    float(order.total_amount),
        "discount_amount": float(order.discount_amount),
        "items": [
            {
                "id":             i.id,
                "product_id":     i.product_id,
                "quantity":       i.quantity,
                "price_at_order": float(i.price_at_order),
            }
            for i in items
        ],
        "created_at": order.created_at,
        "updated_at": order.updated_at,
    }


async def _resolve_promo(
    code: str,
    subtotal: Decimal,
    now: datetime,
    session: AsyncSession,
    existing_promo_id: uuid.UUID | None = None,
) -> tuple[Decimal, uuid.UUID | None]:
    """
    Validates and applies a promo code.
    Returns (discount_amount, promo_code_id).
    On failure raises a JSONResponse — callers must handle.
    """
    promo = (await session.execute(select(PromoCodeDB).where(PromoCodeDB.code == code))).scalars().first()

    if not promo or not promo.active or promo.current_uses >= promo.max_uses:
        raise _PromoError(error_resp(422, "PROMO_CODE_INVALID", "Promo code is invalid, exhausted, or inactive"))
    if not (promo.valid_from <= now <= promo.valid_until):
        raise _PromoError(error_resp(422, "PROMO_CODE_INVALID", "Promo code is outside its validity window"))
    if subtotal < Decimal(str(promo.min_order_amount)):
        raise _PromoError(error_resp(422, "PROMO_CODE_MIN_AMOUNT", f"Order total must be at least {promo.min_order_amount}"))

    if promo.discount_type == "PERCENTAGE":
        discount = (subtotal * Decimal(str(promo.discount_value)) / 100).quantize(Decimal("0.01"))
        # Per task spec: if discount > 70% of total, zeroed out entirely
        if discount > subtotal * Decimal("0.70"):
            discount = Decimal("0")
    else:
        discount = min(Decimal(str(promo.discount_value)), subtotal)

    # Only increment uses if this is a newly applied promo (not the same one already on the order)
    if existing_promo_id != promo.id:
        promo.current_uses += 1

    return discount, promo.id


class _PromoError(Exception):
    def __init__(self, response: JSONResponse):
        self.response = response


async def _reserve_items(
    items_input,
    session: AsyncSession,
) -> tuple[list[OrderItemDB], Decimal]:
    """Validates stock and builds OrderItemDB objects. Returns (items, subtotal)."""
    result   = []
    subtotal = Decimal("0")

    for entry in items_input:
        product = (
            await session.execute(select(ProductDB).where(ProductDB.id == uuid.UUID(str(entry.product_id))))
        ).scalars().first()

        if not product:
            raise _StockError(error_resp(404, "PRODUCT_NOT_FOUND", "Product not found",
                                         {"product_id": str(entry.product_id)}))
        if product.status != ProductStatus.ACTIVE.value:
            raise _StockError(error_resp(409, "PRODUCT_INACTIVE", "Product is not ACTIVE",
                                         {"product_id": str(entry.product_id)}))
        if product.stock < entry.quantity:
            raise _StockError(error_resp(409, "INSUFFICIENT_STOCK", "Not enough stock", {
                "product_id": str(entry.product_id),
                "requested":  entry.quantity,
                "available":  product.stock,
            }))

        price = Decimal(str(product.price))
        product.stock -= entry.quantity
        subtotal += price * entry.quantity
        result.append(OrderItemDB(
            product_id=product.id,
            quantity=entry.quantity,
            price_at_order=price,
        ))

    return result, subtotal


class _StockError(Exception):
    def __init__(self, response: JSONResponse):
        self.response = response


async def _restore_stock(order_id: uuid.UUID, session: AsyncSession):
    items = (await session.execute(select(OrderItemDB).where(OrderItemDB.order_id == order_id))).scalars().all()
    for item in items:
        product = (await session.execute(select(ProductDB).where(ProductDB.id == item.product_id))).scalars().first()
        if product:
            product.stock += item.quantity
    return items


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    body:    OrderCreate,
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_users),
):
    if (err := _auth_guard(user)):
        return err

    user_id = uuid.UUID(user["sub"])
    now     = datetime.now(timezone.utc)

    # 1. Rate limit: check most recent CREATE_ORDER op
    if await _check_rate_limit(user_id, "CREATE_ORDER", session):
        return error_resp(429, "ORDER_LIMIT_EXCEEDED", f"Please wait {RATE_LIMIT_MINUTES} minute(s) between orders")

    # 2. No concurrent active orders
    active = (
        await session.execute(
            select(OrderDB).where(and_(OrderDB.user_id == user_id, OrderDB.status.in_(_OPEN_STATUSES)))
        )
    ).scalars().first()
    if active:
        return error_resp(409, "ORDER_HAS_ACTIVE", "You already have an active order")

    # 3–5. Validate and reserve stock
    try:
        line_items, subtotal = await _reserve_items(body.items, session)
    except _StockError as e:
        return e.response

    # 6–7. Promo code + totals
    discount      = Decimal("0")
    promo_code_id = None

    if body.promo_code:
        try:
            discount, promo_code_id = await _resolve_promo(body.promo_code, subtotal, now, session)
        except _PromoError as e:
            return e.response

    order = OrderDB(
        user_id=user_id,
        status=OrderStatus.CREATED.value,
        promo_code_id=promo_code_id,
        total_amount=subtotal - discount,
        discount_amount=discount,
    )
    session.add(order)
    await session.flush()

    for item in line_items:
        item.order_id = order.id
        session.add(item)

    # 8. Record operation
    session.add(UserOperationDB(user_id=user_id, operation_type="CREATE_ORDER"))
    await session.commit()
    await session.refresh(order)

    return await _build_order_response(order, session)


@router.put("/{id}", response_model=OrderResponse)
async def update_order(
    id:      uuid.UUID    = Path(...),
    body:    OrderUpdate  = ...,
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_users),
):
    if (err := _auth_guard(user)):
        return err

    user_id = uuid.UUID(user["sub"])
    order   = (await session.execute(select(OrderDB).where(OrderDB.id == id))).scalars().first()

    if not order:
        return error_resp(404, "ORDER_NOT_FOUND", "Order not found")

    # 1. Ownership
    if order.user_id != user_id:
        return error_resp(403, "ORDER_OWNERSHIP_VIOLATION", "This order belongs to another user")

    # 2. State check
    if order.status != OrderStatus.CREATED.value:
        return error_resp(409, "INVALID_STATE_TRANSITION", f"Only CREATED orders can be updated, current: {order.status}")

    # 3. Rate limit: check most recent UPDATE_ORDER op
    if await _check_rate_limit(user_id, "UPDATE_ORDER", session):
        return error_resp(429, "ORDER_LIMIT_EXCEEDED", f"Please wait {RATE_LIMIT_MINUTES} minute(s) between updates")

    now = datetime.now(timezone.utc)

    # 4. Restore old stock and remove old items
    old_items = (await session.execute(select(OrderItemDB).where(OrderItemDB.order_id == id))).scalars().all()
    for old in old_items:
        product = (await session.execute(select(ProductDB).where(ProductDB.id == old.product_id))).scalars().first()
        if product:
            product.stock += old.quantity
        await session.delete(old)
    await session.flush()

    # 5. Reserve new items
    try:
        new_items, new_subtotal = await _reserve_items(body.items, session)
    except _StockError as e:
        return e.response

    # 6. Re-validate promo if one was applied
    new_discount      = Decimal("0")
    new_promo_code_id = None

    if order.promo_code_id:
        promo = (await session.execute(select(PromoCodeDB).where(PromoCodeDB.id == order.promo_code_id))).scalars().first()
        if promo:
            try:
                new_discount, new_promo_code_id = await _resolve_promo(
                    promo.code, new_subtotal, now, session, existing_promo_id=order.promo_code_id
                )
            except _PromoError:
                # Promo no longer valid — remove it and decrement its uses
                promo.current_uses -= 1
                new_discount      = Decimal("0")
                new_promo_code_id = None

    order.promo_code_id   = new_promo_code_id
    order.discount_amount = new_discount
    order.total_amount    = new_subtotal - new_discount

    for item in new_items:
        item.order_id = order.id
        session.add(item)

    # 7. Record operation
    session.add(UserOperationDB(user_id=user_id, operation_type="UPDATE_ORDER"))
    await session.commit()
    await session.refresh(order)

    return await _build_order_response(order, session)


@router.get("/{id}", response_model=OrderResponse)
async def get_order(
    id:      uuid.UUID    = Path(...),
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_users_admins),
):
    if (err := _auth_guard(user)):
        return err

    user_id = uuid.UUID(user["sub"])
    order   = (await session.execute(select(OrderDB).where(OrderDB.id == id))).scalars().first()

    if not order:
        return error_resp(404, "ORDER_NOT_FOUND", "Order not found")
    if user["role"] != "ADMIN" and order.user_id != user_id:
        return error_resp(403, "ORDER_OWNERSHIP_VIOLATION", "This order belongs to another user")

    return await _build_order_response(order, session)


@router.post("/{id}/cancel", response_model=OrderResponse, status_code=status.HTTP_200_OK)
async def cancel_order(
    id:      uuid.UUID    = Path(...),
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_users_admins),
):
    if (err := _auth_guard(user)):
        return err

    user_id = uuid.UUID(user["sub"])
    order   = (await session.execute(select(OrderDB).where(OrderDB.id == id))).scalars().first()

    if not order:
        return error_resp(404, "ORDER_NOT_FOUND", "Order not found")

    # 1. Ownership
    if user["role"] != "ADMIN" and order.user_id != user_id:
        return error_resp(403, "ORDER_OWNERSHIP_VIOLATION", "This order belongs to another user")

    # 2. State check
    if order.status not in _CANCELLABLE:
        return error_resp(409, "INVALID_STATE_TRANSITION", f"Cannot cancel an order with status {order.status}")

    # 3. Restore stock
    await _restore_stock(id, session)

    # 4. Decrement promo usage
    if order.promo_code_id:
        promo = (await session.execute(select(PromoCodeDB).where(PromoCodeDB.id == order.promo_code_id))).scalars().first()
        if promo:
            promo.current_uses -= 1

    # 5. Set status
    order.status = OrderStatus.CANCELED.value
    await session.commit()
    await session.refresh(order)

    return await _build_order_response(order, session)
