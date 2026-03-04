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
from src.routers.auth import RoleChecker

router = APIRouter()

RATE_LIMIT_WINDOW_MINUTES = 1
RATE_LIMIT_MAX_OPS        = 3

allow_users          = RoleChecker(["USER"])
allow_users_admins   = RoleChecker(["USER", "ADMIN"])

_OPEN_STATUSES      = [OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING]
_CANCELLABLE        = [OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING]


def error_resp(code: int, err_code: str, msg: str, details: dict = None):
    return JSONResponse(
        status_code=code,
        content={"error_code": err_code, "message": msg, "details": details},
    )


def _auth_guard(user):
    if user is None:
        return JSONResponse(status_code=401, content={"error_code": "UNAUTHORIZED", "message": "Authentication required"})
    if user is False:
        return JSONResponse(status_code=403, content={"error_code": "FORBIDDEN", "message": "Access denied"})
    return None


async def _build_order_response(order: OrderDB, session: AsyncSession) -> dict:
    items = (
        await session.execute(select(OrderItemDB).where(OrderItemDB.order_id == order.id))
    ).scalars().all()

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

    # Rate limit: max N CREATE_ORDER ops per window
    window_start = now - timedelta(minutes=RATE_LIMIT_WINDOW_MINUTES)
    recent_count = (
        await session.execute(
            select(UserOperationDB).where(
                and_(
                    UserOperationDB.user_id        == user_id,
                    UserOperationDB.operation_type == "CREATE_ORDER",
                    UserOperationDB.created_at     >= window_start,
                )
            )
        )
    ).scalars().all()

    if len(recent_count) >= RATE_LIMIT_MAX_OPS:
        return error_resp(429, "RATE_LIMIT_EXCEEDED", f"Max {RATE_LIMIT_MAX_OPS} orders per {RATE_LIMIT_WINDOW_MINUTES} min")

    # One active order at a time
    active = (
        await session.execute(
            select(OrderDB).where(
                and_(OrderDB.user_id == user_id, OrderDB.status.in_(_OPEN_STATUSES))
            )
        )
    ).scalars().first()

    if active:
        return error_resp(409, "ACTIVE_ORDER_EXISTS", "You already have an active order")

    # Reserve stock and build line items
    subtotal     = Decimal("0")
    items_to_add = []

    for entry in body.items:
        product = (
            await session.execute(select(ProductDB).where(ProductDB.id == uuid.UUID(str(entry.product_id))))
        ).scalars().first()

        if not product:
            return error_resp(404, "PRODUCT_NOT_FOUND", "Product not found", {"product_id": str(entry.product_id)})
        if product.status != ProductStatus.ACTIVE.value:
            return error_resp(409, "PRODUCT_UNAVAILABLE", "Product is not ACTIVE", {"product_id": str(entry.product_id)})
        if product.stock < entry.quantity:
            return error_resp(409, "INSUFFICIENT_STOCK", "Not enough stock", {"product_id": str(entry.product_id), "available": product.stock})

        price = Decimal(str(product.price))
        product.stock -= entry.quantity
        subtotal += price * entry.quantity
        items_to_add.append(OrderItemDB(product_id=product.id, quantity=entry.quantity, price_at_order=price))

    # Promo code
    discount       = Decimal("0")
    promo_code_id  = None

    if body.promo_code:
        promo = (
            await session.execute(select(PromoCodeDB).where(PromoCodeDB.code == body.promo_code))
        ).scalars().first()

        if not promo or not promo.active or promo.current_uses >= promo.max_uses:
            return error_resp(422, "PROMO_INVALID", "Promo code is invalid or exhausted")
        if not (promo.valid_from <= now <= promo.valid_until):
            return error_resp(422, "PROMO_EXPIRED", "Promo code is outside its validity window")
        if subtotal < Decimal(str(promo.min_order_amount)):
            return error_resp(422, "PROMO_MIN_AMOUNT", f"Order total must reach {promo.min_order_amount}")

        if promo.discount_type == "PERCENTAGE":
            discount = (subtotal * Decimal(str(promo.discount_value)) / 100).quantize(Decimal("0.01"))
        else:
            discount = min(Decimal(str(promo.discount_value)), subtotal)

        promo.current_uses += 1
        promo_code_id = promo.id

    order = OrderDB(
        user_id=user_id,
        status=OrderStatus.CREATED.value,
        promo_code_id=promo_code_id,
        total_amount=subtotal - discount,
        discount_amount=discount,
    )
    session.add(order)
    await session.flush()

    for item in items_to_add:
        item.order_id = order.id
        session.add(item)

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
    if order.user_id != user_id:
        return error_resp(403, "FORBIDDEN", "Not your order")
    if order.status != OrderStatus.CREATED.value:
        return error_resp(409, "ORDER_NOT_EDITABLE", "Only CREATED orders may be updated")

    # Restore stock for existing items then delete them
    old_items = (await session.execute(select(OrderItemDB).where(OrderItemDB.order_id == id))).scalars().all()
    for old in old_items:
        product = (await session.execute(select(ProductDB).where(ProductDB.id == old.product_id))).scalars().first()
        if product:
            product.stock += old.quantity
        await session.delete(old)
    await session.flush()

    # Re-reserve with new items
    new_subtotal  = Decimal("0")
    new_items     = []

    for entry in body.items:
        product = (
            await session.execute(select(ProductDB).where(ProductDB.id == uuid.UUID(str(entry.product_id))))
        ).scalars().first()

        if not product:
            return error_resp(404, "PRODUCT_NOT_FOUND", "Product not found", {"product_id": str(entry.product_id)})
        if product.status != ProductStatus.ACTIVE.value:
            return error_resp(409, "PRODUCT_UNAVAILABLE", "Product is not ACTIVE")
        if product.stock < entry.quantity:
            return error_resp(409, "INSUFFICIENT_STOCK", "Not enough stock", {"available": product.stock})

        price = Decimal(str(product.price))
        product.stock -= entry.quantity
        new_subtotal += price * entry.quantity
        new_items.append(OrderItemDB(order_id=order.id, product_id=product.id, quantity=entry.quantity, price_at_order=price))

    order.total_amount = new_subtotal - Decimal(str(order.discount_amount))
    for item in new_items:
        session.add(item)

    session.add(UserOperationDB(user_id=user_id, operation_type="UPDATE_ORDER"))
    await session.commit()
    await session.refresh(order)

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
    if user["role"] != "ADMIN" and order.user_id != user_id:
        return error_resp(403, "FORBIDDEN", "Not your order")
    if order.status not in [s.value for s in _CANCELLABLE]:
        return error_resp(409, "INVALID_STATE_TRANSITION", f"Cannot cancel an order in status {order.status}")

    # Restore stock
    items = (await session.execute(select(OrderItemDB).where(OrderItemDB.order_id == order.id))).scalars().all()
    for item in items:
        product = (await session.execute(select(ProductDB).where(ProductDB.id == item.product_id))).scalars().first()
        if product:
            product.stock += item.quantity

    # Roll back promo usage
    if order.promo_code_id:
        promo = (await session.execute(select(PromoCodeDB).where(PromoCodeDB.id == order.promo_code_id))).scalars().first()
        if promo:
            promo.current_uses -= 1

    order.status = OrderStatus.CANCELED.value
    await session.commit()
    await session.refresh(order)

    return await _build_order_response(order, session)
